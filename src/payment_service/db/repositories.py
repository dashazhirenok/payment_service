"""Работа с таблицами payments и outbox.

Репозитории получают уже открытое соединение и сами не открывают и не коммитят
транзакции — это делает вызывающий код (`async with engine.begin() as conn:`).
Строки из БД приходят словарями (`RowMapping`) и сразу превращаются в доменные объекты.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import Executable, RowMapping, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from payment_service.db.tables import outbox, payments
from payment_service.domain.models import (
    CreatePaymentCommand,
    Currency,
    OutboxMessage,
    Payment,
    PaymentStatus,
)


def _to_payment(row: RowMapping) -> Payment:
    return Payment(
        id=row["id"],
        amount=row["amount"],
        currency=Currency(row["currency"]),
        description=row["description"],
        metadata=row["metadata"],
        status=PaymentStatus(row["status"]),
        idempotency_key=row["idempotency_key"],
        request_fingerprint=row["request_fingerprint"],
        webhook_url=row["webhook_url"],
        created_at=row["created_at"],
        processed_at=row["processed_at"],
    )


def _to_outbox_message(row: RowMapping) -> OutboxMessage:
    return OutboxMessage(
        id=row["id"],
        aggregate_type=row["aggregate_type"],
        aggregate_id=row["aggregate_id"],
        event_type=row["event_type"],
        routing_key=row["routing_key"],
        payload=row["payload"],
        created_at=row["created_at"],
        attempts=row["attempts"],
    )


class PaymentRepository:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def _one_or_none(self, stmt: Executable) -> Payment | None:
        row = (await self._conn.execute(stmt)).mappings().one_or_none()
        return _to_payment(row) if row is not None else None

    async def insert_if_absent(
        self, payment_id: UUID, command: CreatePaymentCommand
    ) -> Payment | None:
        """Создать платёж, если платежа с таким ключом идемпотентности ещё нет.

        Если есть — возвращает None. Работает через уникальный индекс, поэтому два
        одновременных запроса с одним ключом не создадут два платежа: второй дождётся
        первого и ничего не вставит.
        """
        stmt = (
            pg_insert(payments)
            .values(
                id=payment_id,
                amount=command.amount,
                currency=command.currency.value,
                description=command.description,
                metadata=command.metadata,
                status=PaymentStatus.PENDING.value,
                idempotency_key=command.idempotency_key,
                request_fingerprint=command.fingerprint(),
                webhook_url=command.webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[payments.c.idempotency_key])
            # RETURNING сразу отдаёт вставленную строку вместе с полями, которые
            # заполнила БД (created_at), без второго запроса.
            .returning(payments)
        )
        return await self._one_or_none(stmt)

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._one_or_none(select(payments).where(payments.c.id == payment_id))

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(payments).where(payments.c.idempotency_key == idempotency_key)
        return await self._one_or_none(stmt)

    async def exists(self, payment_id: UUID) -> bool:
        stmt = select(exists().where(payments.c.id == payment_id))
        return bool(await self._conn.scalar(stmt))

    async def lock_for_processing(self, payment_id: UUID) -> Payment | None:
        """Заблокировать строку платежа до конца транзакции.

        Возвращает None, если платежа нет или его уже заблокировал другой consumer.
        Дубль сообщения не ждёт и не мешает тому, кто уже обрабатывает платёж.
        """
        stmt = select(payments).where(payments.c.id == payment_id).with_for_update(skip_locked=True)
        return await self._one_or_none(stmt)

    async def mark_processed(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment | None:
        """Перевести платёж из `pending` в итоговый статус. Итоговый статус не перезаписывается."""
        if not status.is_final:
            raise ValueError(f"{status} is not a final status")
        stmt = (
            update(payments)
            .where(payments.c.id == payment_id, payments.c.status == PaymentStatus.PENDING.value)
            .values(status=status.value, processed_at=processed_at, updated_at=func.now())
            .returning(payments)
        )
        return await self._one_or_none(stmt)


class OutboxRepository:
    def __init__(self, conn: AsyncConnection) -> None:
        self._conn = conn

    async def add(self, message: OutboxMessage) -> None:
        await self._conn.execute(
            outbox.insert().values(
                id=message.id,
                aggregate_type=message.aggregate_type,
                aggregate_id=message.aggregate_id,
                event_type=message.event_type,
                routing_key=message.routing_key,
                payload=message.payload,
            )
        )

    async def claim_batch(self, limit: int, lease: timedelta) -> list[OutboxMessage]:
        """Взять в аренду до `limit` событий, которые пора публиковать, — сначала старые.

        1. Выбрать свободные строки и заблокировать их до конца транзакции.
        2. Пометить их занятыми до `now() + lease`.

        После коммита блокировок в БД уже нет, но аренда не даёт другим relay-ям взять эти
        строки. Если relay упадёт, аренда истечёт, и события подхватит кто-то другой.
        """
        now = func.now()
        free_ids = await self._conn.scalars(
            select(outbox.c.id)
            .where(
                outbox.c.published_at.is_(None),  # ещё не опубликовано
                outbox.c.next_attempt_at <= now,  # пауза после прошлой ошибки прошла
                or_(outbox.c.locked_until.is_(None), outbox.c.locked_until < now),  # не в аренде
            )
            .order_by(outbox.c.created_at)
            .limit(limit)
            # Строки, которые прямо сейчас захватывает другой relay, пропускаем, а не ждём.
            .with_for_update(skip_locked=True)
        )
        ids = free_ids.all()
        if not ids:
            return []

        claimed = await self._conn.execute(
            update(outbox)
            .where(outbox.c.id.in_(ids))
            .values(locked_until=now + lease)
            .returning(outbox)
        )
        return [_to_outbox_message(row) for row in claimed.mappings()]

    async def mark_published(self, message_ids: Sequence[UUID]) -> None:
        if not message_ids:
            return
        await self._conn.execute(
            update(outbox)
            .where(outbox.c.id.in_(message_ids))
            .values(published_at=func.now(), locked_until=None, attempts=outbox.c.attempts + 1)
        )

    async def record_failure(self, message_id: UUID, error: str, retry_in: timedelta) -> None:
        """Записать ошибку, снять аренду и отложить следующую попытку на `retry_in`."""
        await self._conn.execute(
            update(outbox)
            .where(outbox.c.id == message_id)
            .values(
                attempts=outbox.c.attempts + 1,
                last_error=error,
                locked_until=None,
                next_attempt_at=func.now() + retry_in,
            )
        )
