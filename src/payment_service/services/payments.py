import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.db.repositories import OutboxRepository, PaymentRepository
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.domain.models import CreatePaymentCommand, OutboxMessage, Payment
from payment_service.messaging.contracts import PAYMENT_CREATED, PaymentCreatedEvent
from payment_service.messaging.topology import PAYMENTS_NEW

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool  # False, если это повтор запроса с уже использованным ключом


def _payment_created_message(payment: Payment) -> OutboxMessage:
    event = PaymentCreatedEvent(
        event_id=uuid4(), payment_id=payment.id, occurred_at=datetime.now(UTC)
    )
    return OutboxMessage(
        id=event.event_id,
        aggregate_type="payment",
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED,
        routing_key=PAYMENTS_NEW,
        payload=event.model_dump(mode="json"),
    )


class PaymentService:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def create(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        # Одна транзакция: платёж и событие сохраняются вместе или не сохраняются вовсе.
        # В RabbitMQ отсюда ничего не отправляем — это сделает outbox relay после коммита.
        existing: Payment | None = None
        async with self._engine.begin() as conn:
            payments = PaymentRepository(conn)
            payment = await payments.insert_if_absent(uuid4(), command)
            if payment is not None:
                await OutboxRepository(conn).add(_payment_created_message(payment))
            else:
                # Ключ уже занят. Этот SELECT увидит платёж, который сохранил
                # первый из одновременных запросов.
                existing = await payments.get_by_idempotency_key(command.idempotency_key)

        # Логируем после коммита: в логе только то, что действительно сохранено.
        if payment is not None:
            logger.info(
                "payment accepted",
                extra={
                    "payment_id": str(payment.id),
                    "idempotency_key": command.idempotency_key,
                    "amount": str(payment.amount),
                    "currency": payment.currency.value,
                },
            )
            return CreatePaymentResult(payment=payment, created=True)

        if existing is None:  # pragma: no cover - невозможно из-за уникального индекса
            raise RuntimeError("idempotency key conflict without an existing payment")
        log_ctx = {"payment_id": str(existing.id), "idempotency_key": command.idempotency_key}
        if existing.request_fingerprint != command.fingerprint():
            logger.warning("idempotency key reused with a different body", extra=log_ctx)
            raise IdempotencyKeyReusedError(command.idempotency_key)
        logger.info("payment request replayed", extra=log_ctx)
        return CreatePaymentResult(payment=existing, created=False)

    async def get(self, payment_id: UUID) -> Payment:
        async with self._engine.connect() as conn:
            payment = await PaymentRepository(conn).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
