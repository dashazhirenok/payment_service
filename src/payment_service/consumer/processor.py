"""Обработка платежа. Ничего не знает о RabbitMQ, её можно безопасно запускать повторно.

Обработка идёт в два шага:

1. Провести платёж: вызвать шлюз и поменять статус с `pending` на итоговый. Строка платежа
   при этом заблокирована, а статус меняется только из `pending`. Поэтому платёж проводится
   ровно один раз, даже если сообщение пришло дважды или в две реплики сразу.
2. Уведомить клиента: отправить webhook. Это делается вне транзакции.

При повторной доставке шаг 1 пропускается, если платёж уже проведён, а webhook отправляется
снова. Доставка webhook-ов — «хотя бы один раз», дубли отсеивает получатель по X-Webhook-Id.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.consumer.gateway import GatewayUnavailableError, PaymentGateway
from payment_service.db.repositories import PaymentRepository
from payment_service.domain.models import Payment, PaymentStatus
from payment_service.messaging.retry import NonRetryableError

logger = logging.getLogger(__name__)

# Сколько ждать ответа шлюза, с. Дольше — считаем шлюз недоступным и повторяем позже.
GATEWAY_TIMEOUT = 10.0


class PaymentMissingError(NonRetryableError):
    pass


def _elapsed_ms(started: float) -> int:
    return round((time.monotonic() - started) * 1000)


class WebhookNotifier(Protocol):
    async def send(self, payment: Payment) -> None: ...


class PaymentProcessor:
    def __init__(
        self,
        engine: AsyncEngine,
        gateway: PaymentGateway,
        webhooks: WebhookNotifier,
        *,
        gateway_timeout: float = GATEWAY_TIMEOUT,
    ) -> None:
        self._engine = engine
        self._gateway = gateway
        self._webhooks = webhooks
        self._gateway_timeout = gateway_timeout

    async def process(self, payment_id: UUID) -> None:
        payment = await self._settle(payment_id)
        if payment is None:
            # Платёж сейчас обрабатывает другой consumer. Его сообщение не подтверждено,
            # пока он не закончит, и вернётся, если он упадёт. Этот дубль можно пропустить.
            logger.info(
                "payment is being processed elsewhere", extra={"payment_id": str(payment_id)}
            )
            return
        started = time.monotonic()
        await self._webhooks.send(payment)
        logger.info(
            "webhook delivered",
            extra={"payment_id": str(payment.id), "webhook_ms": _elapsed_ms(started)},
        )

    async def _settle(self, payment_id: UUID) -> Payment | None:
        async with self._engine.begin() as conn:
            payments = PaymentRepository(conn)
            payment = await payments.lock_for_processing(payment_id)
            if payment is None:
                if await payments.exists(payment_id):
                    return None
                raise PaymentMissingError(f"payment {payment_id} does not exist")

            if payment.status is not PaymentStatus.PENDING:
                logger.info(
                    "payment already processed, gateway skipped",
                    extra={"payment_id": str(payment_id), "status": payment.status.value},
                )
                return payment

            # Блокировка держится, пока ждём шлюз. Это занимает соединение с БД на пару
            # секунд, зато два consumer-а не смогут провести один платёж одновременно.
            # Таймаут не даёт зависшему шлюзу держать блокировку бесконечно: по нему
            # транзакция откатывается, платёж остаётся `pending`, сообщение уходит на повтор.
            started = time.monotonic()
            try:
                async with asyncio.timeout(self._gateway_timeout):
                    result = await self._gateway.charge(payment)
            except TimeoutError as exc:
                raise GatewayUnavailableError(
                    f"gateway did not respond in {self._gateway_timeout}s"
                ) from exc
            settled = await payments.mark_processed(
                payment.id, result.status, processed_at=datetime.now(UTC)
            )
            if settled is None:  # pragma: no cover - невозможно, пока строка заблокирована
                raise RuntimeError(f"payment {payment_id} left pending state concurrently")
            logger.info(
                "payment processed",
                extra={
                    "payment_id": str(payment_id),
                    "status": settled.status.value,
                    "reason": result.reason,
                    "gateway_ms": _elapsed_ms(started),
                },
            )
            return settled
