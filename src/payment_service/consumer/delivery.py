"""Решает, что сделать с сообщением после обработки: подтвердить, повторить позже или в DLQ.

Сообщение подтверждается вручную и только когда результат уже сохранён:
- успех — ack;
- временная ошибка — кладём копию в очередь с задержкой, потом подтверждаем оригинал;
- копию положить не удалось — ждём задержку и возвращаем оригинал в очередь (nack);
- окончательная ошибка или попытки кончились — reject без возврата в очередь,
  RabbitMQ отправит сообщение в DLQ.

Попытки считаются по `x-attempt` и по счётчику повторных доставок брокера, поэтому лимит
работает при любом пути повтора.

Если процесс упадёт посередине, неподтверждённое сообщение придёт снова. Обработка
повторяема, поэтому в худшем случае будет лишняя попытка, но сообщение не потеряется.
"""

import asyncio
import logging

from faststream.rabbit import RabbitBroker
from faststream.rabbit.message import RabbitMessage
from pydantic import ValidationError

from payment_service.consumer.processor import PaymentProcessor
from payment_service.messaging.contracts import PaymentCreatedEvent
from payment_service.messaging.retry import (
    ATTEMPT_HEADER,
    BROKER_HEADERS,
    RetryPolicy,
    attempt_from_headers,
)
from payment_service.messaging.topology import RETRY_EXCHANGE, retry_routing_key

logger = logging.getLogger(__name__)

LAST_ERROR_HEADER = "x-last-error"
# Сколько ждать подтверждения брокера при отправке копии на повтор.
RETRY_PUBLISH_TIMEOUT = 5.0


class DeliveryHandler:
    def __init__(
        self, processor: PaymentProcessor, retry_policy: RetryPolicy, broker: RabbitBroker
    ) -> None:
        self._processor = processor
        self._retry_policy = retry_policy
        self._broker = broker

    async def handle(self, message: RabbitMessage) -> None:
        attempt = attempt_from_headers(message.headers)
        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError as exc:
            logger.error(
                "malformed message, dead-lettering",
                extra={"message_id": message.message_id, "error": str(exc)},
            )
            await message.reject(requeue=False)
            return

        log_ctx = {
            "payment_id": str(event.payment_id),
            "message_id": message.message_id,
            "attempt": attempt,
        }
        logger.info("message received", extra=log_ctx)
        try:
            await self._processor.process(event.payment_id)
        except Exception as exc:
            await self._on_failure(message, attempt, exc, log_ctx)
            return
        await message.ack()
        logger.info("message acked", extra=log_ctx)

    async def _on_failure(
        self,
        message: RabbitMessage,
        attempt: int,
        error: Exception,
        log_ctx: dict[str, object],
    ) -> None:
        error_text = f"{type(error).__name__}: {error}"[:500]

        if self._retry_policy.should_retry(error, attempt):
            delay_ms = self._retry_policy.delay_ms(attempt)
            logger.warning(
                "processing failed, scheduling retry",
                extra={**log_ctx, "error": error_text, "retry_in_ms": delay_ms},
            )
            own_headers = {k: v for k, v in message.headers.items() if k not in BROKER_HEADERS}
            try:
                # Ждём подтверждения брокера; mandatory — если копии некуда попасть, это ошибка.
                await self._broker.publish(
                    message.body,
                    exchange=RETRY_EXCHANGE,
                    routing_key=retry_routing_key(delay_ms),
                    headers={
                        **own_headers,
                        ATTEMPT_HEADER: attempt + 1,
                        LAST_ERROR_HEADER: error_text,
                    },
                    message_id=message.message_id,
                    content_type="application/json",
                    persist=True,
                    mandatory=True,
                    timeout=RETRY_PUBLISH_TIMEOUT,
                )
            except Exception:
                # Копию отложить не удалось: нет маршрута, брокер не подтвердил или таймаут.
                # Исходное сообщение не подтверждаем — оно остаётся единственной копией.
                # Ждём ту же задержку здесь и возвращаем его в очередь. RabbitMQ увеличит
                # x-delivery-count, это засчитается как попытка, и после последней сообщение
                # уйдёт в DLQ обычным путём.
                # Если копия всё же дошла, а подтверждение потерялось, будет дубль — обработка
                # к нему готова.
                logger.exception("failed to schedule retry, requeueing", extra=log_ctx)
                await asyncio.sleep(delay_ms / 1000)
                await message.nack(requeue=True)
                return
            # Исходное сообщение подтверждаем только после того, как копия надёжно в очереди.
            await message.ack()
            return

        logger.error(
            "processing failed permanently, dead-lettering",
            extra={**log_ctx, "error": error_text},
        )
        await message.reject(requeue=False)
