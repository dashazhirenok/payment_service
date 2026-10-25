"""Outbox relay: переносит сохранённые события из таблицы `outbox` в RabbitMQ.

Каждая пачка проходит три шага, и транзакция никогда не остаётся открытой во время
сетевого вызова к брокеру:

1. Короткая транзакция: взять события в аренду (`locked_until`).
2. Без транзакции: опубликовать их и дождаться подтверждения брокера.
3. Короткая транзакция: отметить опубликованные, а неудачным назначить следующую попытку.

Если relay упадёт между шагами 2 и 3, аренда истечёт, и событие уйдёт ещё раз. Поэтому
событие может прийти дважды, но не потеряется, а consumer к дублям готов.
"""

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import Settings
from payment_service.db.repositories import OutboxRepository
from payment_service.messaging.publisher import RabbitEventPublisher, create_broker
from payment_service.messaging.retry import RetryPolicy
from payment_service.messaging.topology import declare_topology

logger = logging.getLogger(__name__)

POLL_INTERVAL = 0.5  # пауза между опросами таблицы, с
BATCH_SIZE = 100
MAX_BACKOFF = 30.0  # самая длинная пауза при недоступном брокере, с
# На сколько relay «арендует» взятые события. Заметно больше таймаута публикации (5 с).
LEASE = timedelta(seconds=30)


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    failed: bool


async def _sleep_until_stopped(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


class OutboxRelay:
    def __init__(self, engine: AsyncEngine, publisher: RabbitEventPublisher) -> None:
        self._engine = engine
        self._publisher = publisher

    @staticmethod
    def _retry_delay(attempts: int) -> timedelta:
        """Пауза перед следующей попыткой события: 1, 2, 4, … секунд, но не больше максимума."""
        return timedelta(seconds=min(2.0**attempts, MAX_BACKOFF))

    async def publish_batch(self) -> BatchResult:
        async with self._engine.begin() as conn:
            messages = await OutboxRepository(conn).claim_batch(BATCH_SIZE, LEASE)
        if not messages:
            return BatchResult(published=0, failed=False)

        # Публикуем параллельно: пачка занимает примерно одно ожидание брокера, а не сумму
        # всех, поэтому аренда (30 с) с запасом перекрывает таймаут публикации (5 с).
        results = await asyncio.gather(
            *(self._publisher.publish(message) for message in messages), return_exceptions=True
        )
        published = [
            m for m, r in zip(messages, results, strict=True) if not isinstance(r, BaseException)
        ]
        failed = [
            (m, r) for m, r in zip(messages, results, strict=True) if isinstance(r, BaseException)
        ]

        async with self._engine.begin() as conn:
            outbox = OutboxRepository(conn)
            await outbox.mark_published([m.id for m in published])
            for message, error in failed:
                await outbox.record_failure(
                    message.id, repr(error), retry_in=self._retry_delay(message.attempts)
                )

        for message in published:
            logger.info(
                "event published",
                extra={
                    "payment_id": str(message.aggregate_id),
                    "outbox_id": str(message.id),
                    "event_type": message.event_type,
                },
            )
        for message, error in failed:
            logger.warning(
                "event publish failed",
                extra={
                    "payment_id": str(message.aggregate_id),
                    "outbox_id": str(message.id),
                    "attempt": message.attempts + 1,
                    "error": repr(error),
                },
            )
        return BatchResult(published=len(published), failed=bool(failed))

    async def run(self, stop: asyncio.Event) -> None:
        backoff = POLL_INTERVAL
        while not stop.is_set():
            try:
                result = await self.publish_batch()
            except Exception:
                logger.exception("outbox batch crashed")
                result = BatchResult(published=0, failed=True)

            if result.failed:
                backoff = min(backoff * 2, MAX_BACKOFF)
                await _sleep_until_stopped(stop, backoff)
                continue
            backoff = POLL_INTERVAL
            # Пачка полная — значит, событий, скорее всего, ещё много. Берём следующую сразу.
            if result.published < BATCH_SIZE:
                await _sleep_until_stopped(stop, POLL_INTERVAL)


async def run_relay(
    settings: Settings,
    engine: AsyncEngine,
    stop: asyncio.Event,
) -> None:
    """Подключиться к RabbitMQ (дождаться его, если нужно) и публиковать до сигнала `stop`.

    Недоступный RabbitMQ не должен ронять API: платежи принимаются и копятся в outbox,
    а публикуются, когда брокер снова доступен.
    """
    broker = create_broker(settings.rabbitmq_url)
    retry_policy = RetryPolicy(settings.max_attempts, settings.retry_base_delay)
    backoff = 1.0
    while not stop.is_set():
        try:
            await broker.connect()
            await declare_topology(broker, retry_policy)
            break
        except Exception as exc:
            logger.warning(
                "RabbitMQ unavailable, outbox relay will retry",
                extra={"error": repr(exc), "retry_in": backoff},
            )
            await _sleep_until_stopped(stop, backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    try:
        if stop.is_set():
            return
        logger.info("outbox relay started")
        relay = OutboxRelay(engine, RabbitEventPublisher(broker))
        await relay.run(stop)
    finally:
        await broker.stop()
        logger.info("outbox relay stopped")
