import logging

from faststream.rabbit import Channel, RabbitBroker

from payment_service.domain.models import OutboxMessage
from payment_service.messaging.topology import PAYMENTS_EXCHANGE


def create_broker(url: str, *, prefetch_count: int | None = None) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(
            prefetch_count=prefetch_count,
            # Брокер подтверждает публикацию, только когда сообщение попало в очередь.
            publisher_confirms=True,
            # Если сообщению с `mandatory` не нашлось очереди, брокер вернёт его.
            # Превращаем это в ошибку, чтобы сообщение не пропало молча.
            on_return_raises=True,
        ),
        graceful_timeout=30.0,
        logger=logging.getLogger("payment_service.broker"),
    )


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, timeout: float = 5.0) -> None:
        self._broker = broker
        self._timeout = timeout

    async def publish(self, message: OutboxMessage) -> None:
        """Вернуться, только когда брокер надёжно принял сообщение. Иначе — исключение."""
        await self._broker.publish(
            message.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=message.routing_key,
            # id из outbox становится id сообщения: по нему видно дубли.
            message_id=str(message.id),
            message_type=message.event_type,
            persist=True,
            mandatory=True,
            timeout=self._timeout,
        )
