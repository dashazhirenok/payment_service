import logging

from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage

from payment_service.config import Settings
from payment_service.consumer.delivery import DeliveryHandler
from payment_service.consumer.gateway import EmulatedGateway
from payment_service.consumer.processor import PaymentProcessor
from payment_service.consumer.webhooks import WebhookSender
from payment_service.db.engine import create_engine
from payment_service.messaging.publisher import create_broker
from payment_service.messaging.retry import RetryPolicy
from payment_service.messaging.topology import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_topology,
)

# Сколько сообщений процесс обрабатывает одновременно: каждое неподтверждённое сообщение
# обрабатывается в своей задаче. Пул соединений с БД (db/engine.py) не меньше этого числа.
PREFETCH_COUNT = 10
WEBHOOK_TIMEOUT = 10.0  # с


async def _keep_raw_body(message: RabbitMessage) -> bytes:
    """Не даём FastStream разбирать JSON до нашего обработчика.

    Иначе битое сообщение падает внутри FastStream, до нашего кода не доходит и остаётся
    неподтверждённым. Разбор и отправку битых сообщений в DLQ делает DeliveryHandler.
    """
    body: bytes = message.body
    return body


def create_app(settings: Settings) -> FastStream:
    broker = create_broker(settings.rabbitmq_url, prefetch_count=PREFETCH_COUNT)
    retry_policy = RetryPolicy(settings.max_attempts, settings.retry_base_delay)
    engine = create_engine(settings)
    webhooks = WebhookSender(settings.webhook_secret.get_secret_value(), timeout=WEBHOOK_TIMEOUT)
    processor = PaymentProcessor(engine, EmulatedGateway(), webhooks)
    handler = DeliveryHandler(processor, retry_policy, broker)

    @broker.subscriber(
        queue=PAYMENTS_NEW_QUEUE,
        exchange=PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=_keep_raw_body,
    )
    async def on_payment_created(message: RabbitMessage) -> None:
        await handler.handle(message)

    async def start_resources() -> None:
        # Создаём очереди повторов и DLQ до начала чтения,
        # чтобы первой же ошибке было куда отправить сообщение.
        await broker.connect()
        await declare_topology(broker, retry_policy)

    async def close_resources() -> None:
        await webhooks.aclose()
        await engine.dispose()

    return FastStream(
        broker,
        logger=logging.getLogger("payment_service.broker"),
        on_startup=[start_resources],
        after_shutdown=[close_resources],
    )
