"""Устройство RabbitMQ: exchange-и, очереди и путь сообщения между ними.

[exchange]  (очередь)

    relay ─► [payments] ─► (payments.new) ─► consumer: шлюз → статус в БД → webhook, затем:
      успех       ─► ack
      повторить   ─► [payments.retry] ─► (payments.new.retry.<N>ms) ─TTL─► [payments]
      не повторять ─► reject ─► [payments.dlx] ─► (payments.new.dlq)

Чтобы повторить позже, сообщение кладётся в очередь задержки. Когда истекает её TTL,
RabbitMQ сам возвращает сообщение в основную очередь. На каждую задержку своя очередь:
в общей очереди сообщение с задержкой 4 с ждало бы, пока истечёт соседнее на 16 с.

Создавать очереди повторно безопасно, поэтому их создают и relay, и consumer,
и неважно, кто из них стартует первым.
"""

from aio_pika import RobustExchange, RobustQueue
from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.messaging.retry import RetryPolicy

PAYMENTS_NEW = "payments.new"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

# Страховочный лимит доставок одного сообщения. Обычно до него не доходит: после
# `max_attempts` попыток consumer сам отправляет сообщение в DLQ. Лимит нужен, если consumer
# падает раньше, чем успевает что-то решить: тогда RabbitMQ сам отправит сообщение в DLQ.
DELIVERY_LIMIT = 10

# Quorum-очереди хранят копии на нескольких узлах. Рекомендуемый тип для важных данных.
PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW,
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_NEW,
        "x-delivery-limit": DELIVERY_LIMIT,
    },
)

PAYMENTS_NEW_DLQ = RabbitQueue(
    f"{PAYMENTS_NEW}.dlq",
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=PAYMENTS_NEW,
)


def retry_routing_key(delay_ms: int) -> str:
    return f"{PAYMENTS_NEW}.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    # Задержка входит в имя очереди. Если поменять настройку задержки, создадутся новые
    # очереди, а не будет ошибки из-за старой очереди с другими параметрами.
    return RabbitQueue(
        retry_routing_key(delay_ms),
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=retry_routing_key(delay_ms),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


async def _declare_bound(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RobustExchange
) -> RobustQueue:
    declared: RobustQueue = await broker.declare_queue(queue)
    await declared.bind(exchange, routing_key=queue.routing())
    return declared


async def declare_topology(broker: RabbitBroker, retry_policy: RetryPolicy) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry = await broker.declare_exchange(RETRY_EXCHANGE)
    dlx = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    await _declare_bound(broker, PAYMENTS_NEW_QUEUE, payments)
    await _declare_bound(broker, PAYMENTS_NEW_DLQ, dlx)
    for delay_ms in retry_policy.delays_ms():
        await _declare_bound(broker, retry_queue(delay_ms), retry)
