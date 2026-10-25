"""Что consumer делает с сообщением: ack, повтор или DLQ. Без брокера и БД."""

import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from payment_service.consumer.delivery import DeliveryHandler
from payment_service.consumer.webhooks import WebhookRejectedError, WebhookUnavailableError
from payment_service.messaging.retry import RetryPolicy


class FakeMessage:
    def __init__(self, body: bytes, headers: dict[str, Any] | None = None) -> None:
        self.body = body
        self.headers = headers or {}
        self.message_id = "msg-1"
        self.outcome: str | None = None

    async def ack(self) -> None:
        self.outcome = "ack"

    async def reject(self, requeue: bool = False) -> None:
        self.outcome = "requeue" if requeue else "dead-letter"

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.outcome = "requeue" if requeue else "dead-letter"


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[dict[str, Any]] = []
        self.fail = fail

    async def publish(self, body: bytes, **kwargs: Any) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.published.append({"body": body, **kwargs})


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def process(self, payment_id: UUID) -> None:
        if self.error:
            raise self.error


def event_body(payment_id: UUID | None = None) -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid4()),
            "payment_id": str(payment_id or uuid4()),
            "occurred_at": "2026-10-09T12:00:00Z",
        }
    ).encode()


def make_handler(
    processor: FakeProcessor, broker: FakeBroker | None = None, base_delay: float = 2.0
) -> tuple[DeliveryHandler, FakeBroker]:
    broker = broker or FakeBroker()
    handler = DeliveryHandler(
        processor,  # type: ignore[arg-type]
        RetryPolicy(max_attempts=3, base_delay=base_delay),
        broker,  # type: ignore[arg-type]
    )
    return handler, broker


def assert_acked(message: FakeMessage, broker: FakeBroker) -> None:
    """Сообщение подтверждено, на повтор ничего не отправлено."""
    __tracebackhide__ = True
    assert message.outcome == "ack"
    assert broker.published == [], "при успехе копий на повтор быть не должно"


def assert_dead_lettered(message: FakeMessage, broker: FakeBroker) -> None:
    """Сообщение отвергнуто (RabbitMQ отправит его в DLQ), копий на повтор нет."""
    __tracebackhide__ = True
    assert message.outcome == "dead-letter"
    assert broker.published == [], "в DLQ уходит само сообщение, копию на повтор не публикуем"


def assert_retry_scheduled(
    message: FakeMessage, broker: FakeBroker, *, delay_queue: str, next_attempt: int
) -> dict[str, Any]:
    """Копия ушла в нужную очередь задержки, оригинал подтверждён. Возвращает копию."""
    __tracebackhide__ = True
    (retry,) = broker.published
    assert retry["exchange"].name == "payments.retry"
    assert retry["routing_key"] == f"payments.new.retry.{delay_queue}"
    assert retry["headers"]["x-attempt"] == next_attempt
    assert retry["body"] == message.body
    assert message.outcome == "ack", "оригинал подтверждаем только после публикации копии"
    return retry


async def test_success_acks() -> None:
    """Успешная обработка: сообщение подтверждается, на повтор ничего не публикуется.

    Arrange: обработчик, у которого обработка проходит без ошибок; корректное сообщение.
    Act: обрабатываем сообщение.
    Assert: сообщение подтверждено (ack), копий на повтор нет.
    """
    # Arrange
    handler, broker = make_handler(FakeProcessor())
    message = FakeMessage(event_body())

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    assert_acked(message, broker)


@pytest.mark.parametrize(("attempt", "delay_queue"), [(1, "2000ms"), (2, "4000ms")])
async def test_transient_failure_is_retried_with_backoff(attempt: int, delay_queue: str) -> None:
    """Временная ошибка: копия уходит в очередь задержки (2 с, потом 4 с), оригинал — ack.

    Arrange: обработка падает с 503; сообщение на 1-й или 2-й попытке.
    Act: обрабатываем сообщение.
    Assert: одна копия ушла в payments.retry с ключом 2000ms или 4000ms, x-attempt увеличен, ошибка
        в x-last-error, тело то же; оригинал подтверждён.
    """
    # Arrange
    handler, broker = make_handler(FakeProcessor(WebhookUnavailableError("HTTP 503")))
    headers = {} if attempt == 1 else {"x-attempt": attempt}
    message = FakeMessage(event_body(), headers)

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    retry = assert_retry_scheduled(
        message, broker, delay_queue=delay_queue, next_attempt=attempt + 1
    )
    assert "HTTP 503" in retry["headers"]["x-last-error"]


async def test_exhausted_retries_go_to_dlq() -> None:
    """Третья неудачная попытка — последняя: сообщение отвергается и уходит в DLQ.

    Arrange: обработка падает с 503; сообщение на 3-й, последней попытке.
    Act: обрабатываем сообщение.
    Assert: сообщение отвергнуто (уходит в DLQ), копий на повтор нет.
    """
    # Arrange
    processor = FakeProcessor(WebhookUnavailableError("HTTP 503"))
    handler, broker = make_handler(processor)
    message = FakeMessage(event_body(uuid4()), {"x-attempt": 3})

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    assert_dead_lettered(message, broker)


async def test_non_retryable_error_goes_to_dlq_immediately() -> None:
    """Окончательная ошибка (получатель ответил 400) — сразу DLQ, без повторов.

    Arrange: обработка падает с 400 от получателя; сообщение на 1-й попытке.
    Act: обрабатываем сообщение.
    Assert: сообщение отвергнуто (уходит в DLQ), копий на повтор нет.
    """
    # Arrange
    processor = FakeProcessor(WebhookRejectedError("HTTP 400"))
    handler, broker = make_handler(processor)
    message = FakeMessage(event_body())

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    assert_dead_lettered(message, broker)


@pytest.mark.parametrize("body", [b"not json", b'{"payment_id": "nope"}', b"{}"])
async def test_malformed_message_goes_to_dlq(body: bytes) -> None:
    """Битое сообщение не повторяется: повтор его не починит, оно сразу уходит в DLQ.

    Arrange: сообщение с битым телом (тело — из параметров).
    Act: обрабатываем сообщение.
    Assert: сообщение отвергнуто (уходит в DLQ), копий на повтор нет.
    """
    # Arrange
    handler, broker = make_handler(FakeProcessor())
    message = FakeMessage(body)

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    assert_dead_lettered(message, broker)


async def test_retry_copy_does_not_inherit_broker_headers() -> None:
    """В копию на повтор переносятся наши заголовки, но не служебные заголовки брокера.

    Иначе у копии сразу был бы чужой счётчик доставок, и попытки считались бы неверно.

    Arrange: обработка падает с 503; в сообщении наш заголовок trace и служебные x-delivery-count и
        x-death.
    Act: обрабатываем сообщение.
    Assert: в копии нет служебных заголовков, trace есть, x-attempt = 3, задан таймаут публикации.
    """
    # Arrange
    handler, broker = make_handler(FakeProcessor(WebhookUnavailableError("HTTP 503")))
    message = FakeMessage(
        event_body(),
        {"x-attempt": 1, "x-delivery-count": 1, "x-death": [{"queue": "q"}], "trace": "t-1"},
    )

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert: попытка 2 (x-attempt 1 + одна повторная доставка) → копия на 3-ю через 4 с
    retry = assert_retry_scheduled(message, broker, delay_queue="4000ms", next_attempt=3)
    assert "x-delivery-count" not in retry["headers"]
    assert "x-death" not in retry["headers"]
    assert retry["headers"]["trace"] == "t-1"  # свои заголовки переносятся
    assert retry["timeout"] is not None


async def test_original_is_kept_when_retry_cannot_be_scheduled() -> None:
    """Если копию на повтор положить не удалось, оригинал не теряется, а возвращается в очередь.

    Arrange: обработка падает, а брокер не принимает копию на повтор.
    Act: обрабатываем сообщение.
    Assert: оригинал не подтверждён, а возвращён в очередь (nack).
    """
    # Arrange
    handler, _ = make_handler(
        FakeProcessor(ConnectionError("db down")), FakeBroker(fail=True), base_delay=0.01
    )
    message = FakeMessage(event_body())

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert: оригинал не подтверждён, а возвращён в очередь — он остаётся единственной копией
    assert message.outcome == "requeue"


async def test_redeliveries_count_as_attempts() -> None:
    """Сообщение, возвращённое через nack, не крутится бесконечно.

    Повторные доставки засчитываются, и после последней попытки сообщение уходит в DLQ
    через наш код.

    Arrange: обработка падает с 503, брокер не принимает копии; сообщение доставлено в 3-й раз
        (x-delivery-count = 2).
    Act: обрабатываем сообщение.
    Assert: сообщение отвергнуто (уходит в DLQ), копий на повтор нет.
    """
    # Arrange
    processor = FakeProcessor(WebhookUnavailableError("HTTP 503"))
    handler, broker = make_handler(processor, FakeBroker(fail=True), base_delay=0.01)
    message = FakeMessage(event_body(uuid4()), {"x-delivery-count": 2})  # 3-я доставка

    # Act
    await handler.handle(message)  # type: ignore[arg-type]

    # Assert
    assert_dead_lettered(message, broker)
