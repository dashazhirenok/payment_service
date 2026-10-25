"""Обработка платежа на настоящей PostgreSQL: проведение, повторы, дубли, таймаут шлюза."""

import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.consumer.gateway import GatewayResult, GatewayUnavailableError
from payment_service.consumer.processor import PaymentMissingError, PaymentProcessor
from payment_service.consumer.webhooks import WebhookUnavailableError
from payment_service.db.repositories import PaymentRepository
from payment_service.domain.models import (
    CreatePaymentCommand,
    Currency,
    Payment,
    PaymentStatus,
)
from payment_service.services.payments import PaymentService


class FakeGateway:
    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED, delay: float = 0) -> None:
        self.status = status
        self.delay = delay
        self.calls: list[UUID] = []

    async def charge(self, payment: Payment) -> GatewayResult:
        self.calls.append(payment.id)
        await asyncio.sleep(self.delay)
        return GatewayResult(self.status)


class FakeWebhooks:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.sent: list[Payment] = []

    async def send(self, payment: Payment) -> None:
        if self.failures:
            self.failures -= 1
            raise WebhookUnavailableError("HTTP 503 from webhook receiver")
        self.sent.append(payment)


@pytest.fixture
async def payment_id(engine: AsyncEngine) -> UUID:
    result = await PaymentService(engine).create(
        CreatePaymentCommand(
            amount=Decimal("250.00"),
            currency=Currency.EUR,
            description="test",
            webhook_url="https://merchant.example/hook",
            idempotency_key=str(uuid4()),
        )
    )
    return result.payment.id


async def load(engine: AsyncEngine, payment_id: UUID) -> Payment:
    async with engine.connect() as conn:
        payment = await PaymentRepository(conn).get(payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processes_payment_and_notifies(
    engine: AsyncEngine, payment_id: UUID, outcome: PaymentStatus
) -> None:
    """Платёж получает итоговый статус и время обработки, клиенту уходит webhook.

    Отказ шлюза — тоже итоговый статус (`failed`), и о нём тоже отправляется webhook.

    Arrange: шлюз отвечает succeeded или failed (из параметров).
    Act: обрабатываем платёж.
    Assert: у платежа этот статус и время обработки; ушёл один webhook с этим статусом.
    """
    # Arrange
    webhooks = FakeWebhooks()
    processor = PaymentProcessor(engine, FakeGateway(outcome), webhooks)

    # Act
    await processor.process(payment_id)

    # Assert
    payment = await load(engine, payment_id)
    assert payment.status is outcome
    assert payment.processed_at is not None
    assert [p.status for p in webhooks.sent] == [outcome]


async def test_redelivery_after_success_does_not_charge_again(
    engine: AsyncEngine, payment_id: UUID
) -> None:
    """Повторная доставка того же сообщения не вызывает шлюз второй раз.

    Webhook при этом уходит снова: доставка «хотя бы один раз», дубли отсеивает получатель.

    Arrange: платёж уже обработан.
    Act: обрабатываем то же сообщение ещё раз.
    Assert: шлюз вызван один раз, webhook отправлен дважды.
    """
    # Arrange
    gateway, webhooks = FakeGateway(), FakeWebhooks()
    processor = PaymentProcessor(engine, gateway, webhooks)
    await processor.process(payment_id)

    # Act
    await processor.process(payment_id)

    # Assert
    assert len(gateway.calls) == 1
    assert len(webhooks.sent) == 2


async def test_webhook_failure_keeps_payment_result_and_retry_only_resends_webhook(
    engine: AsyncEngine, payment_id: UUID
) -> None:
    """Ошибка webhook-а не откатывает списание, а повтор отправляет только уведомление.

    Arrange: первый webhook падает с 503.
    Act: обрабатываем платёж (ошибка), затем повторяем обработку.
    Assert: после ошибки статус succeeded; шлюз вызван один раз; webhook доставлен один раз.
    """
    # Arrange
    gateway, webhooks = FakeGateway(), FakeWebhooks(failures=1)
    processor = PaymentProcessor(engine, gateway, webhooks)

    # Act
    with pytest.raises(WebhookUnavailableError):
        await processor.process(payment_id)
    status_after_failure = (await load(engine, payment_id)).status
    await processor.process(payment_id)  # повтор

    # Assert
    assert status_after_failure is PaymentStatus.SUCCEEDED  # списание уже случилось
    assert len(gateway.calls) == 1  # повторно не списали
    assert len(webhooks.sent) == 1


async def test_concurrent_duplicate_deliveries_charge_once(
    engine: AsyncEngine, payment_id: UUID
) -> None:
    """Две реплики consumer одновременно получили одно сообщение — шлюз вызван один раз.

    Arrange: шлюз отвечает за 0,3 с; две «реплики» consumer.
    Act: обе одновременно обрабатывают одно и то же сообщение.
    Assert: шлюз вызван один раз, webhook один, статус succeeded.
    """
    # Arrange
    gateway, webhooks = FakeGateway(delay=0.3), FakeWebhooks()
    processors = [PaymentProcessor(engine, gateway, webhooks) for _ in range(2)]

    # Act
    await asyncio.gather(*(p.process(payment_id) for p in processors))

    # Assert
    assert len(gateway.calls) == 1
    assert len(webhooks.sent) == 1
    assert (await load(engine, payment_id)).status is PaymentStatus.SUCCEEDED


async def test_hanging_gateway_times_out_and_leaves_payment_pending(
    engine: AsyncEngine, payment_id: UUID
) -> None:
    """Зависший шлюз обрывается по таймауту, транзакция откатывается, платёж остаётся `pending`.

    Следующая попытка с работающим шлюзом проводит платёж.

    Arrange: шлюз отвечает за 5 с при таймауте 0,1 с.
    Act: обрабатываем платёж (таймаут), затем повторяем с работающим шлюзом.
    Assert: после таймаута статус pending, после повтора — succeeded.
    """
    # Arrange
    hanging = PaymentProcessor(engine, FakeGateway(delay=5), FakeWebhooks(), gateway_timeout=0.1)

    # Act
    with pytest.raises(GatewayUnavailableError):
        await hanging.process(payment_id)
    status_after_timeout = (await load(engine, payment_id)).status
    await PaymentProcessor(engine, FakeGateway(), FakeWebhooks()).process(payment_id)

    # Assert
    assert status_after_timeout is PaymentStatus.PENDING
    assert (await load(engine, payment_id)).status is PaymentStatus.SUCCEEDED


async def test_unknown_payment_is_not_retryable(
    engine: AsyncEngine,
) -> None:
    """Сообщение о несуществующем платеже — окончательная ошибка, повторять бессмысленно.

    Arrange: processor и случайный id платежа, которого нет в базе.
    Act: обрабатываем его.
    Assert: PaymentMissingError — окончательная ошибка.
    """
    # Arrange
    processor = PaymentProcessor(engine, FakeGateway(), FakeWebhooks())

    # Act / Assert
    with pytest.raises(PaymentMissingError):
        await processor.process(uuid4())
