"""Отправка webhook-а: тело, заголовки, подпись и какие ответы получателя повторять.

Получатель — настоящий маленький HTTP-сервер из `aiohttp.test_utils`, поднятый на случайном
порту. Он отвечает заданным кодом и запоминает запросы, так что проверяется настоящий HTTP.
"""

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from payment_service.consumer.webhooks import (
    WebhookRejectedError,
    WebhookSender,
    WebhookUnavailableError,
    sign,
)
from payment_service.domain.models import Payment
from tests.factories import make_payment

SECRET = "s3cret"


@dataclass
class ReceivedRequest:
    headers: dict[str, str]
    body: bytes


@dataclass
class Receiver:
    """Тестовый получатель webhook-ов: отвечает кодом `status` и запоминает запросы."""

    url: str = ""
    status: int = 204
    requests: list[ReceivedRequest] = field(default_factory=list)

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append(ReceivedRequest(dict(request.headers), await request.read()))
        headers = {"Location": "http://elsewhere.example/"} if 300 <= self.status < 400 else None
        return web.Response(status=self.status, headers=headers)

    def payment(self) -> Payment:
        """Платёж, у которого webhook_url указывает на этот получатель."""
        return replace(make_payment(), webhook_url=self.url)


@pytest.fixture
async def receiver() -> AsyncIterator[Receiver]:
    receiver = Receiver()
    app = web.Application()
    app.router.add_post("/hook", receiver.handle)
    async with TestServer(app) as server:
        receiver.url = str(server.make_url("/hook"))
        yield receiver


@pytest.fixture
async def sender() -> AsyncIterator[WebhookSender]:
    async with WebhookSender(SECRET, timeout=2) as sender:
        yield sender


async def test_sends_signed_payload(receiver: Receiver, sender: WebhookSender) -> None:
    """Webhook несёт результат платежа, стабильный `X-Webhook-Id` и верную HMAC-подпись.

    Arrange: тестовый получатель отвечает 204 и запоминает запрос; платёж.
    Act: отправляем webhook.
    Assert: в теле id платежа, событие и сумма строкой; X-Webhook-Id равен id платежа; подпись
        совпадает с пересчитанной.
    """
    # Arrange
    payment = receiver.payment()

    # Act
    await sender.send(payment)

    # Assert
    (request,) = receiver.requests
    body = json.loads(request.body)
    assert body["payment_id"] == str(payment.id)
    assert body["event"] == "payment.succeeded"
    assert body["amount"] == "10.50"
    assert request.headers["X-Webhook-Id"] == str(payment.id)
    timestamp = int(request.headers["X-Webhook-Timestamp"])
    expected_signature = sign(SECRET, timestamp, request.body)
    assert request.headers["X-Webhook-Signature"] == f"sha256={expected_signature}"


@pytest.mark.parametrize("status", [500, 502, 503, 408, 429])
async def test_transient_statuses_are_retryable(
    receiver: Receiver, sender: WebhookSender, status: int
) -> None:
    """5xx, 408 и 429 значат «попробуйте позже» — ошибка повторяемая.

    Arrange: получатель отвечает кодом из параметров (5xx, 408, 429).
    Act: отправляем webhook.
    Assert: WebhookUnavailableError — повторяемая ошибка.
    """
    # Arrange
    receiver.status = status

    # Act / Assert
    with pytest.raises(WebhookUnavailableError):
        await sender.send(receiver.payment())


@pytest.mark.parametrize("status", [400, 401, 404, 410, 302])
async def test_client_errors_and_redirects_are_not_retryable(
    receiver: Receiver, sender: WebhookSender, status: int
) -> None:
    """Прочие 4xx и редиректы не исправятся повтором — ошибка окончательная.

    По редиректу отправщик не идёт: запрос к получателю ровно один.

    Arrange: получатель отвечает 4xx или редиректом (код из параметров).
    Act: отправляем webhook.
    Assert: WebhookRejectedError — окончательная ошибка; получатель получил один запрос.
    """
    # Arrange
    receiver.status = status

    # Act / Assert
    with pytest.raises(WebhookRejectedError):
        await sender.send(receiver.payment())
    assert len(receiver.requests) == 1


async def test_network_errors_are_retryable(sender: WebhookSender) -> None:
    """Получатель недоступен по сети — ошибка повторяемая.

    Arrange: поднимаем получатель и сразу останавливаем: по его адресу никто не слушает.
    Act: отправляем webhook.
    Assert: WebhookUnavailableError — повторяемая ошибка.
    """
    # Arrange
    async with TestServer(web.Application()) as server:
        stopped_url = str(server.make_url("/hook"))
    payment = replace(make_payment(), webhook_url=stopped_url)

    # Act / Assert
    with pytest.raises(WebhookUnavailableError):
        await sender.send(payment)
