"""Отправка webhook-а клиенту о результате платежа."""

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from types import TracebackType
from typing import Any, Self
from urllib.parse import urlsplit
from uuid import UUID

import aiohttp
from pydantic import BaseModel

from payment_service.domain.models import Currency, Payment, PaymentStatus
from payment_service.messaging.retry import NonRetryableError, RetryableError

logger = logging.getLogger(__name__)

# Ответы 4xx, которые значат «попробуйте позже», а не «получатель отказывается принимать».
_RETRYABLE_CLIENT_ERRORS = frozenset(
    {HTTPStatus.REQUEST_TIMEOUT, HTTPStatus.TOO_EARLY, HTTPStatus.TOO_MANY_REQUESTS}
)


class WebhookUnavailableError(RetryableError):
    """Сетевая ошибка, таймаут, 5xx или 429: получатель может принять позже."""


class WebhookRejectedError(NonRetryableError):
    """Получатель ответил 4xx или редиректом: повтор не поможет."""


class WebhookPayload(BaseModel):
    event: str
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "WebhookPayload":
        return cls(
            event=f"payment.{payment.status.value}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.metadata,
            processed_at=payment.processed_at,
        )


def sign(secret: str, timestamp: int, body: bytes) -> str:
    """Подпись HMAC-SHA256 от `<timestamp>.<body>`.

    По времени в подписи получатель может отбросить старый перехваченный запрос.
    """
    message = f"{timestamp}.".encode() + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


@dataclass(frozen=True, slots=True)
class WebhookRequest:
    body: bytes
    headers: dict[str, str]


def build_request(payment: Payment, secret: str) -> WebhookRequest:
    """Тело и заголовки webhook-а. Подписываются ровно те байты, которые уйдут в запрос."""
    payload = WebhookPayload.from_payment(payment)
    body = payload.model_dump_json().encode()
    timestamp = int(time.time())
    return WebhookRequest(
        body=body,
        headers={
            "Content-Type": "application/json",
            # Не меняется между повторами: по нему получатель отсеивает дубли.
            "X-Webhook-Id": str(payment.id),
            "X-Webhook-Event": payload.event,
            "X-Webhook-Timestamp": str(timestamp),
            "X-Webhook-Signature": f"sha256={sign(secret, timestamp, body)}",
        },
    )


def _log_response(payment: Payment, request: WebhookRequest, status: int) -> None:
    """Что отправили и что ответил получатель.

    Пишутся только заголовки для поиска и отладки. Подпись не пишется: она получена из
    секрета. От адреса — только хост: в пути и параметрах клиенты иногда передают токены.
    """
    level = logging.INFO if 200 <= status < 300 else logging.WARNING
    logger.log(
        level,
        "webhook response",
        extra={
            "payment_id": str(payment.id),
            "receiver_host": urlsplit(payment.webhook_url).hostname,
            "http_status": status,
            "x_webhook_id": request.headers["X-Webhook-Id"],
            "x_webhook_event": request.headers["X-Webhook-Event"],
            "x_webhook_timestamp": request.headers["X-Webhook-Timestamp"],
        },
    )


def _raise_for_status(status: int) -> None:
    """Решить по коду ответа: успех, повторить позже или не повторять."""
    if 200 <= status < 300:
        return
    error = f"HTTP {status} from webhook receiver"
    if status >= 500 or status in _RETRYABLE_CLIENT_ERRORS:
        raise WebhookUnavailableError(error)
    raise WebhookRejectedError(error)


class WebhookSender:
    """Отправляет webhook-и через одну HTTP-сессию на процесс.

    Сессия создаётся при первой отправке — уже внутри работающего цикла событий, как требует
    aiohttp — и держит пул соединений, так что повторные запросы к тому же хосту не открывают
    новое соединение. Закрыть: `await sender.aclose()` или `async with WebhookSender(...)`.
    """

    def __init__(self, secret: str, *, timeout: float = 10.0) -> None:
        self._secret = secret
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, payment: Payment) -> None:
        request = build_request(payment, self._secret)
        status = await self._post(payment.webhook_url, request)
        _log_response(payment, request, status)
        _raise_for_status(status)

    async def _post(self, url: str, request: WebhookRequest) -> int:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        try:
            async with self._session.post(
                url,
                data=request.body,
                headers=request.headers,
                # Не идём по редиректам: получатель не должен перенаправить наш запрос
                # на другой хост. В aiohttp по умолчанию редиректы выполняются.
                allow_redirects=False,
            ) as response:
                return response.status
        except (aiohttp.ClientError, TimeoutError) as exc:  # сеть, таймаут, протокол
            raise WebhookUnavailableError(f"{type(exc).__name__}: {exc}") from exc
