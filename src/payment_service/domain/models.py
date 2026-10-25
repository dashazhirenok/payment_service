"""Доменные модели. Обычные dataclass-ы, без привязки к БД, HTTP и брокеру."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

# У всех поддерживаемых валют два знака после запятой.
MONEY_QUANTUM = Decimal("0.01")


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self is not PaymentStatus.PENDING


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    idempotency_key: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Хеш тела запроса, не зависящий от форматирования.

        Хранится рядом с ключом идемпотентности. Если тот же ключ придёт с другим телом,
        мы это заметим, а не вернём молча платёж, о котором клиент не просил.
        """
        canonical = json.dumps(
            {
                "amount": str(self.amount.quantize(MONEY_QUANTUM)),
                "currency": self.currency.value,
                "description": self.description,
                "metadata": self.metadata,
                "webhook_url": self.webhook_url,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    request_fingerprint: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """Событие, ожидающее публикации. Пишется в той же транзакции, что и сам платёж."""

    id: UUID
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    routing_key: str
    payload: dict[str, Any]
    created_at: datetime | None = None
    attempts: int = 0
