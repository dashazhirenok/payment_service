"""Схемы HTTP-запросов и ответов.

Отделены от доменных моделей, чтобы API можно было менять независимо.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from payment_service.domain.models import (
    MONEY_QUANTUM,
    CreatePaymentCommand,
    Currency,
    Payment,
    PaymentStatus,
)

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: Annotated[str, Field(max_length=1000)]
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    def to_command(self, idempotency_key: str) -> CreatePaymentCommand:
        return CreatePaymentCommand(
            amount=self.amount.quantize(MONEY_QUANTUM),
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=str(self.webhook_url),
            idempotency_key=idempotency_key,
        )


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_domain(cls, payment: Payment) -> "PaymentAcceptedResponse":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_domain(cls, payment: Payment) -> "PaymentResponse":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    detail: str
