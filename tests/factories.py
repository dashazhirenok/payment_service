from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from payment_service.domain.models import (
    Currency,
    Payment,
    PaymentStatus,
)


def make_payment(status: PaymentStatus = PaymentStatus.SUCCEEDED) -> Payment:
    now = datetime.now(UTC)
    return Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=Currency.RUB,
        description="d",
        metadata={"order": 1},
        status=status,
        idempotency_key="k",
        request_fingerprint="f",
        webhook_url="https://merchant.example/hook",
        created_at=now,
        processed_at=None if status is PaymentStatus.PENDING else now,
    )
