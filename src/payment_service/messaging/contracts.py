"""Формат сообщений в RabbitMQ. Меняется независимо от схемы БД."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    """Событие «создан платёж». Содержит только id, данные consumer берёт из PostgreSQL.

    Так БД остаётся единственным источником правды, и старое или повторное сообщение
    не может принести в обработку устаревшие данные.
    """

    # Незнакомые поля игнорируем: в событие можно добавлять поля, не ломая consumer.
    model_config = ConfigDict(frozen=True, extra="ignore")

    event_id: UUID
    payment_id: UUID
    occurred_at: datetime
