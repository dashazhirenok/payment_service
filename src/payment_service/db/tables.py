"""Описание таблиц для SQLAlchemy Core.

Это описание схемы, а не объекты-строки: запросы строятся по этим таблицам, а результат
приходит обычными строками. Отсюда же Alembic берёт схему, когда сравнивает её с базой.
"""

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

# Шаблон имён ограничений и индексов. Без него PostgreSQL придумает имена сам,
# и миграции не смогут стабильно на них ссылаться.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)


payments = Table(
    "payments",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("amount", Numeric(18, 2), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("description", Text, nullable=False),
    Column("metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("status", String(16), nullable=False),
    Column("idempotency_key", String(255), nullable=False, unique=True),
    Column("request_fingerprint", String(64), nullable=False),
    Column("webhook_url", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("processed_at", DateTime(timezone=True)),
    CheckConstraint("amount > 0", name="amount_positive"),
    CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_supported"),
    CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_valid"),
    # У итогового статуса всегда есть время обработки, у `pending` — нет.
    CheckConstraint(
        "(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"
    ),
)


outbox = Table(
    "outbox",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("aggregate_type", String(64), nullable=False),
    Column("aggregate_id", Uuid, nullable=False),
    Column("event_type", String(128), nullable=False),
    Column("routing_key", String(255), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("last_error", Text),
    # До какого момента событие «арендовано» relay-ем. NULL или прошлое — свободно.
    Column("locked_until", DateTime(timezone=True)),
    # Не раньше какого момента пробовать опубликовать снова (пауза после ошибки).
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    # Relay ищет только неопубликованные строки, поэтому индекс только по ним.
    Index("ix_outbox_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
)
