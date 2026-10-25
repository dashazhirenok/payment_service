"""Интеграционные тесты идут на настоящей PostgreSQL с настоящими миграциями Alembic.

Запуск базы: `docker compose up -d postgres`. Тесты сами пересоздают базу `payments_test`.
Если PostgreSQL недоступна, эти тесты пропускаются.
"""

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import Settings
from payment_service.db.engine import create_engine

ROOT = Path(__file__).resolve().parents[2]
ADMIN_DSN = os.environ.get(
    "TEST_POSTGRES_ADMIN_DSN", "postgresql://payments:payments@localhost:5433/payments"
)
TEST_DB = "payments_test"
TEST_DATABASE_URL = (
    ADMIN_DSN.rsplit("/", 1)[0].replace("postgresql://", "postgresql+asyncpg://") + f"/{TEST_DB}"
)


@pytest.fixture(scope="session")
async def _test_database() -> str:
    try:
        admin = await asyncpg.connect(ADMIN_DSN, timeout=3)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL is not available ({exc}); run `docker compose up -d postgres`")
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{TEST_DB}" WITH (FORCE)')
        await admin.execute(f'CREATE DATABASE "{TEST_DB}"')
    finally:
        await admin.close()

    # Накатываем настоящие миграции, а не create_all: заодно проверяем и их.
    migrate = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        "head",
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": TEST_DATABASE_URL},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await migrate.communicate()
    assert migrate.returncode == 0, output.decode()
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
def settings(_test_database: str) -> Settings:
    return Settings(
        database_url=_test_database,
        api_key="test-api-key",
        outbox_relay_enabled=False,
        webhook_secret="test-webhook-secret",
    )


@pytest.fixture(scope="session")
async def _engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture
async def engine(_engine: AsyncEngine) -> AsyncEngine:
    """Пул соединений с очищенными таблицами: каждый тест начинает с пустой базы."""
    async with _engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    return _engine
