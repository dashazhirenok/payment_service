"""HTTP API на настоящей PostgreSQL: создание, получение, идемпотентность, авторизация."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import Table, func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.api.main import create_app
from payment_service.config import Settings
from payment_service.db.tables import outbox, payments

AUTH = {"X-API-Key": "test-api-key"}


def payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "1500.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42, "customer": {"id": "c-1"}},
        "webhook_url": "https://merchant.example/webhooks",
    }
    body.update(overrides)
    return body


def headers(idempotency_key: str | None = None) -> dict[str, str]:
    return {**AUTH, "Idempotency-Key": idempotency_key or str(uuid4())}


@pytest.fixture
async def client(settings: Settings, engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c,
    ):
        yield c


async def count(engine: AsyncEngine, table: Table) -> int:
    async with engine.connect() as conn:
        return int(await conn.scalar(select(func.count()).select_from(table)) or 0)


async def test_create_payment_writes_payment_and_outbox_event_atomically(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    """POST отвечает 202 и в одной транзакции пишет платёж и событие в outbox.

    Событие ещё не опубликовано: в RabbitMQ его отправит relay уже после коммита.

    Arrange: тело платежа.
    Act: отправляем POST /api/v1/payments.
    Assert: 202, в ответе три поля и статус pending; в outbox одно событие payment.created с ключом
        payments.new, ещё не опубликованное.
    """
    # Arrange
    body = payment_body()

    # Act
    response = await client.post("/api/v1/payments", json=body, headers=headers())

    # Assert
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert set(data) == {"payment_id", "status", "created_at"}
    async with engine.connect() as conn:
        event = (await conn.execute(select(outbox))).mappings().one()
    assert str(event["aggregate_id"]) == data["payment_id"]
    assert event["event_type"] == "payment.created"
    assert event["routing_key"] == "payments.new"
    assert event["payload"]["payment_id"] == data["payment_id"]
    assert event["published_at"] is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    """GET возвращает все поля платежа; сумма — строкой, а не float.

    Arrange: создаём платёж через POST.
    Act: запрашиваем его через GET.
    Assert: 200, все поля на месте, сумма «1500.50» строкой.
    """
    # Arrange
    key = str(uuid4())
    created = await client.post("/api/v1/payments", json=payment_body(), headers=headers(key))
    payment_id = created.json()["payment_id"]

    # Act
    response = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH)

    # Assert
    assert response.status_code == 200
    data = response.json()
    assert data["payment_id"] == payment_id
    assert data["amount"] == "1500.50"
    assert data["currency"] == "RUB"
    assert data["metadata"] == {"order_id": 42, "customer": {"id": "c-1"}}
    assert data["idempotency_key"] == key
    assert data["status"] == "pending"
    assert data["processed_at"] is None


async def test_get_unknown_payment_returns_404(client: httpx.AsyncClient) -> None:
    """Запрос несуществующего платежа — 404.

    Arrange: случайный id, которого нет в базе.
    Act: запрашиваем платёж через GET.
    Assert: 404.
    """
    # Arrange
    unknown_id = uuid4()

    # Act
    response = await client.get(f"/api/v1/payments/{unknown_id}", headers=AUTH)

    # Assert
    assert response.status_code == 404


async def test_replay_with_same_key_returns_original_payment(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    """Повтор с тем же ключом и тем же телом возвращает исходный платёж, новый не создаётся.

    Сумма во втором запросе записана иначе (`1500.5`), но по смыслу это тот же запрос.

    Arrange: создаём платёж с ключом K.
    Act: повторяем POST с тем же ключом K и суммой, записанной как «1500.5».
    Assert: 202, тот же ответ и заголовок Idempotent-Replayed: true; в базе один платёж и одно
        событие.
    """
    # Arrange
    key = str(uuid4())
    first = await client.post("/api/v1/payments", json=payment_body(), headers=headers(key))

    # Act
    second = await client.post(
        "/api/v1/payments", json=payment_body(amount="1500.5"), headers=headers(key)
    )

    # Assert
    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert await count(engine, payments) == 1
    assert await count(engine, outbox) == 1


async def test_same_key_with_different_body_is_rejected(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    """Тот же ключ с другим телом — 409, второй платёж не создаётся.

    Arrange: создаём платёж с ключом K.
    Act: отправляем POST с тем же ключом K и другой суммой.
    Assert: 409, в базе по-прежнему один платёж.
    """
    # Arrange
    key = str(uuid4())
    await client.post("/api/v1/payments", json=payment_body(), headers=headers(key))

    # Act
    response = await client.post(
        "/api/v1/payments", json=payment_body(amount="99.00"), headers=headers(key)
    )

    # Assert
    assert response.status_code == 409
    assert await count(engine, payments) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, engine: AsyncEngine
) -> None:
    """10 одновременных запросов с одним ключом создают ровно один платёж и одно событие.

    Arrange: 10 одинаковых запросов с одним ключом.
    Act: отправляем их одновременно.
    Assert: все ответили 202 с одним payment_id, новым был только один ответ; в базе один платёж и
        одно событие.
    """
    # Arrange
    key = str(uuid4())
    requests = (
        client.post("/api/v1/payments", json=payment_body(), headers=headers(key))
        for _ in range(10)
    )

    # Act
    responses = await asyncio.gather(*requests)

    # Assert
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert sum("Idempotent-Replayed" not in r.headers for r in responses) == 1
    assert await count(engine, payments) == 1
    assert await count(engine, outbox) == 1


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid4()}")],
)
@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_endpoints_require_api_key(
    client: httpx.AsyncClient, method: str, path: str, api_key: str | None
) -> None:
    """Без `X-API-Key` или с неверным ключом оба эндпоинта отвечают 401.

    Arrange: заголовки без X-API-Key или с неверным ключом; эндпоинт — из параметров.
    Act: отправляем запрос.
    Assert: 401.
    """
    # Arrange
    request_headers = {"Idempotency-Key": "k"}
    if api_key is not None:
        request_headers["X-API-Key"] = api_key

    # Act
    response = await client.request(method, path, json=payment_body(), headers=request_headers)

    # Assert
    assert response.status_code == 401


async def test_missing_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    """Заголовок `Idempotency-Key` обязателен: без него запрос отклоняется.

    Arrange: заголовки только с API-ключом, без Idempotency-Key.
    Act: отправляем POST.
    Assert: 422.
    """
    # Arrange
    headers_without_key = AUTH

    # Act
    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers=headers_without_key
    )

    # Assert
    assert response.status_code == 422


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": "0"},
        {"amount": "-10.00"},
        {"amount": "10.001"},  # дробные копейки не округляются молча
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"webhook_url": "ftp://merchant.example/hook"},
        {"description": "x" * 1001},
        {"unexpected": "field"},
    ],
)
async def test_invalid_body_is_rejected(
    client: httpx.AsyncClient,
    engine: AsyncEngine,
    overrides: dict[str, Any],
) -> None:
    """Некорректное тело запроса — 422, в базу ничего не пишется.

    Arrange: тело платежа с одним некорректным полем (поле — из параметров).
    Act: отправляем POST.
    Assert: 422, платежей в базе нет.
    """
    # Arrange
    body = payment_body(**overrides)

    # Act
    response = await client.post("/api/v1/payments", json=body, headers=headers())

    # Assert
    assert response.status_code == 422
    assert await count(engine, payments) == 0


async def test_health(client: httpx.AsyncClient) -> None:
    """Health-check-и отвечают без API-ключа; readiness видит PostgreSQL.

    Arrange: клиент без API-ключа.
    Act: запрашиваем /health/live и /health/ready.
    Assert: 200 и {"status": "ok"}.
    """
    # Arrange: клиент без заголовка X-API-Key

    # Act
    live = await client.get("/health/live")
    ready = await client.get("/health/ready")

    # Assert
    assert live.status_code == 200
    assert ready.json() == {"status": "ok"}
