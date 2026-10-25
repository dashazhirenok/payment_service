from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, status
from sqlalchemy import text

from payment_service.api.dependencies import PaymentServiceDep, require_api_key
from payment_service.api.schemas import (
    CreatePaymentRequest,
    ErrorResponse,
    PaymentAcceptedResponse,
    PaymentResponse,
)

payments_router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}},
)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Client-generated unique key; retries with the same key and body "
        "return the original payment instead of creating a new one.",
    ),
]


@payments_router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    responses={409: {"model": ErrorResponse, "description": "Key reused with another body"}},
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: IdempotencyKey,
    response: Response,
    service: PaymentServiceDep,
) -> PaymentAcceptedResponse:
    result = await service.create(body.to_command(idempotency_key))
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAcceptedResponse.from_domain(result.payment)


@payments_router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    responses={404: {"model": ErrorResponse}},
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentResponse:
    return PaymentResponse.from_domain(await service.get(payment_id))


health_router = APIRouter(prefix="/health", tags=["health"])


@health_router.get("/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@health_router.get("/ready")
async def readiness(request: Request, response: Response) -> dict[str, str]:
    # Проверяем только PostgreSQL. Благодаря outbox API принимает платежи,
    # даже когда RabbitMQ недоступен.
    try:
        async with request.app.state.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable"}
    return {"status": "ok"}
