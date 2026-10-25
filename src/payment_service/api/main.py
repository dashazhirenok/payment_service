import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from payment_service.api.routes import health_router, payments_router
from payment_service.config import Settings, get_settings
from payment_service.db.engine import create_engine
from payment_service.domain.errors import IdempotencyKeyReusedError, PaymentNotFoundError
from payment_service.logging_config import configure_logging
from payment_service.outbox.relay import run_relay
from payment_service.services.payments import PaymentService

logger = logging.getLogger(__name__)

# Сколько ждать остановки relay при выключении API. Пачка публикуется не дольше 5 с.
RELAY_SHUTDOWN_TIMEOUT = 10.0


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        app.state.settings = settings
        app.state.engine = engine
        app.state.payment_service = PaymentService(engine)

        stop = asyncio.Event()
        relay_task: asyncio.Task[None] | None = None
        if settings.outbox_relay_enabled:
            relay_task = asyncio.create_task(run_relay(settings, engine, stop), name="outbox-relay")
        try:
            yield
        finally:
            stop.set()
            try:
                if relay_task is not None:
                    # Relay дорабатывает текущую пачку. Если не успел — задача отменяется,
                    # неопубликованные события останутся в outbox и уйдут после рестарта.
                    await asyncio.wait_for(relay_task, timeout=RELAY_SHUTDOWN_TIMEOUT)
            except TimeoutError:
                logger.warning("outbox relay did not stop in time, cancelled")
            except Exception:
                logger.exception("outbox relay crashed")
            finally:
                # Соединения с БД закрываем при любом исходе остановки relay.
                await engine.dispose()

    app = FastAPI(
        title="Payment Service",
        version="0.1.0",
        description="Accepts payments, processes them asynchronously and reports via webhook.",
        lifespan=lifespan,
    )
    app.include_router(payments_router)
    app.include_router(health_router)

    @app.exception_handler(RequestValidationError)
    async def _invalid_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Само значение не пишем: в description или metadata может оказаться что угодно,
        # вплоть до персональных данных. Поля и причины достаточно, чтобы понять ошибку.
        logger.warning(
            "request rejected: invalid input",
            extra={
                "method": request.method,
                "path": request.url.path,
                "idempotency_key": request.headers.get("Idempotency-Key"),
                "errors": "; ".join(
                    f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
                    for error in exc.errors()
                ),
            },
        )
        return await request_validation_exception_handler(request, exc)

    @app.exception_handler(IdempotencyKeyReusedError)
    async def _idempotency_conflict(_: Request, exc: IdempotencyKeyReusedError) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)})

    @app.exception_handler(PaymentNotFoundError)
    async def _not_found(_: Request, exc: PaymentNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})

    return app


def app() -> FastAPI:
    """Точка входа для `uvicorn --factory payment_service.api.main:app`."""
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_app(settings)
