import logging
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from payment_service.config import Settings
from payment_service.services.payments import PaymentService

logger = logging.getLogger(__name__)

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


def get_payment_service(request: Request) -> PaymentService:
    service: PaymentService = request.app.state.payment_service
    return service


async def require_api_key(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Security(_api_key_header)],
) -> None:
    expected = settings.api_key.get_secret_value()
    # Сравнение за постоянное время: по скорости ответа нельзя угадать ключ по частям.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        # Присланный ключ в лог не пишем — даже неверный он может оказаться чужим настоящим.
        logger.warning(
            "request rejected: invalid or missing API key",
            extra={
                "method": request.method,
                "path": request.url.path,
                "api_key_present": api_key is not None,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
