import asyncio
import random
from dataclasses import dataclass
from typing import Protocol

from payment_service.domain.models import Payment, PaymentStatus
from payment_service.messaging.retry import RetryableError


@dataclass(frozen=True, slots=True)
class GatewayResult:
    status: PaymentStatus
    reason: str | None = None


class GatewayUnavailableError(RetryableError):
    """Шлюз недоступен или не ответил вовремя. Чем закончилось списание, неизвестно."""


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> GatewayResult:
        """Списать деньги. Повторный вызов с тем же `payment.id` должен дать тот же результат."""


class EmulatedGateway:
    """Имитация платёжного шлюза: отвечает за 2–5 секунд, одобряет около 90% платежей.

    Случайность зависит от id платежа, поэтому один и тот же платёж всегда получает один
    и тот же ответ. Так ведёт себя настоящий шлюз, если передать ему ключ идемпотентности.
    Благодаря этому повторная обработка после падения не может поменять результат.
    """

    def __init__(
        self, *, min_delay: float = 2.0, max_delay: float = 5.0, success_rate: float = 0.9
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate

    async def charge(self, payment: Payment) -> GatewayResult:
        rng = random.Random(payment.id.int)
        await asyncio.sleep(rng.uniform(self._min_delay, self._max_delay))
        if rng.random() < self._success_rate:
            return GatewayResult(PaymentStatus.SUCCEEDED)
        return GatewayResult(PaymentStatus.FAILED, reason="declined by issuer")
