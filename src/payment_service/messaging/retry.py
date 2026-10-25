from dataclasses import dataclass

# Заголовок с номером попытки (с единицы). Ставим мы при отправке на повтор.
ATTEMPT_HEADER = "x-attempt"
# Сколько раз RabbitMQ уже возвращал это сообщение в очередь (после nack или падения
# consumer-а). Ставит сама quorum-очередь.
DELIVERY_COUNT_HEADER = "x-delivery-count"
# Заголовки, которые ставит брокер. В копию для повтора их переносить нельзя: у новой
# копии свой счётчик доставок и своя история.
BROKER_HEADERS = frozenset(
    {
        DELIVERY_COUNT_HEADER,
        "x-death",
        "x-first-death-exchange",
        "x-first-death-queue",
        "x-first-death-reason",
        "x-last-death-exchange",
        "x-last-death-queue",
        "x-last-death-reason",
    }
)


class NonRetryableError(Exception):
    """Повтор не поможет: битое сообщение, нет платежа, получатель отказал."""


class RetryableError(Exception):
    """Временная ошибка: сеть, таймаут, 5xx. Незнакомые ошибки считаем такими же."""


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 2.0

    def delay_ms(self, failed_attempt: int) -> int:
        """Задержка после неудачной попытки, каждый раз вдвое больше: base, 2*base, 4*base…"""
        return round(self.base_delay * 2.0 ** (failed_attempt - 1) * 1000)

    def delays_ms(self) -> list[int]:
        return [self.delay_ms(attempt) for attempt in range(1, self.max_attempts)]

    def should_retry(self, error: BaseException, attempt: int) -> bool:
        return attempt < self.max_attempts and not isinstance(error, NonRetryableError)


def _as_int(value: object, default: int) -> int:
    if isinstance(value, int | str | bytes):
        try:
            return int(value)
        except ValueError:
            pass
    return default


def attempt_from_headers(headers: dict[str, object] | None) -> int:
    """Номер текущей попытки: наш счётчик `x-attempt` плюс повторные доставки брокера.

    Повторная доставка тоже считается попыткой. Иначе сообщение, которое не удаётся
    отложить через retry-очередь и приходится возвращать через nack, крутилось бы без счёта.
    """
    headers = headers or {}
    attempt = max(1, _as_int(headers.get(ATTEMPT_HEADER), 1))
    redeliveries = max(0, _as_int(headers.get(DELIVERY_COUNT_HEADER), 0))
    return attempt + redeliveries
