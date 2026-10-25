"""Политика повторов: задержки, какие ошибки повторять, номер попытки по заголовкам."""

import pytest

from payment_service.messaging.retry import (
    NonRetryableError,
    RetryableError,
    RetryPolicy,
    attempt_from_headers,
)


def test_exponential_backoff() -> None:
    """Задержка каждый раз удваивается, а повторов на один меньше, чем попыток.

    Arrange: политики «3 попытки, база 2 с» и «5 попыток, база 0,5 с».
    Act: получаем список задержек каждой.
    Assert: [2000, 4000] и [500, 1000, 2000, 4000] мс.
    """
    # Arrange
    default_policy = RetryPolicy(max_attempts=3, base_delay=2.0)
    longer_policy = RetryPolicy(max_attempts=5, base_delay=0.5)

    # Act
    default_delays = default_policy.delays_ms()
    longer_delays = longer_policy.delays_ms()

    # Assert
    assert default_delays == [2000, 4000]  # 3 попытки = 2 повтора
    assert longer_delays == [500, 1000, 2000, 4000]


@pytest.mark.parametrize(
    ("error", "attempt", "expected"),
    [
        (RetryableError(), 1, True),
        (RetryableError(), 2, True),
        (RetryableError(), 3, False),  # попытки кончились
        (ConnectionError(), 1, True),  # незнакомые ошибки считаем временными
        (NonRetryableError(), 1, False),
    ],
)
def test_should_retry(error: Exception, attempt: int, expected: bool) -> None:
    """Повторяем временные и незнакомые ошибки, пока не исчерпаны попытки; окончательные — нет.

    Arrange: политика на 3 попытки; ошибка и номер попытки — из параметров.
    Act: спрашиваем, повторять ли.
    Assert: ответ совпадает с ожидаемым.
    """
    # Arrange
    policy = RetryPolicy(max_attempts=3)

    # Act
    decision = policy.should_retry(error, attempt)

    # Assert
    assert decision is expected


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (None, 1),
        ({}, 1),
        ({"x-attempt": 2}, 2),
        ({"x-attempt": "3"}, 3),
        ({"x-attempt": "x"}, 1),
        ({"x-delivery-count": 1}, 2),  # первая попытка вернулась через nack
        ({"x-attempt": 2, "x-delivery-count": 1}, 3),
    ],
)
def test_attempt_from_headers(headers: dict[str, object] | None, expected: int) -> None:
    """Номер попытки = наш `x-attempt` + повторные доставки брокера; мусор в заголовке — 1.

    Arrange: заголовки сообщения — из параметров.
    Act: вычисляем номер попытки.
    Assert: он совпадает с ожидаемым.
    """
    # Arrange: заголовки сообщения заданы параметрами теста

    # Act
    attempt = attempt_from_headers(headers)

    # Assert
    assert attempt == expected
