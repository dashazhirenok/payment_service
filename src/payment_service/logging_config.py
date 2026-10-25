"""Формат логов: одна строка на событие.

    2026-10-14 11:15:33.272 INFO consumer.processor: payment processed payment_id=… status=succeeded

Сначала время (UTC), уровень и источник, потом сообщение, потом поля из `extra=` в виде
`ключ=значение` — по ним, например по `payment_id`, логи удобно искать через grep.
"""

import json
import logging
import time

LOG_FORMAT = "%(asctime)s.%(msecs)03d %(levelname)s %(object_name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Общий префикс имён наших логгеров: в строке лога он только занимает место.
_OWN_PREFIX = "payment_service."

# Стандартные поля LogRecord. Всё остальное пришло через `extra=` и дописывается в конец.
_STANDARD_ATTRS = frozenset(vars(logging.makeLogRecord({}))) | {
    "message",
    "asctime",
    "object_name",
    "color_message",  # uvicorn дублирует каждое сообщение с цветами
}


def _format_value(value: object) -> str:
    """Значение для `ключ=значение`; строку с пробелами берём в кавычки, чтобы не разъехалась."""
    text = str(value)
    if not text or any(ch in text for ch in ' "='):
        return json.dumps(text, ensure_ascii=False)
    return text


class KeyValueFormatter(logging.Formatter):
    @staticmethod
    def converter(seconds: float | None) -> time.struct_time:
        # Время в UTC, чтобы логи не зависели от часового пояса машины, где запущен сервис.
        return time.gmtime(seconds)

    def __init__(self) -> None:
        super().__init__(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        record.object_name = record.name.removeprefix(_OWN_PREFIX)
        line = super().format(record)
        fields = " ".join(
            f"{key}={_format_value(value)}"
            for key, value in vars(record).items()
            if key not in _STANDARD_ATTRS
        )
        if not fields:
            return line
        # Трейсбек (если есть) идёт с новой строки — поля ставим перед ним, на первой строке.
        first_line, sep, rest = line.partition("\n")
        return f"{first_line} {fields}{sep}{rest}"


def _skip_health_checks(record: logging.LogRecord) -> bool:
    """Не писать в лог запросы к /health/*: Docker делает их каждые 5 секунд, это шум."""
    args = record.args
    return not (isinstance(args, tuple) and len(args) >= 3 and str(args[2]).startswith("/health"))


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(KeyValueFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # У uvicorn свои обработчики логов. Отключаем их, чтобы всё шло в одном формате.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    logging.getLogger("uvicorn.access").addFilter(_skip_health_checks)
