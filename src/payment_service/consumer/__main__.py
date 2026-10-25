"""Запуск consumer: `python -m payment_service.consumer`."""

import asyncio

from payment_service.config import get_settings
from payment_service.consumer.app import create_app
from payment_service.logging_config import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    # FastStream сам ловит SIGTERM/SIGINT и дожидается сообщений, которые уже в работе.
    asyncio.run(create_app(settings).run())


if __name__ == "__main__":
    main()
