from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки процесса. Читаются из переменных окружения, локально — ещё и из `.env`.

    Здесь только то, что отличается между окружениями. Остальные параметры (таймауты,
    размеры пачек, параметры эмулятора шлюза) — константы рядом с кодом, который их использует.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5433/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Значения по умолчанию нет специально: сервис не должен стартовать с угадываемым ключом.
    api_key: SecretStr
    webhook_secret: SecretStr = SecretStr("change-me")
    log_level: str = "INFO"

    # Повторы обработки сообщения: всего попыток и задержка перед первым повтором (с).
    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay: float = Field(default=2.0, gt=0)

    # Outbox relay работает фоновой задачей внутри API. Выключается только в тестах,
    # чтобы API не подключался к RabbitMQ.
    outbox_relay_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()  # значения берутся из окружения
