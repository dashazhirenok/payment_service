from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from payment_service.config import Settings

# Должен быть не меньше числа сообщений, которые consumer обрабатывает одновременно
# (PREFETCH_COUNT в consumer/app.py): каждое держит соединение, пока ждёт шлюз.
POOL_SIZE = 10
# Страховка от зависших транзакций: если транзакция простаивает дольше, PostgreSQL сама
# закроет соединение и снимет блокировки. Больше таймаута шлюза (10 с), чтобы не обрывать
# обычный вызов.
IDLE_IN_TRANSACTION_TIMEOUT_MS = 30_000


def create_engine(settings: Settings) -> AsyncEngine:
    """Пул соединений с БД, один на процесс.

    Транзакция открывается так: `async with engine.begin() as conn:` — при выходе из блока
    без ошибки будет COMMIT, с ошибкой — ROLLBACK. Только чтение: `engine.connect()`.
    """
    return create_async_engine(
        settings.database_url,
        pool_size=POOL_SIZE,
        pool_pre_ping=True,
        connect_args={
            "server_settings": {
                "idle_in_transaction_session_timeout": str(IDLE_IN_TRANSACTION_TIMEOUT_MS),
            }
        },
    )
