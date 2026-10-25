# Payment Service

Сервис принимает платежи по HTTP, обрабатывает их через эмулятор платёжного шлюза и отправляет результат клиенту через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

Архитектура и поведение при сбоях описаны в [ARCHITECTURE.md](ARCHITECTURE.md). Исходное задание — в [description.md](description.md).

## Запуск

```bash
docker compose --profile demo up -d --build
```

Профиль `demo` добавляет к четырём сервисам из задания `httpbin` — локальную заглушку получателя webhook-ов для примеров. Останавливать с тем же профилем, иначе `httpbin` останется работать:

```bash
docker compose --profile demo down
```

| Сервис | Адрес |
|---|---|
| `api` (вместе с outbox relay) | http://localhost:8000/docs |
| `consumer` | — |
| `postgres` | `localhost:5433` |
| `rabbitmq` | `localhost:5672`, UI: http://localhost:15672 (`guest`/`guest`) |
| `httpbin` (профиль `demo`) | `http://httpbin:8080` внутри docker-сети |

При старте `api` применяет миграции, consumer запускается после него. API-ключ по умолчанию — `dev-api-key` (`API_KEY`).

Данные PostgreSQL и RabbitMQ хранятся в томах и переживают перезапуски, в том числе `down` + `up`. Начать с чистой базы и пустым брокером: `docker compose --profile demo down -v`.

## Примеры

Создать платёж:

```bash
curl -s -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Заказ #42",
       "metadata": {"order_id": 42}, "webhook_url": "http://httpbin:8080/post"}'
```

```json
{"payment_id": "0b6e…", "status": "pending", "created_at": "2026-10-09T11:14:38.512Z"}
```

Через 2–5 секунд статус станет `succeeded` или `failed`:

```bash
curl -s localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
```

### Повторный запрос с тем же ключом

Тот же запрос с тем же `Idempotency-Key` новый платёж не создаёт — сервис вернёт прежний, с текущим статусом (заголовки видны с `curl -i`):

```http
HTTP/1.1 202 Accepted
idempotent-replayed: true

{"payment_id":"ebfa53db-…","status":"succeeded","created_at":"2026-10-14T11:17:45.052235Z"}
```

```text
2026-10-14 11:18:25.805 INFO services.payments: payment request replayed payment_id=ebfa53db-… idempotency_key=order-42
```

Consumer при этом ничего не делает. Тот же ключ с другим телом — `409`. Для нового платежа нужен новый ключ.

### Что видно в логах

```bash
docker compose logs -f api consumer
```

Строка: время (UTC), уровень, источник, сообщение и поля `ключ=значение`. Для платежа из примера (id сокращены):

```text
api-1       | 2026-10-14 11:31:16.097 INFO services.payments: payment accepted payment_id=eff7e45f-… amount=1500.00 currency=RUB
consumer-1  | 2026-10-14 11:31:16.113 INFO consumer.delivery: message received payment_id=eff7e45f-… attempt=1
api-1       | 2026-10-14 11:31:16.114 INFO outbox.relay: event published payment_id=eff7e45f-…
consumer-1  | 2026-10-14 11:31:19.798 INFO consumer.processor: payment processed payment_id=eff7e45f-… status=succeeded gateway_ms=3675
consumer-1  | 2026-10-14 11:31:19.805 INFO consumer.webhooks: webhook response payment_id=eff7e45f-… receiver_host=httpbin http_status=200
consumer-1  | 2026-10-14 11:31:19.806 INFO consumer.processor: webhook delivered payment_id=eff7e45f-… webhook_ms=6
consumer-1  | 2026-10-14 11:31:19.806 INFO consumer.delivery: message acked payment_id=eff7e45f-… attempt=1
```

| Строка | Что произошло |
|---|---|
| `payment accepted` | Платёж и событие записаны в БД одной транзакцией, API ответил `202` |
| `event published` | Relay отправил событие в RabbitMQ (очередь `payments.new`) |
| `message received` | Consumer получил сообщение |
| `payment processed` | Шлюз ответил, статус записан в БД |
| `webhook response` / `webhook delivered` | Webhook отправлен, получатель ответил `2xx` |
| `message acked` | Сообщение подтверждено и удалено из очереди |

Отклонённые запросы пишутся как `WARNING` с указанием причины, без самих значений: `request rejected: invalid input … errors="body.amount: Input should be a valid decimal"` (ответ `422`) или `request rejected: invalid or missing API key` (`401`). Такой запрос не доходит до БД и RabbitMQ, поэтому и в DLQ не попадает. Запросы Docker к `/health/*` в лог не пишутся.

История одного платежа — во всех строках есть `payment_id`:

```bash
docker compose logs --no-log-prefix api consumer | grep <payment_id>
```

### Что нужно знать о webhook-ах

- Один webhook может прийти несколько раз: доставка работает по принципу «хотя бы один раз». Получателю следует отсекать дубли по `X-Webhook-Id` — он равен `payment_id` и не меняется при повторах.
- Подпись проверяется по `X-Webhook-Signature`: это HMAC-SHA256 от `<X-Webhook-Timestamp>.<тело запроса>` с секретом `WEBHOOK_SECRET`.
- Любой ответ `2xx` считается успехом. Для сетевых ошибок, таймаута (10 секунд), `5xx`, `408`, `425` и `429` выполняются повторы; для остальных `4xx` и редиректов `3xx` — нет.

### Как проверить обработку ошибок

Подставьте в `webhook_url` один из адресов:

| `webhook_url` | Результат | В логе consumer |
|---|---|---|
| `http://httpbin:8080/post` | Успех с первой попытки | `webhook delivered` |
| `http://httpbin:8080/status/503` | 3 попытки с паузами 2 и 4 с, затем DLQ | `scheduling retry` ×2, затем `dead-lettering` |
| `http://httpbin:8080/status/400` | Сразу в DLQ, без повтора | `dead-lettering` |

Статус платежа при ошибке webhook-а не меняется. Сообщения из DLQ видны в RabbitMQ UI: http://localhost:15672/#/queues/%2F/payments.new.dlq.

Если остановить RabbitMQ (`docker compose stop rabbitmq`), API продолжит принимать платежи: события останутся в `outbox` (в логе API — `event publish failed`) и уйдут после `docker compose start rabbitmq`.

## API

Все эндпоинты `/api/v1/*` требуют заголовок `X-API-Key`.

| Метод и путь | Ответы |
|---|---|
| `POST /api/v1/payments` | `202`, `401`, `409` (ключ уже использован с другим телом), `422` |
| `GET /api/v1/payments/{payment_id}` | `200`, `401`, `404` |
| `GET /health/live`, `/health/ready` | `200` / `503`; readiness проверяет PostgreSQL |

Проверки входных данных: `amount` больше нуля и не более двух знаков после запятой; `currency` — `RUB`, `USD` или `EUR`; `webhook_url` — HTTP(S)-адрес; неизвестные поля запрещены.

## Принятые решения

Некоторые требования задания допускают разные трактовки. В этом проекте они работают так:

- **«10% ошибка»** означает отказ банка, а не технический сбой. Платёж получает `failed`, клиенту отправляется webhook; повторять обработку не нужно.
- **Три попытки — всего:** первая обработка и два повтора через 2 и 4 секунды. Повторно списывать деньги сервис не будет: если платёж уже обработан, шлюз второй раз не вызывается.
- **Ошибка webhook-а не отменяет платёж.** Если уведомление не удалось доставить за три попытки, сообщение уходит в DLQ, а статус платежа остаётся прежним. Отдельной записи о доставке в БД нет — результат можно увидеть в логах consumer и DLQ.
- **Webhook доставляется «хотя бы один раз».** При повторной обработке сообщения уведомление может отправиться повторно, даже если прошлый запрос дошёл. Получателю нужно дедуплицировать запросы по `X-Webhook-Id`.
- **Ответ `4xx` от получателя не повторяется:** сообщение сразу отправляется в DLQ, так как повтор вряд ли изменит результат.
- **Идемпотентность работает на двух уровнях.** `Idempotency-Key` защищает от повторных запросов клиента, а consumer — от повторной обработки сообщения. Дубли сообщений возможны, поэтому обе защиты нужны.
- **Ключ идемпотентности общий для всех клиентов** и не устаревает: в реализации используется один API-ключ. Если появятся отдельные клиенты, ключ нужно ограничить парой `(client_id, idempotency_key)`.
- **Outbox relay запускается внутри API**, поскольку в задании для него не предусмотрен отдельный сервис. По той же причине миграции применяются при старте `api`, а не отдельным сервисом.
- **Health-check-и `/health/*` не требуют API-ключа**, чтобы их могли вызывать Docker и балансировщик.

## Конфигурация

Основные переменные окружения:

| Переменная | Значение по умолчанию | Для чего нужна |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5433/payments` | Подключение к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Подключение к RabbitMQ |
| `API_KEY` | Обязательна | Ключ для заголовка `X-API-Key` |
| `WEBHOOK_SECRET` | `change-me` | Секрет HMAC-подписи webhook-ов |
| `MAX_ATTEMPTS` | `3` | Число попыток до DLQ |
| `RETRY_BASE_DELAY` | `2.0` | Базовая задержка повтора в секундах: `base`, `2 × base` и т. д. |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

Таймауты, размер пачки и параметры эмулятора шлюза оставлены константами рядом с кодом, где они используются.

Consumer можно масштабировать:

```bash
docker compose up -d --scale consumer=3
```

## Разработка

Создать окружение и установить зависимости:

```bash
python -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"
```

Тесты:

```text
tests/unit/         без БД и брокера: ack/повтор/DLQ, политика повторов, отправка webhook-ов
tests/integration/  на PostgreSQL: API и идемпотентность, обработка платежа и дубли
```

Интеграционным тестам нужна PostgreSQL из Compose. Они создают базу `payments_test` и применяют к ней миграции. Без PostgreSQL можно запустить только unit-тесты.

```bash
docker compose up -d postgres
pytest
ruff check src tests && mypy
```

Запуск без Docker (PostgreSQL и RabbitMQ при этом должны быть доступны, настройки берутся из `.env`):

```bash
cp .env.example .env && alembic upgrade head
uvicorn --factory payment_service.api.main:app --reload
python -m payment_service.consumer
```

## Структура проекта

```text
src/payment_service/
├── api/        роуты, Pydantic-схемы и аутентификация
├── services/   создание платежа и границы транзакций
├── domain/     доменные модели (dataclasses) и ошибки
├── db/         таблицы (SQLAlchemy Core), подключение к БД и репозитории
├── messaging/  контракты сообщений, топология RabbitMQ и retry-политика
├── outbox/     публикация событий из outbox
└── consumer/   обработка платежей, эмулятор шлюза, webhooks и ack/retry/DLQ
```
