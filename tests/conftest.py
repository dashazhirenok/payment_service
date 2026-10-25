import os

# Без API_KEY настройки не создаются. Тесты не должны зависеть от окружения разработчика.
os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("WEBHOOK_SECRET", "test-webhook-secret")
