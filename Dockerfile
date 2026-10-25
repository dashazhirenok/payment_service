FROM python:3.12-slim AS build
WORKDIR /build
COPY pyproject.toml README.md ./
# Сначала только библиотеки: этот слой кэшируется и пересобирается, лишь когда меняется
# pyproject.toml. Изменение кода не заставляет ставить все зависимости заново.
RUN python -c "import tomllib; print('\n'.join(tomllib.load(open('pyproject.toml', 'rb'))\
['project']['dependencies']))" > requirements.txt \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install --no-deps .

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=build /install /usr/local
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --system --uid 1000 app
USER app
EXPOSE 8000
# Один образ на все роли, команду запуска задаёт docker-compose.
CMD ["uvicorn", "--factory", "payment_service.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
