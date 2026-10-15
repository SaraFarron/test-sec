# Асинхронный процессинг платежей

Сервис принимает платеж, сохраняет его вместе с outbox-событием и возвращает
`202 Accepted`. Один consumer эмулирует платежный шлюз, сохраняет результат
и отправляет webhook. Исходное задание: [PDF](./test%20task%20secunda.pdf).

Стек: Python 3.12, FastAPI, Pydantic v2, асинхронный SQLAlchemy, PostgreSQL,
RabbitMQ / FastStream, Alembic.

## Запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
# Замените API_KEY, POSTGRES_PASSWORD и RABBITMQ_PASSWORD в .env.
docker compose up --build -d
docker compose logs -f api consumer
```

`API_KEY` должен содержать минимум 16 символов. Для Compose используйте
URL-safe логины и пароли: они подставляются в строки подключения. Если
запускаете сервис локально, обновите также `DATABASE_URL` и `RABBITMQ_URL`.

Compose запускает PostgreSQL, RabbitMQ, API и consumer. Служебный контейнер
`migrate` применяет миграции до старта приложения; его завершение с кодом 0
нормально. База и брокер используют постоянные volumes.

- API: `http://localhost:8000`.
- RabbitMQ Management: `http://localhost:15672`, учетные данные из `.env`.
- Порты можно переопределить через `API_PORT`, `POSTGRES_PORT`,
  `RABBITMQ_PORT`, `RABBITMQ_MANAGEMENT_PORT`.
- Порты привязаны к `127.0.0.1`. Для внешнего доступа нужен reverse proxy.
- Swagger и публичный OpenAPI отключены: все API endpoints требуют ключ.

Остановка без удаления данных:

```bash
docker compose down
```

## API

Оба endpoint требуют заголовок `X-API-Key`. Отсутствующий или неверный
ключ возвращает `401`.

### Создать платеж

`POST /api/v1/payments`, обязательный заголовок `Idempotency-Key`
(1–255 символов, без пробельных символов).

```bash
export API_KEY='ваш-ключ-из-.env'

curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Заказ 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/webhooks/payments"
  }'
```

Ответ `202`:

```json
{
  "payment_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "pending",
  "created_at": "2026-10-15T09:00:00Z"
}
```

`amount` — положительное decimal-число: до 16 цифр в целой части
и до 2 в дробной. Строка рекомендуется, чтобы избежать ошибок float.
Поддерживаются `RUB`, `USD`, `EUR`. `description` обязателен, максимум
2000 символов; `metadata` — JSON-объект, по умолчанию `{}`.
`webhook_url` — обязательный HTTP(S) URL, максимум 2048 символов.
Неизвестные поля, неверное тело и отсутствующий idempotency key дают `422`.

Повтор с тем же ключом и теми же данными возвращает тот же платеж, его
текущий статус и исходное время создания, тоже с `202`. При сравнении
сумма и URL нормализуются. Повтор с другими данными возвращает `409`.
Уникальное ограничение в БД защищает и от конкурентных запросов.
Ключи не удаляются автоматически.

### Получить платеж

```bash
curl http://localhost:8000/api/v1/payments/123e4567-e89b-12d3-a456-426614174000 \
  -H "X-API-Key: $API_KEY"
```

Ответ содержит `payment_id`, `amount` (строка), `currency`, `description`,
`metadata`, `status`, `idempotency_key`, `webhook_url`, `created_at`,
`processed_at`, `webhook_delivered_at`. Последние два поля изначально `null`.
Неизвестный платеж дает `404`, неверный UUID — `422`.

## Обработка и гарантии

1. API сохраняет `payments` и `outbox` одной транзакцией. Доступность
   RabbitMQ не нужна для приема платежей после запуска API.
2. Outbox relay работает в процессе consumer. Раз в секунду выбирает до
   50 неопубликованных событий с `FOR UPDATE SKIP LOCKED`, публикует
   persistent-сообщения в `payments.new` и ожидает publisher confirm.
   Только после подтверждения выставляет `published_at`.
3. Единственный обработчик с `prefetch=1` блокирует строку платежа,
   эмулирует шлюз: задержка 2–5 секунд, вероятность успеха 90%.
   Затем фиксирует `succeeded` или `failed` и `processed_at`.
4. После commit результата отправляется webhook. Любой `2xx` — успех;
   ошибки сети, timeout, `3xx`, `4xx`, `5xx` — ошибки доставки.
   Timeout по умолчанию 10 секунд, редиректы не выполняются.
5. При ошибке обработки сообщение публикуется в retry-очередь, а исходное
   подтверждается только после publisher confirm. Всего 3 попытки:
   исходная и 2 повторные, с задержками 2 и 4 секунды. После третьей
   ошибки сообщение уходит в `payments.dead`.

Очереди `payments.new`, `payments.retry.2s`, `payments.retry.4s`,
`payments.dead` — durable quorum queues. Retry-очереди используют TTL и
at-least-once dead-lettering, чтобы сообщение не потерялось при временной
недоступности целевой очереди. Если сам перенос в retry/DLQ не подтвержден,
исходное сообщение возвращается в очередь; брокер отдельно ограничивает
такие повторные доставки и отправляет исчерпанные сообщения в DLQ.

Outbox не удаляет неопубликованные события и повторяет публикацию при
ошибках брокера без ограничения по числу попыток. Ошибки логируются;
`publish_attempts` и `last_error` сохраняются в БД. Это отдельный механизм,
не трехпопыточный retry consumer.

Доставка **at-least-once**, не exactly-once. Сбой между publisher confirm
и commit outbox может создать дубликат. Блокировка строки и сохраненный
статус не позволяют повторно обработать уже завершенный платеж.
Сохраненный `webhook_delivered_at` предотвращает повтор уже подтвержденной
доставки. Но сбой после приема webhook клиентом и до commit в нашей БД
может привести к повторному HTTP-запросу: клиент должен дедуплицировать
по стабильному `event_id` / заголовку `Idempotency-Key`.

`failed` означает отказ платежного шлюза, а не ошибку webhook. Такой
результат тоже отправляется клиенту. Ошибка webhook не меняет результат
платежа и не вызывает шлюз повторно. При техническом сбое до сохранения
результата платеж может остаться `pending`, а его событие — оказаться в DLQ.

Шлюз является эмуляцией. Для реального провайдера нужна отдельная
идемпотентность вызова шлюза: одного локального статуса недостаточно,
если процесс упадет после списания, но до commit.

### Webhook

```json
{
  "event_id": "67df7bf0-c9da-470b-9f60-26a0d5ee4c92",
  "type": "payment.processed",
  "payment_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "description": "Заказ 42",
  "metadata": {"order_id": 42},
  "created_at": "2026-10-15T09:00:00+00:00",
  "processed_at": "2026-10-15T09:00:03+00:00"
}
```

Получатель должен быть доступен **из контейнера consumer**. `localhost`
в webhook URL указывает на сам consumer, не на вашу машину.
Webhook URL задает авторизованный клиент; в production следует ограничить
исходящий трафик разрешенными адресами партнеров и закрыть доступ к
внутренним сетям. Общий API ключ не является многопользовательской авторизацией.
Подпись webhook не входит в задание и здесь не реализована.

### Работа с DLQ

В RabbitMQ Management можно открыть `payments.dead`, посмотреть тело,
`x-attempt` и `x-last-error`. Исправьте причину ошибки до ручного повтора.
Для повтора опубликуйте исходное JSON-тело в default exchange с routing key
`payments.new`, сохранив event/payment ID, и задайте `x-attempt: 1` как
integer. Это начинает новый трехпопыточный цикл. Автоматического replay
нет; менять статус платежа для повтора webhook не нужно.

## Локальная разработка

Нужны Python 3.12+ и [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
# Обновите ключи и обе строки подключения.
uv sync --frozen
docker compose up -d postgres rabbitmq
set -a
. ./.env
set +a
uv run alembic upgrade head
uv run uvicorn app.api:create_app --factory --reload
```

В другом терминале с теми же переменными окружения:

```bash
uv run python -m app.worker
```

API и consumer используют `.env`; Alembic читает `DATABASE_URL` из
окружения. Для генерации миграций:

```bash
uv run alembic revision --autogenerate -m "Describe change"
uv run alembic upgrade head
uv run alembic check
```

Настройки: `OUTBOX_POLL_SECONDS` (по умолчанию 1),
`WEBHOOK_TIMEOUT_SECONDS` (по умолчанию 10).

## Проверки

```bash
uv run ruff check app migrations tests
uv run ruff format --check app migrations tests
uv run pytest -m 'not integration'
```

Интеграционные тесты работают с настоящими PostgreSQL и RabbitMQ.
**Они пересоздают таблицы и очищают очереди. Используйте только отдельную
тестовую БД и отдельный vhost, никогда рабочие данные.**

Для поднятого Compose:

```bash
docker compose exec postgres psql -U payments -d payments \
  -c 'CREATE DATABASE payments_test;'
docker compose exec rabbitmq rabbitmqctl add_vhost payments_test
docker compose exec rabbitmq rabbitmqctl set_permissions \
  -p payments_test payments '.*' '.*' '.*'

export TEST_DATABASE_URL='postgresql+asyncpg://payments:YOUR_DB_PASSWORD@localhost:5432/payments_test'
export TEST_RABBITMQ_URL='amqp://payments:YOUR_BROKER_PASSWORD@localhost:5672/payments_test'
uv run pytest
```

Замените логины, пароли и порты на свои. Если переменные не заданы,
соответствующие интеграционные тесты пропускаются. Не запускайте их
параллельно на одной тестовой БД/vhost.

Тесты проверяют API и decimal-валидацию, конкурентную идемпотентность,
атомарность payment/outbox, оба результата шлюза, дубликаты, сбой commit
после подтвержденной публикации, восстановление доставки webhook,
фактические задержки retry и попадание в DLQ, включая некорректный JSON.
