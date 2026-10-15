import asyncio
import json
import os
import time
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.messaging import (
    DEAD_QUEUE,
    PAYMENTS_QUEUE,
    RETRY_DELAYS_SECONDS,
    RETRY_QUEUES,
    create_broker,
    declare_queues,
    publish_confirmed,
)
from app.outbox import OutboxRelay
from app.processor import PaymentProcessor
from app.schemas import PaymentEvent
from app.worker import PaymentConsumer, register_consumer
from tests.test_processing import create_event

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def broker(settings):
    if not os.environ.get("TEST_RABBITMQ_URL"):
        pytest.skip("Set TEST_RABBITMQ_URL to a dedicated RabbitMQ test instance")
    broker = create_broker(settings)
    async with broker:
        await declare_queues(broker)
        for queue in (PAYMENTS_QUEUE, DEAD_QUEUE, *RETRY_QUEUES):
            declared = await broker.declare_queue(queue)
            await declared.purge()
        yield broker


async def wait_for_message(queue):
    async with asyncio.timeout(15):
        while True:
            incoming = await queue.get(fail=False)
            if incoming is not None:
                return incoming
            await asyncio.sleep(0.05)


async def test_real_outbox_delivery(client, sessions, payment_body, broker):
    event = await create_event(client, sessions, payment_body)
    assert await OutboxRelay(sessions, broker, 1).publish_batch() == 1
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    incoming = await wait_for_message(queue)
    assert json.loads(incoming.body) == event.model_dump(mode="json")
    assert incoming.message_id == str(event.event_id)
    assert incoming.delivery_mode.value == 2
    await incoming.ack()


async def test_publish_before_commit_failure_is_safe(client, sessions, payment_body, broker):
    event = await create_event(client, sessions, payment_body)
    async with sessions.begin() as db:
        await db.execute(
            text(
                "ALTER TABLE outbox ADD CONSTRAINT fail_publish_commit CHECK (published_at IS NULL)"
            )
        )
    relay = OutboxRelay(sessions, broker, 1)
    with pytest.raises(IntegrityError, match="fail_publish_commit"):
        await relay.publish_batch()
    async with sessions.begin() as db:
        await db.execute(text("ALTER TABLE outbox DROP CONSTRAINT fail_publish_commit"))
    assert await relay.publish_batch() == 1
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    first = await wait_for_message(queue)
    second = await wait_for_message(queue)
    assert first.message_id == second.message_id == str(event.event_id)
    assert first.body == second.body
    gateway = AsyncMock(return_value=True)
    webhook = Mock(return_value=httpx.Response(204))
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, http, gateway)
        for incoming in (first, second):
            await processor.process(PaymentEvent.model_validate_json(incoming.body))
            await incoming.ack()
    gateway.assert_awaited_once()
    assert webhook.call_count == 1


async def test_real_retry_recovers(client, sessions, payment_body, broker):
    event = await create_event(client, sessions, payment_body)
    delivered = asyncio.Event()
    calls = 0

    def webhook(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503)
        delivered.set()
        return httpx.Response(204)

    gateway = AsyncMock(return_value=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        register_consumer(
            broker, PaymentConsumer(broker, PaymentProcessor(sessions, http, gateway))
        )
        await broker.start()
        await OutboxRelay(sessions, broker, 1).publish_batch()
        async with asyncio.timeout(10):
            await delivered.wait()
            while True:
                payment = (await client.get(f"/api/v1/payments/{event.payment_id}")).json()
                if payment["webhook_delivered_at"] is not None:
                    break
                await asyncio.sleep(0.05)
        assert calls == 2
        gateway.assert_awaited_once()
        dead = await broker.declare_queue(DEAD_QUEUE)
        assert await dead.get(fail=False) is None


async def test_real_retries_and_dead_letter(client, sessions, payment_body, broker):
    event = await create_event(client, sessions, payment_body)
    calls = []
    payloads = []

    def webhook(request):
        calls.append(time.monotonic())
        payloads.append(json.loads(request.content))
        return httpx.Response(503)

    gateway = AsyncMock(return_value=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        consumer = PaymentConsumer(broker, PaymentProcessor(sessions, http, gateway))
        register_consumer(broker, consumer)
        await broker.start()
        assert await OutboxRelay(sessions, broker, 1).publish_batch() == 1
        dead = await broker.declare_queue(DEAD_QUEUE)
        incoming = await wait_for_message(dead)
        assert incoming.headers["x-attempt"] == 3
        assert incoming.headers["x-last-error"] == "HTTPStatusError"
        assert json.loads(incoming.body) == event.model_dump(mode="json")
        await incoming.ack()
    gateway.assert_awaited_once()
    assert len(calls) == 3
    assert payloads[0] == payloads[1] == payloads[2]
    for actual, expected in zip(
        (calls[1] - calls[0], calls[2] - calls[1]), RETRY_DELAYS_SECONDS, strict=True
    ):
        assert actual >= expected - 0.1
    payment = (await client.get(f"/api/v1/payments/{event.payment_id}")).json()
    assert payment["status"] == "succeeded"
    assert payment["webhook_delivered_at"] is None


async def test_malformed_message_reaches_dlq(broker):
    processor = AsyncMock()
    register_consumer(broker, PaymentConsumer(broker, processor))
    await broker.start()
    await publish_confirmed(
        broker, b"not json", PAYMENTS_QUEUE, message_id="malformed", headers={"x-attempt": 1}
    )
    incoming = await wait_for_message(await broker.declare_queue(DEAD_QUEUE))
    assert incoming.body == b"not json"
    assert incoming.headers["x-attempt"] == 3
    processor.process.assert_not_awaited()
    await incoming.ack()
