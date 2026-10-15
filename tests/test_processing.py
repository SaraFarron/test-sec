import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from pamqp.commands import Basic
from sqlalchemy import select

from app.models import Outbox, Payment
from app.outbox import OutboxRelay
from app.processor import PaymentProcessor, simulate_gateway
from app.schemas import PaymentEvent


async def create_event(client, sessions, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "processing"}
    )
    assert response.status_code == 202
    async with sessions() as db:
        outbox = await db.scalar(select(Outbox))
        return PaymentEvent.model_validate(outbox.payload)


@pytest.mark.integration
@pytest.mark.parametrize("succeeds", [True, False])
async def test_outcome_and_duplicate_delivery(client, sessions, payment_body, succeeds):
    event = await create_event(client, sessions, payment_body)
    received = []

    def webhook(request):
        received.append(request)
        return httpx.Response(204)

    gateway = AsyncMock(return_value=succeeds)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, http, gateway)
        await asyncio.gather(processor.process(event), processor.process(event))
        await processor.process(event)
    gateway.assert_awaited_once()
    assert len(received) == 1
    assert received[0].headers["Idempotency-Key"] == str(event.event_id)
    response = await client.get(f"/api/v1/payments/{event.payment_id}")
    assert response.json()["status"] == ("succeeded" if succeeds else "failed")
    assert response.json()["processed_at"] is not None
    assert response.json()["webhook_delivered_at"] is not None


@pytest.mark.integration
async def test_webhook_retry_preserves_outcome(client, sessions, payment_body):
    event = await create_event(client, sessions, payment_body)
    received = []

    def webhook(request):
        received.append(request.content)
        return httpx.Response(503 if len(received) == 1 else 200)

    gateway = AsyncMock(return_value=True)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as http:
        processor = PaymentProcessor(sessions, http, gateway)
        with pytest.raises(httpx.HTTPStatusError):
            await processor.process(event)
        async with sessions() as db:
            payment = await db.get(Payment, event.payment_id)
            assert payment.status == "succeeded"
            assert payment.processed_at is not None
            assert payment.webhook_delivered_at is None
        await processor.process(event)
    gateway.assert_awaited_once()
    assert received[0] == received[1]


@pytest.mark.integration
async def test_outbox_failure_remains_publishable(client, sessions, payment_body):
    await create_event(client, sessions, payment_body)
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("broker offline")
    relay = OutboxRelay(sessions, broker, 1)
    assert await relay.publish_batch() == 0
    async with sessions() as db:
        outbox = await db.scalar(select(Outbox))
        assert outbox.published_at is None
        assert outbox.publish_attempts == 1
        assert outbox.last_error == "ConnectionError"
    broker.publish.side_effect = None
    broker.publish.return_value = Basic.Ack()
    assert await relay.publish_batch() == 1
    assert await relay.publish_batch() == 0
    async with sessions() as db:
        outbox = await db.scalar(select(Outbox))
        assert outbox.published_at is not None
        assert outbox.publish_attempts == 2
        assert outbox.last_error is None


@pytest.mark.integration
async def test_concurrent_relays_do_not_double_publish(client, sessions, payment_body):
    await create_event(client, sessions, payment_body)
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    results = await asyncio.gather(
        OutboxRelay(sessions, broker, 1).publish_batch(),
        OutboxRelay(sessions, broker, 1).publish_batch(),
    )
    assert sum(results) == 1
    broker.publish.assert_awaited_once()


async def test_gateway_delay_and_success_threshold(monkeypatch):
    sleep = AsyncMock()
    uniform = Mock(return_value=3.5)
    monkeypatch.setattr("app.processor.asyncio.sleep", sleep)
    monkeypatch.setattr("app.processor.random.uniform", uniform)
    monkeypatch.setattr("app.processor.random.random", lambda: 0.899999)
    assert await simulate_gateway() is True
    uniform.assert_called_once_with(2, 5)
    sleep.assert_awaited_once_with(3.5)
    monkeypatch.setattr("app.processor.random.random", lambda: 0.9)
    assert await simulate_gateway() is False
