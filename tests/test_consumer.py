from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream.rabbit.message import RabbitMessage
from pamqp.commands import Basic

from app.messaging import DEAD_QUEUE, RETRY_QUEUES, publish_confirmed
from app.schemas import PaymentEvent
from app.worker import PaymentConsumer


def message(attempt=1, body=None):
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    raw = AsyncMock()
    raw.locked = False
    return RabbitMessage(
        raw_message=raw,
        body=body if body is not None else event.model_dump_json().encode(),
        headers={"x-attempt": attempt},
        message_id=str(event.event_id),
    )


async def test_success_acknowledges_without_retry():
    broker = AsyncMock()
    processor = AsyncMock()
    incoming = message()
    await PaymentConsumer(broker, processor).handle(incoming)
    processor.process.assert_awaited_once()
    incoming.raw_message.ack.assert_awaited_once()
    broker.publish.assert_not_awaited()


@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_retry_then_dead_letter(attempt):
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    processor = AsyncMock()
    processor.process.side_effect = RuntimeError("webhook unavailable")
    incoming = message(attempt)
    await PaymentConsumer(broker, processor).handle(incoming)
    call = broker.publish.await_args
    expected = DEAD_QUEUE if attempt == 3 else RETRY_QUEUES[attempt - 1]
    assert call.kwargs["queue"] == expected
    assert call.kwargs["headers"]["x-attempt"] == min(attempt + 1, 3)
    assert call.args[0] == incoming.body
    assert call.kwargs["message_id"] == incoming.message_id
    assert call.kwargs["persist"] is True
    assert call.kwargs["mandatory"] is True
    incoming.raw_message.ack.assert_awaited_once()


async def test_failed_transfer_requeues_original(monkeypatch):
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("broker unavailable")
    processor = AsyncMock()
    processor.process.side_effect = RuntimeError("webhook unavailable")
    incoming = message()
    monkeypatch.setattr("app.worker.asyncio.sleep", AsyncMock())
    await PaymentConsumer(broker, processor).handle(incoming)
    incoming.raw_message.ack.assert_not_awaited()
    incoming.raw_message.nack.assert_awaited_once_with(multiple=False, requeue=True)


async def test_malformed_body_uses_retry_path():
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    processor = AsyncMock()
    incoming = message(body=b"not json")
    await PaymentConsumer(broker, processor).handle(incoming)
    processor.process.assert_not_awaited()
    assert broker.publish.await_args.kwargs["queue"] == RETRY_QUEUES[0]
    incoming.raw_message.ack.assert_awaited_once()


@pytest.mark.parametrize("attempt", ["1", 0, 4, True])
async def test_invalid_attempt_is_dead_lettered(attempt):
    broker = AsyncMock()
    broker.publish.return_value = Basic.Ack()
    incoming = message(attempt)
    processor = AsyncMock()
    await PaymentConsumer(broker, processor).handle(incoming)
    processor.process.assert_not_awaited()
    assert broker.publish.await_args.kwargs["queue"] == DEAD_QUEUE


@pytest.mark.parametrize("confirmation", [None, Basic.Nack()])
async def test_publish_requires_positive_confirmation(confirmation):
    broker = AsyncMock()
    broker.publish.return_value = confirmation
    with pytest.raises(RuntimeError, match="did not acknowledge"):
        await publish_confirmed(broker, {}, DEAD_QUEUE, message_id="event", headers={})
