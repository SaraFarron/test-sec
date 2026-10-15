import asyncio
import logging
import signal

import httpx
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage

from app.config import get_settings
from app.db import create_database
from app.messaging import (
    DEAD_QUEUE,
    MAX_ATTEMPTS,
    PAYMENTS_QUEUE,
    RETRY_QUEUES,
    create_broker,
    declare_queues,
    publish_confirmed,
)
from app.outbox import OutboxRelay
from app.processor import PaymentProcessor
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


async def decode_raw(message: RawRabbitMessage) -> bytes:
    return message.body


class PaymentConsumer:
    def __init__(self, broker: RabbitBroker, processor: PaymentProcessor) -> None:
        self.broker = broker
        self.processor = processor

    async def handle(self, message: RawRabbitMessage) -> None:
        attempt = message.headers.get("x-attempt", 1)
        valid_attempt = type(attempt) is int and 1 <= attempt <= MAX_ATTEMPTS
        if not valid_attempt:
            attempt = MAX_ATTEMPTS
        try:
            if not valid_attempt:
                raise ValueError("Invalid attempt header")
            event = PaymentEvent.model_validate_json(message.body)
            await self.processor.process(event)
        except Exception as exc:
            logger.exception(
                "Payment message failed: message_id=%s attempt=%s", message.message_id, attempt
            )
            destination = DEAD_QUEUE if attempt == MAX_ATTEMPTS else RETRY_QUEUES[attempt - 1]
            headers = {
                "x-attempt": attempt if destination == DEAD_QUEUE else attempt + 1,
                "x-last-error": type(exc).__name__,
                "x-original-queue": PAYMENTS_QUEUE.name,
            }
            try:
                await publish_confirmed(
                    self.broker,
                    message.body,
                    destination,
                    message_id=message.message_id,
                    headers=headers,
                )
            except Exception:
                logger.exception(
                    "Retry/DLQ transfer failed; requeueing: message_id=%s", message.message_id
                )
                await asyncio.sleep(1)
                await message.nack(requeue=True)
                return
            logger.warning(
                "Message transferred: message_id=%s destination=%s",
                message.message_id,
                destination.name,
            )
        await message.ack()


def register_consumer(broker: RabbitBroker, consumer: PaymentConsumer) -> None:
    @broker.subscriber(PAYMENTS_QUEUE, ack_policy=AckPolicy.MANUAL, decoder=decode_raw)
    async def handle_payment(message: RabbitMessage) -> None:
        await consumer.handle(message)


async def run() -> None:
    settings = get_settings()
    engine, sessions = create_database(settings)
    broker = create_broker(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with (
            httpx.AsyncClient(
                timeout=settings.webhook_timeout_seconds,
                follow_redirects=False,
                trust_env=False,
            ) as http,
            broker,
        ):
            consumer = PaymentConsumer(broker, PaymentProcessor(sessions, http))
            register_consumer(broker, consumer)
            await declare_queues(broker)
            await broker.start()
            relay = OutboxRelay(sessions, broker, settings.outbox_poll_seconds)
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(relay.run(stop))
                await stop.wait()
    finally:
        await engine.dispose()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())
