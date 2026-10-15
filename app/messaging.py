from typing import Any

from faststream.rabbit import Channel, QueueType, RabbitBroker, RabbitQueue
from pamqp.commands import Basic

from app.config import Settings

MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (2, 4)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-delivery-limit": MAX_ATTEMPTS,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "payments.dead",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
DEAD_QUEUE = RabbitQueue("payments.dead", queue_type=QueueType.QUORUM, durable=True)
RETRY_QUEUES = tuple(
    RabbitQueue(
        f"payments.retry.{delay}s",
        queue_type=QueueType.QUORUM,
        durable=True,
        arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": PAYMENTS_QUEUE.name,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )
    for delay in RETRY_DELAYS_SECONDS
)


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30,
    )


async def declare_queues(broker: RabbitBroker) -> None:
    for queue in (PAYMENTS_QUEUE, DEAD_QUEUE, *RETRY_QUEUES):
        await broker.declare_queue(queue)


async def publish_confirmed(
    broker: RabbitBroker,
    payload: dict[str, Any] | bytes,
    queue: RabbitQueue,
    *,
    message_id: str,
    headers: dict[str, Any],
) -> None:
    confirmation = await broker.publish(
        payload,
        queue=queue,
        persist=True,
        mandatory=True,
        timeout=10,
        message_id=message_id,
        headers=headers,
        content_type="application/json",
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not acknowledge publication")
