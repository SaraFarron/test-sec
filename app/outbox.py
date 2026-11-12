import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import PAYMENTS_QUEUE, publish_confirmed
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        poll_seconds: float,
    ) -> None:
        self.sessions = sessions
        self.broker = broker
        self.poll_seconds = poll_seconds

    async def publish_batch(self) -> int:
        published = 0
        async with self.sessions.begin() as db:
            outbox = OutboxRepository(db)
            events = await outbox.get_unpublished_for_update(limit=50)
            for event in events:
                outbox.record_publish_attempt(event)
                try:
                    await publish_confirmed(
                        self.broker,
                        outbox.payload(event),
                        PAYMENTS_QUEUE,
                        message_id=str(event.id),
                        headers={"x-attempt": 1},
                    )
                except Exception as exc:
                    logger.exception("Outbox publication failed: event_id=%s", event.id)
                    outbox.record_publish_failure(event, exc)
                else:
                    outbox.mark_published(event, datetime.now(UTC))
                    published += 1
        return published

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.publish_batch()
            except Exception:
                logger.exception("Outbox batch failed; unpublished events remain in PostgreSQL")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
