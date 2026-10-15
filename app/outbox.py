import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging import PAYMENTS_QUEUE, publish_confirmed
from app.models import Outbox

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
            events = await db.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None))
                .order_by(Outbox.created_at, Outbox.id)
                .limit(50)
                .with_for_update(skip_locked=True)
            )
            for event in events:
                event.publish_attempts += 1
                try:
                    await publish_confirmed(
                        self.broker,
                        event.payload,
                        PAYMENTS_QUEUE,
                        message_id=str(event.id),
                        headers={"x-attempt": 1},
                    )
                except Exception as exc:
                    logger.exception("Outbox publication failed: event_id=%s", event.id)
                    event.last_error = type(exc).__name__
                else:
                    event.published_at = datetime.now(UTC)
                    event.last_error = None
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
