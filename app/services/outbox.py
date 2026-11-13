import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from app.repositories.uow import UnitOfWork

logger = logging.getLogger(__name__)

Publisher = Callable[[dict[str, Any], str], Awaitable[None]]


class OutboxService:
    def __init__(self, uow: UnitOfWork, publish: Publisher, batch_size: int = 50) -> None:
        self.uow = uow
        self.publish = publish
        self.batch_size = batch_size

    async def publish_pending(self) -> int:
        published = 0
        async with self.uow.begin() as repos:
            events = await repos.outbox.get_unpublished_for_update(self.batch_size)
            for event in events:
                repos.outbox.record_publish_attempt(event)
                try:
                    await self.publish(event.payload, str(event.id))
                except Exception as exc:
                    logger.exception("Outbox publication failed: event_id=%s", event.id)
                    repos.outbox.record_publish_failure(event, exc)
                else:
                    repos.outbox.mark_published(event, datetime.now(UTC))
                    published += 1
        return published
