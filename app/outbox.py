import asyncio
import logging

from app.services.outbox import OutboxService

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(self, service: OutboxService, poll_seconds: float) -> None:
        self.service = service
        self.poll_seconds = poll_seconds

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.service.publish_pending()
            except Exception:
                logger.exception("Outbox batch failed; unpublished events remain in PostgreSQL")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
