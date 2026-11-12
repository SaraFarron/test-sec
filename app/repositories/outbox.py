from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox
from app.schemas import PaymentEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    def add_event(self, event: PaymentEvent) -> None:
        self.session.add(
            Outbox(
                id=event.event_id,
                payment_id=event.payment_id,
                payload=event.model_dump(mode="json"),
            )
        )

    async def get(self, event_id: UUID) -> Outbox | None:
        return await self.session.get(Outbox, event_id)

    async def get_unpublished_for_update(self, limit: int) -> list[Outbox]:
        events = await self.session.scalars(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(events)

    def mark_published(self, event: Outbox, published_at: datetime) -> None:
        event.published_at = published_at
        event.last_error = None

    def record_publish_failure(self, event: Outbox, error: Exception) -> None:
        event.last_error = type(error).__name__

    def record_publish_attempt(self, event: Outbox) -> None:
        event.publish_attempts += 1

    def payload(self, event: Outbox) -> dict[str, Any]:
        return event.payload
