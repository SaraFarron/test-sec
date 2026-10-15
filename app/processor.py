import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Outbox, Payment
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


async def simulate_gateway() -> bool:
    await asyncio.sleep(random.uniform(2, 5))
    return random.random() < 0.9


class PaymentProcessor:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        http: httpx.AsyncClient,
        gateway: Callable[[], Awaitable[bool]] = simulate_gateway,
    ) -> None:
        self.sessions = sessions
        self.http = http
        self.gateway = gateway

    async def process(self, event: PaymentEvent) -> None:
        async with self.sessions.begin() as db:
            outbox = await db.get(Outbox, event.event_id)
            if outbox is None or outbox.payment_id != event.payment_id:
                raise ValueError("Event does not match a persisted outbox entry")
            payment = await db.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if payment is None:
                raise ValueError("Payment does not exist")
            if payment.status == "pending":
                payment.status = "succeeded" if await self.gateway() else "failed"
                payment.processed_at = datetime.now(UTC)
                logger.info(
                    "Payment processed: payment_id=%s status=%s", payment.id, payment.status
                )

        # Commit the outcome before HTTP; redelivery must not call the gateway again.
        async with self.sessions.begin() as db:
            payment = await db.scalar(
                select(Payment).where(Payment.id == event.payment_id).with_for_update()
            )
            if payment is None:
                raise ValueError("Payment disappeared before webhook delivery")
            if payment.webhook_delivered_at is not None:
                return
            if payment.processed_at is None:
                raise RuntimeError("Cannot notify a payment without a persisted outcome")
            payload = {
                "event_id": str(event.event_id),
                "type": "payment.processed",
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "description": payment.description,
                "metadata": payment.metadata_json,
                "created_at": payment.created_at.isoformat(),
                "processed_at": payment.processed_at.isoformat(),
            }
            response = await self.http.post(
                payment.webhook_url,
                json=payload,
                headers={"Idempotency-Key": str(event.event_id)},
            )
            response.raise_for_status()
            payment.webhook_delivered_at = datetime.now(UTC)
            logger.info("Webhook delivered: payment_id=%s event_id=%s", payment.id, event.event_id)
