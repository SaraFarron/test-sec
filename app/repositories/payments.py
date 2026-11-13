from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_or_get(
        self,
        idempotency_key: str,
        *,
        amount: Decimal,
        currency: str,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
    ) -> tuple[Payment, bool]:
        inserted_id = await self.session.scalar(
            insert(Payment)
            .values(
                id=uuid4(),
                idempotency_key=idempotency_key,
                amount=amount,
                currency=currency,
                description=description,
                metadata_json=metadata,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.id)
        )
        payment = await self.session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment is None:
            raise RuntimeError("Payment missing after insert")
        return payment, inserted_id is not None

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        return await self.session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )

    def mark_processed(
        self, payment: Payment, status: PaymentStatus, processed_at: datetime
    ) -> None:
        payment.status = status
        payment.processed_at = processed_at

    def mark_webhook_delivered(self, payment: Payment, delivered_at: datetime) -> None:
        payment.webhook_delivered_at = delivered_at
