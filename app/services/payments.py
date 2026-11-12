from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentEvent, PaymentRequest


class IdempotencyConflictError(Exception):
    pass


class PaymentNotFoundError(Exception):
    pass


class PaymentsService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def create(self, body: PaymentRequest, idempotency_key: str) -> Payment:
        values = body.database_values()
        async with self.session.begin():
            payment, created = await self.payments.create_or_get(idempotency_key, values)
            if not created:
                if any(getattr(payment, key) != value for key, value in values.items()):
                    raise IdempotencyConflictError
            else:
                self.outbox.add_event(PaymentEvent(event_id=uuid4(), payment_id=payment.id))
        return payment

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError
        return payment
