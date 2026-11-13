from uuid import UUID, uuid4

from app.models import Payment
from app.repositories.uow import UnitOfWork
from app.schemas import PaymentAccepted, PaymentDetails, PaymentEvent, PaymentRequest


class IdempotencyConflictError(Exception):
    pass


class PaymentNotFoundError(Exception):
    pass


def _matches_request(payment: Payment, body: PaymentRequest) -> bool:
    return (
        payment.amount == body.amount
        and payment.currency == body.currency.value
        and payment.description == body.description
        and payment.metadata_json == body.metadata
        and payment.webhook_url == str(body.webhook_url)
    )


def _accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


def _details(payment: Payment) -> PaymentDetails:
    return PaymentDetails(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_json,
        webhook_url=payment.webhook_url,
        idempotency_key=payment.idempotency_key,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
    )


class PaymentsService:
    def __init__(self, uow: UnitOfWork) -> None:
        self.uow = uow

    async def create(self, body: PaymentRequest, idempotency_key: str) -> PaymentAccepted:
        async with self.uow.begin() as repos:
            payment, created = await repos.payments.create_or_get(
                idempotency_key,
                amount=body.amount,
                currency=body.currency.value,
                description=body.description,
                metadata=body.metadata,
                webhook_url=str(body.webhook_url),
            )
            if not created:
                if not _matches_request(payment, body):
                    raise IdempotencyConflictError
            else:
                event = PaymentEvent(event_id=uuid4(), payment_id=payment.id)
                repos.outbox.add_event(
                    event.event_id, event.payment_id, event.model_dump(mode="json")
                )
        return _accepted(payment)

    async def get(self, payment_id: UUID) -> PaymentDetails:
        async with self.uow.begin() as repos:
            payment = await repos.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError
        return _details(payment)
