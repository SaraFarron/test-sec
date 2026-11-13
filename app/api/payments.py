from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, status

from app.api.deps import PaymentsServiceDep
from app.schemas import PaymentAccepted, PaymentDetails, PaymentRequest
from app.services.payments import IdempotencyConflictError, PaymentNotFoundError

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"},
        status.HTTP_409_CONFLICT: {
            "description": "Idempotency key already used with a different payload"
        },
    },
)
async def create_payment(
    body: PaymentRequest,
    service: PaymentsServiceDep,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
    ],
) -> PaymentAccepted:
    try:
        return await service.create(body, idempotency_key)
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key already used with a different payload",
        ) from exc


@router.get(
    "/{payment_id}",
    response_model=PaymentDetails,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"},
        status.HTTP_404_NOT_FOUND: {"description": "Payment not found"},
    },
)
async def get_payment(payment_id: UUID, service: PaymentsServiceDep) -> PaymentDetails:
    try:
        return await service.get(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        ) from exc
