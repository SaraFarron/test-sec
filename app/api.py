import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from app.config import Settings, get_settings
from app.db import create_database
from app.repositories.uow import UnitOfWork
from app.schemas import PaymentAccepted, PaymentDetails, PaymentRequest
from app.services.payments import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentsService,
)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    engine, sessions = create_database(settings)
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.sessions = sessions
        try:
            yield
        finally:
            await engine.dispose()

    async def authenticate(
        api_key: Annotated[str | None, Depends(api_key_header)],
    ) -> None:
        if api_key is None or not secrets.compare_digest(
            api_key.encode(), settings.api_key.get_secret_value().encode()
        ):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    def payments_service(request: Request) -> PaymentsService:
        return PaymentsService(UnitOfWork(request.app.state.sessions))

    app = FastAPI(
        title="Payment Processor",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    service_dependency = Annotated[PaymentsService, Depends(payments_service)]

    @app.post(
        "/api/v1/payments",
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
        service: service_dependency,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
        ],
    ) -> PaymentAccepted:
        try:
            payment = await service.create(body, idempotency_key)
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=409,
                detail="Idempotency key already used with a different payload",
            ) from exc
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get(
        "/api/v1/payments/{payment_id}",
        response_model=PaymentDetails,
        responses={
            status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"},
            status.HTTP_404_NOT_FOUND: {"description": "Payment not found"},
        },
    )
    async def get_payment(payment_id: UUID, service: service_dependency) -> PaymentDetails:
        try:
            payment = await service.get(payment_id)
        except PaymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail="Payment not found") from exc
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

    return app
