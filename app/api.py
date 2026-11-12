import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.db import create_database
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

    async def session(request: Request) -> AsyncIterator[AsyncSession]:
        async with request.app.state.sessions() as db:
            yield db

    app = FastAPI(
        title="Payment Processor",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    db_dependency = Annotated[AsyncSession, Depends(session)]

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
        db: db_dependency,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
        ],
    ) -> PaymentAccepted:
        try:
            payment = await PaymentsService(db).create(body, idempotency_key)
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
    async def get_payment(payment_id: UUID, db: db_dependency) -> PaymentDetails:
        try:
            payment = await PaymentsService(db).get(payment_id)
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
