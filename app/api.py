import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.db import create_database
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentDetails, PaymentEvent, PaymentRequest


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    engine, sessions = create_database(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.sessions = sessions
        try:
            yield
        finally:
            await engine.dispose()

    async def authenticate(
        api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
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
        "/api/v1/payments", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted
    )
    async def create_payment(
        body: PaymentRequest,
        db: db_dependency,
        idempotency_key: Annotated[
            str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^\S+$")
        ],
    ) -> PaymentAccepted:
        values = body.database_values()
        async with db.begin():
            payment_id = await db.scalar(
                insert(Payment)
                .values(id=uuid4(), idempotency_key=idempotency_key, **values)
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment.id)
            )
            payment = await db.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if payment is None:
                raise RuntimeError("Payment missing after insert")
            if payment_id is None:
                if any(getattr(payment, key) != value for key, value in values.items()):
                    raise HTTPException(
                        status_code=409,
                        detail="Idempotency key already used with a different payload",
                    )
            else:
                event = PaymentEvent(event_id=uuid4(), payment_id=payment.id)
                db.add(
                    Outbox(
                        id=event.event_id,
                        payment_id=payment.id,
                        payload=event.model_dump(mode="json"),
                    )
                )
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(payment_id: UUID, db: db_dependency) -> PaymentDetails:
        payment = await db.get(Payment, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Payment not found")
        return PaymentDetails(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_json,
            webhook_url=payment.webhook_url,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )

    return app
