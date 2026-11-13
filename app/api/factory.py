from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.deps import authenticate
from app.api.payments import router as payments_router
from app.config import Settings, get_settings
from app.db import create_database
from app.repositories.uow import UnitOfWork
from app.services.payments import PaymentsService


def create_app(settings: Settings | None = None) -> FastAPI:
    """Composition root: the only API module that wires services to the data layer."""
    settings = settings or get_settings()
    engine, sessions = create_database(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.payments_service = PaymentsService(UnitOfWork(sessions))
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Payment Processor",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.include_router(payments_router)
    return app
