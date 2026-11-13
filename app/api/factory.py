from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from app.api.deps import authenticate
from app.api.payments import router as payments_router
from app.config import Settings, get_settings
from app.db import create_database


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
