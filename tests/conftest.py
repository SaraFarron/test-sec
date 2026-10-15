import os
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api import create_app
from app.config import Settings
from app.models import Base

API_KEY = "test-api-key-long-enough"


@pytest.fixture
def settings() -> Settings:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated PostgreSQL test database")
    return Settings(
        _env_file=None,
        api_key=API_KEY,
        database_url=database_url,
        rabbitmq_url=os.environ.get("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost/"),
    )


@pytest_asyncio.fixture
async def sessions(settings: Settings) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(settings.database_url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(settings, sessions):
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        app.state.sessions = sessions
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": API_KEY},
        ) as client:
            yield client


@pytest.fixture
def payment_body():
    return {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhook",
    }
