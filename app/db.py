from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings


def create_database(settings: Settings):
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    return engine, async_sessionmaker(engine, expire_on_commit=False)
