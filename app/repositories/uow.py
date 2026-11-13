from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository


@dataclass(frozen=True)
class Repositories:
    payments: PaymentRepository
    outbox: OutboxRepository


class UnitOfWork:
    """Owns the database transaction so services never touch sessions."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[Repositories]:
        async with self._sessions.begin() as session:
            yield Repositories(PaymentRepository(session), OutboxRepository(session))
