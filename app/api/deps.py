import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader

from app.repositories.uow import UnitOfWork
from app.services.payments import PaymentsService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(
    request: Request, api_key: Annotated[str | None, Depends(api_key_header)]
) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def get_payments_service(request: Request) -> PaymentsService:
    return PaymentsService(UnitOfWork(request.app.state.sessions))


PaymentsServiceDep = Annotated[PaymentsService, Depends(get_payments_service)]
