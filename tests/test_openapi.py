import httpx
import pytest

from app.api import create_app
from app.config import Settings


@pytest.fixture
def app():
    settings = Settings(
        _env_file=None,
        api_key="test-api-key-long-enough",
        database_url="postgresql+asyncpg://user:pass@127.0.0.1:5432/payments",
        rabbitmq_url="amqp://user:pass@127.0.0.1:5672/",
    )
    return create_app(settings)


def test_openapi_documents_auth_and_responses(app):
    schema = app.openapi()
    create = schema["paths"]["/api/v1/payments"]["post"]
    get = schema["paths"]["/api/v1/payments/{payment_id}"]["get"]

    assert create["security"] == [{"APIKeyHeader": []}]
    assert get["security"] == [{"APIKeyHeader": []}]
    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }
    assert "Idempotency-Key" in {parameter["name"] for parameter in create["parameters"]}
    assert {"202", "401", "409", "422"} <= create["responses"].keys()
    assert {"200", "401", "404", "422"} <= get["responses"].keys()


@pytest.mark.asyncio
async def test_http_authentication_and_validation_without_database(app):
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            unauthorized = await client.get(
                "/api/v1/payments/not-a-uuid", headers={"X-API-Key": "invalid"}
            )
            invalid_uuid = await client.get(
                "/api/v1/payments/not-a-uuid",
                headers={"X-API-Key": "test-api-key-long-enough"},
            )
            missing_idempotency_key = await client.post(
                "/api/v1/payments",
                headers={"X-API-Key": "test-api-key-long-enough"},
                json={
                    "amount": "10.00",
                    "currency": "USD",
                    "description": "test",
                    "webhook_url": "https://merchant.example/webhook",
                },
            )

    assert unauthorized.status_code == 401
    assert invalid_uuid.status_code == 422
    assert missing_idempotency_key.status_code == 422
