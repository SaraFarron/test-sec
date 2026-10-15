import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.models import Outbox, Payment

pytestmark = pytest.mark.integration


async def test_create_and_get(client, sessions, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "order-42"}
    )
    assert response.status_code == 202
    accepted = response.json()
    assert set(accepted) == {"payment_id", "status", "created_at"}
    assert accepted["status"] == "pending"
    response = await client.get(f"/api/v1/payments/{accepted['payment_id']}")
    assert response.status_code == 200
    payment = response.json()
    assert payment["amount"] == "1250.50"
    assert payment["metadata"] == {"order_id": 42}
    assert payment["processed_at"] is None
    async with sessions() as db:
        event = await db.scalar(select(Outbox))
        assert event.payload["payment_id"] == accepted["payment_id"]
        assert event.payload["event_id"] == str(event.id)
        assert event.published_at is None


async def test_concurrent_idempotency(client, sessions, payment_body):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "same-key"}
            )
            for _ in range(10)
        ]
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Payment)) == 1
        assert await db.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_idempotency_conflict(client, payment_body):
    headers = {"Idempotency-Key": "same-key"}
    original = await client.post("/api/v1/payments", json=payment_body, headers=headers)
    conflict = await client.post(
        "/api/v1/payments", json={**payment_body, "amount": "100.00"}, headers=headers
    )
    assert conflict.status_code == 409
    response = await client.get(f"/api/v1/payments/{original.json()['payment_id']}")
    assert response.json()["amount"] == "1250.50"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "-1"),
        ("amount", "1.001"),
        ("amount", "10000000000000000"),
        ("amount", "NaN"),
        ("currency", "GBP"),
        ("webhook_url", "ftp://example.com"),
        ("metadata", []),
        ("description", "x" * 2001),
    ],
)
async def test_invalid_payload(client, payment_body, field, value):
    response = await client.post(
        "/api/v1/payments",
        json={**payment_body, field: value},
        headers={"Idempotency-Key": "test"},
    )
    assert response.status_code == 422


async def test_headers_and_missing_payment(client, payment_body):
    assert (await client.post("/api/v1/payments", json=payment_body)).status_code == 422
    for key in ("", "bad key"):
        response = await client.post(
            "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": key}
        )
        assert response.status_code == 422
    for path in ("/api/v1/payments", f"/api/v1/payments/{uuid4()}"):
        method = "POST" if path.endswith("payments") else "GET"
        for key in ("", "invalid"):
            response = await client.request(
                method, path, headers={"X-API-Key": key}, json=payment_body
            )
            assert response.status_code == 401
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_outbox_failure_rolls_back_payment(client, sessions, payment_body):
    from sqlalchemy import text

    async with sessions.begin() as db:
        await db.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT force_failure CHECK (false) NOT VALID")
        )
    with pytest.raises(IntegrityError, match="force_failure"):
        await client.post(
            "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "rollback"}
        )
    async with sessions() as db:
        assert await db.scalar(select(func.count()).select_from(Payment)) == 0
