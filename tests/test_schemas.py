from decimal import Decimal

from app.schemas import PaymentRequest


def test_decimal_and_url_normalization(payment_body):
    body = PaymentRequest.model_validate({**payment_body, "amount": "0.10"})
    assert body.amount == Decimal("0.10")
    assert body.database_values()["webhook_url"] == "https://merchant.example/webhook"
