from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
    currency: Currency
    description: str = Field(max_length=2000)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    def database_values(self) -> dict[str, Any]:
        return {
            "amount": self.amount,
            "currency": self.currency.value,
            "description": self.description,
            "metadata_json": self.metadata,
            "webhook_url": str(self.webhook_url),
        }


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
