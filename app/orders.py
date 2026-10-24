"""Persist orders before acknowledging their source reservation events."""

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from uuid import UUID

import psycopg


@dataclass(frozen=True)
class ReservationEvent:
    reservation_id: UUID
    product_id: str
    user_id: str

    @classmethod
    def from_fields(cls, fields: Mapping[str, str]) -> "ReservationEvent":
        if fields.get("event_type") != "reservation.created" or fields.get("schema_version") != "1":
            raise ValueError("Unsupported reservation event")
        try:
            event = cls(UUID(fields["reservation_id"]), fields["product_id"], fields["user_id"])
        except (KeyError, ValueError) as error:
            raise ValueError("Invalid reservation event") from error
        if not event.product_id or not 1 <= len(event.user_id) <= 100:
            raise ValueError("Invalid product or user ID")
        return event


class PostgresOrderStore:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def initialize(self) -> None:
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            # Serialize first-time table creation if two workers start together.
            connection.execute("SELECT pg_advisory_xact_lock(724291)")
            connection.execute(Path(__file__).with_name("orders.sql").read_text())

    def save(self, event: ReservationEvent) -> None:
        # The context commits on successful exit; any error rolls back. Returning
        # from this method therefore means the transaction has committed.
        with psycopg.connect(self.database_url, connect_timeout=5) as connection:
            connection.execute(
                """INSERT INTO orders (reservation_id, product_id, user_id)
                   VALUES (%s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                (event.reservation_id, event.product_id, event.user_id),
            )
            existing = connection.execute(
                "SELECT product_id, user_id FROM orders WHERE reservation_id = %s",
                (event.reservation_id,),
            ).fetchone()
            if existing != (event.product_id, event.user_id):
                raise ValueError("Reservation conflicts with an existing order")
