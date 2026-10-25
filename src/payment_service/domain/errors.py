from uuid import UUID


class DomainError(Exception):
    pass


class IdempotencyKeyReusedError(DomainError):
    """Этот ключ идемпотентности уже использован для запроса с другим телом."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"Idempotency key {idempotency_key!r} was used with a different request")
        self.idempotency_key = idempotency_key


class PaymentNotFoundError(DomainError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id
