"""Shared exceptions for service internals."""


class PetscanServiceError(RuntimeError):
    def __init__(self, message: str, public_message: str | None = None) -> None:
        super().__init__(message)
        self.public_message = public_message


class GilLinkEnrichmentError(PetscanServiceError):
    pass
