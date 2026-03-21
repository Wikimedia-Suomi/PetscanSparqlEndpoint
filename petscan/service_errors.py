"""Shared exceptions for service internals."""


class PetscanServiceError(RuntimeError):
    pass


class GilLinkEnrichmentError(PetscanServiceError):
    pass
