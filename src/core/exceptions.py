"""Custom exception hierarchy."""


class ClinicalTrialPlatformError(Exception):
    """Base exception for all platform errors."""
    pass


class IngestionError(ClinicalTrialPlatformError):
    """Errors during data ingestion."""
    pass


class ValidationError(ClinicalTrialPlatformError):
    """Data validation failures."""

    def __init__(self, message: str, field: str = None, rule: str = None):
        super().__init__(message)
        self.field = field
        self.rule = rule


class HarmonizationError(ClinicalTrialPlatformError):
    """Errors during data transformation."""
    pass


class StorageError(ClinicalTrialPlatformError):
    """Database or storage failures."""
    pass


class QueueSaturationError(ClinicalTrialPlatformError):
    """Queue overflow condition."""
    pass


class IntegrityViolationError(ClinicalTrialPlatformError):
    """Data integrity check failed."""

    def __init__(self, record_id: str, expected_hash: str, actual_hash: str):
        message = f"Integrity violation for {record_id}"
        super().__init__(message)
        self.record_id = record_id
        self.expected_hash = expected_hash
        self.actual_hash = actual_hash
