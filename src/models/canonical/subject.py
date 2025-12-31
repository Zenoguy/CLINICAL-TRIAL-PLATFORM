"""Canonical Subject entity model."""
from dataclasses import dataclass
from datetime import date
from typing import Optional
from src.models.metadata.provenance import Provenance
from src.models.metadata.lineage import Lineage
from src.models.metadata.quality import QualityScore


@dataclass
class Subject:
    """Canonical subject representation."""

    # Primary Key
    subject_id: str

    # Foreign Keys
    site_id: str

    # Demographics
    age_years: Optional[int] = None
    sex: Optional[str] = None
    race: Optional[str] = None
    ethnicity: Optional[str] = None

    # Enrollment
    cohort: Optional[str] = None
    enrollment_date: Optional[date] = None
    randomization_date: Optional[date] = None
    discontinuation_date: Optional[date] = None
    discontinuation_reason: Optional[str] = None

    # Metadata (required)
    provenance: Provenance
    lineage: Lineage
    quality: QualityScore

    # System Fields
    record_hash: Optional[str] = None

    def __post_init__(self):
        """Validate after initialization."""
        if self.discontinuation_date and self.enrollment_date:
            if self.discontinuation_date < self.enrollment_date:
                raise ValueError("Discontinuation date cannot precede enrollment date")
