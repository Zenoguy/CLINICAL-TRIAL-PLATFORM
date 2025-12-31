"""Provenance metadata model."""
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class Provenance:
    """Data provenance metadata."""

    source_system: Literal['medidata_rave', 'veeva_vault', 'central_labs', 'site_portal']
    source_file: str
    source_row: int
    source_record_id: str

    ingestion_timestamp: datetime
    ingestion_method: Literal['webhook', 'hl7', 'batch']
    ingestion_latency_seconds: float

    priority_tier: Literal['P0', 'P1', 'P2', 'P3']
    processor_version: str
    data_steward: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSONB storage."""
        return {
            'source_system': self.source_system,
            'source_file': self.source_file,
            'source_row': self.source_row,
            'source_record_id': self.source_record_id,
            'ingestion_timestamp': self.ingestion_timestamp.isoformat(),
            'ingestion_method': self.ingestion_method,
            'ingestion_latency_seconds': self.ingestion_latency_seconds,
            'priority_tier': self.priority_tier,
            'processor_version': self.processor_version,
            'data_steward': self.data_steward
        }
