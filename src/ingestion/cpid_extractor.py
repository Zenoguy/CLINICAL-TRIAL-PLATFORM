import pandas as pd
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, Tuple, Optional


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _clean(text: str) -> str:
    """
    Normalize text into snake_case metric-safe tokens.
    """
    text = text.lower()
    text = re.sub(r"[^a-z0-9_ ]", "", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip("_")


def _extract_base_and_bucket(label: str) -> Tuple[str, Optional[int]]:
    """
    Handles Excel duplicate headers:
      'Page status'   -> ('Page status', None)
      'Page status.1' -> ('Page status', 1)
    """
    if isinstance(label, str) and "." in label:
        base, suffix = label.rsplit(".", 1)
        if suffix.isdigit():
            return base, int(suffix)
    return label, None


# ---------------------------------------------------------------------
# Identity / context columns (never emitted as metrics)
# ---------------------------------------------------------------------
IDENTITY_COLS = {
    ("Project Name", "Unnamed: 0_level_1"),
    ("Region", "Unnamed: 1_level_1"),
    ("Country", "Unnamed: 2_level_1"),
    ("Site ID", "Unnamed: 3_level_1"),
    ("Subject ID", "Unnamed: 4_level_1"),
    ("Latest Visit (SV) (Source: Rave EDC: BO4)", "Unnamed: 5_level_1"),
    ("Subject Status (Source: PRIMARY Form)", "Unnamed: 6_level_1"),
}


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def extract_cpid_metrics(filepath: str) -> pd.DataFrame:
    """
    Extract CPID EDC Metrics into canonical MetricSnapshot rows.

    Parameters
    ----------
    filepath : str
        Path to CPID_EDC_Metrics_*.xlsx file

    Returns
    -------
    pd.DataFrame
        Columns:
        - entity_type
        - entity_id
        - site_id
        - metric_name
        - metric_value
        - snapshot_time
        - source
    """

    # Load Excel with two-row header
    df = pd.read_excel(filepath, header=[0, 1])

    # -----------------------------------------------------------------
    # STEP 1: Pre-compute bucket indices per COLUMN (global, deterministic)
    # -----------------------------------------------------------------
    column_bucket_map: Dict[Tuple[str, str], list] = defaultdict(list)

    for group, label in df.columns:
        base_label, bucket = _extract_base_and_bucket(label)
        column_bucket_map[(group, base_label)].append((label, bucket))

    column_bucket_index: Dict[Tuple[str, str], int] = {}

    for (group, base_label), entries in column_bucket_map.items():
        # If multiple columns share the same base → bucketed metric
        if len(entries) > 1:
            for label, bucket in entries:
                # Unsuffixed column becomes bucket_0
                bucket_idx = bucket if bucket is not None else 0
                column_bucket_index[(group, label)] = bucket_idx

    # -----------------------------------------------------------------
    # Metric name normalizer (contract-frozen)
    # -----------------------------------------------------------------
    def normalize_metric(group: str, label: str) -> str:
        base_label, _ = _extract_base_and_bucket(label)
        base_name = f"{_clean(group)}__{_clean(base_label)}"

        key = (group, label)
        if key in column_bucket_index:
            return f"{base_name}__bucket_{column_bucket_index[key]}"

        return base_name

    # -----------------------------------------------------------------
    # Emit MetricSnapshots
    # -----------------------------------------------------------------
    snapshots = []
    snapshot_time = datetime.now(timezone.utc).isoformat()

    for _, row in df.iterrows():
        subject_id = row.get(("Subject ID", "Unnamed: 4_level_1"))
        site_id = row.get(("Site ID", "Unnamed: 3_level_1"))

        # Skip header / aggregate / empty rows
        if pd.isna(subject_id):
            continue

        for col in df.columns:
            if col in IDENTITY_COLS:
                continue

            group, label = col
            value = row[col]

            if pd.isna(value):
                continue

            snapshots.append(
                {
                    "entity_type": "subject",
                    "entity_id": subject_id,
                    "site_id": site_id,
                    "metric_name": normalize_metric(group, label),
                    "metric_value": value,
                    "snapshot_time": snapshot_time,
                    "source": "CPID_EDC_Metrics",
                }
            )

    return pd.DataFrame(
        snapshots,
        columns=[
            "entity_type",
            "entity_id",
            "site_id",
            "metric_name",
            "metric_value",
            "snapshot_time",
            "source",
        ],
    )
