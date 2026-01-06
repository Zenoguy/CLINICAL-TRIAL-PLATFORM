import pandas as pd
from typing import Optional

# ---------------------------------------------------------------------
# Column contract (LOCKED)
# ---------------------------------------------------------------------
COLUMN_MAP = {
    "Discrepancy ID": "event_id",
    "Study ID": "study_id",
    "Country": "country",
    "Site": "site_id",
    "Patient ID": "subject_id",
    "Form Name": "form_name",
    "Discrepancy Created Timestamp in Dashboard": "created_timestamp",
    "Review Status": "review_status",
    "Action Status": "action_status",
}


def extract_sae_events(
    filepath: str,
    *,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize SAE Dashboard into canonical sae_events rows.
    """

    df = pd.read_excel(filepath)

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Ensure all expected columns exist
    for col in COLUMN_MAP.values():
        if col not in df.columns:
            df[col] = pd.NA

    # Parse + serialize timestamp
    df["created_timestamp"] = (
        pd.to_datetime(df["created_timestamp"], errors="coerce", utc=True)
        .dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )

    # Inject study_id if needed
    if study_id_override is not None:
        df["study_id"] = study_id_override

    # Add source tag
    df["source"] = "SAE_Dashboard"

    # Drop rows without subject_id (guardrail)
    df = df[df["subject_id"].notna()]
    # Drop rows without subject_id (guardrail)
    df = df[df["subject_id"].notna()]

    # 🔑 FINAL: force JSON-safe Python primitives
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    # Explicitly cast known string fields
    string_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "event_id",
        "form_name",
        "review_status",
        "action_status",
        "created_timestamp",
        "source",
    ]

    for col in string_cols:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    return df[
        [
            "study_id",
            "site_id",
            "subject_id",
            "event_id",
            "form_name",
            "review_status",
            "action_status",
            "created_timestamp",
            "source",
        ]
    ]
