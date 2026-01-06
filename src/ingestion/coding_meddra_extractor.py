import pandas as pd
from typing import Optional

# ---------------------------------------------------------------------
# Column contract (LOCKED)
# ---------------------------------------------------------------------
COLUMN_MAP = {
    "Study": "study_id",
    "Subject": "subject_id",
    "Dictionary": "dictionary",
    "Dictionary Version number": "dictionary_version",
    "Form OID": "form_oid",
    "Logline": "logline",
    "Field OID": "field_oid",
    "Coding Status": "coding_status",
    "Require Coding": "require_coding",
}


def extract_coding_meddra_events(
    filepath: str,
    *,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize MedDRA Coding Report into canonical coding_meddra_events.
    """

    df = pd.read_excel(filepath)

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Ensure required columns exist
    required_cols = [
        "study_id",
        "subject_id",
        "dictionary",
        "dictionary_version",
        "form_oid",
        "logline",
        "field_oid",
        "coding_status",
        "require_coding",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Numeric coercion
    df["logline"] = pd.to_numeric(df["logline"], errors="coerce")

    # Inject study_id if needed
    if study_id_override is not None:
        df["study_id"] = study_id_override

    # Add source
    df["source"] = "Coding_MedDRA"

    # Drop rows without subject_id (guardrail)
    df = df[df["subject_id"].notna()]

    # 🔑 JSON-safe conversion
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    # Explicit string casting
    string_cols = [
        "study_id",
        "subject_id",
        "dictionary",
        "dictionary_version",
        "form_oid",
        "field_oid",
        "coding_status",
        "require_coding",
        "source",
    ]
    for col in string_cols:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    # Ensure logline is int or None
    df["logline"] = df["logline"].apply(
        lambda x: int(x) if isinstance(x, (int, float)) and x is not None else None
    )

    return df[
        [
            "study_id",
            "subject_id",
            "dictionary",
            "dictionary_version",
            "form_oid",
            "logline",
            "field_oid",
            "coding_status",
            "require_coding",
            "source",
        ]
    ]
