import pandas as pd
from typing import Optional

# ---------------------------------------------------------------------
# Column contract (locked)
# ---------------------------------------------------------------------
COLUMN_MAP = {
    "Country": "country",
    "Site number": "site_id",
    "Site": "site_id",
    "Subject": "subject_id",
    "Visit": "visit_name",
    "Form Name": "form_name",
    "Lab category": "lab_category",
    "Lab Date": "lab_date",
    "Test Name": "test_name",
    "Test description": "test_description",
    "Issue": "issue",
    "Comments": "comments",
}

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def normalize_missing_lab_ranges(
    filepath: str,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize Missing Lab Name / Missing Ranges report
    into missing_lab_ranges_events.
    """

    df = pd.read_excel(filepath)

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Ensure required columns exist
    required_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "visit_name",
        "form_name",
        "lab_category",
        "lab_date",
        "test_name",
        "issue",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Parse lab date
    df["lab_date"] = pd.to_datetime(df["lab_date"], errors="coerce")

    # Inject study_id if needed
    if study_id_override is not None:
        df["study_id"] = study_id_override

    # Add source
    df["source"] = "Missing_Lab_Ranges"

    # ✅ Convert lab_date to ISO format string (None if NaT)
    df["lab_date"] = df["lab_date"].apply(
        lambda x: x.isoformat() if pd.notna(x) else None
    )

    # ✅ Convert to object dtype and replace all NaN/NA with None
    df = df.astype(object).where(pd.notna(df), None)

    # ✅ Explicit string casting for text columns
    string_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "visit_name",
        "form_name",
        "lab_category",
        "test_name",
        "issue",
        "source",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    # ✅ Guardrail: Filter rows with at least one meaningful value
    meaningful_cols = [
        "visit_name",
        "form_name",
        "lab_category",
        "lab_date",
        "test_name",
        "issue",
    ]
    df = df[df[meaningful_cols].notna().any(axis=1)]

    return df[
        [
            "study_id",
            "site_id",
            "subject_id",
            "visit_name",
            "form_name",
            "lab_category",
            "lab_date",
            "test_name",
            "issue",
            "source",
        ]
    ]