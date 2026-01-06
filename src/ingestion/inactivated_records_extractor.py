import pandas as pd
import warnings
from typing import Optional

# ---------------------------------------------------------------------
# Column contract (locked)
# ---------------------------------------------------------------------
COLUMN_MAP = {
    "Country": "country",
    "Study Site Number": "site_id",
    "Site": "site_id",
    "Subject": "subject_id",
    "Folder": "folder_name",
    "Form": "form_name",
    "Data on Form/Record": "record_name",
    "Record": "record_name",            # some studies use this
    "RecordPosition": "record_position",
    "Audit Action": "audit_action",
}

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def normalize_inactivated_records(
    filepath: str,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize Inactivated Forms / Pages / Records report
    into inactivated_records_events.
    """

    df = pd.read_excel(filepath)

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Ensure required columns exist
    required_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "folder_name",
        "form_name",
        "record_name",
        "record_position",
        "audit_action",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Inject study_id if needed
    if study_id_override is not None:
        df["study_id"] = study_id_override

    # Drop junk rows (Excel artifacts)
    df = df.dropna(subset=["site_id", "subject_id"], how="all")

    # Add source
    df["source"] = "Inactivated_Records"

    # ✅ Convert to object dtype and replace all NaN/NA with None
    df = df.astype(object).where(pd.notna(df), None)

    # ✅ Explicit string casting for text columns
    string_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "folder_name",
        "form_name",
        "record_name",
        "record_position",
        "audit_action",
        "source",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    # ✅ Guardrail: Filter rows with at least one meaningful value
    meaningful_cols = [
        "folder_name",
        "form_name",
        "record_name",
        "record_position",
        "audit_action",
    ]
    df = df[df[meaningful_cols].notna().any(axis=1)]

    return df[
        [
            "study_id",
            "site_id",
            "subject_id",
            "folder_name",
            "form_name",
            "record_name",
            "record_position",
            "audit_action",
            "source",
        ]
    ]