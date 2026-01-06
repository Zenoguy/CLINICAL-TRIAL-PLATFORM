import pandas as pd
from typing import Optional

# ---------------------------------------------------------------------
# Column contract (LOCKED)
# ---------------------------------------------------------------------
COLUMN_MAP = {
    "Country": "country",
    "Site": "site_id",
    "Site number": "site_id",
    "Subject": "subject_id",
    "Visit": "visit_name",
    "Projected Date": "projected_date",
    "# Days Outstanding": "days_outstanding",
    "# Days Outstanding (TODAY - PROJECTED DATE)": "days_outstanding",
}


def extract_visit_projection_events(
    filepath: str,
    *,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize Visit Projection Tracker into canonical visit_projection_events.
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
        "projected_date",
        "days_outstanding",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # Parse date → ISO string
    df["projected_date"] = (
        pd.to_datetime(df["projected_date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )

    # Numeric coercion
    df["days_outstanding"] = pd.to_numeric(
        df["days_outstanding"], errors="coerce"
    )

    # Inject study_id if needed
    if study_id_override is not None:
        df["study_id"] = study_id_override

    # Add source
    df["source"] = "Visit_Projection"

    # Drop rows without subject_id
    df = df[df["subject_id"].notna()]

    # 🔑 JSON-safe conversion
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    # Explicit casting (paranoid but safe)
    string_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "visit_name",
        "projected_date",
        "source",
    ]
    for col in string_cols:
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    df["days_outstanding"] = df["days_outstanding"].apply(
        lambda x: int(x) if isinstance(x, (int, float)) and x is not None else None
    )

    return df[
        [
            "study_id",
            "site_id",
            "subject_id",
            "visit_name",
            "projected_date",
            "days_outstanding",
            "source",
        ]
    ]
