import pandas as pd
import math
from typing import Optional

COLUMN_MAP = {
    "Study Name": "study_id",
    "SiteGroupName(CountryName)": "country",
    "SiteNumber": "site_id",
    "SubjectName": "subject_id",
    "Overall Subject Status": "overall_subject_status",
    "Visit Level Subject Status": "visit_subject_status",
    "FolderName": "folder_name",
    "Visit date": "visit_date",
    "Form Type (Summary or Visit)": "form_type",
    "FormName": "form_name",
    "No. #Days Page Missing": "days_missing",
}

def extract_missing_pages_events(
    filepath: str,
    *,
    study_id_override: Optional[str] = None,
) -> pd.DataFrame:
    df = pd.read_excel(filepath)
    df = df.rename(columns=COLUMN_MAP)
    required_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "overall_subject_status",
        "visit_subject_status",
        "folder_name",
        "form_name",
        "form_type",
        "visit_date",
        "days_missing",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Parse + coerce
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce")
    df["days_missing"] = pd.to_numeric(df["days_missing"], errors="coerce")
    
    if study_id_override is not None:
        df["study_id"] = study_id_override
    
    # Guardrail 1: must have subject
    df = df[df["subject_id"].notna()]
    
    df["source"] = "Missing_Pages"
    
    # Timestamp → ISO
    df["visit_date"] = df["visit_date"].apply(
        lambda x: x.isoformat() if pd.notna(x) else None
    )
    
    # ✅ Convert to object dtype and replace NaN with None
    df = df.astype(object).where(pd.notna(df), None)
    
    # Explicit string casting
    string_cols = [
        "study_id",
        "site_id",
        "subject_id",
        "overall_subject_status",
        "visit_subject_status",
        "folder_name",
        "form_name",
        "form_type",
        "source",
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if x is not None else None)
    
    # days_missing: keep as float or None
    if "days_missing" in df.columns:
        df["days_missing"] = df["days_missing"].apply(
            lambda x: float(x) if x is not None else None
        )
    
    # Strengthened guardrail
    meaningful_cols = [
        "days_missing",
        "visit_date",
        "form_name",
        "folder_name",
        "form_type",
        "overall_subject_status",
        "visit_subject_status",
    ]
    df = df[df[meaningful_cols].notna().any(axis=1)]
    
    return df[
        [
            "study_id",
            "site_id",
            "subject_id",
            "overall_subject_status",
            "visit_subject_status",
            "folder_name",
            "form_name",
            "form_type",
            "visit_date",
            "days_missing",
            "source",
        ]
    ]