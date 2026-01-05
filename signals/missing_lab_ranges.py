"""
SIGNAL: Missing Lab Ranges/Units
---------------------------------------------------------
Description:
    This signal detects specific lab records where Reference Ranges or Units 
    are missing, which prevents safety assessment.

    Trigger Logic:
    1. Filter: Records where `issue` indicates missing ranges or units.
    2. Severity Assessment:
       - P0: (Skipped - requires 'grade' field)
       - P1: Safety Labs (e.g., LFTs, CBC, Coag) -> Immediate attention.
       - P2: Routine Chemistry -> Weekly review.
    
    Inputs:
    Supabase table `missing_lab_ranges_events` 
    (fields: study_id, site_id, subject_id, visit_name, form_name, 
     lab_category, lab_date, test_name, issue, source)

    Outputs:
    List[JSON] containing individual record alerts with calculated severity.
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any

# --- 1. Setup & Configuration ---
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    raise ValueError(f"Could not load keys from {env_path}")

supabase: Client = create_client(url, key)

TABLE_NAME = "missing_lab_ranges_events"

# Define Safety Labs for P1 Severity Logic (LFTs, CBC, Coag, etc.)
SAFETY_LABS = {
    "ALT", "AST", "ALP", "BILI", "CREAT", "HGB", "HCT", "WBC", "PLT", "NEUT",
    "PT", "PTT", "INR"
}

# --- 2. Helper Logic ---
def determine_severity_and_rationale(test_name: str, issue: str) -> tuple[str, str]:
    """
    Determines severity (P1/P2) and rationale based on the test name.
    Note: P0 (Grade 3+) logic is omitted as 'grade' is not in the current schema.
    """
    normalized_test = test_name.upper() if test_name else ""
    
    # Check for Safety Labs (P1)
    # Checks if the test name contains any of the safety keys (e.g. "ALT_CLINICAL")
    is_safety = any(lab in normalized_test for lab in SAFETY_LABS)

    if is_safety:
        return "P1", "Cannot assess safety without reference ranges (safety lab)"
    else:
        return "P2", "Cannot assess safety without reference ranges (routine)"

# --- 3. Main Detection Logic ---
def detect_missing_lab_ranges() -> List[Dict[str, Any]]:
    print("Running detection logic for missing lab ranges/units...")

    # Fetch Data
    # We filter for issues related to ranges or units.
    # Using .or_ to capture variations of the issue text.
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .or_("issue.ilike.%range%,issue.ilike.%unit%")\
        .execute()
    
    raw_data = response.data
    signals = []

    for row in raw_data:
        subject_id = row.get('subject_id')
        test_name = row.get('test_name', 'Unknown')
        lab_date = row.get('lab_date')
        issue_desc = row.get('issue')
        
        # Calculate Severity
        severity, rationale = determine_severity_and_rationale(test_name, issue_desc)

        # Construct Signal
        signal = {
            "signal_type": "missing_lab_ranges",
            "subject_id": subject_id,
            "test_name": test_name,
            # 'test_description' is not in schema, mapping test_name as fallback
            "test_description": test_name, 
            "lab_date": lab_date,
            "issue": issue_desc,
            "severity": severity,
            "rationale": rationale,
            "recommended_action": "Immediate site query for lab ranges"
        }
        
        signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_missing_lab_ranges()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal signals generated: {len(generated_signals)}")
    else:
        print("No missing lab range signals detected.")