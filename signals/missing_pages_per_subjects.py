"""
SIGNAL: Missing Pages Per Subject
---------------------------------------------------------
Description:
    This signal identifies gaps in data collection by scanning the 
    `missing_pages_events` table.
    
    Trigger Logic:
    1. Status: Subject is Active (Status is 'On Trial' or 'Screening').
       (Excludes 'Screen Failure').
    2. Latency: The page has been missing for > 14 days.
    
    Impact Logic (Time-based):
    - > 45 days: Critical (Long-term missing)
    - > 30 days: High
    - > 14 days: Moderate

    Inputs:
    Supabase table `missing_pages_events` 
    (fields: days_missing, overall_subject_status, visit_subject_status)

    Outputs:
    List[JSON] containing targeted queries for site remediation.
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

TABLE_NAME = "missing_pages_events" 

# --- 2. Helper Logic ---
def determine_impact(days: int) -> str:
    if days > 45:
        return "CRITICAL: Long-term missing data (> 45 days)"
    elif days > 30:
        return "HIGH: Significant delay (> 30 days)"
    return "MODERATE: Routine latency (> 14 days)"

def generate_action(form_name: str, days: int) -> str:
    if days > 30:
        return f"Urgent follow-up with Site Coordinator for {form_name}"
    return f"Query to site for {form_name}"

# --- 3. Main Detection Logic ---
def detect_missing_pages() -> List[Dict[str, Any]]:
    print("Running detection logic for missing pages...")
    
    # Fetch Data
    # 1. Filter for ACTIVE subjects ('On Trial' or 'Screening')
    #    We use .in_() to select multiple allowed values
    # 2. Filter for days_missing > 14
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .in_("overall_subject_status", ["On Trial", "Screening"])\
        .gt("days_missing", 14)\
        .execute()
    
    missing_pages_data = response.data
    signals = []

    for page in missing_pages_data:
        days = page.get('days_missing', 0)
        form_name = page.get('form_name', 'Unknown Form')
        
        # Construct Signal
        signal = {
            "signal_type": "missing_pages",
            "study_id": page.get('study_id'),
            "site_id": page.get('site_id'),
            "subject_id": page.get('subject_id'),
            
            # Status Context
            "overall_subject_status": page.get('overall_subject_status'),
            "visit_subject_status": page.get('visit_subject_status'),
            
            "folder_name": page.get('folder_name'),
            "form_name": form_name,
            "form_type": page.get('form_type'),
            "days_missing": days,
            "visit_date": page.get('visit_date'),
            
            "impact": determine_impact(days),
            "recommended_action": generate_action(form_name, days)
        }
        
        signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_missing_pages()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal signals generated: {len(generated_signals)}")
    else:
        print("No missing page signals detected.")