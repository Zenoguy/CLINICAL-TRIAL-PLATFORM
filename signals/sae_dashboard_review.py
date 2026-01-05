"""
SIGNAL: SAE Dashboard Review Status Gaps
---------------------------------------------------------
Description:
    This signal detects delays in SAE review (> 24 hours) and inconsistencies 
    between review status and action status.
    
    Trigger Logic:
    1. Filter: Records where `review_status` is "Pending for Review".
    2. Time Check: Calculate `pending_hours` (Current Time - Created Timestamp).
       Trigger if pending_hours > 24.
    3. Inconsistency Check: 
       Trigger if `review_status` is "Pending for Review" AND `action_status` is "No action required".

    Inputs:
    Supabase table `sae_dashboard_events`
    (fields: discrepancy_id, site_id, form_name, created_timestamp, 
     review_status, action_status, source)
    
    *Removed Input Fields*: `patient_id` and `review_track` were removed as per instructions.

    Outputs:
    List[JSON] containing alerts for pending reviews and status inconsistencies.
"""

import os
import json
from pathlib import Path
from datetime import datetime, timezone
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

TABLE_NAME = "sae_ops_events"

# --- 2. Helper Logic ---
def calculate_pending_hours(created_ts_str: str) -> float:
    """
    Calculates hours elapsed since the created_timestamp.
    Assumes created_ts_str is in ISO format (e.g., "2025-12-29T14:23:00Z" or similar).
    """
    if not created_ts_str:
        return 0.0
    
    try:
        # Handle cases with or without 'Z' or offset.
        # Ideally, dates should be parsed robustly.
        created_dt = datetime.fromisoformat(created_ts_str.replace('Z', '+00:00'))
        
        # Ensure created_dt is timezone-aware for comparison with now(timezone.utc)
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
            
        now = datetime.now(timezone.utc)
        delta = now - created_dt
        return delta.total_seconds() / 3600.0
    except ValueError:
        return 0.0

# --- 3. Main Detection Logic ---
def detect_sae_review_gaps() -> List[Dict[str, Any]]:
    print("Running detection logic for SAE review gaps...")

    # Fetch Data
    # Filter for "Pending for Review" status
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .eq("review_status", "Pending for Review")\
        .execute()
    
    raw_data = response.data
    signals = []

    for row in raw_data:
        # Calculate Pending Duration
        created_ts = row.get('created_ts')
        pending_hours = calculate_pending_hours(created_ts)

        # Trigger 1: Pending > 24 Hours
        if pending_hours > 24:
            
            # Check for Status Inconsistency
            # "Pending" status usually requires action; if "No action required", it's inconsistent.
            review_status = row.get('review_status')
            action_status = row.get('action_status')
            
            inconsistent = (
                review_status == 'Pending for Review' and 
                action_status == 'No action required'
            )

            # Determine Recommended Action (Fallback generic since 'review_track' is missing)
            rec_action = "Immediate review required for pending SAE"
            
            # Construct Signal
            signal = {
                "signal_type": "sae_review_pending",
                "discrepancy_id": row.get('discrepancy_id'),
                "site_id": row.get('site_id'),
                "form_name": row.get('form_name'),
                "pending_since": created_ts,
                "pending_hours": round(pending_hours, 1),
                "current_status": review_status,
                "action_status": action_status,
                "status_inconsistent": inconsistent,
                "alert": "Status inconsistency detected" if inconsistent else None,
                "recommended_action": rec_action
            }
            
            signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_sae_review_gaps()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal signals generated: {len(generated_signals)}")
    else:
        print("No SAE review gaps detected.")