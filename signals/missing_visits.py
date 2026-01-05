import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any
from datetime import datetime


"""
SIGNAL: Missing Visits Beyond Expected Date
---------------------------------------------------------
Description:
    This signal implements a Risk-Based Monitoring (RBM) logic to detect 
    missed clinical trial visits. It queries the `visit_projections` table 
    to identify visits that have exceeded their projected date.

    It performs three key functions:
    1. Triage: Categorizes lateness into severity buckets (Yellow/Red/Critical).
    2. Contextualize: Analyzes site-level patterns to detect if the issue is 
       isolated to one subject or systemic across the site.
    3. Act: Generates specific recommended actions (e.g., "CRA Reminder" vs 
       "Escalation") based on the severity and context.

Logic:
    - Yellow   (7-14 days):   Warning (CRA Reminder)
    - Red      (15-30 days):  Risk (Immediate Follow-up)
    - Critical (>30 days):    Failure (CTM Escalation)

Inputs: 
    Supabase table `visit_projections` (fields: days_outstanding, projected_date, site_id)

Outputs: 
    List[JSON] containing actionable signals with site pattern analysis.
"""


# 1. Define the path to the .env file
# Path(__file__) is the current script. .parent is the folder it's in.
# .parent.parent moves up one level.
env_path = Path(__file__).resolve().parent.parent / '.env'

# 2. Load the specific .env file
load_dotenv(dotenv_path=env_path)

# 3. Retrieve keys & Initialize
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    raise ValueError(f"Could not load keys from {env_path}")

supabase: Client = create_client(url, key)

TABLE_NAME = "visit_projection_events"  # Replace with your actual table name

def generate_action(severity: str) -> str:
    """Maps severity to the recommended action."""
    actions = {
        "yellow": "CRA reminder",
        "red": "CRA immediate follow-up",
        "critical": "CTM escalation + site coordinator call"
    }
    return actions.get(severity, "Monitor")

def calculate_site_patterns(data: List[Dict]) -> Dict[str, int]:
    """
    Pre-calculates how many overdue visits exist per site.
    Returns a dictionary: {'site_042': 5, 'site_001': 2}
    """
    site_counts = {}
    for row in data:
        site_id = row.get('site_id')
        if site_id:
            site_counts[site_id] = site_counts.get(site_id, 0) + 1
    return site_counts

def detect_overdue_visits():
    print(f"[{datetime.now()}] Running detection logic...")

    # 2. Fetch Data
    # We filter for > 7 immediately to reduce data load (Yellow threshold start)
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .gt("days_outstanding", 7)\
        .execute()
    
    overdue_visits = response.data
    
    # 3. Context Enrichment: Pre-calculate site patterns
    # We do this once here so we don't query the DB inside the loop below
    site_stats = calculate_site_patterns(overdue_visits)

    signals = []

    # 4. Signal Generation Loop
    for visit in overdue_visits:
        days = visit.get('days_outstanding', 0)
        site_id = visit.get('site_id')
        
        # Determine Severity
        if days > 30:
            severity = 'critical'
        elif days > 15:
            severity = 'red'
        elif days > 7:
            severity = 'yellow'
        else:
            continue # Should be caught by DB filter, but safe to keep

        # Generate Site Pattern Context
        # We subtract 1 to not count the current subject against themselves
        other_overdue_count = max(0, site_stats.get(site_id, 0) - 1)
        
        if other_overdue_count > 0:
            pattern_msg = f"{other_overdue_count} other subjects at this site also overdue"
        else:
            pattern_msg = "No other overdue subjects at this site"

        # Construct the Signal Object
        signal = {
            "signal_type": "missing_visit_overdue",
            "site_id": site_id,
            "subject_id": visit.get('subject_id'),
            # Assuming 'visit_name' exists in DB, otherwise use a placeholder
            "visit_name": visit.get('visit_name', 'Unspecified Visit'), 
            "projected_date": visit.get('projected_date'),
            "days_overdue": days,
            "severity": severity,
            "site_pattern": pattern_msg,
            "recommended_action": generate_action(severity)
        }
        
        signals.append(signal)

    return signals

# --- Execution ---
if __name__ == "__main__":
    generated_signals = detect_overdue_visits()
    print(f"Total Signals Generated: {len(generated_signals)}")
    
    # Print the first result as a test
    if generated_signals:
        import json
        print(json.dumps(generated_signals, indent=2))
    else:
        print("No overdue signals detected.")