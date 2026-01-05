"""
SIGNAL: Missing Lab Names (Site Pattern)
---------------------------------------------------------
Description:
    This signal detects systemic issues where a site is failing to record 
    the Lab Name (e.g., "Central Lab" vs "Local Hospital").
    
    Trigger Logic:
    1. Filter: Records where `issue` indicates a missing lab name.
    2. Aggregation: Group failures by `site_id`.
    3. Threshold: Trigger if a site has >= 3 missing records.
    
    Pattern Context:
    - Identifies the most frequently affected Visit (e.g., "Cycle 1").
    - Identifies the most frequently affected Lab Category (e.g., "Hematology").

    Inputs:
    Supabase table `missing_lab_ranges_events` 
    (fields: issue, site_id, subject_id, visit_name, lab_category)

    Outputs:
    List[JSON] containing site-level pattern alerts.
"""

import os
import json
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import List, Dict, Any
from collections import defaultdict, Counter

# --- 1. Setup & Configuration ---
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not url or not key:
    raise ValueError(f"Could not load keys from {env_path}")

supabase: Client = create_client(url, key)

TABLE_NAME = "missing_lab_ranges_events"

# --- 2. Helper Logic ---
def get_most_common(rows: List[Dict], field: str) -> str:
    """
    Finds the most frequent value in a specific field (e.g., finding the 'Unscheduled' visit).
    Returns 'Unknown' if the list is empty or values are None.
    """
    values = [row.get(field) for row in rows if row.get(field)]
    
    if not values:
        return "Unknown"
    
    # Returns the top 1 most common value, e.g., "CHEMISTRY"
    return Counter(values).most_common(1)[0][0]

# --- 3. Main Detection Logic ---
def detect_missing_lab_names() -> List[Dict[str, Any]]:
    print("Running detection logic for missing lab names...")

    # Fetch Data
    # Filter where 'issue' is "Missing Lab Name"
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .eq("issue", "Missing Lab name")\
        .execute()
    
    raw_data = response.data
    signals = []

    # Group by Site ID
    site_groups = defaultdict(list)
    for row in raw_data:
        site_id = row.get('site_id')
        if site_id:
            site_groups[site_id].append(row)

    # Analyze Patterns per Site
    for site_id, rows in site_groups.items():
        
        # Threshold: Only alert if >= 3 errors exist at this site
        if len(rows) >= 3:
            
            # 1. Subject Count (How many different people is this affecting?)
            unique_subjects = set(row.get('subject_id') for row in rows)
            
            # 2. Visit Pattern (Is it always happening at "Week 1"?)
            visit_affected = get_most_common(rows, 'visit_name')
            
            # 3. Category Pattern (Is it only "Hematology" labs?)
            category_affected = get_most_common(rows, 'lab_category')
            
            # Construct Signal
            signal = {
                "signal_type": "missing_lab_name",
                "site_id": site_id,
                "subject_count": len(unique_subjects),
                
                # Context Fields
                "visit_affected": visit_affected,
                "lab_category": category_affected,
                
                "hypothesis": "Site not capturing local lab name in EDC",
                "recommended_action": "CRA training on lab name entry requirement"
            }
            
            signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_missing_lab_names()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal site signals generated: {len(generated_signals)}")
    else:
        print("No site-level lab name patterns detected.")