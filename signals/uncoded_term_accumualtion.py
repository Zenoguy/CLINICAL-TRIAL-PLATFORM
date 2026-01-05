"""
SIGNAL: Uncoded Terms Accumulation (Volume Based)
---------------------------------------------------------
Description:
    This signal detects a high volume of medical terms requiring coding (MedDRA/WHODrug)
    that have accumulated in the system.

    Trigger Logic:
    1. Filter: Records where `coding_status` is "UnCoded Term" AND `require_coding` is "True".
    2. Grouping: Aggregate terms by `dictionary` and `dictionary_version`.
    3. Threshold Check:
       Trigger if:
       - Total uncoded terms in group >= 5

    Inputs:
    Supabase table `global_coding_report`
    (fields: study_id, dictionary, dictionary_version, form_oid, coding_status, require_coding)

    *Removed Input Fields*: `created_timestamp` (removed as per instructions), `source`, `logline`, `field_oid`.

    Outputs:
    List[JSON] containing alerts for coding backlogs per dictionary version.
"""

import os
import json
from pathlib import Path
from collections import defaultdict, Counter
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

TABLE_NAME = "coding_meddra_events"

# --- 2. Helper Logic ---
# (No time calculation helpers needed for this version)

# --- 3. Main Detection Logic ---
def detect_coding_backlog() -> List[Dict[str, Any]]:
    print("Running detection logic for Coding Backlogs (Volume Only)...")

    # Fetch Data
    # Filter for "UnCoded Term" where coding is required
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .eq("coding_status", "UnCoded Term")\
        .eq("require_coding", "Yes")\
        .execute()
    
    raw_data = response.data
    signals = []

    # 1. Aggregate Data by Dictionary + Version
    # Key: (dictionary, dictionary_version)
    # Value: List of record dictionaries
    backlog_groups = defaultdict(list)

    for row in raw_data:
        # Create a unique key for the coding dictionary context
        dict_key = (row.get('dictionary'), row.get('dictionary_version'))
        backlog_groups[dict_key].append(row)

    # 2. Analyze Groups
    for (dictionary_name, dict_version), terms in backlog_groups.items():
        
        uncoded_count = len(terms)

        # Trigger Logic: Count >= 5
        if uncoded_count >= 5:
            
            # Segmentation: Count distribution by Form Name
            # Extract form_oids from the terms list
            form_counts = Counter(t.get('form_oid', 'Unknown') for t in terms)
            
            # Construct Signal
            signal = {
                "signal_type": "coding_backlog",
                "dictionary": dictionary_name,
                "dictionary_version": dict_version,
                "uncoded_count": uncoded_count,
                "form_distribution": dict(form_counts),
                "alert": f"Backlog detected: {uncoded_count} terms pending coding",
                "recommended_action": "Coder assignment + auto-suggest for common terms"
            }
            
            signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_coding_backlog()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal signals generated: {len(generated_signals)}")
    else:
        print("No coding backlogs detected.")