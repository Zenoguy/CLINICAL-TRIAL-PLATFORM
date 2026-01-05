"""
SIGNAL: Repeat Uncoded Terms (Training Gap)
---------------------------------------------------------
Description:
    This signal detects specific verbatim terms that remain uncoded across multiple 
    occurrences (>= 3). This "repetition" pattern suggests a systematic issue:
    either a common typo (site training needed) or a dictionary gap (synonym list update needed).

    Trigger Logic:
    1. Filter: Records where `coding_status` is "UnCoded Term" AND `require_coding` is "Yes".
    2. Grouping: Normalize `source` (verbatim term) to lowercase and group.
    3. Threshold Check:
       Trigger if:
       - Unique verbatim term appears >= 3 times.

    Inputs:
    Supabase table `global_coding_report`
    (fields: study_id, subject_id, dictionary, dictionary_version, form_oid, coding_status, require_coding, source)

    *Mapped Fields*: 
    - `source` is used as `verbatim_term`.
    - `is_repeat_term` is calculated dynamically (count > 1).

    Outputs:
    List[JSON] containing alerts for specific repeating uncoded terms.
"""

import os
import json
from pathlib import Path
from collections import defaultdict
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

# Using the table name implied by the source dataset description
TABLE_NAME = "coding_whodrug_events"

# --- 2. Helper Logic ---
def extract_site_id(subject_id: str) -> str:
    """
    Extracts site ID from subject_id based on standard convention (first 3 chars).
    Returns 'Unknown' if subject_id is missing or too short.
    """
    if subject_id and len(subject_id) >= 3:
        return subject_id[:3]
    return "Unknown"

# --- 3. Main Detection Logic ---
def detect_repeat_uncoded_terms() -> List[Dict[str, Any]]:
    print("Running detection logic for Repeat Uncoded Terms...")

    # Fetch Data
    # Filter for "UnCoded Term" where coding is required
    response = supabase.table(TABLE_NAME)\
        .select("*")\
        .eq("coding_status", "UnCoded Term")\
        .eq("require_coding", "Yes")\
        .execute()
    
    raw_data = response.data
    signals = []

    # 1. Group by Verbatim Term (Source)
    # Key: Lowercase verbatim term (for normalization)
    # Value: List of record dictionaries
    term_frequency = defaultdict(list)

    for row in raw_data:
        # 'source' is mapped to verbatim_term
        verbatim = row.get('source', '').strip().lower()
        if verbatim:
            term_frequency[verbatim].append(row)

    # 2. Analyze Groups
    for verbatim, occurrences in term_frequency.items():
        
        count = len(occurrences)

        # Trigger Logic: Frequency >= 3
        if count >= 3:
            
            # Segmentation: Extract affected sites
            # Using a set to get unique sites
            sites_affected = sorted(list(set(
                extract_site_id(t.get('subject_id')) for t in occurrences
            )))

            # Infer hypothesis based on the nature of the repetition
            # (Simple rule: If sites > 1, likely a dictionary gap; if 1 site, likely a typo/training)
            hypothesis = "Common typo or lack of autocomplete"
            if len(sites_affected) > 1:
                hypothesis += " (Systemic/Dictionary Issue)"
            else:
                hypothesis += " (Site Training Issue)"

            # Construct Signal
            signal = {
                "signal_type": "repeat_uncoded_term",
                "term": verbatim, # The specific term causing the issue
                "frequency": count,
                "sites_affected": sites_affected,
                "hypothesis": hypothesis,
                "recommended_action": "EDC dictionary update + site notification",
                "alert": f"Repeated Uncoded Term detected: '{verbatim}' appeared {count} times."
            }
            
            signals.append(signal)

    return signals

# --- 4. Execution & Output ---
if __name__ == "__main__":
    generated_signals = detect_repeat_uncoded_terms()
    
    if generated_signals:
        print(json.dumps(generated_signals, indent=2))
        print(f"\nTotal signals generated: {len(generated_signals)}")
    else:
        print("No repeat uncoded terms detected.")