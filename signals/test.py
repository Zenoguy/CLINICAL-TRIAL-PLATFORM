import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

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

# Test the connection
response = supabase.table("sae_ops_events").select("*").limit(1).execute()
print("Connection successful:", response.data)