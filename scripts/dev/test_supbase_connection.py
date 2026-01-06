from storage.supabase_client import get_supabase_client
from core.config import get_settings

def main():
    client = get_supabase_client()
    result = client.table("cpid_metric_snapshots").select("*").limit(1).execute()
    print("Connected OK:", result.data)

if __name__ == "__main__":
    main()
