from supabase import create_client, Client

from core.config import get_settings

_supabase_client: Client | None = None


def get_supabase_client() -> Client:
    """
    Returns a singleton Supabase client.
    """
    global _supabase_client

    if _supabase_client is None:
        settings = get_settings()

        if (
            not settings.supabase.supabase_url
            or not settings.supabase.service_role_key
        ):
            raise RuntimeError("Supabase credentials not configured")

        _supabase_client = create_client(
            settings.supabase.supabase_url,
            settings.supabase.service_role_key,
        )

    return _supabase_client
