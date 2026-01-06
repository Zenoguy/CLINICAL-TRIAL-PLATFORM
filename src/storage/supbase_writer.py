from typing import Iterable
from src.storage.supabase_client import get_supabase_client


def insert_rows(
    table: str,
    rows: Iterable[dict],
    batch_size: int = 1000,
):
    client = get_supabase_client()
    rows = list(rows)

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        response = (
            client
            .table(table)
            .insert(batch)
            .execute()
        )

        if response.error:
            raise RuntimeError(
                f"Supabase insert failed for {table}: {response.error}"
            )
