from typing import Iterable
import math
import pandas as pd

from supabase import Client
from storage.supabase_client import get_supabase_client


DEFAULT_BATCH_SIZE = 1000


class SupabaseInsertError(RuntimeError):
    pass

def insert_rows(
    table_name: str,
    rows: Iterable[dict],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> None:
    rows = list(rows)
    total = len(rows)

    if total == 0:
        return

    if dry_run:
        print(
            f"[DRY RUN] Would insert {total} rows into '{table_name}' "
            f"({math.ceil(total / batch_size)} batches)"
        )
        return

    client: Client = get_supabase_client()
    batches = math.ceil(total / batch_size)

    for i in range(batches):
        start = i * batch_size
        end = start + batch_size
        batch = rows[start:end]

        try:
            (
                client
                .table(table_name)
                .insert(batch)
                .execute()
            )
        except Exception as e:
            raise SupabaseInsertError(
                f"Insert failed for table '{table_name}' "
                f"(batch {i + 1}/{batches})"
            ) from e

    print(
        f"✅ Inserted {total} rows into '{table_name}' "
        f"({batches} batches)"
    )


def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> None:
    insert_rows(
        table_name=table_name,
        rows=df.to_dict(orient="records"),
        batch_size=batch_size,
        dry_run=dry_run,
    )
