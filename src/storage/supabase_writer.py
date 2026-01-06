import math
import pandas as pd
from typing import Iterable

from supabase import Client
from storage.supabase_client import get_supabase_client


DEFAULT_BATCH_SIZE = 1000


class SupabaseInsertError(RuntimeError):
    pass


def clean_nan_values(records: list[dict]) -> list[dict]:
    """Replace NaN values with None for JSON serialization."""
    cleaned = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            # Check for NaN (works for both float nan and pd.NA)
            if isinstance(value, float) and math.isnan(value):
                cleaned_record[key] = None
            elif pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned.append(cleaned_record)
    return cleaned


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
    records = df.to_dict(orient="records")
    records = clean_nan_values(records)  # ✅ Clean NaN values
    
    insert_rows(
        table_name=table_name,
        rows=records,
        batch_size=batch_size,
        dry_run=dry_run,
    )