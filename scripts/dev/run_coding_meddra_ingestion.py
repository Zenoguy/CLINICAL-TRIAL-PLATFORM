from pathlib import Path
from ingestion.coding_meddra_extractor import extract_coding_meddra_events
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")


def run_coding_meddra_ingestion():
    total = 0

    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        study_id = study_dir.name

        for file in study_dir.glob("*.xlsx"):
            if "MedDRA" not in file.name:
                continue

            print(
                f"\n▶ Processing MedDRA Coding "
                f"study='{study_id}' file='{file.name}'"
            )

            df = extract_coding_meddra_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} MedDRA coding rows")

            if not df.empty:
                insert_dataframe(
                    df=df,
                    table_name="coding_meddra_events",
                    batch_size=1000,
                )
                total += len(df)

    print(
        f"\n✅ MedDRA coding ingestion completed "
        f"(total rows inserted: {total})"
    )


if __name__ == "__main__":
    run_coding_meddra_ingestion()
