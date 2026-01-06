from pathlib import Path
from ingestion.sae_extractor import extract_sae_events
from storage.supabase_writer import insert_dataframe

SAE_FILENAME_KEYWORD = "SAE"
STUDY_ROOT_DIR = Path("QC Anonymized Study Files")


def run_sae_ingestion():
    total = 0

    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        study_id = study_dir.name

        for file in study_dir.glob("*.xlsx"):
            if "eSAE" not in file.name:
                continue

            print(
                f"\n▶ Processing SAE study='{study_id}' file='{file.name}'"
            )

            df = extract_sae_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} SAE events")

            if not df.empty:
                insert_dataframe(
                    df=df,
                    table_name="sae_events",
                    batch_size=1000,
                )
                total += len(df)

    print(f"\n✅ SAE ingestion completed (total rows inserted: {total})")


if __name__ == "__main__":
    run_sae_ingestion()
