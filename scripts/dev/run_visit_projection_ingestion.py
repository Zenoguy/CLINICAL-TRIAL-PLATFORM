from pathlib import Path
from ingestion.visit_projection_extractor import extract_visit_projection_events
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")


def run_visit_projection_ingestion():
    total = 0

    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        study_id = study_dir.name

        for file in study_dir.glob("*.xlsx"):
            if "Visit" not in file.name:
                continue

            print(
                f"\n▶ Processing Visit Projection "
                f"study='{study_id}' file='{file.name}'"
            )

            df = extract_visit_projection_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} visit projection rows")

            if not df.empty:
                insert_dataframe(
                    df=df,
                    table_name="visit_projection_events",
                    batch_size=1000,
                )
                total += len(df)

    print(
        f"\n✅ Visit Projection ingestion completed "
        f"(total rows inserted: {total})"
    )


if __name__ == "__main__":
    run_visit_projection_ingestion()
