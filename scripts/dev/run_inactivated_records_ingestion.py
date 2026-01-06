import re
from pathlib import Path
from ingestion.inactivated_records_extractor import normalize_inactivated_records
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
TARGET_TABLE = "inactivated_records_events"


def is_inactivated_file(filename: str) -> bool:
    """
    Match Inactivated files with various naming patterns:
    - Inactivated Forms, Folders and Records Report
    - Inactivated pages
    - Inactivated report
    - Inactivated Page Report
    """
    # Simple case-insensitive check for "inac" (covers all variations)
    return "inac" in filename.lower()


def run_inactivated_records_ingestion():
    study_folders_found = 0
    studies_attempted = set()
    files_attempted = 0
    total_inserted = 0
    studies_skipped = []

    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        study_folders_found += 1
        study_id = study_dir.name
        study_had_file = False

        for file in study_dir.glob("*.xlsx"):
            # Use the flexible pattern matcher
            if not is_inactivated_file(file.name):
                continue

            study_had_file = True
            files_attempted += 1
            studies_attempted.add(study_id)

            print(
                f"\n▶ Processing Inactivated Records "
                f"study='{study_id}' file='{file.name}'"
            )

            try:
                df = normalize_inactivated_records(
                    filepath=str(file),
                    study_id_override=study_id,
                )

                print(f"📦 Extracted {len(df)} inactivated record rows")

                if df.empty:
                    print("⚠️ Empty extract — skipping insert")
                    continue

                insert_dataframe(
                    df=df,
                    table_name=TARGET_TABLE,
                    batch_size=1000,
                )

                total_inserted += len(df)

            except Exception as e:
                print(f"❌ Error processing {file.name}: {e}")
                continue

        if not study_had_file:
            studies_skipped.append(study_id)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("📊 INACTIVATED RECORDS INGESTION SUMMARY")
    print("=" * 72)
    print(f"📁 Study folders found       : {study_folders_found}")
    print(f"📂 Studies attempted         : {len(studies_attempted)}")
    print(f"📄 Files attempted           : {files_attempted}")
    print(f"📥 Total rows inserted       : {total_inserted}")

    if studies_skipped:
        print(f"⚠️ Studies skipped           : {studies_skipped}")

    print("=" * 72)
    print("✅ Inactivated Records ingestion completed successfully")


if __name__ == "__main__":
    run_inactivated_records_ingestion()