import re
from pathlib import Path
from ingestion.missing_lab_ranges_extractor import normalize_missing_lab_ranges
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
TARGET_TABLE = "missing_lab_ranges_events"


def is_missing_lab_file(filename: str) -> bool:
    """
    Match Missing Lab/Range files with various naming patterns:
    - Missing_Lab_Name_and_Missing_Ranges
    - Missing Lab & Range Report (with spaces and ampersand)
    - Missing LNR (Lab Name & Ranges abbreviation)
    """
    # Use regex to handle spaces, underscores, and variations
    pattern = r"missing[_\s]*(lab|lnr|range)"
    return bool(re.search(pattern, filename, re.IGNORECASE))


def run_missing_lab_ranges_ingestion():
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
            if not is_missing_lab_file(file.name):
                continue

            study_had_file = True
            files_attempted += 1
            studies_attempted.add(study_id)

            print(
                f"\n▶ Processing Missing Lab Ranges "
                f"study='{study_id}' file='{file.name}'"
            )

            df = normalize_missing_lab_ranges(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} missing lab rows")

            if df.empty:
                print("⚠️ Empty extract — skipping insert")
                continue

            insert_dataframe(
                df=df,
                table_name=TARGET_TABLE,
                batch_size=1000,
            )

            total_inserted += len(df)

        if not study_had_file:
            studies_skipped.append(study_id)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("📊 MISSING LAB RANGES INGESTION SUMMARY")
    print("=" * 72)
    print(f"📁 Study folders found       : {study_folders_found}")
    print(f"📂 Studies attempted         : {len(studies_attempted)}")
    print(f"📄 Files attempted           : {files_attempted}")
    print(f"📥 Total rows inserted       : {total_inserted}")

    if studies_skipped:
        print(f"⚠️ Studies skipped           : {studies_skipped}")

    print("=" * 72)
    print("✅ Missing Lab Ranges ingestion completed successfully")


if __name__ == "__main__":
    run_missing_lab_ranges_ingestion()