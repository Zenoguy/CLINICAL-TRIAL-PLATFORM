from pathlib import Path
from ingestion.missing_pages_extractor import extract_missing_pages_events
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
TARGET_TABLE = "missing_pages_events"


def run_missing_pages_ingestion():
    studies_seen = 0
    studies_attempted = set()
    studies_skipped = []
    files_attempted = 0
    total_inserted = 0

    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        studies_seen += 1
        study_id = study_dir.name
        study_had_candidate = False

        # scan both xls + xlsx
        for file in list(study_dir.glob("*.xlsx")) + list(study_dir.glob("*.xls")):
            name = file.name.lower()

            # 🔑 robust Missing Pages detection
            if not any(
                k in name
                for k in [
                    "missing page",
                    "missing pages",
                    "global_missing_pages",
                    "missing_pages",
                    "missing page report",
                ]
            ):
                continue

            study_had_candidate = True
            studies_attempted.add(study_id)
            files_attempted += 1

            print(
                f"\n▶ Processing Missing Pages "
                f"study='{study_id}' file='{file.name}'"
            )

            df = extract_missing_pages_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} missing page rows")

            if df.empty:
                print("⚠️ Empty extract — skipping insert")
                continue

            insert_dataframe(
                df=df,
                table_name=TARGET_TABLE,
                batch_size=1000,
            )

            total_inserted += len(df)

        if not study_had_candidate:
            studies_skipped.append(study_id)
            print(f"⚠️ No Missing Pages file detected for study '{study_id}'")

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("📊 MISSING PAGES INGESTION SUMMARY")
    print("=" * 72)
    print(f"📁 Study folders found       : {studies_seen}")
    print(f"📂 Studies attempted         : {len(studies_attempted)}")
    print(f"📄 Files attempted           : {files_attempted}")
    print(f"📥 Total rows inserted       : {total_inserted}")

    if studies_skipped:
        print(f"⚠️ Studies skipped           : {studies_skipped}")

    print("=" * 72)
    print("✅ Missing Pages ingestion completed successfully")


if __name__ == "__main__":
    run_missing_pages_ingestion()
