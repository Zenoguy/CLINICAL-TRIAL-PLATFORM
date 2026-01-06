from pathlib import Path
from ingestion.coding_meddra_extractor import extract_coding_meddra_events
from storage.supabase_writer import insert_dataframe

STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
TARGET_TABLE = "coding_meddra_events"


def run_coding_meddra_ingestion():
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

            # 🔑 robust MedDRA detection
            if not any(k in name for k in ["meddra", "medra"]):
                continue

            study_had_candidate = True
            studies_attempted.add(study_id)
            files_attempted += 1

            print(
                f"\n▶ Processing MedDRA Coding "
                f"study='{study_id}' file='{file.name}'"
            )

            df = extract_coding_meddra_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} MedDRA coding rows")

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
            print(f"⚠️ No MedDRA file detected for study '{study_id}'")

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("📊 MedDRA CODING INGESTION SUMMARY")
    print("=" * 72)
    print(f"📁 Study folders found       : {studies_seen}")
    print(f"📂 Studies attempted         : {len(studies_attempted)}")
    print(f"📄 Files attempted           : {files_attempted}")
    print(f"📥 Total rows inserted       : {total_inserted}")

    if studies_skipped:
        print(f"⚠️ Studies skipped           : {studies_skipped}")

    print("=" * 72)
    print("✅ MedDRA coding ingestion completed successfully")


if __name__ == "__main__":
    run_coding_meddra_ingestion()
