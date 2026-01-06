from pathlib import Path

from ingestion.coding_whodrug_extractor import extract_coding_whodrug_events
from storage.supabase_writer import insert_dataframe


STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
TARGET_TABLE = "coding_whodrug_events"


def run_coding_whodrug_ingestion():
    # ------------------------------------------------------------------
    # Counters / diagnostics
    # ------------------------------------------------------------------
    studies_seen = 0
    studies_attempted: set[str] = set()
    files_attempted = 0
    total_inserted = 0

    print(f"📂 Scanning WHODrug root directory: {STUDY_ROOT_DIR}")

    # ------------------------------------------------------------------
    # Traverse study folders
    # ------------------------------------------------------------------
    for study_dir in STUDY_ROOT_DIR.iterdir():
        if not study_dir.is_dir():
            continue

        studies_seen += 1
        study_id = study_dir.name

        for file in study_dir.glob("*.xlsx"):
            name = file.name.lower()

            # 🔑 Robust WHODrug discovery (covers ALL observed variants)
            if not any(
                k in name
                for k in ["who", "whodd", "whodrug", "whodra"]
            ):
                continue

            studies_attempted.add(study_id)
            files_attempted += 1

            print(
                f"\n▶ Processing WHODrug Coding "
                f"study='{study_id}' file='{file.name}'"
            )

            # -------------------------------------------------
            # 1️⃣ Extract WHODrug coding events
            # -------------------------------------------------
            df = extract_coding_whodrug_events(
                filepath=str(file),
                study_id_override=study_id,
            )

            print(f"📦 Extracted {len(df)} WHODrug coding rows")

            if df.empty:
                print("⚠️ Empty extract — skipping insert")
                continue

            # -------------------------------------------------
            # 2️⃣ Insert into Supabase
            # -------------------------------------------------
            insert_dataframe(
                df=df,
                table_name=TARGET_TABLE,
                batch_size=1000,
            )

            total_inserted += len(df)

    # ------------------------------------------------------------------
    # Final diagnostic summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("📊 WHODRUG CODING INGESTION SUMMARY")
    print("=" * 72)
    print(f"📁 Study folders found       : {studies_seen}")
    print(f"📂 Studies attempted         : {len(studies_attempted)}")
    print(f"📄 Files attempted           : {files_attempted}")
    print(f"📥 Total rows inserted       : {total_inserted}")

    skipped = {
        d.name for d in STUDY_ROOT_DIR.iterdir() if d.is_dir()
    } - studies_attempted

    if skipped:
        print(f"⚠️ Studies skipped           : {sorted(skipped)}")

    print("=" * 72)

    if files_attempted == 0:
        print("⚠️ No WHODrug files were ingested.")
    else:
        print("✅ WHODrug coding ingestion completed successfully")


if __name__ == "__main__":
    run_coding_whodrug_ingestion()
