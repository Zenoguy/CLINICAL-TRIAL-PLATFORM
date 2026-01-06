from pathlib import Path

from core.config import get_settings
from ingestion.cpid_extractor import extract_cpid_metrics
from storage.supabase_writer import insert_dataframe


CPID_FILENAME_KEYWORD = "CPID_EDC_Metrics"
TARGET_TABLE = "cpid_metric_snapshots"


def find_cpid_files(root_dir: Path) -> list[Path]:
    """
    Discover all CPID EDC Metrics Excel files across study folders.
    """
    files: list[Path] = []

    for study_dir in root_dir.iterdir():
        if not study_dir.is_dir():
            continue

        for file in study_dir.glob("*.xlsx"):
            if CPID_FILENAME_KEYWORD in file.name:
                files.append(file)

    return files


def main():
    settings = get_settings()
    root_dir = Path(settings.data.cpid_root_dir)

    if not root_dir.exists():
        raise FileNotFoundError(f"CPID root directory not found: {root_dir}")

    cpid_files = find_cpid_files(root_dir)

    print(f"🔍 Found {len(cpid_files)} CPID files")

    if not cpid_files:
        print("⚠️ No CPID files found — exiting")
        return

    total_inserted = 0

    for file_path in cpid_files:
        study_folder = file_path.parent.name

        print(
            f"\n▶ Processing study_folder='{study_folder}' "
            f"file='{file_path.name}'"
        )

        # -------------------------------------------------
        # 1️⃣ Extract CPID metrics (numeric-only enforced)
        # -------------------------------------------------
        df = extract_cpid_metrics(str(file_path))

        if df.empty:
            print("⚠️ No numeric metrics extracted — skipping")
            continue

        print(f"📦 Extracted {len(df)} metric rows")

        # -------------------------------------------------
        # 2️⃣ Insert into Supabase
        # -------------------------------------------------
        insert_dataframe(
            df=df,
            table_name=TARGET_TABLE,
        )

        total_inserted += len(df)

    print(
        f"\n✅ CPID ingestion completed successfully "
        f"(total rows inserted: {total_inserted})"
    )


if __name__ == "__main__":
    main()
