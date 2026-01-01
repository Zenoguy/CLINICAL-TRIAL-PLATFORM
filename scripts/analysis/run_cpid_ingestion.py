from pathlib import Path
import pandas as pd
from ingestion.cpid_extractor import extract_cpid_metrics


CPID_FILENAME_KEYWORD = "CPID_EDC_Metrics"
STUDY_ROOT_DIR = Path("QC Anonymized Study Files")
OUTPUT_DIR = Path("artifacts")
OUTPUT_DIR.mkdir(exist_ok=True)


def find_cpid_files(root_dir: Path) -> list[Path]:
    """
    Find all CPID EDC Metrics Excel files across all study folders.
    """
    cpid_files = []

    for study_dir in root_dir.iterdir():
        if not study_dir.is_dir():
            continue

        for file in study_dir.glob("*.xlsx"):
            if CPID_FILENAME_KEYWORD in file.name:
                cpid_files.append(file)

    return cpid_files


def run_dataset_ingestion() -> pd.DataFrame:
    """
    Run CPID ingestion across all studies and collect all snapshots.
    """
    all_snapshots = []

    cpid_files = find_cpid_files(STUDY_ROOT_DIR)

    print(f"🔍 Found {len(cpid_files)} CPID files")

    for file_path in cpid_files:
        print(f"▶ Processing {file_path}")

        try:
            df = extract_cpid_metrics(str(file_path))
            df["study_folder"] = file_path.parent.name
            all_snapshots.append(df)

        except Exception as e:
            print(f"❌ Failed processing {file_path}: {e}")

    if not all_snapshots:
        raise RuntimeError("No CPID files were successfully processed")

    combined = pd.concat(all_snapshots, ignore_index=True)
    return combined


def main():
    combined_df = run_dataset_ingestion()
    # -------------------------------------------------
    # Normalize dtypes before Parquet write (CRITICAL)
    # -------------------------------------------------
    string_cols = [
        "entity_type",
        "entity_id",
        "site_id",
        "metric_name",
        "snapshot_time",
        "source",
        "study_folder",
    ]

    for col in string_cols:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].astype("string")

    combined_df["metric_value"] = pd.to_numeric(
        combined_df["metric_value"], errors="coerce"
    )

    # -------------------------------------------------
    # Safe Parquet write
    # -------------------------------------------------
    snapshot_path = OUTPUT_DIR / "cpid_metric_snapshots.parquet"
    combined_df.to_parquet(snapshot_path, index=False, engine="pyarrow")

    # Save raw combined snapshot (DO NOT COMMIT)
    snapshot_path = OUTPUT_DIR / "cpid_metric_snapshots.parquet"
    

    print(f"✅ Saved combined snapshot to {snapshot_path}")

    # Save unique metric registry (THIS feeds metrics.md)
    metrics = (
        combined_df[["metric_name"]]
        .drop_duplicates()
        .sort_values("metric_name")
        .reset_index(drop=True)
    )

    metrics_path = OUTPUT_DIR / "cpid_metric_registry.csv"
    metrics.to_csv(metrics_path, index=False)

    print(f"📘 Saved metric registry to {metrics_path}")
    print(f"📊 Total unique metrics: {len(metrics)}")


if __name__ == "__main__":
    main()
