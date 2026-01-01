from ingestion.cpid_extractor import extract_cpid_metrics
from pathlib import Path

def test_cpid_extractor_smoke():
    filepath = Path(
        "QC Anonymized Study Files/"
        "Study 7_CPID_Input Files - Anonymization/"
        "Study 7_CPID_EDC_Metrics_URSV2.0_updated.xlsx"
    )

    assert filepath.exists(), "Test CPID file does not exist"

    df = extract_cpid_metrics(str(filepath))

    assert not df.empty
    assert "metric_name" in df.columns
    assert any("__bucket_0" in m for m in df["metric_name"])
