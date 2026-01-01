from pathlib import Path
import pandas as pd
import re
from collections import defaultdict


REGISTRY_PATH = Path("artifacts/cpid_metric_registry.csv")
OUTPUT_PATH = Path("docs/data-dictionary/metrics.md")


def parse_metric(metric_name: str):
    """
    Splits metric into:
    - base metric name
    - bucket index (or None)
    """
    match = re.match(r"(.+?)__bucket_(\d+)$", metric_name)
    if match:
        return match.group(1), int(match.group(2))
    return metric_name, None


def infer_group(base_metric: str) -> str:
    """
    Infers logical group from metric prefix.
    """
    prefix = base_metric.split("__", 1)[0]
    return {
        "cpmd": "CPMD",
        "ssm": "SSM",
        "input_files": "Input Files",
    }.get(prefix, prefix.upper())


def main():
    df = pd.read_csv(REGISTRY_PATH)

    grouped = defaultdict(list)

    for metric in df["metric_name"]:
        base, bucket = parse_metric(metric)
        grouped[base].append(bucket)

    lines = []
    lines.append("# CPID Metric Registry\n")
    lines.append(
        "This document enumerates all CPID-derived metrics available in the system.\n"
        "These metrics are **structural contracts** derived from CPID EDC files.\n"
        "No thresholds or interpretations are defined here.\n\n"
        "---\n"
    )

    for base_metric in sorted(grouped.keys()):
        buckets = sorted(b for b in grouped[base_metric] if b is not None)
        is_bucketed = len(buckets) > 0

        group = infer_group(base_metric)

        lines.append(f"## `{base_metric}`\n")
        lines.append(f"- **Entity level**: subject")
        lines.append(f"- **Source**: CPID_EDC_Metrics")
        lines.append(f"- **Group**: {group}")

        if is_bucketed:
            lines.append(
                f"- **Buckets**: bucket_0 … bucket_{max(buckets)}"
            )
        else:
            lines.append(f"- **Buckets**: ❌ (not bucketed)")

        lines.append(
            "- **Description**: Operational metric derived directly from CPID EDC data."
        )
        lines.append("")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("\n".join(lines))

    print(f"✅ Generated {OUTPUT_PATH}")
    print(f"📊 Total metric families: {len(grouped)}")


if __name__ == "__main__":
    main()
