# CPID Metric Registry

This document enumerates all CPID-derived metrics available in the system.
These metrics are **structural contracts** derived from CPID EDC files.
No thresholds or interpretations are defined here.

---

## Naming Convention

Metric names follow the pattern:

`<group>__<base_metric>__[bucket_<n>]`

Where:
- `group` indicates the CPID domain (e.g. `cpmd`, `ssm`)
- `base_metric` is derived from the CPID column header
- `bucket_n` is used only when CPID provides repeated columns

---
## `cpmd__page_action_status_source_rave_edc_bo4`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: CPMD
- **Buckets**: bucket_0 … bucket_5
- **Description**: Operational metric derived directly from CPID EDC data.

## `cpmd__page_status_source_rave_edc_bo4`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: CPMD
- **Buckets**: bucket_0 … bucket_1
- **Description**: Operational metric derived directly from CPID EDC data.

## `cpmd__protocol_deviations_sourcerave_edc_bo4`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: CPMD
- **Buckets**: bucket_0 … bucket_1
- **Description**: Operational metric derived directly from CPID EDC data.

## `cpmd__queries_status_sourcerave_edc_bo4`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: CPMD
- **Buckets**: bucket_0 … bucket_7
- **Description**: Operational metric derived directly from CPID EDC data.

## `cpmd__visit_status`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: CPMD
- **Buckets**: ❌ (not bucketed)
- **Description**: Operational metric derived directly from CPID EDC data.

## `ssm__pi_signatures_source_rave_edc_bo4`

- **Entity level**: subject
- **Source**: CPID_EDC_Metrics
- **Group**: SSM
- **Buckets**: bucket_0 … bucket_5
- **Description**: Operational metric derived directly from CPID EDC data.
 

 ---
>Buckets represent repeated CPID columns (e.g. Page Status.1, Page Status.2) and do not imply ordering, severity, or priority.
