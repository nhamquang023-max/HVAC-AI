# Data Audit — LBNL Single-Duct AHU

This document consolidates the repository's verified dataset evidence for GitHub
readers, project reviewers, reproducibility work, and later benchmark development.
It distinguishes two evidence classes:

- **Official-source metadata**: statements transcribed from the LBNL landing page,
  the official inventory PDF, or the published license.
- **Project-observed facts**: machine-verified properties of the pinned archive,
  audited CSV contents, canonical Parquet outputs, and immutable manifests.

Detailed evidence remains in the linked dataset-specific records; this document
does not replace those artifacts or repeat the complete field dictionary and
scenario registry.

## Audit status

| Item | Verified value | Evidence class |
| --- | --- | --- |
| Dataset | LBNL Single-Duct AHU FDD | Official-source metadata |
| Project role | Primary V1.0 public benchmark | Project scope |
| Audit status | `verified` | Project-observed fact |
| Ingestion status | `complete_and_verified` | Project-observed fact |
| Canonical archive SHA-256 | `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471` | Project-observed fact |
| Full-ingestion manifest | [Machine-readable manifest](datasets/lbnl_sd_ahu_full_ingestion_manifest.json) | Project-observed fact |
| Manifest SHA-256 | `12AD25A5B01EC79F920318AAC72C5A6734ADA235DA55F113E9D535DC933D3970` | Project-observed fact |

## Source and licensing

| Item | Recorded value | Evidence class |
| --- | --- | --- |
| Canonical DOI used by HVAC-AI | `10.25984/1881324` | Current LBNL landing page and DOE/Data.gov catalog |
| DOI printed in the inventory PDF | `10.25984/1881321` | Official inventory PDF |
| DOI reconciliation | Official-source discrepancy retained | Project documentation policy |
| Dataset license | CC BY 4.0 | Official-source metadata |
| HVAC-AI source-code license | MIT | Repository license |

The project does not claim that either DOI is erroneous or treat the two DOI
values as equivalent canonical citations. The dataset's CC BY 4.0 license is
separate from the MIT license for HVAC-AI source code. Raw dataset artifacts are
not committed to Git.

## Archive integrity

The values below are machine observations for the pinned downloaded archive.

| Check | Result |
| --- | ---: |
| ZIP size | 607,666,899 bytes |
| Regular files | 21 |
| CSV files | 21 |
| Unsafe members | 0 |
| Encrypted members | 0 |
| CRC integrity | PASS |
| SHA-256 | `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471` |

The current LBNL webpage reports 20 CSV files, while the official inventory lists
21. The pinned downloaded archive contains 21 CSV members. All three statements
are retained; the project does not rewrite the webpage's reported value.

## Dataset shape and schema

| Layer | Composition | Columns |
| --- | --- | ---: |
| Raw CSV | 1 `Datetime` column + 30 monitored features | 31 |
| Canonical metadata | Scenario and provenance fields added by ingestion | 11 |
| Canonical processed Parquet | Raw columns + canonical metadata | 42 |

The dataset contains 21 logical scenarios and 10,818,901 processed rows. Twenty
full annual scenarios contain 525,540 rows each. One short scenario contains
308,101 rows.

## Time-series quality

The quality audit verified the following project-observed facts across all 21
logical scenarios:

| Check | Result |
| --- | ---: |
| Sample interval | 1 minute |
| Datetime parse failures | 0 |
| Duplicate timestamps | 0 |
| Non-one-minute intervals | 0 |
| Missing grid timestamps within observed bounds | 0 |
| Numerical missing values | 0 |
| Numerical parse failures | 0 |
| Positive or negative infinity | 0 |
| All-missing columns | 0 |

Each of the 20 full annual scenarios spans `2018-01-01T01:00:00` through
`2018-12-31T23:59:00` and contains 525,540 rows on a complete observed one-minute
grid. The standard 365-day reference is 525,600 rows; it is a comparison value,
not the observed row count.

## Short scenario

`damper_stuck_100_annual_short.csv` contains 308,101 rows from
`2018-04-01T01:00:00` through `2018-11-01T00:00:00`. Its observed range is a
contiguous one-minute timeline with no internal timestamp gaps. Ingestion applied
no padding, interpolation, or synthetic extension to annual coverage. A later
benchmark must preserve and explicitly account for this observed coverage.

## Duplicate-content audit

The archive contains 21 logical CSV scenarios but only 15 unique CSV byte
contents. Complete source-content SHA-256 hashes establish two byte-identical
groups.

### `duplicate_group_001`

- `oa_bias_-2_annual.csv`
- `oa_bias_-4_annual.csv`
- `oa_bias_2_annual.csv`
- `oa_bias_4_annual.csv`

### `duplicate_group_002`

- `coi_leakage_010_annual.csv`
- `coi_leakage_025_annual.csv`
- `coi_leakage_040_annual.csv`
- `coi_leakage_050_annual.csv`

All logical scenarios were retained as independent canonical Parquet files; no
deduplication was performed. Within each group, the source contents are
byte-identical. Different severity tokens therefore do not establish
distinguishable raw feature content.

## Label contract

### Level 0: fault presence

`fault_present` separates the fault-free scenario from faulted scenarios.

### Level 1: fault family

The canonical values are:

- `fault_free`
- `coi_bias`
- `coi_leakage`
- `coi_stuck`
- `damper_stuck`
- `oa_bias`

### Level 2: severity

`severity_token` preserves the source filename token, while `severity_value`
provides its direct integer form where applicable. The primary V1.0 benchmark is
fault detection and fault-family diagnosis. Severity classification must not be
the default primary benchmark. In particular, the byte-identical `oa_bias` and
`coi_leakage` severity scenarios cannot support naive severity classification
from their raw features.

## Source naming discrepancy

The pinned archive contains four `coi_bias_*` files, while the corresponding
positions in the official inventory use `sa_bias_*`. The project records this as
`unresolved_filename_mismatch`. It does not assert that `coi_bias` and `sa_bias`
are equivalent. The `coi_bias` canonical severity unit remains null; the candidate
inventory unit is not promoted to a verified fact.

## Constant-column findings

The following columns are constant within every one of the 21 scenarios:

- `OA_CFM`
- `SA_SPSPT`
- `SA_TEMPSPT`
- `SF_SPD`

`CHWC_VLV` is additionally constant within the `coi_stuck` scenarios.
`OA_DMPR` and `RA_DMPR` are additionally constant within the `damper_stuck`
scenarios, including the short scenario.

A variable must not be removed mechanically because its variance is zero within
one scenario. Any later feature pruning must use cross-scenario analysis, be fit
only on the training partition, and prevent information leakage.

## Canonical ingestion

| Property | Verified value |
| --- | --- |
| Input method | ZIP streaming |
| Raw CSV extraction to disk | No |
| Output format | Parquet |
| Engine | PyArrow 25.0.1 |
| Compression | ZSTD level 3 |
| Chunk size | 50,000 rows |
| Canonical Parquet files | 21 |
| Canonical schema | 42 columns |
| Full dataset validation | PASS |
| Total Parquet size | 1,033,858,823 bytes (approximately 0.963 GiB) |
| Atomic publication | PASS |
| Remaining staging directories | 0 |

## Repository data policy

Git tracks the dataset configs, provenance records, schemas, documentation, and
manifests required to reproduce and audit the pipeline. Git does not track the
raw ZIP, raw CSV files, or processed Parquet files. Repository records use relative
paths and omit machine-specific locations.

## Benchmark-readiness assessment

| Item | Status | Implication |
| --- | --- | --- |
| Provenance | Verified | Source identity, DOI discrepancy, and artifact hashes are recorded. |
| Archive integrity | Verified | The pinned archive passed structure, safety, CRC, and hash checks. |
| Schema | Verified | The 31-column raw and 42-column processed contracts are fixed. |
| Field semantics | Verified with one source naming discrepancy | All 30 monitored fields are documented; `coi_bias`/`sa_bias` remains unresolved. |
| Labels | Verified with severity constraints | Fault presence and family labels are usable; severity requires a separate policy. |
| Numerical quality | Verified | Audited values contain no missing, parse-failure, or infinite values. |
| Timeline quality | Verified | Observed grids are continuous; the short scenario has distinct coverage. |
| Duplicate-content issue | Verified constraint | Split design must prevent identical-content leakage. |
| Short-scenario coverage | Verified constraint | Evaluation must respect observed coverage without synthetic filling. |
| Full ingestion | `complete_and_verified` | Twenty-one canonical Parquet artifacts passed dataset-level validation. |
| Train/validation/test split | Not defined | A reproducible time-aware split contract is still required. |
| Feature engineering | Not performed | Transformations and pruning must be defined after the split contract. |
| Model benchmark | Not performed | Training and evaluation results cannot yet be reported. |

Data ingestion is benchmark-preparation ready; the benchmark protocol remains to
be defined.

## Downstream benchmark guardrails

1. Do not use a naive random row split as the default benchmark split.
2. Use a fixed, reproducible, time-aware train/validation/test policy.
3. Prevent identical-content leakage across data partitions.
4. Do not pad or interpolate the short scenario.
5. Fit feature preprocessing and summary statistics only on the training partition.
6. Do not prune constant columns globally before partitioning the data.
7. Do not use scenarios with `severity_content_distinguishable = false` in a naive
   severity benchmark.
8. Do not silently remove logical scenarios because their source bytes are identical.
9. Report fault detection, fault-family diagnosis, and any separately designed
   severity task as distinct evaluations.
10. Preserve dataset and artifact provenance in every later benchmark.

This audit intentionally does not define train, validation, or test time boundaries.

## Evidence links

- [Dataset overview](datasets/lbnl_sd_ahu.md)
- [Archive provenance](datasets/lbnl_sd_ahu_archive_provenance.md)
- [Archive manifest](datasets/lbnl_sd_ahu_archive_manifest.json)
- [Inventory reconciliation](datasets/lbnl_sd_ahu_inventory_reconciliation.md)
- [Data dictionary](datasets/lbnl_sd_ahu_data_dictionary.md)
- [Data quality audit](datasets/lbnl_sd_ahu_data_quality.md)
- [Data quality manifest](datasets/lbnl_sd_ahu_data_quality.json)
- [Duplicate-content audit](datasets/lbnl_sd_ahu_duplicate_audit.md)
- [Label contract](datasets/lbnl_sd_ahu_label_contract.md)
- [Ingestion contract](datasets/lbnl_sd_ahu_ingestion.md)
- [Smoke-test manifest](datasets/lbnl_sd_ahu_ingestion_smoke.json)
- [Full-ingestion manifest](datasets/lbnl_sd_ahu_full_ingestion_manifest.json)
- [Canonical schema](../configs/lbnl_sd_ahu_schema.yaml)
- [Canonical scenario registry](../configs/lbnl_sd_ahu_scenarios.yaml)
- [Dataset license record](../DATA_LICENSES.md)
