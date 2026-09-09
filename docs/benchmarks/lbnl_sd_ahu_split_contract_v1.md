# LBNL SD-AHU Primary Benchmark Split Contract V1

## Purpose

This contract freezes a reproducible, time-aware, duplicate-aware, and
short-scenario-safe split architecture for the LBNL Single-Duct AHU primary V1.0
benchmark. Its split ID is `lbnl_sd_ahu_primary_split_v1`. It applies one
timestamp schedule to all 21 logical scenarios.

**This document defines the split contract only. No split artifact has yet been
materialized.**

The machine-readable contract is
[`lbnl_sd_ahu_split_v1.yaml`](../../configs/benchmarks/lbnl_sd_ahu_split_v1.yaml).

## Source evidence

The contract derives its counts, coverage, labels, and duplicate groups from
existing verified repository evidence:

- [Consolidated data audit](../data_audit.md)
- [Label contract](../datasets/lbnl_sd_ahu_label_contract.md)
- [Data quality audit](../datasets/lbnl_sd_ahu_data_quality.md)
- [Duplicate-content audit](../datasets/lbnl_sd_ahu_duplicate_audit.md)
- [Full-ingestion manifest](../datasets/lbnl_sd_ahu_full_ingestion_manifest.json)
- [Scenario registry](../../configs/lbnl_sd_ahu_scenarios.yaml)
- [Schema contract](../../configs/lbnl_sd_ahu_schema.yaml)

The pinned archive SHA-256 is
`8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`.
The full-ingestion manifest SHA-256 is
`12AD25A5B01EC79F920318AAC72C5A6734ADA235DA55F113E9D535DC933D3970`.

## Primary benchmark universe

Primary V1 uses the common observed temporal overlap across all 21 logical
scenarios. The range is `2018-04-01T01:00:00` through
`2018-11-01T00:00:00`, inclusive. This is the complete observed coverage of
`damper_stuck_100_annual_short.csv`.

Every scenario contributes 308,101 rows, for 6,470,121 rows across 21 scenarios.
This shared support keeps labels on the same seasonal and operating-time basis,
retains the short scenario without synthetic data, and preserves all logical
duplicate members.

## Selected split

Primary V1 uses common overlap, fixed chronological boundaries, and two 24-hour
purge gaps. Every logical scenario uses the same boundaries. Purge rows remain
accounted for in the common universe but are excluded from train, validation,
and test.

## Exact boundary table

| Segment | Inclusive start | Inclusive end | Modeling role |
| --- | --- | --- | --- |
| Train | `2018-04-01T01:00:00` | `2018-07-31T23:59:00` | Training |
| Train → validation purge | `2018-08-01T00:00:00` | `2018-08-01T23:59:00` | Excluded purge gap |
| Validation | `2018-08-02T00:00:00` | `2018-09-15T23:59:00` | Validation |
| Validation → test purge | `2018-09-16T00:00:00` | `2018-09-16T23:59:00` | Excluded purge gap |
| Test | `2018-09-17T00:00:00` | `2018-11-01T00:00:00` | Testing |

## Exact row-count table

| Segment | Rows per scenario | Rows across 21 scenarios |
| --- | ---: | ---: |
| Train | 175,620 | 3,688,020 |
| Train → validation purge | 1,440 | 30,240 |
| Validation | 64,800 | 1,360,800 |
| Validation → test purge | 1,440 | 30,240 |
| Test | 64,801 | 1,360,821 |
| **Common universe** | **308,101** | **6,470,121** |

The independently calculated inclusive-minute invariants are:

```text
175620 + 1440 + 64800 + 1440 + 64801 = 308101
3688020 + 30240 + 1360800 + 30240 + 1360821 = 6470121
308101 × 21 = 6470121
```

## Purge-gap rationale

Each purge gap is 1,440 minutes, or 24 hours. The gaps reduce leakage from
adjacent-minute autocorrelation, operating episodes, and later causal lag or
rolling features. Purge rows are not modeling samples. If a future feature needs
more than 1,440 minutes of historical lookback, the purge policy and split
contract must be reviewed rather than silently reused.

## Short-scenario policy

`damper_stuck_100_annual_short.csv` remains unchanged at 308,101 rows from
`2018-04-01T01:00:00` through `2018-11-01T00:00:00`. It participates in train,
validation, and test under the shared boundaries. Padding, interpolation,
extrapolation, annual reindexing, and synthetic extension are prohibited.

## Duplicate-content leakage policy

All members of both verified byte-identical groups remain as logical scenarios:

- `duplicate_group_001`: `oa_bias_-2_annual.csv`, `oa_bias_-4_annual.csv`,
  `oa_bias_2_annual.csv`, and `oa_bias_4_annual.csv`
- `duplicate_group_002`: `coi_leakage_010_annual.csv`,
  `coi_leakage_025_annual.csv`, `coi_leakage_040_annual.csv`, and
  `coi_leakage_050_annual.csv`

Physical deduplication is prohibited. For any timestamp, every member of a group
must receive the same partition assignment. The shared timestamp boundaries
enforce this invariant and prevent the same source content at the same timestamp
from appearing in train for one member and validation or test for another.

## Duplicate-weighting unresolved requirement

Equal row weighting would multiply byte-identical source content fourfold in each
duplicate group and can overweight those fault families. The final weighting or
sampling algorithm is not defined here. Its status is
`required_before_model_training`, and formal model training must not begin until
that policy is frozen. Logical scenarios must not be deleted as a workaround.

## Class imbalance guardrail

The universe has one fault-free and 20 faulted logical scenarios, creating
structural imbalance in raw binary fault-detection rows. No downsampling or
oversampling occurs before splitting. Validation and test must preserve the
contract population and must not be resampled to manufacture balance. Future
class weighting or sampling may be selected only from the training partition.
The concrete model `class_weight` and evaluation metrics remain undefined; a
later metric contract must select measures appropriate for imbalance.

## Preprocessing leakage policy

Every data-driven parameter must be fit on train and then applied unchanged to
validation and test. This includes means, standard deviations, minima, maxima,
quantiles, feature-selection statistics, PCA, imputation statistics, scalers,
learned transforms, and resampling rules. Computing these parameters from the
full dataset is prohibited.

## Time-feature and rolling-feature policy

Lag, rolling, trend, and window features must be causal and past-only. Future
looking windows are prohibited. Feature computation must not cross a partition
boundary to consume observations assigned to another partition. The 1,440-minute
purge contract supports historical lookbacks no longer than 1,440 minutes; a
longer maximum lookback requires explicit contract review.

## Outside-primary rows

The 20 full annual scenarios contain verified observations before
`2018-04-01T01:00:00` and after `2018-11-01T00:00:00`. These rows remain unchanged
and are marked `outside_primary_benchmark_scope`. They belong to no Primary V1
train, validation, test, or purge segment and must not be silently added to
Primary V1 training. They may later support auxiliary analysis, robustness
experiments, or a separately defined secondary benchmark.

## Task applicability

This split applies to the primary V1.0 tasks defined by the existing label
contract: fault detection and fault-family diagnosis. It does not redefine label
semantics. Severity is `not_primary`. Naive severity classification is prohibited
for `oa_bias` and `coi_leakage`, whose duplicate-group members have
`severity_content_distinguishable = false`.

## Rejected alternatives

### Naive random row split — FORBIDDEN

`train_test_split(..., shuffle=True)` is not a valid primary benchmark split.
Adjacent-minute autocorrelation, future or near-future leakage, rolling/lag
feature leakage, and random separation of one operating episode would inflate
apparent generalization and would not represent forward deployment.

### Independent per-scenario percentage split — FORBIDDEN

Splitting each scenario independently and then concatenating partitions would
misalign timestamps, expose labels to different seasonal support, and compound
coverage differences between full and short scenarios.

### Padding the short scenario — FORBIDDEN

The short scenario must not be extended to January–December coverage.

### Scenario holdout — not Primary V1

Scenario or severity holdout may be designed later as a robustness benchmark. It
is not part of Primary Benchmark Split V1, and no second split is introduced here.

## Reproducibility invariants

- All 21 scenarios use identical inclusive timestamp boundaries.
- No segment overlaps another and no timestamp is assigned twice.
- The five segments account for every common-universe timestamp exactly once.
- Purge rows are explicit but excluded from all three modeling partitions.
- Duplicate-group members receive synchronized timestamp assignments.
- Source Datetime values remain naive with timezone unspecified. The project does
  not infer a timezone, localize timestamps, or invent daylight-saving correction.
- Boundary strings use quoted ISO-8601-like naive timestamps in YAML.
- Rows outside the common universe remain intact and excluded from Primary V1.

## Known limitations

- Duplicate-content weighting and sampling remain unresolved.
- The final class-imbalance metric and training strategy remain undefined.
- The 1,440-minute purge requires review for longer historical feature windows.
- This common-overlap design excludes real full-scenario rows from Primary V1.
- Scenario or severity holdout robustness protocols are deferred.
- No feature, model, or evaluation result is established by this contract.

## Next implementation step

After architecture review and repository approval, a separate implementation step
may materialize deterministic split membership from this contract and validate
the resulting row assignments. That step must not alter source Parquet files and
must preserve all provenance and leakage controls defined here.
