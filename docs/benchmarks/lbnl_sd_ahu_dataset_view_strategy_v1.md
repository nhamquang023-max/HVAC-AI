# LBNL SD-AHU Primary Benchmark Dataset View Strategy V1

## 1. Purpose

This document freezes the data-access architecture for the LBNL SD-AHU Primary
Benchmark V1. It decides how later training, validation, and test consumers will
access the verified canonical data without changing the already frozen split.

**No train, validation, or test dataset has been physically materialized.**

The machine-readable strategy is
[`lbnl_sd_ahu_dataset_view_v1.yaml`](../../configs/benchmarks/lbnl_sd_ahu_dataset_view_v1.yaml).
This step defines architecture only. It does not implement a dataset loader.

## 2. Current benchmark state

The canonical full ingestion is `complete_and_verified`. It contains 21 logical
scenarios and 10,818,901 rows in 21 Parquet files. Primary Benchmark Split V1
defines one shared chronological schedule for all scenarios, and the split
engine validates that contract and assigns timestamps. A read-only dry-run of
the engine previously assigned all 10,818,901 real rows successfully.

The authoritative inputs are:

- `data/processed/lbnl_sd_ahu/scenarios`
- `docs/datasets/lbnl_sd_ahu_full_ingestion_manifest.json`
- `configs/lbnl_sd_ahu_scenarios.yaml`
- `configs/lbnl_sd_ahu_schema.yaml`
- `configs/benchmarks/lbnl_sd_ahu_split_v1.yaml`
- `src/hvac_ai/data/lbnl_sd_ahu_split.py`

## 3. Problem statement

Downstream code needs partition-correct access while preserving a single
canonical dataset. The architecture must avoid redundant data, avoid millions
of derivable row-index records, exclude purge and outside rows from ordinary
modeling access, project only requested columns, and support bounded-memory
iteration. It must also preserve all 21 logical scenarios and the existing
provenance chain.

## 4. Evaluated strategies

| Strategy | Benefit | Main costs | Primary V1 decision |
| --- | --- | --- | --- |
| A — Physical partition materialization | Simple downstream reads | Duplicated storage, a second copy of canonical truth, provenance synchronization, regeneration after contract changes, stale artifacts, and possible repeated feature-pipeline materialization | Not recommended as the default |
| B — Row-level index manifest | Explicit membership | Millions of redundant records, extra artifact/version synchronization, additional index-reading complexity, and no clear benefit when membership is deterministically derived from a contiguous one-minute timestamp contract | Not recommended as the default |
| C — Dynamic logical dataset views | One canonical dataset, deterministic membership, projection, streaming, and compatible timestamp predicates | Runtime work remains and performance depends on reader behavior and Parquet layout | Selected |

## 5. Selected Strategy C

Primary V1 selects `dynamic_logical_views`. A view combines the canonical
Parquet files, the frozen Split V1 contract, the existing split engine, and an
inclusive timestamp predicate for the requested modeling partition. The view is
logical: it does not copy rows or become a second source of truth.

The later implementation must load and validate the split contract. It must
reuse `src/hvac_ai/data/lbnl_sd_ahu_split.py` for contract, assignment, and
evidence semantics. It must not implement a separate partition decision tree or
copy the timestamps into a second hard-coded boundary table.

## 6. Why physical materialization is rejected

Separate train, validation, and test files would duplicate millions of values
that already exist in the canonical Parquet files. They would introduce a
second data copy whose hashes, provenance, schema, compression, and lifecycle
would need synchronization. A split-contract revision would require complete
regeneration, and stale partitions could silently survive. Later feature
pipelines may also require controlled derived artifacts, making default raw
partition copies an unnecessary earlier materialization step.

Physical materialization is therefore prohibited by default. A future exception
requires a separate explicit contract with provenance, validation, and lifecycle
rules.

## 7. Why a row-level index manifest is rejected

Primary V1 membership is a deterministic function of `Datetime` and the frozen
inclusive boundaries. Saving `scenario + row number + partition` for 6,470,121
common-universe rows would repeat information already recoverable from the
contract. The artifact would add millions of records, its own version and
synchronization burden, and an extra input that every reader would need to join
or interpret. The continuous one-minute timelines provide no current benefit
that outweighs this complexity.

No row-level index manifest will be created for the default architecture.

## 8. Canonical source-of-truth chain

```text
Pinned archive
    ↓
Canonical full-ingestion manifest
    ↓
21 canonical Parquet files
    ↓
Primary Split V1 contract
    ↓
Split engine
    ↓
Dynamic logical dataset views
    ↓
Later preprocessing / feature pipeline
    ↓
Model
```

A view introduces no new data fact. The full-ingestion manifest identifies the
files and their hashes, the scenario registry and manifest provide deterministic
scenario identity and order, and the split contract defines membership.
Filesystem glob order is never the formal benchmark order.

## 9. Modeling-view semantics

The ordinary modeling API exposes exactly three views:

- `train`
- `validation`
- `test`

Each view retains all 21 logical scenarios. The dataset-view layer performs no
physical deduplication and does not drop byte-identical scenarios. It returns
the contract-defined population for the requested partition.

The states `purge_train_validation`, `purge_validation_test`, and
`outside_primary_benchmark_scope` remain valid split outcomes, but they are
excluded from ordinary modeling views. Audit or debugging access to an excluded
state must be an explicit opt-in.

## 10. Expected row counts

| View or excluded population | Rows per scenario | Rows across scenarios | Modeling view |
| --- | ---: | ---: | --- |
| Train | 175,620 | 3,688,020 | Yes |
| Validation | 64,800 | 1,360,800 | Yes |
| Test | 64,801 | 1,360,821 | Yes |
| Train → validation purge | 1,440 | 30,240 | No |
| Validation → test purge | 1,440 | 30,240 | No |
| Outside Primary V1 | Varies; zero for the short scenario | 4,348,780 | No |

The three modeling views contain 6,409,641 rows:

```text
3,688,020 + 1,360,800 + 1,360,821 = 6,409,641
```

The two purge populations contain 60,480 rows. They are part of the 6,470,121
row common universe but not part of modeling views. The 4,348,780 outside rows
remain in the canonical annual files and are excluded from Primary V1.

## 11. Streaming and memory policy

The default access mode is `streaming_batches`. The future API should expose an
iterator, Arrow `RecordBatch`, or chunked DataFrame interface. It must not return
the entire 6,409,641-row modeling population, or the complete train view, as one
pandas DataFrame by default.

Batch size is configurable and does not affect logical membership. A practical
starting range is 50,000–200,000 rows. A caller may explicitly request a larger
in-memory result and accept its memory cost, but that is not the default API.

The view layer remains neutral to scikit-learn, XGBoost, LightGBM, PyTorch, and
TensorFlow. Framework adapters may be added later without changing membership.

## 12. Column projection policy

Every call must explicitly request columns. `Datetime` may be included as the
filtering and validation column. Labels and scenario metadata are returned only
when requested for the task. The view layer must not read all 42 columns when a
consumer needs only a subset.

The future feature contract, rather than this strategy, owns the final ML
feature subset. This strategy defines neither mechanism features nor purely
data-driven features.

## 13. Predicate filtering and Parquet metadata audit

The read-only audit inspected only Parquet metadata, Arrow schema, and
`Datetime` row-group statistics. It did not scan feature rows.

| Metadata check | Observed result |
| --- | ---: |
| Canonical Parquet files | 21 |
| Filename set equals full manifest | PASS |
| File row counts equal manifest | PASS |
| Row-group counts equal manifest | PASS |
| Total row groups | 227 |
| Minimum row groups per file | 7 |
| Maximum row groups per file | 11 |
| `Datetime` Arrow type | `timestamp[ns]` |
| Row groups with `Datetime` min/max statistics | 227 / 227 |
| Files with complete `Datetime` statistics | 21 / 21 |
| Files with missing `Datetime` statistics | 0 |
| Row-group ranges chronologically non-decreasing | PASS |
| Row-group ranges follow source order without overlap | PASS |
| First/last statistics match manifest endpoints | PASS |

These observations support row-group pruning for timestamp predicates. A future
reader can request the logical equivalent of `Datetime >= partition_start AND
Datetime <= partition_end` through a PyArrow-compatible filter. The actual
performance benefit still depends on the chosen reader correctly applying
predicate or row-group pruning; this strategy does not promise a particular
speedup.

Membership semantics always come from the frozen Split V1 contract. Metadata
pruning is an execution optimization, not a second definition of membership.
No metadata issue currently blocks dynamic filtering.

## 14. Duplicate-content boundary

The view layer preserves all logical members of both verified duplicate groups.
It performs no physical deduplication, duplicate sampling, duplicate weighting,
class weighting, or class balancing. Its responsibility ends at correct
partition access.

The status remains `required_before_model_training`. Dataset View implementation
may finish before the weighting or sampling contract, but formal model training
must not begin until that policy is frozen.

## 15. Class-imbalance boundary

The view layer does not oversample, downsample, rebalance, or apply class
weights. Any future training sampling policy must operate on the train view
only. Validation and test preserve their natural contract populations so that
evaluation is not altered by sampling policy.

## 16. Purge and outside handling

Purge rows belong to neither train, validation, nor test. An ordinary request
for a modeling view never returns them. A future audit interface may return
purge rows only through an explicit opt-in.

Rows labeled `outside_primary_benchmark_scope` likewise belong to no Primary V1
modeling view. The fact that 20 canonical files contain full annual coverage
must not cause those rows to enter training implicitly.

## 17. Short-scenario handling

`damper_stuck_100_annual_short.parquet` uses the same timestamp predicates as
the other 20 scenarios. It naturally contributes 175,620 train rows, 64,800
validation rows, and 64,801 test rows. It requires no padding, interpolation,
reindexing, extrapolation, or synthetic fill.

## 18. Lightweight manifest policy

A future reproducibility step may create a lightweight view manifest. It may
record the view and split IDs and versions, archive SHA-256, full-ingestion
manifest SHA-256, ordered source filenames, existing Parquet hashes from the
full manifest, expected counts, and software provenance.

It must not contain per-row indices. No lightweight view manifest is generated
in this architecture-design step.

## 19. Reproducibility and source immutability

Views are read-only. They must not modify canonical Parquet, append a partition
column, rewrite compression, alter schema, reorder rows, or persist temporary
labels into source data. Deterministic scenario iteration comes from canonical
metadata, and membership comes from the validated split contract and engine.

The pinned archive SHA-256 is
`8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`.
The full-ingestion manifest SHA-256 is
`12AD25A5B01EC79F920318AAC72C5A6734ADA235DA55F113E9D535DC933D3970`.

## 20. Known limitations

- Dynamic views do not solve duplicate weighting.
- Dynamic views do not solve class imbalance.
- This strategy does not define features.
- This strategy does not define preprocessing.
- This strategy does not define model sampling.
- The Primary V1 common window is not a complete annual benchmark.
- Runtime performance depends on Parquet row-group layout and reader pruning.
- Some ML algorithms may later require controlled in-memory materialization of
  selected training batches or samples.

These concerns remain owned by later explicit contracts; the dataset-view layer
does not resolve them implicitly.

## 21. Next implementation step

After architecture review, a separate step may implement a read-only,
contract-driven Dataset View loader with projected columns, timestamp predicate
filtering, deterministic manifest/registry ordering, and streaming batches. That
step must validate its outputs against the frozen row counts and must not begin
feature engineering, weighting, or model training.
