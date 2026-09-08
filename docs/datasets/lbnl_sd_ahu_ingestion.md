# LBNL SD-AHU Ingestion Pipeline

## Input contract

The pipeline accepts one scenario basename from the canonical scenario registry. It
requires the ZIP archive SHA-256 to match the registry and resolves the basename to
exactly one safe regular ZIP member. The raw header must exactly match `Datetime`
followed by the 30 features in the schema contract, including column order.

## Output contract

Each processed Parquet file has 42 columns:

- 31 raw columns: `Datetime` plus 30 monitored features
- 11 canonical scenario metadata columns

Model targets, encoded labels, data splits, and predictions are outside this
ingestion contract.

## Data types

`Datetime` is stored as Arrow `timestamp[ns]`. Each monitored feature is stored as
Arrow `float64`. Scenario identifiers and text metadata use Arrow strings, flags use
Arrow booleans, and `severity_value` uses nullable Arrow `int64`. Missing fault-free
severity values and singleton duplicate-group values remain Arrow nulls.

The pipeline rejects invalid timestamps, invalid numeric strings, NaT, NaN, infinity,
non-increasing timestamps, and intervals other than exactly one minute.

## Streaming implementation

The selected CSV member is streamed directly from `zipfile.ZipFile.open()` into
`pandas.read_csv()` chunks. No raw CSV is extracted to disk. Each normalized chunk is
converted independently to an Arrow table and written with
`pyarrow.parquet.ParquetWriter`; the pipeline never concatenates a complete annual
scenario in memory.

## Metadata provenance

All scenario metadata comes from `configs/lbnl_sd_ahu_scenarios.yaml`. The Parquet
schema records the dataset name, pipeline version, archive and source-content hashes,
scenario identity, resolved member path, contract paths, and PyArrow version. It does
not contain local absolute paths, hostnames, or user information.

## Compression

Parquet output uses ZSTD compression level 3 and dictionary encoding. Each normalized
input chunk is written as one row group.

## Atomic output policy

The writer creates a unique temporary sibling of the requested output. It closes and
validates that file before atomically replacing the final path. Existing outputs fail
by default and may be replaced only when `overwrite=True` is explicitly requested.

## Failure policy

Schema, registry, member-resolution, conversion, timeline, write, and read-back
failures stop the scenario immediately. A failed run removes only the temporary file
created by that run and leaves any pre-existing destination untouched.

## Smoke test

The smoke test processed the first 1,440 rows of `AHU_annual.csv` with a 500-row
chunk size, exercising three chunk writes and their boundaries. The output contains
1,440 rows and 42 columns from `2018-01-01T01:00:00` through
`2018-01-02T00:59:00`. Exact comparison of all 31 raw columns against the ignored
CSV sample passed after canonical timestamp and float64 conversion.

The tracked [smoke manifest](lbnl_sd_ahu_ingestion_smoke.json) records the generated
artifact's size, SHA-256, versions, and provenance. The local Parquet artifact remains
ignored by Git. Its byte hash applies to pipeline version 1 with PyArrow 25.0.1 and is
not asserted to remain identical across PyArrow versions or operating systems.

## Full ingestion status

Full 21-scenario ingestion has **NOT** been run yet.
