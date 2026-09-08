"""Stream canonical LBNL SD-AHU scenarios from ZIP members into Parquet."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

PIPELINE_VERSION = 1
DATASET_NAME = "LBNL_SD_AHU"
DEFAULT_CHUNK_SIZE = 50_000
ARCHIVE_SHA256 = "8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471"

METADATA_COLUMNS = (
    "scenario_id",
    "source_filename",
    "fault_present",
    "fault_family",
    "severity_token",
    "severity_value",
    "severity_unit",
    "is_short",
    "content_sha256",
    "byte_identical_group",
    "severity_content_distinguishable",
)

FILE_METADATA_KEYS = (
    "dataset",
    "pipeline_version",
    "archive_sha256",
    "scenario_id",
    "source_filename",
    "source_member",
    "source_content_sha256",
    "schema_contract",
    "scenario_registry",
    "pyarrow_version",
)


class IngestionError(ValueError):
    """Raised when source data violates the canonical ingestion contract."""


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise IngestionError(f"{label} must be a YAML mapping: {path}")
    return value


def load_schema_contract(path: Path) -> dict[str, Any]:
    """Load and validate the raw 31-column schema contract."""
    contract = _load_yaml_mapping(Path(path), "Schema contract")
    timestamp = contract.get("timestamp_column")
    features = contract.get("feature_columns")
    if timestamp != "Datetime":
        raise IngestionError("Schema timestamp_column must be Datetime")
    if not isinstance(features, list) or len(features) != 30:
        raise IngestionError("Schema contract must declare exactly 30 feature columns")
    if any(not isinstance(column, str) or not column for column in features):
        raise IngestionError("Every feature column must be a non-empty string")
    if len(set(features)) != len(features) or timestamp in features:
        raise IngestionError("Schema feature columns must be unique and exclude Datetime")
    if contract.get("feature_count") != 30 or contract.get("total_csv_columns") != 31:
        raise IngestionError("Schema count fields must declare 30 features and 31 raw columns")
    return contract


def _load_scenario(path: Path, filename: str) -> tuple[dict[str, Any], dict[str, Any]]:
    registry = _load_yaml_mapping(Path(path), "Scenario registry")
    scenarios = registry.get("scenarios")
    if not isinstance(scenarios, list):
        raise IngestionError("Scenario registry must contain a scenarios list")
    matches = [item for item in scenarios if item.get("filename") == filename]
    if len(matches) != 1:
        raise IngestionError(f"Expected one registry scenario for {filename}, got {len(matches)}")
    scenario = matches[0]
    required = {
        "scenario_id",
        "filename",
        "fault_present",
        "fault_family",
        "severity_token",
        "severity_value",
        "severity_unit",
        "is_short",
        "content_sha256",
        "byte_identical_group",
        "severity_content_distinguishable",
    }
    missing = required - set(scenario)
    if missing:
        raise IngestionError(f"Registry scenario {filename} lacks fields: {sorted(missing)}")
    source = registry.get("source")
    if not isinstance(source, dict):
        raise IngestionError("Scenario registry must contain a source mapping")
    archive_hash = source.get("archive_sha256")
    if not isinstance(archive_hash, str) or len(archive_hash) != 64:
        raise IngestionError("Scenario registry source.archive_sha256 must be a SHA-256")
    return registry, scenario


def _safe_regular_member(info: zipfile.ZipInfo) -> bool:
    path = PurePosixPath(info.filename)
    mode = (info.external_attr >> 16) & 0xFFFF
    return (
        not info.is_dir()
        and not path.is_absolute()
        and ".." not in path.parts
        and "\\" not in info.filename
        and not (path.parts and ":" in path.parts[0])
        and not stat.S_ISLNK(mode)
    )


def resolve_archive_member(archive: zipfile.ZipFile, filename: str) -> zipfile.ZipInfo:
    """Resolve a registry basename to exactly one safe regular ZIP member."""
    if Path(filename).name != filename:
        raise IngestionError(f"Scenario must be a basename, got: {filename}")
    matches = [
        info
        for info in archive.infolist()
        if not info.is_dir() and PurePosixPath(info.filename).name == filename
    ]
    if len(matches) != 1:
        raise IngestionError(
            f"Expected exactly one ZIP member with basename {filename}, got {len(matches)}"
        )
    if not _safe_regular_member(matches[0]):
        raise IngestionError(f"Unsafe or non-regular ZIP member: {matches[0].filename}")
    return matches[0]


def validate_raw_columns(actual: Sequence[str], expected: Sequence[str]) -> None:
    """Require the raw header to match canonical names and order exactly."""
    actual_list = list(actual)
    expected_list = list(expected)
    if actual_list != expected_list:
        missing = [column for column in expected_list if column not in actual_list]
        extra = [column for column in actual_list if column not in expected_list]
        raise IngestionError(
            "Raw column order does not match the schema contract; "
            f"missing={missing}, extra={extra}, actual={actual_list}"
        )


def normalize_raw_chunk(
    chunk: pd.DataFrame, timestamp_column: str, feature_columns: Sequence[str]
) -> pd.DataFrame:
    """Convert a raw chunk to timestamp[ns] and finite float64 feature values."""
    expected = [timestamp_column, *feature_columns]
    validate_raw_columns(chunk.columns, expected)
    normalized = chunk.copy()
    try:
        timestamps = pd.to_datetime(normalized[timestamp_column], errors="raise")
    except (TypeError, ValueError) as error:
        raise IngestionError(f"Invalid Datetime value: {error}") from error
    if isinstance(timestamps.dtype, pd.DatetimeTZDtype):
        raise IngestionError("Timezone-aware Datetime values are not supported")
    timestamps = timestamps.astype("datetime64[ns]")
    if timestamps.isna().any():
        raise IngestionError("Datetime contains NaT")
    normalized[timestamp_column] = timestamps

    for column in feature_columns:
        try:
            values = pd.to_numeric(normalized[column], errors="raise").astype("float64")
        except (TypeError, ValueError) as error:
            raise IngestionError(f"Feature {column} contains invalid numeric data: {error}") from error
        if values.isna().any():
            raise IngestionError(f"Feature {column} contains NaN")
        if not np.isfinite(values.to_numpy()).all():
            raise IngestionError(f"Feature {column} contains an infinite value")
        normalized[column] = values
    return normalized


def append_scenario_metadata(
    chunk: pd.DataFrame, scenario: Mapping[str, Any]
) -> pd.DataFrame:
    """Append the canonical 11 metadata columns from the scenario registry."""
    result = chunk.copy()
    values = {
        "scenario_id": scenario["scenario_id"],
        "source_filename": scenario["filename"],
        "fault_present": scenario["fault_present"],
        "fault_family": scenario["fault_family"],
        "severity_token": scenario["severity_token"],
        "severity_value": scenario["severity_value"],
        "severity_unit": scenario["severity_unit"],
        "is_short": scenario["is_short"],
        "content_sha256": scenario["content_sha256"],
        "byte_identical_group": scenario["byte_identical_group"],
        "severity_content_distinguishable": scenario[
            "severity_content_distinguishable"
        ],
    }
    for column in METADATA_COLUMNS:
        result[column] = values[column]
    return result


def build_arrow_schema(
    timestamp_column: str,
    feature_columns: Sequence[str],
    file_metadata: Mapping[str, str] | None = None,
) -> pa.Schema:
    """Build the explicit canonical 42-column Arrow schema."""
    fields = [pa.field(timestamp_column, pa.timestamp("ns"), nullable=False)]
    fields.extend(pa.field(column, pa.float64(), nullable=False) for column in feature_columns)
    fields.extend(
        (
            pa.field("scenario_id", pa.string(), nullable=False),
            pa.field("source_filename", pa.string(), nullable=False),
            pa.field("fault_present", pa.bool_(), nullable=False),
            pa.field("fault_family", pa.string(), nullable=False),
            pa.field("severity_token", pa.string(), nullable=True),
            pa.field("severity_value", pa.int64(), nullable=True),
            pa.field("severity_unit", pa.string(), nullable=True),
            pa.field("is_short", pa.bool_(), nullable=False),
            pa.field("content_sha256", pa.string(), nullable=False),
            pa.field("byte_identical_group", pa.string(), nullable=True),
            pa.field("severity_content_distinguishable", pa.bool_(), nullable=False),
        )
    )
    metadata = None
    if file_metadata is not None:
        metadata = {key.encode(): value.encode() for key, value in file_metadata.items()}
    return pa.schema(fields, metadata=metadata)


def _portable_contract_path(path: Path) -> str:
    path = Path(path)
    if not path.is_absolute() and ".." not in path.parts:
        return path.as_posix()
    if path.parent.name == "configs":
        return f"configs/{path.name}"
    return path.name


def _file_metadata(
    archive_hash: str,
    scenario: Mapping[str, Any],
    source_member: str,
    schema_path: Path,
    scenarios_path: Path,
) -> dict[str, str]:
    return {
        "dataset": DATASET_NAME,
        "pipeline_version": str(PIPELINE_VERSION),
        "archive_sha256": archive_hash,
        "scenario_id": str(scenario["scenario_id"]),
        "source_filename": str(scenario["filename"]),
        "source_member": source_member,
        "source_content_sha256": str(scenario["content_sha256"]),
        "schema_contract": _portable_contract_path(schema_path),
        "scenario_registry": _portable_contract_path(scenarios_path),
        "pyarrow_version": pa.__version__,
    }


def _validate_timestamps(timestamps: pd.Series, previous: pd.Timestamp | None) -> pd.Timestamp:
    if timestamps.empty:
        raise IngestionError("Encountered an empty CSV chunk")
    deltas = timestamps.diff().iloc[1:]
    if not deltas.eq(pd.Timedelta(minutes=1)).all():
        raise IngestionError("Datetime must be strictly increasing in one-minute intervals")
    first = timestamps.iloc[0]
    if previous is not None and first - previous != pd.Timedelta(minutes=1):
        raise IngestionError("Datetime chunk boundary is not a one-minute interval")
    return timestamps.iloc[-1]


def sha256_file(path: Path, block_size: int = 1024 * 1024) -> str:
    """Return an uppercase streaming SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(block_size), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def validate_parquet_output(
    path: Path,
    expected_schema: pa.Schema,
    expected_rows: int,
) -> dict[str, Any]:
    """Read back and validate Parquet structure, schema, and file metadata."""
    parquet_file = pq.ParquetFile(path)
    actual_schema = parquet_file.schema_arrow
    if not actual_schema.equals(expected_schema, check_metadata=True):
        raise IngestionError("Read-back Arrow schema or file metadata differs from contract")
    if parquet_file.metadata.num_rows != expected_rows:
        raise IngestionError(
            f"Read-back row count is {parquet_file.metadata.num_rows}, expected {expected_rows}"
        )
    metadata = {
        key.decode(): value.decode() for key, value in (actual_schema.metadata or {}).items()
    }
    missing_metadata = set(FILE_METADATA_KEYS) - set(metadata)
    if missing_metadata:
        raise IngestionError(f"Parquet file metadata is missing: {sorted(missing_metadata)}")
    return {
        "rows": parquet_file.metadata.num_rows,
        "columns": parquet_file.metadata.num_columns,
        "row_groups": parquet_file.metadata.num_row_groups,
        "metadata": metadata,
    }


def ingest_scenario_to_parquet(
    archive_path: Path,
    schema_path: Path,
    scenarios_path: Path,
    scenario: str,
    output_path: Path,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    row_limit: int | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Stream one registered ZIP scenario into an atomically published Parquet file."""
    archive_path = Path(archive_path)
    schema_path = Path(schema_path)
    scenarios_path = Path(scenarios_path)
    output_path = Path(output_path)
    if chunk_size <= 0:
        raise IngestionError("chunk_size must be positive")
    if row_limit is not None and row_limit <= 0:
        raise IngestionError("row_limit must be positive when provided")
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output_path}")

    contract = load_schema_contract(schema_path)
    timestamp_column = contract["timestamp_column"]
    feature_columns = contract["feature_columns"]
    registry, scenario_metadata = _load_scenario(scenarios_path, scenario)

    archive_hash = sha256_file(archive_path)
    expected_archive_hash = str(registry["source"]["archive_sha256"]).upper()
    if archive_hash != expected_archive_hash:
        raise IngestionError(
            f"Archive SHA-256 is {archive_hash}, expected {expected_archive_hash}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    writer: pq.ParquetWriter | None = None
    try:
        with zipfile.ZipFile(archive_path) as archive:
            member = resolve_archive_member(archive, scenario)
            declared_member = scenario_metadata.get("archive_member")
            if declared_member is not None and declared_member != member.filename:
                raise IngestionError(
                    f"Resolved member {member.filename} differs from registry {declared_member}"
                )
            file_metadata = _file_metadata(
                archive_hash,
                scenario_metadata,
                member.filename,
                schema_path,
                scenarios_path,
            )
            arrow_schema = build_arrow_schema(
                timestamp_column, feature_columns, file_metadata=file_metadata
            )
            with tempfile.NamedTemporaryFile(
                prefix=f"{output_path.name}.",
                suffix=".tmp",
                dir=output_path.parent,
                delete=False,
            ) as temp_file:
                temporary_path = Path(temp_file.name)
            writer = pq.ParquetWriter(
                temporary_path,
                arrow_schema,
                compression="zstd",
                compression_level=3,
                use_dictionary=True,
            )
            processed_rows = 0
            previous_timestamp: pd.Timestamp | None = None
            with archive.open(member, "r") as source:
                chunks = pd.read_csv(source, chunksize=chunk_size)
                for raw_chunk in chunks:
                    remaining = None if row_limit is None else row_limit - processed_rows
                    if remaining is not None and remaining <= 0:
                        break
                    if remaining is not None and len(raw_chunk) > remaining:
                        raw_chunk = raw_chunk.iloc[:remaining].copy()
                    normalized = normalize_raw_chunk(
                        raw_chunk, timestamp_column, feature_columns
                    )
                    previous_timestamp = _validate_timestamps(
                        normalized[timestamp_column], previous_timestamp
                    )
                    processed = append_scenario_metadata(normalized, scenario_metadata)
                    table = pa.Table.from_pandas(
                        processed,
                        schema=arrow_schema,
                        preserve_index=False,
                        safe=True,
                    )
                    writer.write_table(table)
                    processed_rows += len(processed)
                    if row_limit is not None and processed_rows == row_limit:
                        break
            if processed_rows == 0:
                raise IngestionError("Source CSV produced no data rows")
            if row_limit is not None and processed_rows != row_limit:
                raise IngestionError(
                    f"Source ended at {processed_rows} rows before row_limit={row_limit}"
                )
            writer.close()
            writer = None
            validation = validate_parquet_output(
                temporary_path,
                expected_schema=arrow_schema,
                expected_rows=processed_rows,
            )
        os.replace(temporary_path, output_path)
        temporary_path = None
    except Exception:
        if writer is not None:
            writer.close()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise

    return {
        "pipeline_version": PIPELINE_VERSION,
        "archive_sha256": archive_hash,
        "source_member": validation["metadata"]["source_member"],
        "source_content_sha256": scenario_metadata["content_sha256"],
        "scenario_id": scenario_metadata["scenario_id"],
        "source_filename": scenario_metadata["filename"],
        "rows": validation["rows"],
        "columns": validation["columns"],
        "row_groups": validation["row_groups"],
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
    }
