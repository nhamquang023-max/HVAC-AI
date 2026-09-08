"""Batch orchestration for canonical LBNL SD-AHU Parquet ingestion."""

from __future__ import annotations

import json
import math
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from hvac_ai.data.lbnl_sd_ahu_ingest import (
    PIPELINE_VERSION,
    ingest_scenario_to_parquet,
    load_schema_contract,
    sha256_file,
)

EXPECTED_SCENARIO_COUNT = 21
EXPECTED_TOTAL_ROWS = 10_818_901
MINIMUM_FREE_BYTES = 5 * 1024**3


class BatchIngestionError(ValueError):
    """Raised when batch planning, preflight, or validation fails."""


class DiskUsage(Protocol):
    """Required disk-usage result interface."""

    free: int


@dataclass(frozen=True)
class IngestionPlanItem:
    """One canonical scenario in deterministic registry order."""

    order: int
    scenario_id: str
    source_filename: str
    source_member: str
    fault_family: str
    fault_present: bool
    severity_token: str | None
    severity_value: int | None
    severity_unit: str | None
    is_short: bool
    byte_identical_group: str | None
    severity_content_distinguishable: bool
    source_content_sha256: str
    expected_rows: int
    output_filename: str


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise BatchIngestionError(f"{label} must be a YAML mapping: {path}")
    return value


def build_ingestion_plan(
    scenarios_path: Path,
    *,
    expected_scenario_count: int | None = None,
) -> tuple[dict[str, Any], tuple[IngestionPlanItem, ...]]:
    """Build a unique output plan without changing canonical registry order."""
    registry = _load_yaml_mapping(Path(scenarios_path), "Scenario registry")
    scenarios = registry.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise BatchIngestionError("Scenario registry must contain a non-empty scenarios list")
    if expected_scenario_count is not None and len(scenarios) != expected_scenario_count:
        raise BatchIngestionError(
            f"Expected {expected_scenario_count} scenarios, got {len(scenarios)}"
        )

    plan: list[IngestionPlanItem] = []
    for order, scenario in enumerate(scenarios, start=1):
        if not isinstance(scenario, dict):
            raise BatchIngestionError(f"Scenario {order} must be a mapping")
        required = {
            "scenario_id",
            "filename",
            "archive_member",
            "fault_family",
            "fault_present",
            "severity_token",
            "severity_value",
            "severity_unit",
            "is_short",
            "byte_identical_group",
            "severity_content_distinguishable",
            "content_sha256",
            "row_count",
        }
        missing = required - set(scenario)
        if missing:
            raise BatchIngestionError(
                f"Scenario {order} lacks required fields: {sorted(missing)}"
            )
        filename = str(scenario["filename"])
        if Path(filename).name != filename or Path(filename).suffix.lower() != ".csv":
            raise BatchIngestionError(f"Invalid scenario CSV basename: {filename}")
        expected_rows = scenario["row_count"]
        if not isinstance(expected_rows, int) or expected_rows <= 0:
            raise BatchIngestionError(f"Invalid expected row count for {filename}")
        plan.append(
            IngestionPlanItem(
                order=order,
                scenario_id=str(scenario["scenario_id"]),
                source_filename=filename,
                source_member=str(scenario["archive_member"]),
                fault_family=str(scenario["fault_family"]),
                fault_present=bool(scenario["fault_present"]),
                severity_token=scenario["severity_token"],
                severity_value=scenario["severity_value"],
                severity_unit=scenario["severity_unit"],
                is_short=bool(scenario["is_short"]),
                byte_identical_group=scenario["byte_identical_group"],
                severity_content_distinguishable=bool(
                    scenario["severity_content_distinguishable"]
                ),
                source_content_sha256=str(scenario["content_sha256"]),
                expected_rows=expected_rows,
                output_filename=Path(filename).with_suffix(".parquet").name,
            )
        )

    scenario_ids = [item.scenario_id for item in plan]
    source_filenames = [item.source_filename for item in plan]
    output_filenames = [item.output_filename for item in plan]
    if len(set(scenario_ids)) != len(plan):
        raise BatchIngestionError("Scenario IDs must be unique")
    if len(set(source_filenames)) != len(plan):
        raise BatchIngestionError("Source filenames must be unique")
    if len(set(output_filenames)) != len(plan):
        raise BatchIngestionError("Output filenames must be unique")
    return registry, tuple(plan)


def calculate_expected_totals(plan: Sequence[IngestionPlanItem]) -> dict[str, int]:
    """Calculate deterministic scenario and row totals."""
    return {
        "scenario_count": len(plan),
        "expected_total_rows": sum(item.expected_rows for item in plan),
        "full_annual_scenarios": sum(not item.is_short for item in plan),
        "short_scenarios": sum(item.is_short for item in plan),
        "duplicate_groups": len(
            {item.byte_identical_group for item in plan if item.byte_identical_group}
        ),
    }


def calculate_disk_requirement(
    smoke_manifest: Mapping[str, Any], expected_total_rows: int
) -> dict[str, int | float]:
    """Estimate full output size and apply the fixed capacity safety margin."""
    smoke_bytes = smoke_manifest.get("parquet_size_bytes")
    smoke_rows = smoke_manifest.get("output_rows")
    if not isinstance(smoke_bytes, int) or smoke_bytes <= 0:
        raise BatchIngestionError("Smoke manifest parquet_size_bytes must be positive")
    if not isinstance(smoke_rows, int) or smoke_rows <= 0:
        raise BatchIngestionError("Smoke manifest output_rows must be positive")
    if expected_total_rows <= 0:
        raise BatchIngestionError("Expected total rows must be positive")
    bytes_per_row = smoke_bytes / smoke_rows
    projected_bytes = math.ceil(bytes_per_row * expected_total_rows)
    required_bytes = max(MINIMUM_FREE_BYTES, 3 * projected_bytes)
    return {
        "smoke_bytes_per_row": bytes_per_row,
        "projected_total_parquet_bytes": projected_bytes,
        "required_free_bytes": required_bytes,
    }


def _nearest_existing_parent(path: Path) -> Path:
    candidate = Path(path)
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise BatchIngestionError(f"No existing parent found for {path}")
        candidate = parent
    return candidate


def _git_ignored(path: Path, git_root: Path) -> bool:
    result = subprocess.run(
        ["git", "check-ignore", "--quiet", "--", str(path)],
        cwd=git_root,
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


def preflight_full_ingestion(
    archive_path: Path,
    schema_path: Path,
    scenarios_path: Path,
    output_dir: Path,
    smoke_manifest_path: Path,
    manifest_path: Path,
    *,
    expected_scenario_count: int = EXPECTED_SCENARIO_COUNT,
    expected_total_rows: int = EXPECTED_TOTAL_ROWS,
    disk_usage: Callable[[Path], DiskUsage] = shutil.disk_usage,
    ignore_check: Callable[[Path], bool] | None = None,
    git_root: Path | None = None,
) -> dict[str, Any]:
    """Validate a full run without creating staging, output, or manifest files."""
    archive_path = Path(archive_path)
    output_dir = Path(output_dir)
    manifest_path = Path(manifest_path)
    schema_contract = load_schema_contract(schema_path)
    registry, plan = build_ingestion_plan(
        scenarios_path, expected_scenario_count=expected_scenario_count
    )
    totals = calculate_expected_totals(plan)
    if totals["expected_total_rows"] != expected_total_rows:
        raise BatchIngestionError(
            f"Expected {expected_total_rows} total rows, got {totals['expected_total_rows']}"
        )

    source = registry.get("source")
    if not isinstance(source, dict) or not isinstance(source.get("archive_sha256"), str):
        raise BatchIngestionError("Registry source.archive_sha256 is required")
    archive_hash = sha256_file(archive_path)
    expected_archive_hash = source["archive_sha256"].upper()
    if archive_hash != expected_archive_hash:
        raise BatchIngestionError(
            f"Archive SHA-256 is {archive_hash}, expected {expected_archive_hash}"
        )
    if output_dir.exists():
        raise BatchIngestionError(f"Final output target already exists: {output_dir}")
    if manifest_path.exists():
        raise BatchIngestionError(f"Full manifest already exists: {manifest_path}")

    output_parent = output_dir.parent
    staging_conflicts = (
        sorted(path.name for path in output_parent.glob(f".{output_dir.name}.staging-*"))
        if output_parent.exists()
        else []
    )
    if staging_conflicts:
        raise BatchIngestionError(
            f"Pre-existing staging directories require audit: {staging_conflicts}"
        )
    processed_parquet_count = (
        len(list(output_parent.rglob("*.parquet"))) if output_parent.exists() else 0
    )
    if processed_parquet_count:
        raise BatchIngestionError(
            f"Processed area already contains {processed_parquet_count} Parquet files"
        )

    smoke_manifest = json.loads(Path(smoke_manifest_path).read_text(encoding="utf-8"))
    if not isinstance(smoke_manifest, dict):
        raise BatchIngestionError("Smoke manifest must be a JSON object")
    capacity = calculate_disk_requirement(smoke_manifest, expected_total_rows)
    volume_path = _nearest_existing_parent(output_parent)
    free_bytes = int(disk_usage(volume_path).free)
    disk_pass = free_bytes >= capacity["required_free_bytes"]
    if not disk_pass:
        raise BatchIngestionError(
            f"Insufficient disk space: free={free_bytes}, "
            f"required={capacity['required_free_bytes']}"
        )

    probe = output_dir / plan[0].output_filename
    if ignore_check is None:
        ignored = _git_ignored(probe, Path.cwd() if git_root is None else Path(git_root))
    else:
        ignored = ignore_check(probe)
    if not ignored:
        raise BatchIngestionError(f"Processed output is not ignored by Git: {probe}")

    return {
        "status": "passed",
        "archive_sha256": archive_hash,
        **totals,
        "expected_output_files": len(plan),
        "output_filenames_unique": len({item.output_filename for item in plan}) == len(plan),
        "raw_columns": schema_contract["total_csv_columns"],
        "feature_columns": schema_contract["feature_count"],
        "metadata_columns": 11,
        "processed_columns": schema_contract["total_csv_columns"] + 11,
        "final_target_absent": True,
        "staging_conflicts": staging_conflicts,
        "processed_parquet_current_count": processed_parquet_count,
        **capacity,
        "projected_total_parquet_gib": capacity["projected_total_parquet_bytes"]
        / 1024**3,
        "required_free_gib": capacity["required_free_bytes"] / 1024**3,
        "free_bytes": free_bytes,
        "free_gib": free_bytes / 1024**3,
        "disk_space_check": "passed",
        "output_git_ignored": True,
        "plan": [asdict(item) for item in plan],
    }


def _parquet_datetime_bounds(parquet_file: pq.ParquetFile) -> tuple[str, str]:
    first = parquet_file.read_row_group(0, columns=["Datetime"])["Datetime"][0].as_py()
    last_group = parquet_file.metadata.num_row_groups - 1
    last_column = parquet_file.read_row_group(last_group, columns=["Datetime"])["Datetime"]
    last = last_column[-1].as_py()
    return first.isoformat(), last.isoformat()


def validate_full_ingestion(
    staging_dir: Path,
    plan: Sequence[IngestionPlanItem],
    archive_sha256: str,
) -> list[dict[str, Any]]:
    """Validate all staged files and return scenario-level manifest records."""
    staging_dir = Path(staging_dir)
    actual_files = sorted(path.name for path in staging_dir.glob("*.parquet"))
    expected_files = sorted(item.output_filename for item in plan)
    if actual_files != expected_files:
        raise BatchIngestionError(
            f"Staged Parquet files differ; actual={actual_files}, expected={expected_files}"
        )

    canonical_schema: pa.Schema | None = None
    records: list[dict[str, Any]] = []
    for item in plan:
        output = staging_dir / item.output_filename
        parquet_file = pq.ParquetFile(output)
        schema = parquet_file.schema_arrow.remove_metadata()
        if canonical_schema is None:
            canonical_schema = schema
        elif not schema.equals(canonical_schema, check_metadata=False):
            raise BatchIngestionError(f"Schema mismatch for {item.output_filename}")
        if parquet_file.metadata.num_columns != 42:
            raise BatchIngestionError(f"Expected 42 columns for {item.output_filename}")
        if parquet_file.metadata.num_rows != item.expected_rows:
            raise BatchIngestionError(
                f"Row count mismatch for {item.output_filename}: "
                f"{parquet_file.metadata.num_rows} != {item.expected_rows}"
            )
        metadata = {
            key.decode(): value.decode()
            for key, value in (parquet_file.schema_arrow.metadata or {}).items()
        }
        expected_metadata = {
            "archive_sha256": archive_sha256,
            "pipeline_version": str(PIPELINE_VERSION),
            "scenario_id": item.scenario_id,
            "source_filename": item.source_filename,
            "source_content_sha256": item.source_content_sha256,
        }
        for key, expected in expected_metadata.items():
            if metadata.get(key) != expected:
                raise BatchIngestionError(
                    f"Metadata {key} mismatch for {item.output_filename}"
                )
        codecs = {
            parquet_file.metadata.row_group(group).column(column).compression
            for group in range(parquet_file.metadata.num_row_groups)
            for column in range(parquet_file.metadata.num_columns)
        }
        if codecs != {"ZSTD"}:
            raise BatchIngestionError(
                f"Unexpected compression for {item.output_filename}: {sorted(codecs)}"
            )
        datetime_start, datetime_end = _parquet_datetime_bounds(parquet_file)
        records.append(
            {
                "scenario_id": item.scenario_id,
                "source_filename": item.source_filename,
                "source_member": item.source_member,
                "fault_family": item.fault_family,
                "fault_present": item.fault_present,
                "severity_token": item.severity_token,
                "severity_value": item.severity_value,
                "severity_unit": item.severity_unit,
                "is_short": item.is_short,
                "byte_identical_group": item.byte_identical_group,
                "severity_content_distinguishable": item.severity_content_distinguishable,
                "source_content_sha256": item.source_content_sha256,
                "output_filename": item.output_filename,
                "expected_rows": item.expected_rows,
                "actual_rows": parquet_file.metadata.num_rows,
                "datetime_start": datetime_start,
                "datetime_end": datetime_end,
                "columns": parquet_file.metadata.num_columns,
                "row_groups": parquet_file.metadata.num_row_groups,
                "parquet_size_bytes": output.stat().st_size,
                "parquet_sha256": sha256_file(output),
                "validation_status": "passed",
            }
        )

    if sum(record["actual_rows"] for record in records) != sum(
        item.expected_rows for item in plan
    ):
        raise BatchIngestionError("Dataset actual row total differs from the plan")
    if len({record["scenario_id"] for record in records}) != len(plan):
        raise BatchIngestionError("Dataset scenario IDs are not unique")
    if len({record["output_filename"] for record in records}) != len(plan):
        raise BatchIngestionError("Dataset output filenames are not unique")
    if sum(not record["fault_present"] for record in records) != 1:
        raise BatchIngestionError("Dataset must contain exactly one fault-free scenario")
    if sum(record["is_short"] for record in records) != 1:
        raise BatchIngestionError("Dataset must contain exactly one short scenario")
    return records


def build_full_manifest(
    archive_sha256: str,
    chunk_size: int,
    scenarios: Sequence[Mapping[str, Any]],
    *,
    expected_scenario_count: int,
    expected_total_rows: int,
) -> dict[str, Any]:
    """Build a complete dataset manifest after every scenario validates."""
    actual_total_rows = sum(int(item["actual_rows"]) for item in scenarios)
    if len(scenarios) != expected_scenario_count:
        raise BatchIngestionError(
            f"Cannot complete manifest with {len(scenarios)}/{expected_scenario_count} scenarios"
        )
    if actual_total_rows != expected_total_rows:
        raise BatchIngestionError(
            f"Cannot complete manifest with {actual_total_rows}/{expected_total_rows} rows"
        )
    if any(item.get("validation_status") != "passed" for item in scenarios):
        raise BatchIngestionError("Cannot complete manifest with failed scenarios")
    return {
        "pipeline_version": PIPELINE_VERSION,
        "archive_sha256": archive_sha256,
        "scenario_count": len(scenarios),
        "expected_total_rows": expected_total_rows,
        "actual_total_rows": actual_total_rows,
        "raw_columns": 31,
        "metadata_columns": 11,
        "processed_columns": 42,
        "parquet_engine": "pyarrow",
        "pyarrow_version": pa.__version__,
        "compression": "zstd",
        "compression_level": 3,
        "chunk_size": chunk_size,
        "dataset_status": "complete_and_verified",
        "scenarios": list(scenarios),
    }


def _write_manifest_temporary(manifest: Mapping[str, Any], path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=f"{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    ) as stream:
        json.dump(manifest, stream, indent=2, sort_keys=False)
        stream.write("\n")
        temporary = Path(stream.name)
    loaded = json.loads(temporary.read_text(encoding="utf-8"))
    if loaded != manifest or loaded.get("dataset_status") != "complete_and_verified":
        temporary.unlink(missing_ok=True)
        raise BatchIngestionError("Temporary full manifest failed validation")
    return temporary


def ingest_all_scenarios(
    archive_path: Path,
    schema_path: Path,
    scenarios_path: Path,
    output_dir: Path,
    smoke_manifest_path: Path,
    manifest_path: Path,
    *,
    chunk_size: int = 50_000,
    expected_scenario_count: int = EXPECTED_SCENARIO_COUNT,
    expected_total_rows: int = EXPECTED_TOTAL_ROWS,
    disk_usage: Callable[[Path], DiskUsage] = shutil.disk_usage,
    ignore_check: Callable[[Path], bool] | None = None,
    git_root: Path | None = None,
) -> dict[str, Any]:
    """Ingest every planned scenario into staging and atomically publish the dataset."""
    if chunk_size <= 0:
        raise BatchIngestionError("chunk_size must be positive")
    preflight = preflight_full_ingestion(
        archive_path,
        schema_path,
        scenarios_path,
        output_dir,
        smoke_manifest_path,
        manifest_path,
        expected_scenario_count=expected_scenario_count,
        expected_total_rows=expected_total_rows,
        disk_usage=disk_usage,
        ignore_check=ignore_check,
        git_root=git_root,
    )
    _, plan = build_ingestion_plan(
        scenarios_path, expected_scenario_count=expected_scenario_count
    )
    load_schema_contract(schema_path)
    output_dir = Path(output_dir)
    output_parent = output_dir.parent
    parent_created = not output_parent.exists()
    output_parent.mkdir(parents=True, exist_ok=True)
    staging_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=output_parent)
    )
    temporary_manifest: Path | None = None
    published = False
    try:
        for item in plan:
            result = ingest_scenario_to_parquet(
                archive_path=archive_path,
                schema_path=schema_path,
                scenarios_path=scenarios_path,
                scenario=item.source_filename,
                output_path=staging_dir / item.output_filename,
                chunk_size=chunk_size,
                row_limit=None,
                overwrite=False,
            )
            if result["rows"] != item.expected_rows:
                raise BatchIngestionError(
                    f"Ingestion row count mismatch for {item.source_filename}"
                )
        records = validate_full_ingestion(
            staging_dir, plan, str(preflight["archive_sha256"])
        )
        manifest = build_full_manifest(
            str(preflight["archive_sha256"]),
            chunk_size,
            records,
            expected_scenario_count=expected_scenario_count,
            expected_total_rows=expected_total_rows,
        )
        temporary_manifest = _write_manifest_temporary(manifest, Path(manifest_path))
        os.replace(staging_dir, output_dir)
        published = True
        try:
            os.replace(temporary_manifest, manifest_path)
            temporary_manifest = None
        except Exception:
            os.replace(output_dir, staging_dir)
            published = False
            raise
    except Exception:
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)
        if not published and staging_dir.exists():
            shutil.rmtree(staging_dir)
        if parent_created and output_parent.exists() and not any(output_parent.iterdir()):
            output_parent.rmdir()
        raise
    return manifest
