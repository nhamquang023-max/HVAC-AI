from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml

import hvac_ai.data.lbnl_sd_ahu_batch as batch_module
from hvac_ai.data.lbnl_sd_ahu_batch import (
    MINIMUM_FREE_BYTES,
    BatchIngestionError,
    build_full_manifest,
    build_ingestion_plan,
    calculate_disk_requirement,
    calculate_expected_totals,
    ingest_all_scenarios,
    preflight_full_ingestion,
)

FEATURES = [f"FEATURE_{index:02d}" for index in range(30)]


@dataclass(frozen=True)
class BatchCase:
    archive: Path
    schema: Path
    scenarios: Path
    smoke_manifest: Path
    output_dir: Path
    full_manifest: Path
    scenario_count: int = 4
    expected_rows: int = 15


def _frame(rows: int, offset: float = 0.0) -> pd.DataFrame:
    values: dict[str, object] = {
        "Datetime": pd.date_range("2020-01-01", periods=rows, freq="min")
    }
    for index, column in enumerate(FEATURES):
        values[column] = np.arange(rows, dtype="float64") + index + offset
    return pd.DataFrame(values)


def _make_case(tmp_path: Path) -> BatchCase:
    definitions = [
        {
            "scenario_id": "fault_free",
            "filename": "baseline.csv",
            "fault_family": "fault_free",
            "fault_present": False,
            "severity_token": None,
            "severity_value": None,
            "severity_unit": None,
            "is_short": False,
            "byte_identical_group": None,
            "severity_content_distinguishable": True,
            "frame": _frame(4),
        },
        {
            "scenario_id": "duplicate_a",
            "filename": "duplicate_a.csv",
            "fault_family": "test_fault",
            "fault_present": True,
            "severity_token": "1",
            "severity_value": 1,
            "severity_unit": "percent",
            "is_short": False,
            "byte_identical_group": "duplicate_group_001",
            "severity_content_distinguishable": False,
            "frame": _frame(4, 1.0),
        },
        {
            "scenario_id": "duplicate_b",
            "filename": "duplicate_b.csv",
            "fault_family": "test_fault",
            "fault_present": True,
            "severity_token": "2",
            "severity_value": 2,
            "severity_unit": "percent",
            "is_short": False,
            "byte_identical_group": "duplicate_group_001",
            "severity_content_distinguishable": False,
            "frame": _frame(4, 1.0),
        },
        {
            "scenario_id": "short_case",
            "filename": "short_case.csv",
            "fault_family": "test_fault",
            "fault_present": True,
            "severity_token": "100",
            "severity_value": 100,
            "severity_unit": "percent",
            "is_short": True,
            "byte_identical_group": None,
            "severity_content_distinguishable": True,
            "frame": _frame(3, 2.0),
        },
    ]
    archive = tmp_path / "source.zip"
    registry_scenarios = []
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for definition in definitions:
            content = definition.pop("frame").to_csv(index=False).encode("utf-8")
            member = f"synthetic/{definition['filename']}"
            bundle.writestr(member, content)
            registry_scenarios.append(
                {
                    **definition,
                    "archive_member": member,
                    "content_sha256": hashlib.sha256(content).hexdigest().upper(),
                    "row_count": len(content.decode("utf-8").splitlines()) - 1,
                }
            )

    schema = tmp_path / "schema.yaml"
    schema.write_text(
        yaml.safe_dump(
            {
                "timestamp_column": "Datetime",
                "feature_count": 30,
                "total_csv_columns": 31,
                "feature_columns": FEATURES,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    scenarios = tmp_path / "scenarios.yaml"
    scenarios.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest().upper()
                },
                "scenarios": registry_scenarios,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    smoke_manifest = tmp_path / "smoke.json"
    smoke_manifest.write_text(
        json.dumps({"parquet_size_bytes": 1000, "output_rows": 10}),
        encoding="utf-8",
    )
    return BatchCase(
        archive=archive,
        schema=schema,
        scenarios=scenarios,
        smoke_manifest=smoke_manifest,
        output_dir=tmp_path / "processed" / "scenarios",
        full_manifest=tmp_path / "docs" / "full.json",
    )


def _preflight(case: BatchCase, free: int = 10 * 1024**3) -> dict[str, object]:
    return preflight_full_ingestion(
        case.archive,
        case.schema,
        case.scenarios,
        case.output_dir,
        case.smoke_manifest,
        case.full_manifest,
        expected_scenario_count=case.scenario_count,
        expected_total_rows=case.expected_rows,
        disk_usage=lambda _: SimpleNamespace(free=free),
        ignore_check=lambda _: True,
    )


def _run(case: BatchCase) -> dict[str, object]:
    return ingest_all_scenarios(
        case.archive,
        case.schema,
        case.scenarios,
        case.output_dir,
        case.smoke_manifest,
        case.full_manifest,
        chunk_size=2,
        expected_scenario_count=case.scenario_count,
        expected_total_rows=case.expected_rows,
        disk_usage=lambda _: SimpleNamespace(free=10 * 1024**3),
        ignore_check=lambda _: True,
    )


@pytest.fixture
def case(tmp_path: Path) -> BatchCase:
    return _make_case(tmp_path)


@pytest.fixture
def completed(case: BatchCase) -> tuple[BatchCase, dict[str, object]]:
    return case, _run(case)


def test_build_plan_is_deterministic(case: BatchCase) -> None:
    _, plan = build_ingestion_plan(case.scenarios, expected_scenario_count=4)
    assert [item.source_filename for item in plan] == [
        "baseline.csv",
        "duplicate_a.csv",
        "duplicate_b.csv",
        "short_case.csv",
    ]
    assert [item.order for item in plan] == [1, 2, 3, 4]


def test_output_filenames_are_unique(case: BatchCase) -> None:
    _, plan = build_ingestion_plan(case.scenarios)
    outputs = [item.output_filename for item in plan]
    assert len(outputs) == len(set(outputs)) == 4


def test_expected_row_total(case: BatchCase) -> None:
    _, plan = build_ingestion_plan(case.scenarios)
    assert calculate_expected_totals(plan)["expected_total_rows"] == 15


def test_duplicate_group_scenarios_are_retained(case: BatchCase) -> None:
    _, plan = build_ingestion_plan(case.scenarios)
    duplicated = [item for item in plan if item.byte_identical_group]
    assert [item.scenario_id for item in duplicated] == ["duplicate_a", "duplicate_b"]


def test_short_scenario_row_count_is_retained(case: BatchCase) -> None:
    _, plan = build_ingestion_plan(case.scenarios)
    short = [item for item in plan if item.is_short]
    assert len(short) == 1
    assert short[0].expected_rows == 3


def test_preflight_passes_valid_setup(case: BatchCase) -> None:
    report = _preflight(case)
    assert report["status"] == "passed"
    assert report["scenario_count"] == 4
    assert report["expected_total_rows"] == 15
    assert report["raw_columns"] == 31
    assert report["feature_columns"] == 30
    assert report["processed_columns"] == 42


def test_existing_final_output_causes_failure(case: BatchCase) -> None:
    case.output_dir.mkdir(parents=True)
    with pytest.raises(BatchIngestionError, match="already exists"):
        _preflight(case)


def test_preexisting_staging_causes_failure(case: BatchCase) -> None:
    staging = case.output_dir.parent / ".scenarios.staging-review"
    staging.mkdir(parents=True)
    with pytest.raises(BatchIngestionError, match="Pre-existing staging"):
        _preflight(case)
    assert staging.exists()


def test_disk_requirement_calculation() -> None:
    capacity = calculate_disk_requirement(
        {"parquet_size_bytes": 1000, "output_rows": 10}, 15
    )
    assert capacity["smoke_bytes_per_row"] == 100.0
    assert capacity["projected_total_parquet_bytes"] == 1500
    assert capacity["required_free_bytes"] == MINIMUM_FREE_BYTES


def test_insufficient_simulated_space_causes_failure(case: BatchCase) -> None:
    with pytest.raises(BatchIngestionError, match="Insufficient disk space"):
        _preflight(case, free=MINIMUM_FREE_BYTES - 1)


def test_preflight_writes_no_files(case: BatchCase) -> None:
    before = sorted(path.relative_to(case.output_dir.parents[1]) for path in case.output_dir.parents[1].rglob("*"))
    _preflight(case)
    after = sorted(path.relative_to(case.output_dir.parents[1]) for path in case.output_dir.parents[1].rglob("*"))
    assert after == before
    assert not case.output_dir.exists()
    assert not case.full_manifest.exists()


def test_successful_batch_publishes_final_directory(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    case, _ = completed
    assert case.output_dir.is_dir()
    assert len(list(case.output_dir.glob("*.parquet"))) == 4


def test_final_directory_absent_until_all_scenarios_complete(
    case: BatchCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[bool] = []
    original = batch_module.ingest_scenario_to_parquet

    def wrapped(*args: object, **kwargs: object) -> dict[str, object]:
        observed.append(case.output_dir.exists())
        return original(*args, **kwargs)

    monkeypatch.setattr(batch_module, "ingest_scenario_to_parquet", wrapped)
    _run(case)
    assert observed == [False, False, False, False]


def test_scenario_failure_leaves_no_final_directory(
    case: BatchCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(batch_module, "ingest_scenario_to_parquet", fail)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        _run(case)
    assert not case.output_dir.exists()


def test_failure_cleans_own_staging(
    case: BatchCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(batch_module, "ingest_scenario_to_parquet", fail)
    with pytest.raises(RuntimeError):
        _run(case)
    parent = case.output_dir.parent
    assert not parent.exists() or list(parent.glob(".scenarios.staging-*")) == []


def test_failure_preserves_unknown_preexisting_data(
    case: BatchCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    case.output_dir.parent.mkdir(parents=True)
    unknown = case.output_dir.parent / "keep.txt"
    unknown.write_text("keep", encoding="utf-8")

    def fail(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(batch_module, "ingest_scenario_to_parquet", fail)
    with pytest.raises(RuntimeError):
        _run(case)
    assert unknown.read_text(encoding="utf-8") == "keep"


def test_manifest_scenario_count(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    _, manifest = completed
    assert manifest["scenario_count"] == 4
    assert len(manifest["scenarios"]) == 4


def test_manifest_total_rows(completed: tuple[BatchCase, dict[str, object]]) -> None:
    _, manifest = completed
    assert manifest["expected_total_rows"] == 15
    assert manifest["actual_total_rows"] == 15


def test_manifest_contains_output_sha256(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    _, manifest = completed
    assert all(len(item["parquet_sha256"]) == 64 for item in manifest["scenarios"])


def test_manifest_contains_no_absolute_local_paths(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    case, manifest = completed
    text = json.dumps(manifest)
    assert str(case.output_dir.parent) not in text
    assert ":\\" not in text


def test_complete_status_requires_all_scenarios() -> None:
    one_record = {
        "expected_rows": 1,
        "actual_rows": 1,
        "validation_status": "passed",
    }
    with pytest.raises(BatchIngestionError, match="1/2 scenarios"):
        build_full_manifest(
            "A" * 64,
            2,
            [one_record],
            expected_scenario_count=2,
            expected_total_rows=2,
        )


def test_completed_manifest_status(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    _, manifest = completed
    assert manifest["dataset_status"] == "complete_and_verified"


def test_all_output_schemas_are_compatible(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    case, _ = completed
    schemas = [
        pq.read_schema(path).remove_metadata()
        for path in sorted(case.output_dir.glob("*.parquet"))
    ]
    assert schemas
    assert all(schema.equals(schemas[0]) for schema in schemas[1:])


def test_output_metadata_matches_registry(
    completed: tuple[BatchCase, dict[str, object]],
) -> None:
    case, manifest = completed
    for item in manifest["scenarios"]:
        metadata = {
            key.decode(): value.decode()
            for key, value in (
                pq.read_schema(case.output_dir / item["output_filename"]).metadata or {}
            ).items()
        }
        assert metadata["scenario_id"] == item["scenario_id"]
        assert metadata["source_filename"] == item["source_filename"]
        assert metadata["source_content_sha256"] == item["source_content_sha256"]


def test_existing_processed_parquet_causes_failure(case: BatchCase) -> None:
    case.output_dir.parent.mkdir(parents=True)
    (case.output_dir.parent / "unknown.parquet").write_bytes(b"unknown")
    with pytest.raises(BatchIngestionError, match="already contains"):
        _preflight(case)
