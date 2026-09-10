from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

import hvac_ai.data.lbnl_sd_ahu_view as view_module
from hvac_ai.data.lbnl_sd_ahu_split import PartitionWindow, SplitContractError
from hvac_ai.data.lbnl_sd_ahu_view import (
    DatasetViewBatch,
    DatasetViewError,
    ScenarioViewSource,
    _iter_scenario_view_batches,
    build_dataset_view_plan,
    load_dataset_view_strategy,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TRACKED_STRATEGY = (
    REPO_ROOT / "configs" / "benchmarks" / "lbnl_sd_ahu_dataset_view_v1.yaml"
)
TRACKED_SPLIT = REPO_ROOT / "configs" / "benchmarks" / "lbnl_sd_ahu_split_v1.yaml"


@dataclass(frozen=True)
class PlanCase:
    root: Path
    strategy: Path
    manifest: Path
    registry: Path
    canonical_dir: Path
    scenario_ids: tuple[str, ...]


@dataclass(frozen=True)
class StreamCase:
    root: Path
    source: ScenarioViewSource
    window: PartitionWindow


def _strategy_data() -> dict[str, Any]:
    value = yaml.safe_load(TRACKED_STRATEGY.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_strategy(
    tmp_path: Path,
    mutate: Any | None = None,
) -> Path:
    value = copy.deepcopy(_strategy_data())
    if mutate is not None:
        mutate(value)
    path = tmp_path / "strategy.yaml"
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def _write_small_parquet(path: Path, offset: int = 0) -> None:
    timestamps = pd.date_range("2018-04-01T01:00:00", periods=2, freq="min")
    table = pa.table(
        {
            "Datetime": pa.array(timestamps, type=pa.timestamp("ns")),
            "feature_b": pa.array([offset + 10, offset + 11], type=pa.int64()),
            "feature_a": pa.array([offset, offset + 1], type=pa.int64()),
        }
    )
    pq.write_table(table, path)


def _make_plan_case(tmp_path: Path) -> PlanCase:
    root = tmp_path / "repo"
    benchmark_dir = root / "configs" / "benchmarks"
    docs_dir = root / "docs" / "datasets"
    source_dir = root / "src" / "hvac_ai" / "data"
    canonical_dir = root / "data" / "processed" / "lbnl_sd_ahu" / "scenarios"
    for path in (benchmark_dir, docs_dir, source_dir, canonical_dir):
        path.mkdir(parents=True, exist_ok=True)

    strategy = benchmark_dir / "lbnl_sd_ahu_dataset_view_v1.yaml"
    strategy.write_text(TRACKED_STRATEGY.read_text(encoding="utf-8"), encoding="utf-8")
    (benchmark_dir / "lbnl_sd_ahu_split_v1.yaml").write_text(
        TRACKED_SPLIT.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (source_dir / "lbnl_sd_ahu_split.py").write_text("# identity stub\n", encoding="utf-8")
    schema = root / "configs" / "lbnl_sd_ahu_schema.yaml"
    schema.write_text("timestamp_column: Datetime\n", encoding="utf-8")

    scenario_ids = tuple(f"scenario_{index:02d}" for index in range(21))
    manifest_records = []
    registry_records = []
    for index in reversed(range(21)):
        output_filename = f"scenario_{index:02d}.parquet"
        _write_small_parquet(canonical_dir / output_filename, offset=index * 10)
    for index, scenario_id in enumerate(scenario_ids):
        source_filename = f"scenario_{index:02d}.csv"
        output_filename = f"scenario_{index:02d}.parquet"
        manifest_records.append(
            {
                "scenario_id": scenario_id,
                "source_filename": source_filename,
                "output_filename": output_filename,
                "actual_rows": 2,
            }
        )
        registry_records.append(
            {"scenario_id": scenario_id, "filename": source_filename}
        )

    manifest = docs_dir / "lbnl_sd_ahu_full_ingestion_manifest.json"
    manifest.write_text(
        json.dumps({"scenarios": manifest_records}), encoding="utf-8"
    )
    registry = root / "configs" / "lbnl_sd_ahu_scenarios.yaml"
    registry.write_text(
        yaml.safe_dump({"scenarios": registry_records}, sort_keys=False),
        encoding="utf-8",
    )
    return PlanCase(root, strategy, manifest, registry, canonical_dir, scenario_ids)


@pytest.fixture
def plan_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PlanCase:
    case = _make_plan_case(tmp_path)
    monkeypatch.setattr(
        view_module,
        "validate_split_evidence",
        lambda *_: {"status": "passed"},
    )
    return case


@pytest.fixture
def stream_case(tmp_path: Path) -> StreamCase:
    path = tmp_path / "source.parquet"
    timestamps = pd.date_range("2020-01-01T00:00:00", periods=5, freq="min")
    table = pa.table(
        {
            "Datetime": pa.array(timestamps, type=pa.timestamp("ns")),
            "feature_b": pa.array([50, 40, 30, 20, 10], type=pa.int64()),
            "feature_a": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
        }
    )
    pq.write_table(table, path, row_group_size=2)
    source = ScenarioViewSource(
        order=1,
        scenario_id="synthetic_scenario",
        source_filename="synthetic.csv",
        output_filename="source.parquet",
        path=path,
        canonical_rows=5,
    )
    window = PartitionWindow(
        label="train",
        start=pd.Timestamp("2020-01-01T00:01:00"),
        end=pd.Timestamp("2020-01-01T00:03:00"),
        rows_per_scenario=3,
        total_rows=3,
        modeling_role="training",
    )
    return StreamCase(tmp_path, source, window)


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


def _collect(
    case: StreamCase,
    columns: tuple[str, ...] = ("Datetime", "feature_a"),
    batch_size: int = 2,
    window: PartitionWindow | None = None,
) -> list[DatasetViewBatch]:
    return list(
        _iter_scenario_view_batches(
            case.source,
            case.window if window is None else window,
            columns,
            batch_size,
        )
    )


def test_tracked_dataset_view_strategy_loads() -> None:
    strategy = load_dataset_view_strategy(TRACKED_STRATEGY)
    assert strategy.view_id == "lbnl_sd_ahu_primary_dataset_view_v1"
    assert strategy.strategy_version == 1
    assert strategy.selected_strategy == "dynamic_logical_views"
    assert strategy.modeling_partitions == ("train", "validation", "test")
    assert strategy.excluded_partitions == (
        "purge_train_validation",
        "purge_validation_test",
        "outside_primary_benchmark_scope",
    )
    assert strategy.access_mode == "streaming_batches"
    assert strategy.column_projection == "required"
    assert strategy.timestamp_predicate_filtering == "required"


def test_non_dynamic_strategy_fails(tmp_path: Path) -> None:
    path = _write_strategy(
        tmp_path,
        lambda value: value.update(selected_strategy="physical_materialization"),
    )
    with pytest.raises(DatasetViewError, match="selected_strategy"):
        load_dataset_view_strategy(path)


def test_physical_materialization_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["materialization_policy"]["physical_partition_materialization"] = True

    with pytest.raises(DatasetViewError, match="materialization"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_selected_physical_strategy_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["evaluated_strategies"]["physical_partition_materialization"][
            "selected"
        ] = True

    with pytest.raises(DatasetViewError, match="materialization"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_row_level_index_enabled_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["evaluated_strategies"]["row_level_index_manifest"]["selected"] = True

    with pytest.raises(DatasetViewError, match="Row-level"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_absolute_strategy_path_value_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["source_provenance"]["canonical_source"] = str(
            Path("/").joinpath("outside", "data")
        )

    with pytest.raises(DatasetViewError, match="repository-relative"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_alternate_relative_source_path_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["source_provenance"]["canonical_source"] = "data/processed/alternate"

    with pytest.raises(DatasetViewError, match="source paths differ"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_duplicate_boundary_or_source_mutation_policy_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["predicate_policy"]["hard_coded_boundary_copy_in_loader"] = "allowed"
        value["source_immutability"]["modify_canonical_parquet"] = "allowed"

    with pytest.raises(DatasetViewError, match="duplicate split boundaries"):
        load_dataset_view_strategy(_write_strategy(tmp_path, mutate))


def test_duplicate_strategy_yaml_key_fails(tmp_path: Path) -> None:
    path = tmp_path / "strategy.yaml"
    path.write_text(
        TRACKED_STRATEGY.read_text(encoding="utf-8") + "view_id: duplicate\n",
        encoding="utf-8",
    )
    with pytest.raises(DatasetViewError, match="Duplicate YAML"):
        load_dataset_view_strategy(path)


def test_missing_required_strategy_field_fails(tmp_path: Path) -> None:
    path = _write_strategy(tmp_path, lambda value: value.pop("access"))
    with pytest.raises(DatasetViewError, match="lacks fields"):
        load_dataset_view_strategy(path)


def test_valid_synthetic_plan_uses_all_scenarios(plan_case: PlanCase) -> None:
    plan = build_dataset_view_plan(
        plan_case.root, plan_case.strategy, "train", ["feature_b"]
    )
    assert len(plan.sources) == 21
    assert tuple(source.scenario_id for source in plan.sources) == plan_case.scenario_ids
    assert plan.expected_total_rows == 3_688_020


def test_evidence_validation_is_called(
    plan_case: PlanCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    received: list[tuple[Path, Path, Path]] = []

    def validate(contract: Path, manifest: Path, registry: Path) -> dict[str, str]:
        received.append((contract, manifest, registry))
        return {"status": "passed"}

    monkeypatch.setattr(view_module, "validate_split_evidence", validate)
    build_dataset_view_plan(plan_case.root, plan_case.strategy, "train", ["feature_a"])
    assert received == [
        (
            plan_case.root / "configs" / "benchmarks" / "lbnl_sd_ahu_split_v1.yaml",
            plan_case.manifest,
            plan_case.registry,
        )
    ]


def test_nonpassing_evidence_fails(
    plan_case: PlanCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        view_module,
        "validate_split_evidence",
        lambda *_: {"status": "failed"},
    )
    with pytest.raises(DatasetViewError, match="did not return passed"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_split_contract_error_is_not_swallowed(
    plan_case: PlanCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*_: Path) -> dict[str, str]:
        raise SplitContractError("specific evidence failure")

    monkeypatch.setattr(view_module, "validate_split_evidence", fail)
    with pytest.raises(SplitContractError, match="specific evidence failure"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_manifest_registry_identity_mismatch_fails(plan_case: PlanCase) -> None:
    registry = _load_yaml(plan_case.registry)
    registry["scenarios"][0]["scenario_id"] = "different"
    _write_yaml(plan_case.registry, registry)
    with pytest.raises(DatasetViewError, match="order or identity"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_manifest_registry_order_mismatch_fails(plan_case: PlanCase) -> None:
    registry = _load_yaml(plan_case.registry)
    registry["scenarios"][0], registry["scenarios"][1] = (
        registry["scenarios"][1],
        registry["scenarios"][0],
    )
    _write_yaml(plan_case.registry, registry)
    with pytest.raises(DatasetViewError, match="order or identity"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_missing_canonical_parquet_fails(plan_case: PlanCase) -> None:
    (plan_case.canonical_dir / "scenario_00.parquet").unlink()
    with pytest.raises(DatasetViewError, match="missing"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_unexpected_canonical_parquet_fails(plan_case: PlanCase) -> None:
    _write_small_parquet(plan_case.canonical_dir / "unexpected.parquet")
    with pytest.raises(DatasetViewError, match="unexpected"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_plan_order_is_registry_order_not_glob_order(plan_case: PlanCase) -> None:
    plan = build_dataset_view_plan(
        plan_case.root, plan_case.strategy, "validation", ["feature_a"]
    )
    assert tuple(source.order for source in plan.sources) == tuple(range(1, 22))
    assert tuple(source.scenario_id for source in plan.sources) == plan_case.scenario_ids


@pytest.mark.parametrize("partition", ["train", "validation", "test"])
def test_modeling_partition_is_accepted(plan_case: PlanCase, partition: str) -> None:
    plan = build_dataset_view_plan(
        plan_case.root, plan_case.strategy, partition, ["feature_a"]
    )
    assert plan.partition.label == partition


@pytest.mark.parametrize(
    "partition",
    [
        "purge_train_validation",
        "purge_validation_test",
        "outside_primary_benchmark_scope",
    ],
)
def test_excluded_partition_is_rejected(plan_case: PlanCase, partition: str) -> None:
    with pytest.raises(DatasetViewError, match="not a modeling view"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, partition, ["feature_a"]
        )


def test_unknown_partition_is_rejected(plan_case: PlanCase) -> None:
    with pytest.raises(DatasetViewError, match="Unknown modeling partition"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "unknown", ["feature_a"]
        )


@pytest.mark.parametrize("columns", [[], (), "feature_a"])
def test_empty_or_scalar_projection_is_rejected(
    plan_case: PlanCase, columns: Any
) -> None:
    with pytest.raises(DatasetViewError, match="columns"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", columns
        )


def test_duplicate_projection_is_rejected(plan_case: PlanCase) -> None:
    with pytest.raises(DatasetViewError, match="unique"):
        build_dataset_view_plan(
            plan_case.root,
            plan_case.strategy,
            "train",
            ["feature_a", "feature_a"],
        )


def test_unknown_source_column_is_rejected(plan_case: PlanCase) -> None:
    with pytest.raises(DatasetViewError, match="Projected columns"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["unknown_column"]
        )


@pytest.mark.parametrize("batch_size", [0, -1, True])
def test_invalid_plan_batch_size_is_rejected(
    plan_case: PlanCase, batch_size: Any
) -> None:
    with pytest.raises(DatasetViewError, match="batch_size"):
        build_dataset_view_plan(
            plan_case.root,
            plan_case.strategy,
            "train",
            ["feature_a"],
            batch_size=batch_size,
        )


def test_missing_datetime_column_is_rejected(plan_case: PlanCase) -> None:
    path = plan_case.canonical_dir / "scenario_00.parquet"
    pq.write_table(pa.table({"feature_a": [1], "feature_b": [2]}), path)
    with pytest.raises(DatasetViewError, match="Datetime is missing"):
        build_dataset_view_plan(
            plan_case.root, plan_case.strategy, "train", ["feature_a"]
        )


def test_requested_output_order_is_preserved(stream_case: StreamCase) -> None:
    batches = _collect(
        stream_case, columns=("feature_b", "Datetime", "feature_a")
    )
    assert all(
        batch.record_batch.schema.names == ["feature_b", "Datetime", "feature_a"]
        for batch in batches
    )


def test_datetime_filter_works_when_not_projected(stream_case: StreamCase) -> None:
    batches = _collect(stream_case, columns=("feature_b", "feature_a"))
    assert sum(batch.record_batch.num_rows for batch in batches) == 3
    assert all(batch.record_batch.schema.names == ["feature_b", "feature_a"] for batch in batches)


def test_datetime_is_included_only_when_requested(stream_case: StreamCase) -> None:
    without = _collect(stream_case, columns=("feature_a",))
    with_datetime = _collect(stream_case, columns=("Datetime", "feature_a"))
    assert all("Datetime" not in item.record_batch.schema.names for item in without)
    assert all("Datetime" in item.record_batch.schema.names for item in with_datetime)


def test_payload_is_arrow_record_batch(stream_case: StreamCase) -> None:
    assert all(isinstance(item.record_batch, pa.RecordBatch) for item in _collect(stream_case))


def test_batches_are_bounded_and_multiple(stream_case: StreamCase) -> None:
    batches = _collect(stream_case, batch_size=2)
    assert len(batches) > 1
    assert all(0 < item.record_batch.num_rows <= 2 for item in batches)


def test_scenario_wrapper_metadata_is_preserved(stream_case: StreamCase) -> None:
    batch = _collect(stream_case)[0]
    assert batch.scenario_order == 1
    assert batch.scenario_id == "synthetic_scenario"
    assert batch.source_filename == "synthetic.csv"
    assert batch.output_filename == "source.parquet"
    assert batch.partition == "train"


def test_source_row_order_and_inclusive_boundaries_are_preserved(
    stream_case: StreamCase,
) -> None:
    batches = _collect(stream_case, columns=("Datetime", "feature_a"))
    table = pa.Table.from_batches([item.record_batch for item in batches])
    timestamps = table.column("Datetime").to_pylist()
    assert timestamps == list(
        pd.date_range("2020-01-01T00:01:00", periods=3, freq="min").to_pydatetime()
    )
    assert table.column("feature_a").to_pylist() == [1, 2, 3]


def test_rows_immediately_outside_partition_are_excluded(stream_case: StreamCase) -> None:
    batches = _collect(stream_case, columns=("Datetime",))
    table = pa.Table.from_batches([item.record_batch for item in batches])
    values = set(table.column("Datetime").to_pylist())
    assert pd.Timestamp("2020-01-01T00:00:00").to_pydatetime() not in values
    assert pd.Timestamp("2020-01-01T00:04:00").to_pydatetime() not in values


def test_complete_scenario_expected_count_passes(stream_case: StreamCase) -> None:
    batches = _collect(stream_case, batch_size=1)
    assert sum(item.record_batch.num_rows for item in batches) == 3


def test_complete_scenario_count_mismatch_fails(stream_case: StreamCase) -> None:
    wrong = replace(stream_case.window, rows_per_scenario=4, total_rows=4)
    with pytest.raises(DatasetViewError, match="row count mismatch"):
        _collect(stream_case, window=wrong)


def test_early_stop_does_not_claim_count_validation(stream_case: StreamCase) -> None:
    wrong = replace(stream_case.window, rows_per_scenario=4, total_rows=4)
    batches = _iter_scenario_view_batches(
        stream_case.source, wrong, ("feature_a",), 1
    )
    first = next(batches)
    assert first.record_batch.num_rows == 1
    batches.close()


def test_invalid_stream_batch_size_is_rejected(stream_case: StreamCase) -> None:
    with pytest.raises(DatasetViewError, match="batch_size"):
        list(
            _iter_scenario_view_batches(
                stream_case.source, stream_case.window, ("feature_a",), 0
            )
        )


def test_scan_is_read_only_and_creates_no_artifact(stream_case: StreamCase) -> None:
    path = stream_case.source.path
    before_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    before_mtime = path.stat().st_mtime_ns
    before_files = {item.relative_to(stream_case.root) for item in stream_case.root.rglob("*")}
    _collect(stream_case)
    after_files = {item.relative_to(stream_case.root) for item in stream_case.root.rglob("*")}
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_hash
    assert path.stat().st_mtime_ns == before_mtime
    assert after_files == before_files
