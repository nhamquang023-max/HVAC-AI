"""Read-only dynamic Dataset Views for the LBNL SD-AHU Primary Benchmark V1."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import yaml

from hvac_ai.data.lbnl_sd_ahu_split import (
    BenchmarkSplitContract,
    PartitionWindow,
    load_split_contract,
    validate_split_contract,
    validate_split_evidence,
)

VIEW_ID = "lbnl_sd_ahu_primary_dataset_view_v1"
DATASET_ID = "lbnl_sd_ahu"
SELECTED_STRATEGY = "dynamic_logical_views"
DEFAULT_BATCH_SIZE = 100_000
MODELING_PARTITIONS = ("train", "validation", "test")
EXCLUDED_PARTITIONS = (
    "purge_train_validation",
    "purge_validation_test",
    "outside_primary_benchmark_scope",
)
_EXPECTED_SOURCE_PATHS = {
    "canonical_source": "data/processed/lbnl_sd_ahu/scenarios",
    "split_contract": "configs/benchmarks/lbnl_sd_ahu_split_v1.yaml",
    "split_engine": "src/hvac_ai/data/lbnl_sd_ahu_split.py",
    "full_ingestion_manifest": "docs/datasets/lbnl_sd_ahu_full_ingestion_manifest.json",
    "scenario_registry": "configs/lbnl_sd_ahu_scenarios.yaml",
    "schema_contract": "configs/lbnl_sd_ahu_schema.yaml",
}
_EXPECTED_PARTITION_COUNTS = {
    "train": (175_620, 3_688_020, 21),
    "validation": (64_800, 1_360_800, 21),
    "test": (64_801, 1_360_821, 21),
}


class DatasetViewError(ValueError):
    """Raised when Dataset View configuration, planning, or reading is unsafe."""


@dataclass(frozen=True)
class PartitionExpectation:
    """Expected logical population for one modeling partition."""

    label: str
    rows_per_scenario: int
    total_rows: int
    logical_scenarios: int


@dataclass(frozen=True)
class DatasetViewStrategy:
    """Validated machine-readable Dataset View V1 strategy."""

    view_id: str
    strategy_version: int
    dataset: str
    selected_strategy: str
    canonical_source: str
    split_contract: str
    split_engine: str
    full_ingestion_manifest: str
    scenario_registry: str
    schema_contract: str
    modeling_partitions: tuple[str, ...]
    excluded_partitions: tuple[str, ...]
    partition_expectations: tuple[PartitionExpectation, ...]
    modeling_total_rows: int
    purge_total_rows: int
    outside_total_rows: int
    access_mode: str
    column_projection: str
    timestamp_predicate_filtering: str
    deterministic_scenario_order: str
    source_mutation: str
    physical_partition_materialization: bool
    row_level_index_manifest: bool


@dataclass(frozen=True)
class ScenarioViewSource:
    """One canonical Parquet source in registry-defined order."""

    order: int
    scenario_id: str
    source_filename: str
    output_filename: str
    path: Path
    canonical_rows: int


@dataclass(frozen=True)
class DatasetViewPlan:
    """Validated deterministic plan for one logical modeling view."""

    repo_root: Path
    strategy: DatasetViewStrategy
    split_contract: BenchmarkSplitContract
    partition: PartitionWindow
    sources: tuple[ScenarioViewSource, ...]
    columns: tuple[str, ...]
    batch_size: int
    expected_total_rows: int


@dataclass(frozen=True)
class DatasetViewBatch:
    """One Arrow batch with scenario and partition identity outside its schema."""

    scenario_order: int
    scenario_id: str
    source_filename: str
    output_filename: str
    partition: str
    record_batch: pa.RecordBatch


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as error:
            raise DatasetViewError("YAML mapping keys must be hashable") from error
        if duplicate:
            raise DatasetViewError(f"Duplicate YAML mapping key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    try:
        with Path(path).open(encoding="utf-8") as stream:
            value = yaml.load(stream, Loader=_UniqueKeyLoader)
    except OSError as error:
        raise DatasetViewError(f"Unable to read {label}: {path}") from error
    except yaml.YAMLError as error:
        raise DatasetViewError(f"Invalid YAML in {label}: {error}") from error
    if not isinstance(value, dict):
        raise DatasetViewError(f"{label} must be a YAML mapping: {path}")
    return value


def _load_json_mapping(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_bytes())
    except OSError as error:
        raise DatasetViewError(f"Unable to read {label}: {path}") from error
    except json.JSONDecodeError as error:
        raise DatasetViewError(f"Invalid JSON in {label}: {error}") from error
    if not isinstance(value, dict):
        raise DatasetViewError(f"{label} must be a JSON mapping: {path}")
    return value


def _mapping(parent: Mapping[str, Any], key: str, label: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise DatasetViewError(f"{label}.{key} must be a mapping")
    return value


def _string(parent: Mapping[str, Any], key: str, label: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise DatasetViewError(f"{label}.{key} must be a non-empty string")
    return value


def _integer(parent: Mapping[str, Any], key: str, label: str) -> int:
    value = parent.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise DatasetViewError(f"{label}.{key} must be an integer")
    return value


def _boolean(parent: Mapping[str, Any], key: str, label: str) -> bool:
    value = parent.get(key)
    if not isinstance(value, bool):
        raise DatasetViewError(f"{label}.{key} must be a boolean")
    return value


def _string_sequence(parent: Mapping[str, Any], key: str, label: str) -> tuple[str, ...]:
    value = parent.get(key)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise DatasetViewError(f"{label}.{key} must be a non-empty string list")
    if len(set(value)) != len(value):
        raise DatasetViewError(f"{label}.{key} entries must be unique")
    return tuple(value)


def _require_repo_relative_path(value: str, label: str) -> str:
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value.replace("\\", "/"))
    if windows.is_absolute() or windows.drive or posix.is_absolute() or ".." in posix.parts:
        raise DatasetViewError(f"{label} must be a repository-relative path")
    return value


def _required_top_level(raw: Mapping[str, Any]) -> None:
    required = {
        "view_id",
        "strategy_version",
        "dataset",
        "selected_strategy",
        "source_provenance",
        "source_of_truth",
        "evaluated_strategies",
        "view_semantics",
        "expected_counts",
        "access",
        "predicate_policy",
        "materialization_policy",
        "lightweight_manifest",
        "source_immutability",
    }
    missing = required - set(raw)
    if missing:
        raise DatasetViewError(f"Dataset View strategy lacks fields: {sorted(missing)}")


def load_dataset_view_strategy(path: Path) -> DatasetViewStrategy:
    """Load and fail-closed validate the frozen Dataset View V1 strategy."""
    raw = _load_yaml_mapping(Path(path), "Dataset View strategy")
    _required_top_level(raw)

    provenance = _mapping(raw, "source_provenance", "strategy")
    source_paths = {
        key: _require_repo_relative_path(_string(provenance, key, "source_provenance"), key)
        for key in (
            "canonical_source",
            "split_contract",
            "split_engine",
            "full_ingestion_manifest",
            "scenario_registry",
            "schema_contract",
        )
    }
    semantics = _mapping(raw, "view_semantics", "strategy")
    counts = _mapping(raw, "expected_counts", "strategy")
    expectations: list[PartitionExpectation] = []
    for label in MODELING_PARTITIONS:
        item = _mapping(counts, label, "expected_counts")
        expectations.append(
            PartitionExpectation(
                label=label,
                rows_per_scenario=_integer(item, "rows_per_scenario", f"expected_counts.{label}"),
                total_rows=_integer(item, "total_rows", f"expected_counts.{label}"),
                logical_scenarios=_integer(
                    item, "logical_scenarios", f"expected_counts.{label}"
                ),
            )
        )

    evaluated = _mapping(raw, "evaluated_strategies", "strategy")
    physical = _mapping(evaluated, "physical_partition_materialization", "evaluated_strategies")
    row_index = _mapping(evaluated, "row_level_index_manifest", "evaluated_strategies")
    materialization = _mapping(raw, "materialization_policy", "strategy")
    lightweight = _mapping(raw, "lightweight_manifest", "strategy")
    access = _mapping(raw, "access", "strategy")
    predicates = _mapping(raw, "predicate_policy", "strategy")
    source_of_truth = _mapping(raw, "source_of_truth", "strategy")
    immutability = _mapping(raw, "source_immutability", "strategy")
    scenario_order_sources = _string_sequence(
        source_of_truth, "scenario_identity_and_order", "source_of_truth"
    )
    if scenario_order_sources != ("full_ingestion_manifest", "scenario_registry"):
        raise DatasetViewError("Scenario order sources differ from Dataset View V1")
    if _string(source_of_truth, "partition_membership", "source_of_truth") != "split_contract":
        raise DatasetViewError("Partition membership must come from the split contract")
    if (
        _string(source_of_truth, "assignment_and_validation", "source_of_truth")
        != "split_engine"
    ):
        raise DatasetViewError("Assignment and validation must use the split engine")
    if _string(predicates, "boundary_source", "predicate_policy") != "split_contract":
        raise DatasetViewError("Predicate boundaries must come from the split contract")
    if (
        _string(predicates, "hard_coded_boundary_copy_in_loader", "predicate_policy")
        != "prohibited"
        or _string(
            source_of_truth,
            "duplicate_timestamp_boundaries_in_loader",
            "source_of_truth",
        )
        != "prohibited"
    ):
        raise DatasetViewError("The loader must not maintain duplicate split boundaries")
    immutable_fields = (
        "modify_canonical_parquet",
        "change_schema",
        "reorder_rows",
        "rewrite_compression",
    )
    if _string(immutability, "access", "source_immutability") != "read_only" or any(
        _string(immutability, key, "source_immutability") != "prohibited"
        for key in immutable_fields
    ):
        raise DatasetViewError("Canonical source access must remain read-only")

    strategy = DatasetViewStrategy(
        view_id=_string(raw, "view_id", "strategy"),
        strategy_version=_integer(raw, "strategy_version", "strategy"),
        dataset=_string(raw, "dataset", "strategy"),
        selected_strategy=_string(raw, "selected_strategy", "strategy"),
        canonical_source=source_paths["canonical_source"],
        split_contract=source_paths["split_contract"],
        split_engine=source_paths["split_engine"],
        full_ingestion_manifest=source_paths["full_ingestion_manifest"],
        scenario_registry=source_paths["scenario_registry"],
        schema_contract=source_paths["schema_contract"],
        modeling_partitions=_string_sequence(
            semantics, "modeling_partitions", "view_semantics"
        ),
        excluded_partitions=_string_sequence(
            semantics, "excluded_partitions", "view_semantics"
        ),
        partition_expectations=tuple(expectations),
        modeling_total_rows=_integer(counts, "modeling_total_rows", "expected_counts"),
        purge_total_rows=_integer(counts, "purge_total_rows", "expected_counts"),
        outside_total_rows=_integer(
            counts, "outside_primary_benchmark_scope_rows", "expected_counts"
        ),
        access_mode=_string(access, "default_access_mode", "access"),
        column_projection=_string(access, "column_projection", "access"),
        timestamp_predicate_filtering=_string(
            predicates, "timestamp_predicate_filtering", "predicate_policy"
        ),
        deterministic_scenario_order=(
            "required"
            if _string(source_of_truth, "filesystem_glob_order", "source_of_truth")
            == "prohibited"
            else "invalid"
        ),
        source_mutation=(
            "prohibited"
            if _string(immutability, "modify_canonical_parquet", "source_immutability")
            == "prohibited"
            else "invalid"
        ),
        physical_partition_materialization=_boolean(
            materialization,
            "physical_partition_materialization",
            "materialization_policy",
        ),
        row_level_index_manifest=(
            _boolean(row_index, "selected", "evaluated_strategies.row_level_index_manifest")
            or _string(lightweight, "row_level_indices", "lightweight_manifest")
            != "prohibited"
        ),
    )
    if _boolean(
        physical, "selected", "evaluated_strategies.physical_partition_materialization"
    ):
        raise DatasetViewError("Physical partition materialization must not be selected")
    validate_dataset_view_strategy(strategy)
    return strategy


def validate_dataset_view_strategy(strategy: DatasetViewStrategy) -> None:
    """Require the typed strategy to match every loader-relevant V1 invariant."""
    if strategy.view_id != VIEW_ID or strategy.strategy_version != 1:
        raise DatasetViewError("Dataset View identity or strategy version differs from V1")
    if strategy.dataset != DATASET_ID:
        raise DatasetViewError(f"Dataset View dataset must be {DATASET_ID}")
    if strategy.selected_strategy != SELECTED_STRATEGY:
        raise DatasetViewError(f"selected_strategy must be {SELECTED_STRATEGY}")
    actual_paths = {
        "canonical_source": strategy.canonical_source,
        "split_contract": strategy.split_contract,
        "split_engine": strategy.split_engine,
        "full_ingestion_manifest": strategy.full_ingestion_manifest,
        "scenario_registry": strategy.scenario_registry,
        "schema_contract": strategy.schema_contract,
    }
    if actual_paths != _EXPECTED_SOURCE_PATHS:
        raise DatasetViewError("Dataset View source paths differ from V1")
    if strategy.physical_partition_materialization:
        raise DatasetViewError("Physical partition materialization must be false")
    if strategy.row_level_index_manifest:
        raise DatasetViewError("Row-level index manifests must be prohibited")
    if strategy.modeling_partitions != MODELING_PARTITIONS:
        raise DatasetViewError("Modeling partitions must be train, validation, and test")
    if strategy.excluded_partitions != EXCLUDED_PARTITIONS:
        raise DatasetViewError("Excluded partition labels differ from Dataset View V1")
    if strategy.access_mode != "streaming_batches":
        raise DatasetViewError("Default access mode must be streaming_batches")
    if strategy.column_projection != "required":
        raise DatasetViewError("Column projection must be required")
    if strategy.timestamp_predicate_filtering != "required":
        raise DatasetViewError("Timestamp predicate filtering must be required")
    if strategy.deterministic_scenario_order != "required":
        raise DatasetViewError("Deterministic scenario order must be required")
    if strategy.source_mutation != "prohibited":
        raise DatasetViewError("Canonical source mutation must be prohibited")
    if tuple(item.label for item in strategy.partition_expectations) != MODELING_PARTITIONS:
        raise DatasetViewError("Partition expectation order differs from Dataset View V1")
    actual_counts = {
        item.label: (item.rows_per_scenario, item.total_rows, item.logical_scenarios)
        for item in strategy.partition_expectations
    }
    if actual_counts != _EXPECTED_PARTITION_COUNTS:
        raise DatasetViewError("Modeling partition counts differ from Dataset View V1")
    if any(item.logical_scenarios != 21 for item in strategy.partition_expectations):
        raise DatasetViewError("Every modeling view must retain 21 logical scenarios")
    if sum(item.total_rows for item in strategy.partition_expectations) != 6_409_641:
        raise DatasetViewError("Modeling partition totals differ from Dataset View V1")
    if strategy.modeling_total_rows != 6_409_641:
        raise DatasetViewError("modeling_total_rows must be 6409641")
    if strategy.purge_total_rows != 60_480 or strategy.outside_total_rows != 4_348_780:
        raise DatasetViewError("Excluded population counts differ from Dataset View V1")


def _resolve_repo_path(repo_root: Path, value: str, label: str) -> Path:
    root = Path(repo_root).resolve()
    relative = PurePosixPath(value.replace("\\", "/"))
    candidate = root.joinpath(*relative.parts).resolve()
    if not candidate.is_relative_to(root):
        raise DatasetViewError(f"{label} resolves outside the repository")
    return candidate


def _require_regular_file(path: Path, label: str) -> None:
    if not path.exists() or not path.is_file() or path.is_symlink():
        raise DatasetViewError(f"{label} must be an existing regular file: {path}")


def _require_basename(value: str, label: str) -> str:
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value)
    if windows.name != value or posix.name != value or value in {".", ".."}:
        raise DatasetViewError(f"{label} must be a basename")
    return value


def _validate_projection(columns: Sequence[str]) -> tuple[str, ...]:
    if isinstance(columns, (str, bytes)):
        raise DatasetViewError("columns must be a non-empty sequence of column names")
    projected = tuple(columns)
    if not projected or any(not isinstance(column, str) or not column for column in projected):
        raise DatasetViewError("columns must contain non-empty strings")
    if len(set(projected)) != len(projected):
        raise DatasetViewError("Requested columns must be unique")
    return projected


def _partition_window(
    strategy: DatasetViewStrategy,
    contract: BenchmarkSplitContract,
    partition: str,
) -> PartitionWindow:
    if partition not in strategy.modeling_partitions:
        if partition in strategy.excluded_partitions:
            raise DatasetViewError(f"Excluded partition is not a modeling view: {partition}")
        raise DatasetViewError(f"Unknown modeling partition: {partition}")
    matches = [window for window in contract.partitions if window.label == partition]
    if len(matches) != 1:
        raise DatasetViewError(f"Split contract does not define one {partition} window")
    return matches[0]


def _validate_strategy_contract_counts(
    strategy: DatasetViewStrategy, contract: BenchmarkSplitContract
) -> None:
    expectations = {item.label: item for item in strategy.partition_expectations}
    for label in strategy.modeling_partitions:
        matches = [window for window in contract.partitions if window.label == label]
        if len(matches) != 1:
            raise DatasetViewError(f"Split contract does not define one {label} window")
        window = matches[0]
        expected = expectations[label]
        if (
            expected.rows_per_scenario != window.rows_per_scenario
            or expected.total_rows != window.total_rows
            or expected.logical_scenarios != contract.logical_scenario_count
        ):
            raise DatasetViewError(f"Dataset View count differs from split contract: {label}")


def _scenario_sources(
    manifest: Mapping[str, Any],
    registry: Mapping[str, Any],
    canonical_dir: Path,
) -> tuple[ScenarioViewSource, ...]:
    manifest_scenarios = manifest.get("scenarios")
    registry_scenarios = registry.get("scenarios")
    if not isinstance(manifest_scenarios, list) or not isinstance(registry_scenarios, list):
        raise DatasetViewError("Manifest and registry must contain scenario lists")
    if len(manifest_scenarios) != 21 or len(registry_scenarios) != 21:
        raise DatasetViewError("Dataset View V1 requires 21 scenarios")

    sources: list[ScenarioViewSource] = []
    for order, (manifest_item, registry_item) in enumerate(
        zip(manifest_scenarios, registry_scenarios), start=1
    ):
        if not isinstance(manifest_item, dict) or not isinstance(registry_item, dict):
            raise DatasetViewError(f"Scenario record {order} must be a mapping")
        manifest_id = _string(manifest_item, "scenario_id", f"manifest.scenarios[{order}]")
        registry_id = _string(registry_item, "scenario_id", f"registry.scenarios[{order}]")
        source_filename = _require_basename(
            _string(manifest_item, "source_filename", f"manifest.scenarios[{order}]"),
            f"manifest.scenarios[{order}].source_filename",
        )
        registry_filename = _require_basename(
            _string(registry_item, "filename", f"registry.scenarios[{order}]"),
            f"registry.scenarios[{order}].filename",
        )
        output_filename = _require_basename(
            _string(manifest_item, "output_filename", f"manifest.scenarios[{order}]"),
            f"manifest.scenarios[{order}].output_filename",
        )
        if manifest_id != registry_id or source_filename != registry_filename:
            raise DatasetViewError(f"Manifest and registry order or identity differs at {order}")
        if output_filename != Path(registry_filename).with_suffix(".parquet").name:
            raise DatasetViewError(f"Output filename differs from registry identity at {order}")
        if Path(output_filename).suffix.lower() != ".parquet":
            raise DatasetViewError(f"Canonical source must be Parquet: {output_filename}")
        sources.append(
            ScenarioViewSource(
                order=order,
                scenario_id=manifest_id,
                source_filename=source_filename,
                output_filename=output_filename,
                path=canonical_dir / output_filename,
                canonical_rows=_integer(
                    manifest_item, "actual_rows", f"manifest.scenarios[{order}]"
                ),
            )
        )
    return tuple(sources)


def _validate_source_file(source: ScenarioViewSource, columns: Sequence[str]) -> None:
    _require_regular_file(source.path, f"Canonical source {source.output_filename}")
    try:
        parquet_file = pq.ParquetFile(source.path)
    except (OSError, pa.ArrowException) as error:
        raise DatasetViewError(f"Unable to open canonical Parquet: {source.path}") from error
    available = tuple(parquet_file.schema_arrow.names)
    if "Datetime" not in available:
        raise DatasetViewError(f"Datetime is missing from {source.output_filename}")
    missing = [column for column in columns if column not in available]
    if missing:
        raise DatasetViewError(
            f"Projected columns are missing from {source.output_filename}: {missing}"
        )


def build_dataset_view_plan(
    repo_root: Path,
    strategy_path: Path,
    partition: str,
    columns: Sequence[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> DatasetViewPlan:
    """Build a deterministic read-only plan after all V1 evidence and source checks."""
    root = Path(repo_root).resolve()
    if not root.exists() or not root.is_dir():
        raise DatasetViewError(f"repo_root must be an existing directory: {root}")
    strategy_file = Path(strategy_path).resolve()
    if not strategy_file.is_relative_to(root):
        raise DatasetViewError("Dataset View strategy must be inside repo_root")
    _require_regular_file(strategy_file, "Dataset View strategy")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise DatasetViewError("batch_size must be a positive integer")
    projected = _validate_projection(columns)

    strategy = load_dataset_view_strategy(strategy_file)
    paths = {
        "canonical_source": _resolve_repo_path(
            root, strategy.canonical_source, "canonical_source"
        ),
        "split_contract": _resolve_repo_path(root, strategy.split_contract, "split_contract"),
        "split_engine": _resolve_repo_path(root, strategy.split_engine, "split_engine"),
        "full_manifest": _resolve_repo_path(
            root, strategy.full_ingestion_manifest, "full_ingestion_manifest"
        ),
        "registry": _resolve_repo_path(root, strategy.scenario_registry, "scenario_registry"),
        "schema": _resolve_repo_path(root, strategy.schema_contract, "schema_contract"),
    }
    for label in ("split_contract", "split_engine", "full_manifest", "registry", "schema"):
        _require_regular_file(paths[label], label)
    canonical_dir = paths["canonical_source"]
    if not canonical_dir.exists() or not canonical_dir.is_dir():
        raise DatasetViewError(f"canonical_source must be a directory: {canonical_dir}")

    evidence = validate_split_evidence(
        paths["split_contract"], paths["full_manifest"], paths["registry"]
    )
    if evidence.get("status") != "passed":
        raise DatasetViewError("Split evidence validation did not return passed")
    contract = load_split_contract(paths["split_contract"])
    validate_split_contract(contract)
    _validate_strategy_contract_counts(strategy, contract)
    window = _partition_window(strategy, contract, partition)

    manifest = _load_json_mapping(paths["full_manifest"], "Full-ingestion manifest")
    registry = _load_yaml_mapping(paths["registry"], "Scenario registry")
    sources = _scenario_sources(manifest, registry, canonical_dir)
    expected_names = {source.output_filename for source in sources}
    actual_names = {path.name for path in canonical_dir.glob("*.parquet") if path.is_file()}
    if expected_names != actual_names:
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        raise DatasetViewError(
            f"Canonical Parquet file set differs; missing={missing}, unexpected={unexpected}"
        )
    for source in sources:
        _validate_source_file(source, projected)

    expected = next(
        item for item in strategy.partition_expectations if item.label == partition
    )
    if expected.total_rows != window.total_rows:
        raise DatasetViewError("Plan total differs from the split contract")
    return DatasetViewPlan(
        repo_root=root,
        strategy=strategy,
        split_contract=contract,
        partition=window,
        sources=sources,
        columns=projected,
        batch_size=batch_size,
        expected_total_rows=expected.total_rows,
    )


def _predicate(window: PartitionWindow) -> ds.Expression:
    field = ds.field("Datetime")
    return (field >= window.start.to_pydatetime()) & (field <= window.end.to_pydatetime())


def _iter_scenario_view_batches(
    source: ScenarioViewSource,
    partition: PartitionWindow,
    columns: Sequence[str],
    batch_size: int,
) -> Iterator[DatasetViewBatch]:
    projected = _validate_projection(columns)
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise DatasetViewError("batch_size must be a positive integer")
    _validate_source_file(source, projected)
    try:
        scanner = ds.dataset(source.path, format="parquet").scanner(
            columns=list(projected),
            filter=_predicate(partition),
            batch_size=batch_size,
            use_threads=False,
        )
        scenario_rows = 0
        for record_batch in scanner.to_batches():
            if record_batch.num_rows == 0:
                continue
            if record_batch.num_rows > batch_size:
                raise DatasetViewError("Arrow returned a batch larger than batch_size")
            scenario_rows += record_batch.num_rows
            yield DatasetViewBatch(
                scenario_order=source.order,
                scenario_id=source.scenario_id,
                source_filename=source.source_filename,
                output_filename=source.output_filename,
                partition=partition.label,
                record_batch=record_batch,
            )
    except (OSError, pa.ArrowException) as error:
        raise DatasetViewError(f"Unable to scan {source.output_filename}") from error
    if scenario_rows != partition.rows_per_scenario:
        raise DatasetViewError(
            f"Partition row count mismatch for {source.output_filename}: "
            f"{scenario_rows} != {partition.rows_per_scenario}"
        )


def iter_dataset_view_batches(plan: DatasetViewPlan) -> Iterator[DatasetViewBatch]:
    """Yield projected Arrow batches and validate each fully consumed scenario count."""
    validate_dataset_view_strategy(plan.strategy)
    validate_split_contract(plan.split_contract)
    expected_window = _partition_window(
        plan.strategy, plan.split_contract, plan.partition.label
    )
    if plan.partition != expected_window:
        raise DatasetViewError("Plan partition differs from the validated split contract")
    if plan.expected_total_rows != expected_window.total_rows:
        raise DatasetViewError("Plan total differs from the validated split contract")
    if tuple(source.order for source in plan.sources) != tuple(range(1, 22)):
        raise DatasetViewError("Plan must contain 21 sources in deterministic order")

    total_rows = 0
    for source in plan.sources:
        for item in _iter_scenario_view_batches(
            source, plan.partition, plan.columns, plan.batch_size
        ):
            total_rows += item.record_batch.num_rows
            yield item
    if total_rows != plan.expected_total_rows:
        raise DatasetViewError(
            f"Dataset View row count mismatch: {total_rows} != {plan.expected_total_rows}"
        )
