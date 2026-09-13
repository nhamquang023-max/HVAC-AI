"""Runtime duplicate-content weights for the LBNL SD-AHU benchmark."""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import numpy as np
import pyarrow as pa
import yaml

from hvac_ai.data.lbnl_sd_ahu_split import validate_split_contract
from hvac_ai.data.lbnl_sd_ahu_view import (
    DatasetViewBatch,
    DatasetViewPlan,
    iter_dataset_view_batches,
    validate_dataset_view_strategy,
)

POLICY_ID = "lbnl_sd_ahu_duplicate_weighting_v1"
POLICY_VERSION = 1
DATASET_ID = "lbnl_sd_ahu"
ALLOWED_TASKS = ("fault_detection", "fault_family_diagnosis")
MODELING_PARTITIONS = ("train", "validation", "test")
WEIGHT_METHOD = "inverse_duplicate_group_size"
NON_DUPLICATE_WEIGHT = 1.0

_EXPECTED_SOURCE_PATHS = {
    "split_contract": "configs/benchmarks/lbnl_sd_ahu_split_v1.yaml",
    "scenario_registry": "configs/lbnl_sd_ahu_scenarios.yaml",
    "duplicate_audit": "docs/datasets/lbnl_sd_ahu_duplicate_audit.md",
    "dataset_view_strategy": "configs/benchmarks/lbnl_sd_ahu_dataset_view_v1.yaml",
}
_EXPECTED_GROUP_NAMES = ("duplicate_group_001", "duplicate_group_002")


class DuplicateWeightingError(ValueError):
    """Raised when duplicate weighting cannot be derived safely."""


@dataclass(frozen=True)
class DuplicateGroupExpectation:
    """Policy expectation for one canonical duplicate group."""

    name: str
    expected_size: int
    member_weight: float
    severity_content_distinguishable: bool


@dataclass(frozen=True)
class PartitionWeightSupport:
    """Expected raw population and duplicate-corrected weighted support."""

    partition: str
    rows_per_scenario: int
    raw_rows: int
    corrected_weight_sum: float


@dataclass(frozen=True)
class DuplicateWeightingPolicy:
    """Validated machine-readable Duplicate Weighting Policy V1."""

    policy_id: str
    policy_version: int
    dataset: str
    tasks: tuple[str, ...]
    split_contract: str
    scenario_registry: str
    duplicate_audit: str
    dataset_view_strategy: str
    duplicate_group_source: str
    membership_resolution: str
    filename_substring_inference: str
    logical_scenarios: int
    unique_content_units: int
    nonduplicate_scenarios: int
    duplicate_scenarios: int
    all_logical_scenarios_retained: bool
    weight_method: str
    nonduplicate_weight: float
    duplicate_member_formula: str
    batch_independent: bool
    groups: tuple[DuplicateGroupExpectation, ...]
    training_mechanism: str
    weighting_required_for_primary_v1: bool
    physical_deduplication: str
    duplicate_resampling: str
    ignore_weights: str
    unsupported_estimator_policy: str
    validation_resampling: str
    test_resampling: str
    raw_logical_metrics: str
    duplicate_corrected_metrics: str
    primary_aggregate_view: str
    class_balance_weights_in_validation_test: str
    supports: tuple[PartitionWeightSupport, ...]
    raw_modeling_rows: int
    corrected_modeling_weight_sum: float
    class_imbalance_solved: bool
    future_class_weight_source: str
    severity_primary_status: str
    severity_information_restored: bool
    weight_source: str
    feature_value_inference: str
    derive_at_runtime: bool
    row_level_weight_artifact: str
    canonical_parquet_mutation: str
    implementation_recorded_as_complete: bool
    model_training_allowed_before_implementation: bool


@dataclass(frozen=True)
class ScenarioDuplicateWeight:
    """Duplicate weight and identity for one logical scenario."""

    scenario_order: int
    scenario_id: str
    source_filename: str
    output_filename: str
    duplicate_group: str | None
    group_size: int
    duplicate_weight: float


@dataclass(frozen=True)
class DuplicateWeightPlan:
    """Validated duplicate-weight plan bound to one Dataset View partition."""

    policy: DuplicateWeightingPolicy
    task: str
    view_id: str
    partition: str
    scenario_weights: tuple[ScenarioDuplicateWeight, ...]
    effective_content_units: float
    expected_raw_rows: int
    expected_duplicate_corrected_weight_sum: float


@dataclass(frozen=True)
class WeightedDatasetViewBatch:
    """One original Dataset View batch plus a row-aligned duplicate weight."""

    original: DatasetViewBatch
    duplicate_weight: float
    sample_weight: np.ndarray


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
            raise DuplicateWeightingError("YAML mapping keys must be hashable") from error
        if duplicate:
            raise DuplicateWeightingError(f"Duplicate YAML mapping key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    try:
        loaded = yaml.load(Path(path).read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise DuplicateWeightingError(
            f"Could not read duplicate-weighting policy: {path}"
        ) from error
    if not isinstance(loaded, dict):
        raise DuplicateWeightingError("Duplicate-weighting policy must be a YAML mapping")
    return loaded


def _mapping(parent: Mapping[str, Any], key: str, label: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        raise DuplicateWeightingError(f"{label}.{key} must be a mapping")
    return value


def _string(parent: Mapping[str, Any], key: str, label: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise DuplicateWeightingError(f"{label}.{key} must be a non-empty string")
    return value


def _integer(parent: Mapping[str, Any], key: str, label: str) -> int:
    value = parent.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise DuplicateWeightingError(f"{label}.{key} must be an integer")
    return value


def _number(parent: Mapping[str, Any], key: str, label: str) -> float:
    value = parent.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise DuplicateWeightingError(f"{label}.{key} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise DuplicateWeightingError(f"{label}.{key} must be finite")
    return result


def _boolean(parent: Mapping[str, Any], key: str, label: str) -> bool:
    value = parent.get(key)
    if not isinstance(value, bool):
        raise DuplicateWeightingError(f"{label}.{key} must be boolean")
    return value


def _strings(parent: Mapping[str, Any], key: str, label: str) -> tuple[str, ...]:
    value = parent.get(key)
    if not isinstance(value, list) or not value:
        raise DuplicateWeightingError(f"{label}.{key} must be a non-empty list")
    if any(not isinstance(item, str) or not item for item in value):
        raise DuplicateWeightingError(f"{label}.{key} must contain non-empty strings")
    if len(value) != len(set(value)):
        raise DuplicateWeightingError(f"{label}.{key} must not contain duplicates")
    return tuple(value)


def _require_repo_relative_path(value: str, label: str) -> str:
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value)
    if windows.is_absolute() or posix.is_absolute() or windows.drive:
        raise DuplicateWeightingError(f"{label} must be repository-relative")
    if ".." in windows.parts or ".." in posix.parts:
        raise DuplicateWeightingError(f"{label} must not traverse a parent directory")
    return value


def _require_equal(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise DuplicateWeightingError(f"{label} must be {expected!r}")


def _parse_group_expectations(raw: Mapping[str, Any]) -> tuple[DuplicateGroupExpectation, ...]:
    groups = _mapping(raw, "expected_groups", "policy")
    if tuple(groups) != _EXPECTED_GROUP_NAMES:
        raise DuplicateWeightingError(
            "Policy expected duplicate groups must be the two frozen V1 groups in order"
        )
    result = []
    for name in _EXPECTED_GROUP_NAMES:
        group = _mapping(groups, name, "expected_groups")
        expected_size = _integer(group, "expected_size", name)
        member_weight = _number(group, "member_weight", name)
        distinguishable = _boolean(
            group, "severity_content_distinguishable", name
        )
        if expected_size <= 0:
            raise DuplicateWeightingError(f"{name}.expected_size must be positive")
        if not math.isclose(member_weight, 1.0 / expected_size):
            raise DuplicateWeightingError(
                f"{name}.member_weight must equal inverse duplicate group size"
            )
        if distinguishable:
            raise DuplicateWeightingError(
                f"{name} must remain severity-content-indistinguishable"
            )
        result.append(
            DuplicateGroupExpectation(
                name=name,
                expected_size=expected_size,
                member_weight=member_weight,
                severity_content_distinguishable=distinguishable,
            )
        )
    return tuple(result)


def _parse_supports(
    raw: Mapping[str, Any], logical_scenarios: int
) -> tuple[tuple[PartitionWeightSupport, ...], int, float]:
    expected = _mapping(raw, "expected_support", "policy")
    per_group = _mapping(raw, "per_group_weight_sums", "policy")
    supports = []
    for partition in MODELING_PARTITIONS:
        group_support = _mapping(per_group, partition, "per_group_weight_sums")
        rows_per_scenario = _integer(
            group_support, "rows_per_member", f"per_group_weight_sums.{partition}"
        )
        raw_rows = _integer(expected, f"raw_{partition}_rows", "expected_support")
        corrected = _number(
            expected,
            f"duplicate_corrected_{partition}_weight_sum",
            "expected_support",
        )
        if raw_rows != rows_per_scenario * logical_scenarios:
            raise DuplicateWeightingError(
                f"{partition} raw support conflicts with rows per scenario"
            )
        supports.append(
            PartitionWeightSupport(
                partition=partition,
                rows_per_scenario=rows_per_scenario,
                raw_rows=raw_rows,
                corrected_weight_sum=corrected,
            )
        )
    raw_modeling = _integer(expected, "raw_modeling_rows", "expected_support")
    corrected_modeling = _number(
        expected, "duplicate_corrected_modeling_weight_sum", "expected_support"
    )
    if raw_modeling != sum(item.raw_rows for item in supports):
        raise DuplicateWeightingError("Raw modeling rows do not equal partition sum")
    if not math.isclose(
        corrected_modeling, sum(item.corrected_weight_sum for item in supports)
    ):
        raise DuplicateWeightingError(
            "Corrected modeling weight does not equal partition sum"
        )
    if _boolean(expected, "weighted_support_is_physical_row_count", "expected_support"):
        raise DuplicateWeightingError("Weighted support must not be a physical row count")
    return tuple(supports), raw_modeling, corrected_modeling


def load_duplicate_weighting_policy(path: Path) -> DuplicateWeightingPolicy:
    """Load and fail-closed validate Duplicate Weighting Policy V1."""
    raw = _load_yaml_mapping(Path(path))
    required = {
        "policy_id",
        "policy_version",
        "dataset",
        "applies_to_tasks",
        "source_of_truth",
        "population",
        "weight_rule",
        "expected_groups",
        "training",
        "evaluation",
        "expected_support",
        "per_group_weight_sums",
        "class_imbalance",
        "severity",
        "provenance_and_feature_boundary",
        "determinism",
        "materialization",
        "implementation_status",
    }
    missing = sorted(required - set(raw))
    if missing:
        raise DuplicateWeightingError(f"Policy is missing required fields: {missing}")

    source = _mapping(raw, "source_of_truth", "policy")
    source_paths = {
        key: _require_repo_relative_path(_string(source, key, "source_of_truth"), key)
        for key in _EXPECTED_SOURCE_PATHS
    }
    population = _mapping(raw, "population", "policy")
    logical_scenarios = _integer(population, "logical_scenarios", "population")
    unique_content_units = _integer(population, "unique_content_units", "population")
    nonduplicate_scenarios = _integer(
        population, "nonduplicate_logical_scenarios", "population"
    )
    duplicate_scenarios = _integer(
        population, "logical_scenarios_in_duplicate_groups", "population"
    )
    groups = _parse_group_expectations(raw)
    supports, raw_modeling, corrected_modeling = _parse_supports(
        raw, logical_scenarios
    )
    weight_rule = _mapping(raw, "weight_rule", "policy")
    training = _mapping(raw, "training", "policy")
    evaluation = _mapping(raw, "evaluation", "policy")
    imbalance = _mapping(raw, "class_imbalance", "policy")
    severity = _mapping(raw, "severity", "policy")
    provenance = _mapping(raw, "provenance_and_feature_boundary", "policy")
    determinism = _mapping(raw, "determinism", "policy")
    materialization = _mapping(raw, "materialization", "policy")
    implementation = _mapping(raw, "implementation_status", "policy")

    composition = _mapping(imbalance, "composition", "class_imbalance")
    _require_equal(
        _string(composition, "final_sample_weight", "class_imbalance.composition"),
        "duplicate_weight * future_task_class_weight",
        "future combined-weight formula",
    )
    _require_equal(
        _string(
            imbalance,
            "validation_test_training_class_weights",
            "class_imbalance",
        ),
        "prohibited",
        "validation/test training class weights",
    )
    for field in (
        "depends_on_random_seed",
        "depends_on_epoch",
        "depends_on_batch_order",
        "depends_on_batch_size",
        "depends_on_model",
        "depends_on_machine",
        "depends_on_filesystem_order",
    ):
        if _boolean(determinism, field, "determinism"):
            raise DuplicateWeightingError(f"determinism.{field} must be false")
    for field in (
        "weighted_dataset_copy",
        "append_weight_to_canonical_schema",
    ):
        _require_equal(
            _string(materialization, field, "materialization"),
            "prohibited",
            f"materialization.{field}",
        )

    policy = DuplicateWeightingPolicy(
        policy_id=_string(raw, "policy_id", "policy"),
        policy_version=_integer(raw, "policy_version", "policy"),
        dataset=_string(raw, "dataset", "policy"),
        tasks=_strings(raw, "applies_to_tasks", "policy"),
        split_contract=source_paths["split_contract"],
        scenario_registry=source_paths["scenario_registry"],
        duplicate_audit=source_paths["duplicate_audit"],
        dataset_view_strategy=source_paths["dataset_view_strategy"],
        duplicate_group_source=_string(
            source, "duplicate_group_source", "source_of_truth"
        ),
        membership_resolution=_string(
            source, "membership_resolution", "source_of_truth"
        ),
        filename_substring_inference=_string(
            source, "filename_substring_inference", "source_of_truth"
        ),
        logical_scenarios=logical_scenarios,
        unique_content_units=unique_content_units,
        nonduplicate_scenarios=nonduplicate_scenarios,
        duplicate_scenarios=duplicate_scenarios,
        all_logical_scenarios_retained=_boolean(
            population, "all_logical_scenarios_retained", "population"
        ),
        weight_method=_string(weight_rule, "method", "weight_rule"),
        nonduplicate_weight=_number(
            weight_rule, "nonduplicate_weight", "weight_rule"
        ),
        duplicate_member_formula=_string(
            weight_rule, "duplicate_member_weight_formula", "weight_rule"
        ),
        batch_independent=_boolean(weight_rule, "batch_independent", "weight_rule"),
        groups=groups,
        training_mechanism=_string(training, "mechanism", "training"),
        weighting_required_for_primary_v1=_boolean(
            training, "weighting_required_for_primary_v1", "training"
        ),
        physical_deduplication=_string(
            training, "physical_deduplication", "training"
        ),
        duplicate_resampling=_string(
            training, "duplicate_resampling", "training"
        ),
        ignore_weights=_string(training, "ignore_weights", "training"),
        unsupported_estimator_policy=_string(
            training, "unsupported_estimator_policy", "training"
        ),
        validation_resampling=_string(
            evaluation, "validation_resampling", "evaluation"
        ),
        test_resampling=_string(evaluation, "test_resampling", "evaluation"),
        raw_logical_metrics=_string(
            evaluation, "raw_logical_population_metrics", "evaluation"
        ),
        duplicate_corrected_metrics=_string(
            evaluation, "duplicate_corrected_metrics", "evaluation"
        ),
        primary_aggregate_view=_string(
            evaluation, "primary_aggregate_view", "evaluation"
        ),
        class_balance_weights_in_validation_test=_string(
            evaluation, "class_balance_weights_in_validation_test", "evaluation"
        ),
        supports=supports,
        raw_modeling_rows=raw_modeling,
        corrected_modeling_weight_sum=corrected_modeling,
        class_imbalance_solved=_boolean(
            imbalance, "solved_by_this_policy", "class_imbalance"
        ),
        future_class_weight_source=_string(
            imbalance, "future_class_weight_source", "class_imbalance"
        ),
        severity_primary_status=_string(
            severity, "primary_benchmark_status", "severity"
        ),
        severity_information_restored=_boolean(
            severity, "weighting_restores_severity_information", "severity"
        ),
        weight_source=_string(provenance, "weight_source", "provenance"),
        feature_value_inference=_string(
            provenance, "feature_value_inference", "provenance"
        ),
        derive_at_runtime=_boolean(materialization, "derive_at_runtime", "materialization"),
        row_level_weight_artifact=_string(
            materialization, "row_level_weight_artifact", "materialization"
        ),
        canonical_parquet_mutation=_string(
            materialization, "canonical_parquet_mutation", "materialization"
        ),
        implementation_recorded_as_complete=_boolean(
            implementation, "implemented", "implementation_status"
        ),
        model_training_allowed_before_implementation=_boolean(
            implementation,
            "model_training_allowed_before_implementation",
            "implementation_status",
        ),
    )
    validate_duplicate_weighting_policy(policy)
    return policy


def validate_duplicate_weighting_policy(policy: DuplicateWeightingPolicy) -> None:
    """Validate all frozen identity, safety, and statistical policy invariants."""
    _require_equal(policy.policy_id, POLICY_ID, "policy_id")
    _require_equal(policy.policy_version, POLICY_VERSION, "policy_version")
    _require_equal(policy.dataset, DATASET_ID, "dataset")
    _require_equal(policy.tasks, ALLOWED_TASKS, "applies_to_tasks")
    for key, expected in _EXPECTED_SOURCE_PATHS.items():
        _require_equal(getattr(policy, key), expected, f"source_of_truth.{key}")
    _require_equal(
        policy.duplicate_group_source,
        "split_contract_and_scenario_registry",
        "duplicate_group_source",
    )
    _require_equal(
        policy.membership_resolution,
        "read_canonical_scenario_duplicate_metadata",
        "membership_resolution",
    )
    _require_equal(
        policy.filename_substring_inference,
        "prohibited",
        "filename_substring_inference",
    )
    _require_equal(policy.logical_scenarios, 21, "logical_scenarios")
    _require_equal(policy.unique_content_units, 15, "unique_content_units")
    _require_equal(policy.nonduplicate_scenarios, 13, "nonduplicate_scenarios")
    _require_equal(policy.duplicate_scenarios, 8, "duplicate_scenarios")
    if not policy.all_logical_scenarios_retained:
        raise DuplicateWeightingError("All 21 logical scenarios must be retained")
    _require_equal(policy.weight_method, WEIGHT_METHOD, "weight method")
    if not math.isclose(policy.nonduplicate_weight, NON_DUPLICATE_WEIGHT):
        raise DuplicateWeightingError("Nonduplicate weight must be 1.0")
    _require_equal(
        policy.duplicate_member_formula,
        "1 / group_size",
        "duplicate member formula",
    )
    if not policy.batch_independent:
        raise DuplicateWeightingError("Duplicate weights must be batch-independent")
    _require_equal(policy.training_mechanism, "sample_weight", "training mechanism")
    if not policy.weighting_required_for_primary_v1:
        raise DuplicateWeightingError("Duplicate weighting must be required")
    _require_equal(
        policy.physical_deduplication, "prohibited", "physical deduplication"
    )
    _require_equal(
        policy.duplicate_resampling,
        "prohibited_as_primary_default",
        "duplicate resampling",
    )
    _require_equal(policy.ignore_weights, "prohibited", "ignore weights")
    _require_equal(
        policy.unsupported_estimator_policy,
        "noncompliant_without_explicit_adapter",
        "unsupported estimator policy",
    )
    _require_equal(policy.validation_resampling, "prohibited", "validation resampling")
    _require_equal(policy.test_resampling, "prohibited", "test resampling")
    _require_equal(policy.raw_logical_metrics, "required", "raw logical metrics")
    _require_equal(
        policy.duplicate_corrected_metrics, "required", "corrected metrics"
    )
    _require_equal(
        policy.primary_aggregate_view,
        "duplicate_corrected_population",
        "primary aggregate view",
    )
    _require_equal(
        policy.class_balance_weights_in_validation_test,
        "prohibited",
        "validation/test class weights",
    )
    if policy.class_imbalance_solved:
        raise DuplicateWeightingError("Duplicate weighting must not solve class imbalance")
    _require_equal(
        policy.future_class_weight_source,
        "train_partition_only",
        "future class-weight source",
    )
    _require_equal(policy.severity_primary_status, "not_primary", "severity status")
    if policy.severity_information_restored:
        raise DuplicateWeightingError("Duplicate weighting cannot restore severity")
    _require_equal(
        policy.weight_source, "scenario_level_duplicate_metadata", "weight source"
    )
    _require_equal(
        policy.feature_value_inference, "prohibited", "feature-value inference"
    )
    if not policy.derive_at_runtime:
        raise DuplicateWeightingError("Duplicate weights must be derived at runtime")
    _require_equal(
        policy.row_level_weight_artifact, "prohibited", "row-level weight artifact"
    )
    _require_equal(
        policy.canonical_parquet_mutation, "prohibited", "Parquet mutation"
    )
    if policy.implementation_recorded_as_complete:
        raise DuplicateWeightingError(
            "Frozen architecture must retain its pre-implementation status"
        )
    if policy.model_training_allowed_before_implementation:
        raise DuplicateWeightingError("Model training must remain blocked")


def _partition_support(
    policy: DuplicateWeightingPolicy, partition: str
) -> PartitionWeightSupport:
    if partition not in MODELING_PARTITIONS:
        raise DuplicateWeightingError(f"Unsupported weighting partition: {partition}")
    for support in policy.supports:
        if support.partition == partition:
            return support
    raise DuplicateWeightingError(f"Policy has no support for partition: {partition}")


def _validate_source_identities(view_plan: DatasetViewPlan) -> None:
    sources = view_plan.sources
    if len(sources) != 21:
        raise DuplicateWeightingError("Dataset View plan must contain 21 scenarios")
    if tuple(source.order for source in sources) != tuple(range(1, 22)):
        raise DuplicateWeightingError("Scenario order must be the canonical 1..21 order")
    for attribute in ("scenario_id", "source_filename", "output_filename"):
        values = tuple(getattr(source, attribute) for source in sources)
        if len(values) != len(set(values)):
            raise DuplicateWeightingError(f"Scenario {attribute} values must be unique")
    for source in sources:
        if Path(source.source_filename).name != source.source_filename:
            raise DuplicateWeightingError("Source filenames must be basenames")
        if Path(source.output_filename).name != source.output_filename:
            raise DuplicateWeightingError("Output filenames must be basenames")
        expected_output = Path(source.source_filename).with_suffix(".parquet").name
        if source.output_filename != expected_output:
            raise DuplicateWeightingError(
                f"Scenario identity mismatch for {source.scenario_id}"
            )


def build_duplicate_weight_plan(
    view_plan: DatasetViewPlan,
    policy_path: Path,
    task: str,
) -> DuplicateWeightPlan:
    """Build deterministic scenario weights from the validated split identities."""
    policy = load_duplicate_weighting_policy(Path(policy_path))
    validate_dataset_view_strategy(view_plan.strategy)
    validate_split_contract(view_plan.split_contract)
    if not isinstance(task, str) or task not in policy.tasks:
        raise DuplicateWeightingError(f"Unsupported Primary V1 task: {task!r}")
    if view_plan.strategy.dataset != policy.dataset:
        raise DuplicateWeightingError("Policy and Dataset View datasets differ")
    if view_plan.strategy.split_contract != policy.split_contract:
        raise DuplicateWeightingError("Policy and Dataset View split sources differ")
    if view_plan.strategy.scenario_registry != policy.scenario_registry:
        raise DuplicateWeightingError("Policy and Dataset View registries differ")
    if view_plan.partition.label not in MODELING_PARTITIONS:
        raise DuplicateWeightingError(
            f"Unsupported weighting partition: {view_plan.partition.label}"
        )
    contract_windows = {
        partition.label: partition for partition in view_plan.split_contract.partitions
    }
    expected_window = contract_windows.get(view_plan.partition.label)
    if expected_window is None or view_plan.partition != expected_window:
        raise DuplicateWeightingError(
            "Dataset View partition differs from the validated split contract"
        )
    support = _partition_support(policy, view_plan.partition.label)
    if support.rows_per_scenario != view_plan.partition.rows_per_scenario:
        raise DuplicateWeightingError("Rows per scenario conflict with policy")
    if support.raw_rows != view_plan.partition.total_rows:
        raise DuplicateWeightingError("Raw partition support conflicts with policy")
    if view_plan.expected_total_rows != support.raw_rows:
        raise DuplicateWeightingError("Dataset View expected total conflicts with policy")
    _validate_source_identities(view_plan)

    policy_groups = {group.name: group for group in policy.groups}
    contract_groups = {
        group.name: group for group in view_plan.split_contract.duplicate_groups
    }
    if tuple(contract_groups) != tuple(policy_groups):
        raise DuplicateWeightingError("Policy and split duplicate groups differ")
    sources_by_filename = {source.source_filename: source for source in view_plan.sources}
    assigned: dict[str, tuple[str, int, float]] = {}
    for name, contract_group in contract_groups.items():
        expected_group = policy_groups[name]
        group_size = len(contract_group.members)
        if group_size != expected_group.expected_size:
            raise DuplicateWeightingError(f"{name} size differs from policy")
        runtime_weight = 1.0 / group_size
        if not math.isclose(runtime_weight, expected_group.member_weight):
            raise DuplicateWeightingError(f"{name} runtime weight differs from policy")
        if (
            contract_group.severity_content_distinguishable
            != expected_group.severity_content_distinguishable
        ):
            raise DuplicateWeightingError(f"{name} severity evidence differs")
        for member in contract_group.members:
            source = sources_by_filename.get(member)
            if source is None:
                raise DuplicateWeightingError(
                    f"Duplicate group member is absent from Dataset View plan: {member}"
                )
            if source.scenario_id in assigned:
                raise DuplicateWeightingError(
                    f"Scenario belongs to multiple duplicate groups: {source.scenario_id}"
                )
            assigned[source.scenario_id] = (name, group_size, runtime_weight)

    scenario_weights = []
    for source in view_plan.sources:
        duplicate = assigned.get(source.scenario_id)
        group_name, group_size, weight = (
            duplicate if duplicate is not None else (None, 1, policy.nonduplicate_weight)
        )
        scenario_weights.append(
            ScenarioDuplicateWeight(
                scenario_order=source.order,
                scenario_id=source.scenario_id,
                source_filename=source.source_filename,
                output_filename=source.output_filename,
                duplicate_group=group_name,
                group_size=group_size,
                duplicate_weight=weight,
            )
        )

    duplicate_count = sum(item.duplicate_group is not None for item in scenario_weights)
    if duplicate_count != policy.duplicate_scenarios:
        raise DuplicateWeightingError("Duplicate scenario count differs from policy")
    if len(scenario_weights) - duplicate_count != policy.nonduplicate_scenarios:
        raise DuplicateWeightingError("Nonduplicate scenario count differs from policy")
    effective_units = sum(item.duplicate_weight for item in scenario_weights)
    if not math.isclose(effective_units, float(policy.unique_content_units)):
        raise DuplicateWeightingError("Scenario duplicate weights do not sum to 15")
    for name in policy_groups:
        group_total = sum(
            item.duplicate_weight
            for item in scenario_weights
            if item.duplicate_group == name
        )
        if not math.isclose(group_total, 1.0):
            raise DuplicateWeightingError(f"{name} weight does not sum to 1.0")
    calculated_support = view_plan.partition.rows_per_scenario * effective_units
    if not math.isclose(calculated_support, support.corrected_weight_sum):
        raise DuplicateWeightingError(
            "Calculated duplicate-corrected support conflicts with policy"
        )
    return DuplicateWeightPlan(
        policy=policy,
        task=task,
        view_id=view_plan.strategy.view_id,
        partition=view_plan.partition.label,
        scenario_weights=tuple(scenario_weights),
        effective_content_units=effective_units,
        expected_raw_rows=support.raw_rows,
        expected_duplicate_corrected_weight_sum=support.corrected_weight_sum,
    )


def weight_dataset_view_batch(
    batch: DatasetViewBatch,
    weight_plan: DuplicateWeightPlan,
) -> WeightedDatasetViewBatch:
    """Attach an immutable row-aligned duplicate weight to one existing batch."""
    if not isinstance(batch, DatasetViewBatch):
        raise DuplicateWeightingError("batch must be a DatasetViewBatch")
    if batch.partition != weight_plan.partition:
        raise DuplicateWeightingError("Batch partition differs from weighting plan")
    matches = [
        item for item in weight_plan.scenario_weights if item.scenario_id == batch.scenario_id
    ]
    if len(matches) != 1:
        raise DuplicateWeightingError(
            f"Unknown or ambiguous batch scenario: {batch.scenario_id}"
        )
    scenario = matches[0]
    if batch.scenario_order != scenario.scenario_order:
        raise DuplicateWeightingError("Batch scenario order differs from weighting plan")
    if batch.source_filename != scenario.source_filename:
        raise DuplicateWeightingError("Batch source filename differs from weighting plan")
    if batch.output_filename != scenario.output_filename:
        raise DuplicateWeightingError("Batch output filename differs from weighting plan")
    if not isinstance(batch.record_batch, pa.RecordBatch):
        raise DuplicateWeightingError("Batch payload must be a pyarrow.RecordBatch")
    sample_weight = np.full(
        (batch.record_batch.num_rows,),
        scenario.duplicate_weight,
        dtype=np.float64,
    )
    sample_weight.setflags(write=False)
    return WeightedDatasetViewBatch(
        original=batch,
        duplicate_weight=scenario.duplicate_weight,
        sample_weight=sample_weight,
    )


def _validate_plan_binding(
    view_plan: DatasetViewPlan, weight_plan: DuplicateWeightPlan
) -> None:
    if weight_plan.view_id != view_plan.strategy.view_id:
        raise DuplicateWeightingError("Weight plan is bound to a different Dataset View")
    if weight_plan.partition != view_plan.partition.label:
        raise DuplicateWeightingError("Weight plan is bound to a different partition")
    source_identities = tuple(
        (source.order, source.scenario_id, source.source_filename, source.output_filename)
        for source in view_plan.sources
    )
    weight_identities = tuple(
        (
            item.scenario_order,
            item.scenario_id,
            item.source_filename,
            item.output_filename,
        )
        for item in weight_plan.scenario_weights
    )
    if source_identities != weight_identities:
        raise DuplicateWeightingError(
            "Weight plan scenario identities differ from Dataset View plan"
        )


def iter_duplicate_weighted_batches(
    view_plan: DatasetViewPlan,
    weight_plan: DuplicateWeightPlan,
) -> Iterator[WeightedDatasetViewBatch]:
    """Wrap the existing Dataset View stream without buffering or re-reading it."""
    _validate_plan_binding(view_plan, weight_plan)
    for batch in iter_dataset_view_batches(view_plan):
        yield weight_dataset_view_batch(batch, weight_plan)
