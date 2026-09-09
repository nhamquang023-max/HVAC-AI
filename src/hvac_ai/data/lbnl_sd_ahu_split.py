"""Validate and apply the LBNL SD-AHU Primary Benchmark Split V1 contract."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import pandas as pd
import yaml

SPLIT_ID = "lbnl_sd_ahu_primary_split_v1"
DATASET_ID = "lbnl_sd_ahu"
OUTSIDE_PRIMARY_BENCHMARK_SCOPE = "outside_primary_benchmark_scope"
PARTITION_LABELS = (
    "train",
    "purge_train_validation",
    "validation",
    "purge_validation_test",
    "test",
)

_EXPECTED_WINDOWS = (
    ("train", "2018-04-01T01:00:00", "2018-07-31T23:59:00", 175_620, 3_688_020),
    (
        "purge_train_validation",
        "2018-08-01T00:00:00",
        "2018-08-01T23:59:00",
        1_440,
        30_240,
    ),
    (
        "validation",
        "2018-08-02T00:00:00",
        "2018-09-15T23:59:00",
        64_800,
        1_360_800,
    ),
    (
        "purge_validation_test",
        "2018-09-16T00:00:00",
        "2018-09-16T23:59:00",
        1_440,
        30_240,
    ),
    ("test", "2018-09-17T00:00:00", "2018-11-01T00:00:00", 64_801, 1_360_821),
)

_EXPECTED_DUPLICATE_GROUPS = (
    (
        "duplicate_group_001",
        (
            "oa_bias_-2_annual.csv",
            "oa_bias_-4_annual.csv",
            "oa_bias_2_annual.csv",
            "oa_bias_4_annual.csv",
        ),
    ),
    (
        "duplicate_group_002",
        (
            "coi_leakage_010_annual.csv",
            "coi_leakage_025_annual.csv",
            "coi_leakage_040_annual.csv",
            "coi_leakage_050_annual.csv",
        ),
    ),
)


class SplitContractError(ValueError):
    """Raised when split configuration, timestamps, or evidence violate V1."""


@dataclass(frozen=True)
class PartitionWindow:
    """One inclusive chronological segment in the benchmark universe."""

    label: str
    start: pd.Timestamp
    end: pd.Timestamp
    rows_per_scenario: int
    total_rows: int
    modeling_role: str


@dataclass(frozen=True)
class DuplicateGroup:
    """One group of logical scenarios backed by byte-identical source content."""

    name: str
    members: tuple[str, ...]
    severity_content_distinguishable: bool


@dataclass(frozen=True)
class SourceProvenance:
    """Repository-relative evidence identities pinned by the split contract."""

    archive_sha256: str
    full_ingestion_manifest: str
    full_ingestion_manifest_sha256: str
    scenario_registry: str
    schema_contract: str
    data_audit: str


@dataclass(frozen=True)
class BenchmarkSplitContract:
    """Typed representation of the Primary Benchmark Split V1 contract."""

    split_id: str
    contract_version: int
    dataset: str
    artifact_status: str
    task_scope: tuple[str, ...]
    common_start: pd.Timestamp
    common_end: pd.Timestamp
    rows_per_scenario: int
    logical_scenario_count: int
    total_rows: int
    purge_gap_minutes: int
    partitions: tuple[PartitionWindow, ...]
    duplicate_groups: tuple[DuplicateGroup, ...]
    duplicate_weighting_status: str
    severity_primary_status: str
    severity_affected_families: tuple[str, ...]
    short_scenario: str
    short_start: pd.Timestamp
    short_end: pd.Timestamp
    short_rows: int
    assignment_policy: str
    random_row_split_policy: str
    independent_scenario_split_policy: str
    preprocessing_fit_partition: str
    full_dataset_fit_policy: str
    time_feature_direction: str
    future_window_policy: str
    cross_partition_observation_policy: str
    maximum_lookback_without_review: int
    source_provenance: SourceProvenance


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
            raise SplitContractError("YAML mapping keys must be hashable") from error
        if duplicate:
            raise SplitContractError(f"Duplicate YAML mapping key: {key!r}")
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
        raise SplitContractError(f"Unable to read {label}: {path}") from error
    except yaml.YAMLError as error:
        raise SplitContractError(f"Invalid YAML in {label}: {error}") from error
    if not isinstance(value, dict):
        raise SplitContractError(f"{label} must be a YAML mapping: {path}")
    return value


def _load_json_mapping(path: Path, label: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = Path(path).read_bytes()
    except OSError as error:
        raise SplitContractError(f"Unable to read {label}: {path}") from error
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise SplitContractError(f"Invalid JSON in {label}: {error}") from error
    if not isinstance(value, dict):
        raise SplitContractError(f"{label} must be a JSON object: {path}")
    return raw, value


def _mapping(parent: Mapping[str, Any], key: str, label: str) -> Mapping[str, Any]:
    value = parent.get(key)
    if not isinstance(value, dict):
        raise SplitContractError(f"{label}.{key} must be a mapping")
    return value


def _integer(parent: Mapping[str, Any], key: str, label: str) -> int:
    value = parent.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise SplitContractError(f"{label}.{key} must be an integer")
    return value


def _string(parent: Mapping[str, Any], key: str, label: str) -> str:
    value = parent.get(key)
    if not isinstance(value, str) or not value:
        raise SplitContractError(f"{label}.{key} must be a non-empty string")
    return value


def _boolean(parent: Mapping[str, Any], key: str, label: str) -> bool:
    value = parent.get(key)
    if not isinstance(value, bool):
        raise SplitContractError(f"{label}.{key} must be a boolean")
    return value


def _string_sequence(parent: Mapping[str, Any], key: str, label: str) -> tuple[str, ...]:
    value = parent.get(key)
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise SplitContractError(f"{label}.{key} must be a non-empty string list")
    if len(set(value)) != len(value):
        raise SplitContractError(f"{label}.{key} entries must be unique")
    return tuple(value)


def _parse_contract_timestamp(value: Any, label: str) -> pd.Timestamp:
    if not isinstance(value, str):
        raise SplitContractError(f"{label} must be a quoted YAML string")
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as error:
        raise SplitContractError(f"{label} is not a valid timestamp: {value!r}") from error
    if pd.isna(timestamp):
        raise SplitContractError(f"{label} must not be NaT")
    if timestamp.tzinfo is not None:
        raise SplitContractError(f"{label} must be timezone-naive")
    return timestamp


def _parse_assignment_timestamp(value: object) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as error:
        raise SplitContractError(f"Invalid assignment timestamp: {value!r}") from error
    if pd.isna(timestamp):
        raise SplitContractError("Assignment timestamp must not be NaT")
    if timestamp.tzinfo is not None:
        raise SplitContractError("Assignment timestamp must be timezone-naive")
    return timestamp


def _inclusive_minute_count(start: pd.Timestamp, end: pd.Timestamp, label: str) -> int:
    if end < start:
        raise SplitContractError(f"{label} end precedes start")
    duration = end - start
    minute = pd.Timedelta(minutes=1)
    if duration % minute != pd.Timedelta(0):
        raise SplitContractError(f"{label} boundaries must align to whole minutes")
    return int(duration // minute) + 1


def _require_repo_relative_path(value: str, label: str) -> str:
    windows = PureWindowsPath(value)
    posix = PurePosixPath(value.replace("\\", "/"))
    if windows.is_absolute() or windows.drive or posix.is_absolute() or ".." in posix.parts:
        raise SplitContractError(f"{label} must be a repository-relative path")
    return value


def _require_sha256(value: str, label: str) -> str:
    normalized = value.upper()
    if len(normalized) != 64 or any(character not in "0123456789ABCDEF" for character in normalized):
        raise SplitContractError(f"{label} must be a 64-character SHA-256")
    return normalized


def _required_top_level(contract: Mapping[str, Any]) -> None:
    required = {
        "split_id",
        "contract_version",
        "dataset",
        "artifact_status",
        "task_scope",
        "source_provenance",
        "benchmark_universe",
        "partitions",
        "purge_gap_minutes",
        "short_scenario_policy",
        "duplicate_content_policy",
        "preprocessing_policy",
        "time_feature_policy",
        "severity_policy",
    }
    missing = required - set(contract)
    if missing:
        raise SplitContractError(f"Split contract lacks required sections: {sorted(missing)}")


def load_split_contract(path: Path) -> BenchmarkSplitContract:
    """Load, type, and semantically validate the frozen Split V1 YAML contract."""
    raw = _load_yaml_mapping(Path(path), "Split contract")
    _required_top_level(raw)

    universe = _mapping(raw, "benchmark_universe", "contract")
    common_start = _parse_contract_timestamp(universe.get("start"), "benchmark_universe.start")
    common_end = _parse_contract_timestamp(universe.get("end"), "benchmark_universe.end")

    partitions_raw = _mapping(raw, "partitions", "contract")
    if set(partitions_raw) != set(PARTITION_LABELS):
        raise SplitContractError(
            f"partitions must contain exactly the V1 labels: {list(PARTITION_LABELS)}"
        )
    partitions: list[PartitionWindow] = []
    for label in PARTITION_LABELS:
        item = _mapping(partitions_raw, label, "partitions")
        if item.get("inclusive") != "both":
            raise SplitContractError(f"partitions.{label}.inclusive must be both")
        partitions.append(
            PartitionWindow(
                label=label,
                start=_parse_contract_timestamp(item.get("start"), f"partitions.{label}.start"),
                end=_parse_contract_timestamp(item.get("end"), f"partitions.{label}.end"),
                rows_per_scenario=_integer(item, "rows_per_scenario", f"partitions.{label}"),
                total_rows=_integer(item, "total_rows", f"partitions.{label}"),
                modeling_role=_string(item, "modeling_role", f"partitions.{label}"),
            )
        )

    duplicate_policy = _mapping(raw, "duplicate_content_policy", "contract")
    groups_raw = _mapping(duplicate_policy, "groups", "duplicate_content_policy")
    if set(groups_raw) != {name for name, _ in _EXPECTED_DUPLICATE_GROUPS}:
        raise SplitContractError("duplicate_content_policy.groups must contain both V1 groups")
    duplicate_groups: list[DuplicateGroup] = []
    for name, _ in _EXPECTED_DUPLICATE_GROUPS:
        item = _mapping(groups_raw, name, "duplicate_content_policy.groups")
        duplicate_groups.append(
            DuplicateGroup(
                name=name,
                members=_string_sequence(
                    item, "members", f"duplicate_content_policy.groups.{name}"
                ),
                severity_content_distinguishable=_boolean(
                    item,
                    "severity_content_distinguishable",
                    f"duplicate_content_policy.groups.{name}",
                ),
            )
        )

    provenance_raw = _mapping(raw, "source_provenance", "contract")
    provenance = SourceProvenance(
        archive_sha256=_require_sha256(
            _string(provenance_raw, "archive_sha256", "source_provenance"),
            "source_provenance.archive_sha256",
        ),
        full_ingestion_manifest=_require_repo_relative_path(
            _string(provenance_raw, "full_ingestion_manifest", "source_provenance"),
            "source_provenance.full_ingestion_manifest",
        ),
        full_ingestion_manifest_sha256=_require_sha256(
            _string(provenance_raw, "full_ingestion_manifest_sha256", "source_provenance"),
            "source_provenance.full_ingestion_manifest_sha256",
        ),
        scenario_registry=_require_repo_relative_path(
            _string(provenance_raw, "scenario_registry", "source_provenance"),
            "source_provenance.scenario_registry",
        ),
        schema_contract=_require_repo_relative_path(
            _string(provenance_raw, "schema_contract", "source_provenance"),
            "source_provenance.schema_contract",
        ),
        data_audit=_require_repo_relative_path(
            _string(provenance_raw, "data_audit", "source_provenance"),
            "source_provenance.data_audit",
        ),
    )

    task_scope = _mapping(raw, "task_scope", "contract")
    short_policy = _mapping(raw, "short_scenario_policy", "contract")
    split_policy = _mapping(raw, "split_method_policy", "contract")
    preprocessing = _mapping(raw, "preprocessing_policy", "contract")
    time_features = _mapping(raw, "time_feature_policy", "contract")
    severity = _mapping(raw, "severity_policy", "contract")

    contract = BenchmarkSplitContract(
        split_id=_string(raw, "split_id", "contract"),
        contract_version=_integer(raw, "contract_version", "contract"),
        dataset=_string(raw, "dataset", "contract"),
        artifact_status=_string(raw, "artifact_status", "contract"),
        task_scope=_string_sequence(task_scope, "primary", "task_scope"),
        common_start=common_start,
        common_end=common_end,
        rows_per_scenario=_integer(universe, "rows_per_scenario", "benchmark_universe"),
        logical_scenario_count=_integer(
            universe, "logical_scenarios", "benchmark_universe"
        ),
        total_rows=_integer(universe, "total_rows", "benchmark_universe"),
        purge_gap_minutes=_integer(raw, "purge_gap_minutes", "contract"),
        partitions=tuple(partitions),
        duplicate_groups=tuple(duplicate_groups),
        duplicate_weighting_status=_string(
            duplicate_policy, "weighting_status", "duplicate_content_policy"
        ),
        severity_primary_status=_string(
            severity, "primary_benchmark_status", "severity_policy"
        ),
        severity_affected_families=_string_sequence(
            severity, "affected_fault_families", "severity_policy"
        ),
        short_scenario=_string(short_policy, "scenario", "short_scenario_policy"),
        short_start=_parse_contract_timestamp(
            short_policy.get("observed_start"), "short_scenario_policy.observed_start"
        ),
        short_end=_parse_contract_timestamp(
            short_policy.get("observed_end"), "short_scenario_policy.observed_end"
        ),
        short_rows=_integer(short_policy, "observed_rows", "short_scenario_policy"),
        assignment_policy=_string(split_policy, "assignment", "split_method_policy"),
        random_row_split_policy=_string(
            split_policy, "random_row_split", "split_method_policy"
        ),
        independent_scenario_split_policy=_string(
            split_policy,
            "independent_per_scenario_percentage_split",
            "split_method_policy",
        ),
        preprocessing_fit_partition=_string(
            preprocessing, "fit_partition", "preprocessing_policy"
        ),
        full_dataset_fit_policy=_string(
            preprocessing, "full_dataset_fit", "preprocessing_policy"
        ),
        time_feature_direction=_string(time_features, "direction", "time_feature_policy"),
        future_window_policy=_string(
            time_features, "future_looking_windows", "time_feature_policy"
        ),
        cross_partition_observation_policy=_string(
            time_features, "cross_partition_observations", "time_feature_policy"
        ),
        maximum_lookback_without_review=_integer(
            time_features,
            "maximum_historical_lookback_minutes_without_contract_review",
            "time_feature_policy",
        ),
        source_provenance=provenance,
    )
    validate_split_contract(contract)
    return contract


def validate_split_contract(contract: BenchmarkSplitContract) -> None:
    """Fail closed unless a typed contract satisfies all frozen V1 invariants."""
    if contract.split_id != SPLIT_ID:
        raise SplitContractError(f"split_id must be {SPLIT_ID}")
    if contract.contract_version != 1:
        raise SplitContractError("contract_version must be 1")
    if contract.dataset != DATASET_ID:
        raise SplitContractError(f"dataset must be {DATASET_ID}")
    if contract.artifact_status != "not_materialized":
        raise SplitContractError("artifact_status must be not_materialized")
    if contract.task_scope != ("fault_detection", "fault_family_diagnosis"):
        raise SplitContractError("Primary task scope differs from V1")
    if contract.logical_scenario_count != 21:
        raise SplitContractError("V1 must contain 21 logical scenarios")
    if contract.common_start != pd.Timestamp("2018-04-01T01:00:00"):
        raise SplitContractError("Common start differs from V1")
    if contract.common_end != pd.Timestamp("2018-11-01T00:00:00"):
        raise SplitContractError("Common end differs from V1")
    common_rows = _inclusive_minute_count(
        contract.common_start, contract.common_end, "benchmark universe"
    )
    if common_rows != contract.rows_per_scenario:
        raise SplitContractError(
            f"Common universe calculates {common_rows} rows, "
            f"declares {contract.rows_per_scenario}"
        )
    if contract.rows_per_scenario * contract.logical_scenario_count != contract.total_rows:
        raise SplitContractError("Common universe total_rows is inconsistent")
    if contract.rows_per_scenario != 308_101 or contract.total_rows != 6_470_121:
        raise SplitContractError("Common universe counts differ from V1")

    if tuple(window.label for window in contract.partitions) != PARTITION_LABELS:
        raise SplitContractError("Partition ordering differs from V1")
    calculated_rows: list[int] = []
    for window in contract.partitions:
        rows = _inclusive_minute_count(window.start, window.end, window.label)
        if rows != window.rows_per_scenario:
            raise SplitContractError(
                f"{window.label} calculates {rows} rows, declares {window.rows_per_scenario}"
            )
        if window.total_rows != rows * contract.logical_scenario_count:
            raise SplitContractError(f"{window.label}.total_rows is inconsistent")
        calculated_rows.append(rows)
    if contract.partitions[0].start != contract.common_start:
        raise SplitContractError("Train must begin at the common start")
    if contract.partitions[-1].end != contract.common_end:
        raise SplitContractError("Test must end at the common end")
    for previous, current in zip(contract.partitions, contract.partitions[1:]):
        if current.start != previous.end + pd.Timedelta(minutes=1):
            raise SplitContractError(
                f"Partition topology has an overlap or gap between "
                f"{previous.label} and {current.label}"
            )
    if sum(calculated_rows) != contract.rows_per_scenario:
        raise SplitContractError("Partitions do not fully account for the common universe")
    if sum(window.total_rows for window in contract.partitions) != contract.total_rows:
        raise SplitContractError("Partition totals do not equal the common total")

    for window, expected in zip(contract.partitions, _EXPECTED_WINDOWS):
        label, start, end, rows, total = expected
        if (
            window.label != label
            or window.start != pd.Timestamp(start)
            or window.end != pd.Timestamp(end)
            or window.rows_per_scenario != rows
            or window.total_rows != total
        ):
            raise SplitContractError(f"{window.label} differs from the frozen V1 window")
    purge_windows = (contract.partitions[1], contract.partitions[3])
    if contract.purge_gap_minutes != 1_440:
        raise SplitContractError("purge_gap_minutes must be 1440")
    for window in purge_windows:
        if window.rows_per_scenario != contract.purge_gap_minutes:
            raise SplitContractError(f"{window.label} must be a 1440-minute purge")
        if window.modeling_role != "excluded_purge_gap":
            raise SplitContractError(f"{window.label} must be excluded from modeling")

    expected_groups = dict(_EXPECTED_DUPLICATE_GROUPS)
    actual_groups = {group.name: group.members for group in contract.duplicate_groups}
    if actual_groups != expected_groups:
        raise SplitContractError("Duplicate group members differ from V1")
    if any(group.severity_content_distinguishable for group in contract.duplicate_groups):
        raise SplitContractError("V1 duplicate groups must be severity-indistinguishable")
    if contract.duplicate_weighting_status != "required_before_model_training":
        raise SplitContractError("Duplicate weighting status must block model training")
    if contract.severity_primary_status != "not_primary":
        raise SplitContractError("Severity must remain not_primary")
    if contract.severity_affected_families != ("oa_bias", "coi_leakage"):
        raise SplitContractError("Severity-affected families differ from V1")
    if (
        contract.short_scenario != "damper_stuck_100_annual_short.csv"
        or contract.short_start != contract.common_start
        or contract.short_end != contract.common_end
        or contract.short_rows != contract.rows_per_scenario
    ):
        raise SplitContractError("Short scenario must exactly define the common universe")
    if contract.assignment_policy != "fixed_chronological_boundaries_shared_by_all_scenarios":
        raise SplitContractError("All scenarios must share chronological boundaries")
    if contract.random_row_split_policy != "prohibited":
        raise SplitContractError("Random row splitting must be prohibited")
    if contract.independent_scenario_split_policy != "prohibited":
        raise SplitContractError("Independent per-scenario splitting must be prohibited")
    if contract.preprocessing_fit_partition != "train":
        raise SplitContractError("Preprocessing must be fit on train")
    if contract.full_dataset_fit_policy != "prohibited":
        raise SplitContractError("Full-dataset preprocessing fit must be prohibited")
    if contract.time_feature_direction != "causal_past_only":
        raise SplitContractError("Time features must be causal and past-only")
    if contract.future_window_policy != "prohibited":
        raise SplitContractError("Future-looking windows must be prohibited")
    if contract.cross_partition_observation_policy != "prohibited":
        raise SplitContractError("Cross-partition observations must be prohibited")
    if contract.maximum_lookback_without_review != 1_440:
        raise SplitContractError("Maximum lookback without contract review must be 1440")


def assign_partition(timestamp: object, contract: BenchmarkSplitContract) -> str:
    """Assign one valid naive timestamp to a deterministic V1 partition label."""
    validate_split_contract(contract)
    value = _parse_assignment_timestamp(timestamp)
    for window in contract.partitions:
        if window.start <= value <= window.end:
            return window.label
    return OUTSIDE_PRIMARY_BENCHMARK_SCOPE


def assign_partitions(
    datetimes: pd.Series, contract: BenchmarkSplitContract
) -> pd.Series:
    """Vectorize V1 assignment without mutating the input Series."""
    validate_split_contract(contract)
    if not isinstance(datetimes, pd.Series):
        raise SplitContractError("datetimes must be a pandas Series")
    try:
        parsed = pd.to_datetime(datetimes, errors="raise")
    except (TypeError, ValueError) as error:
        raise SplitContractError("datetimes contains an invalid timestamp") from error
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        raise SplitContractError("datetimes must contain timezone-naive values")
    if not pd.api.types.is_datetime64_any_dtype(parsed.dtype):
        raise SplitContractError("datetimes could not be normalized to naive timestamps")
    if parsed.isna().any():
        raise SplitContractError("datetimes must not contain NaT")
    parsed = parsed.astype("datetime64[ns]")

    labels = pd.Series(
        OUTSIDE_PRIMARY_BENCHMARK_SCOPE,
        index=datetimes.index,
        dtype="string",
        name="partition",
    )
    assigned = pd.Series(False, index=datetimes.index)
    for window in contract.partitions:
        mask = parsed.between(window.start, window.end, inclusive="both")
        if (assigned & mask).any():
            raise SplitContractError("A timestamp matched more than one partition")
        labels.loc[mask] = window.label
        assigned |= mask
    if len(labels) != len(datetimes) or labels.isna().any():
        raise SplitContractError("Partition assignment did not label every timestamp")
    return labels


def _validate_manifest(
    manifest_raw: bytes,
    manifest: Mapping[str, Any],
    contract: BenchmarkSplitContract,
) -> tuple[list[Mapping[str, Any]], str]:
    actual_hash = hashlib.sha256(manifest_raw).hexdigest().upper()
    if actual_hash != contract.source_provenance.full_ingestion_manifest_sha256:
        raise SplitContractError(
            "Full-ingestion manifest SHA-256 differs from the split contract"
        )
    expected_top = {
        "dataset_status": "complete_and_verified",
        "scenario_count": 21,
        "actual_total_rows": 10_818_901,
        "processed_columns": 42,
    }
    for key, expected in expected_top.items():
        if manifest.get(key) != expected:
            raise SplitContractError(f"Full manifest {key} must be {expected!r}")
    if manifest.get("archive_sha256") != contract.source_provenance.archive_sha256:
        raise SplitContractError("Manifest archive SHA-256 differs from the split contract")
    records = manifest.get("scenarios")
    if not isinstance(records, list) or len(records) != contract.logical_scenario_count:
        raise SplitContractError("Full manifest must contain 21 scenario records")

    typed_records: list[Mapping[str, Any]] = []
    output_names: list[str] = []
    actual_row_total = 0
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise SplitContractError(f"Manifest scenario {index} must be a mapping")
        label = f"manifest.scenarios[{index}]"
        filename = _string(record, "source_filename", label)
        _string(record, "scenario_id", label)
        output_names.append(_string(record, "output_filename", label))
        start = _parse_contract_timestamp(record.get("datetime_start"), f"{label}.datetime_start")
        end = _parse_contract_timestamp(record.get("datetime_end"), f"{label}.datetime_end")
        if start > contract.common_start or end < contract.common_end:
            raise SplitContractError(f"{filename} does not cover the common universe")
        rows = _integer(record, "actual_rows", label)
        if rows <= 0:
            raise SplitContractError(f"{filename} actual_rows must be positive")
        actual_row_total += rows
        if _integer(record, "columns", label) != 42:
            raise SplitContractError(f"{filename} must contain 42 columns")
        if record.get("validation_status") != "passed":
            raise SplitContractError(f"{filename} validation_status must be passed")
        typed_records.append(record)
    if len(set(output_names)) != len(output_names):
        raise SplitContractError("Manifest output filenames must be unique")
    if actual_row_total != manifest["actual_total_rows"]:
        raise SplitContractError("Manifest scenario row counts do not equal actual_total_rows")
    return typed_records, actual_hash


def _validate_registry(
    registry: Mapping[str, Any],
    contract: BenchmarkSplitContract,
) -> list[Mapping[str, Any]]:
    source = _mapping(registry, "source", "registry")
    if source.get("archive_sha256") != contract.source_provenance.archive_sha256:
        raise SplitContractError("Registry archive SHA-256 differs from the split contract")
    if _integer(source, "scenario_count", "registry.source") != 21:
        raise SplitContractError("Registry source.scenario_count must be 21")
    scenarios = registry.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != 21:
        raise SplitContractError("Registry must contain 21 scenarios")
    typed_scenarios: list[Mapping[str, Any]] = []
    filenames: list[str] = []
    scenario_ids: list[str] = []
    for index, scenario in enumerate(scenarios, start=1):
        if not isinstance(scenario, dict):
            raise SplitContractError(f"Registry scenario {index} must be a mapping")
        label = f"registry.scenarios[{index}]"
        filenames.append(_string(scenario, "filename", label))
        scenario_ids.append(_string(scenario, "scenario_id", label))
        if "byte_identical_group" not in scenario:
            raise SplitContractError(f"{label}.byte_identical_group is required")
        _boolean(
            scenario,
            "severity_content_distinguishable",
            label,
        )
        typed_scenarios.append(scenario)
    if len(set(filenames)) != len(filenames):
        raise SplitContractError("Registry source filenames must be unique")
    if len(set(scenario_ids)) != len(scenario_ids):
        raise SplitContractError("Registry scenario IDs must be unique")
    return typed_scenarios


def validate_split_evidence(
    contract_path: Path,
    full_manifest_path: Path,
    scenarios_path: Path,
) -> dict[str, object]:
    """Validate tracked V1 evidence without reading or writing processed data."""
    contract = load_split_contract(Path(contract_path))
    manifest_raw, manifest = _load_json_mapping(
        Path(full_manifest_path), "Full-ingestion manifest"
    )
    records, manifest_hash = _validate_manifest(manifest_raw, manifest, contract)
    registry = _load_yaml_mapping(Path(scenarios_path), "Scenario registry")
    scenarios = _validate_registry(registry, contract)

    manifest_by_filename = {str(record["source_filename"]): record for record in records}
    registry_by_filename = {str(scenario["filename"]): scenario for scenario in scenarios}
    if set(manifest_by_filename) != set(registry_by_filename):
        raise SplitContractError("Manifest and registry scenario filename sets differ")
    for filename, scenario in registry_by_filename.items():
        if manifest_by_filename[filename]["scenario_id"] != scenario["scenario_id"]:
            raise SplitContractError(f"Scenario ID differs for {filename}")

    short_records = [
        record for record in records if record["source_filename"] == contract.short_scenario
    ]
    if len(short_records) != 1:
        raise SplitContractError("Manifest must contain exactly one canonical short scenario")
    short = short_records[0]
    if (
        short["actual_rows"] != contract.short_rows
        or _parse_contract_timestamp(short["datetime_start"], "short.datetime_start")
        != contract.common_start
        or _parse_contract_timestamp(short["datetime_end"], "short.datetime_end")
        != contract.common_end
    ):
        raise SplitContractError("Short scenario must exactly define the common universe")

    registry_groups: dict[str, list[str]] = {}
    for scenario in scenarios:
        group = scenario["byte_identical_group"]
        if group is not None:
            if not isinstance(group, str) or not group:
                raise SplitContractError("Registry duplicate group names must be strings")
            registry_groups.setdefault(group, []).append(str(scenario["filename"]))
            if scenario["severity_content_distinguishable"] is not False:
                raise SplitContractError(
                    f"{scenario['filename']} must be severity-content-indistinguishable"
                )
    contract_groups = {group.name: list(group.members) for group in contract.duplicate_groups}
    if registry_groups != contract_groups:
        raise SplitContractError("Registry duplicate groups differ from the split contract")

    return {
        "status": "passed",
        "split_id": contract.split_id,
        "manifest_sha256": manifest_hash,
        "manifest_status": manifest["dataset_status"],
        "manifest_scenarios": manifest["scenario_count"],
        "manifest_total_rows": manifest["actual_total_rows"],
        "manifest_columns": manifest["processed_columns"],
        "registry_scenarios": len(scenarios),
        "scenario_sets_match": True,
        "short_scenario": contract.short_scenario,
        "short_exactly_defines_common_universe": True,
        "duplicate_groups": contract_groups,
        "duplicate_groups_match_registry": True,
        "duplicate_weighting_status": contract.duplicate_weighting_status,
        "severity_primary_status": contract.severity_primary_status,
    }
