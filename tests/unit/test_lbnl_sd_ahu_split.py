from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from hvac_ai.data.lbnl_sd_ahu_split import (
    BenchmarkSplitContract,
    SplitContractError,
    assign_partition,
    assign_partitions,
    load_split_contract,
    validate_split_evidence,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TRACKED_CONTRACT = REPO_ROOT / "configs" / "benchmarks" / "lbnl_sd_ahu_split_v1.yaml"
TRACKED_MANIFEST = (
    REPO_ROOT / "docs" / "datasets" / "lbnl_sd_ahu_full_ingestion_manifest.json"
)
TRACKED_REGISTRY = REPO_ROOT / "configs" / "lbnl_sd_ahu_scenarios.yaml"


@dataclass(frozen=True)
class EvidenceCase:
    contract: Path
    manifest: Path
    registry: Path


def _contract_data() -> dict[str, Any]:
    value = yaml.safe_load(TRACKED_CONTRACT.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_contract(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    value = copy.deepcopy(_contract_data())
    if mutate is not None:
        mutate(value)
    path = tmp_path / "split.yaml"
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def _json_data(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _yaml_data(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _make_evidence(tmp_path: Path) -> EvidenceCase:
    manifest = tmp_path / "manifest.json"
    registry = tmp_path / "registry.yaml"
    contract = tmp_path / "split.yaml"
    _write_json(manifest, copy.deepcopy(_json_data(TRACKED_MANIFEST)))
    registry.write_text(
        yaml.safe_dump(copy.deepcopy(_yaml_data(TRACKED_REGISTRY)), sort_keys=False),
        encoding="utf-8",
    )
    contract_data = copy.deepcopy(_contract_data())
    contract_data["source_provenance"]["full_ingestion_manifest_sha256"] = hashlib.sha256(
        manifest.read_bytes()
    ).hexdigest().upper()
    contract.write_text(yaml.safe_dump(contract_data, sort_keys=False), encoding="utf-8")
    return EvidenceCase(contract, manifest, registry)


def _update_manifest(
    case: EvidenceCase,
    mutate: Callable[[dict[str, Any]], None],
    *,
    sync_contract_hash: bool = True,
) -> None:
    manifest = _json_data(case.manifest)
    mutate(manifest)
    _write_json(case.manifest, manifest)
    if sync_contract_hash:
        contract = _yaml_data(case.contract)
        contract["source_provenance"]["full_ingestion_manifest_sha256"] = hashlib.sha256(
            case.manifest.read_bytes()
        ).hexdigest().upper()
        case.contract.write_text(
            yaml.safe_dump(contract, sort_keys=False), encoding="utf-8"
        )


@pytest.fixture
def contract() -> BenchmarkSplitContract:
    return load_split_contract(TRACKED_CONTRACT)


def test_valid_v1_contract_loads(contract: BenchmarkSplitContract) -> None:
    assert contract.split_id == "lbnl_sd_ahu_primary_split_v1"
    assert contract.contract_version == 1
    assert contract.rows_per_scenario == 308_101
    assert contract.total_rows == 6_470_121
    assert [window.label for window in contract.partitions] == [
        "train",
        "purge_train_validation",
        "validation",
        "purge_validation_test",
        "test",
    ]


def test_duplicate_yaml_key_fails(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        TRACKED_CONTRACT.read_text(encoding="utf-8") + "split_id: duplicate\n",
        encoding="utf-8",
    )
    with pytest.raises(SplitContractError, match="Duplicate YAML"):
        load_split_contract(path)


@pytest.mark.parametrize("section", ["source_provenance", "partitions"])
def test_required_section_missing_fails(tmp_path: Path, section: str) -> None:
    path = _write_contract(tmp_path, lambda value: value.pop(section))
    with pytest.raises(SplitContractError, match="required sections"):
        load_split_contract(path)


def test_timestamp_yaml_value_must_be_string(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["benchmark_universe"]["start"] = datetime(
            2018, 4, 1, 1, tzinfo=UTC
        )

    with pytest.raises(SplitContractError, match="quoted YAML string"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_invalid_contract_timestamp_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["partitions"]["train"]["start"] = "not-a-timestamp"

    with pytest.raises(SplitContractError, match="valid timestamp"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_timezone_aware_contract_timestamp_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["partitions"]["train"]["start"] = "2018-04-01T01:00:00+00:00"

    with pytest.raises(SplitContractError, match="timezone-naive"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_exact_v1_inclusive_minute_counts_pass(contract: BenchmarkSplitContract) -> None:
    assert [window.rows_per_scenario for window in contract.partitions] == [
        175_620,
        1_440,
        64_800,
        1_440,
        64_801,
    ]
    assert sum(window.rows_per_scenario for window in contract.partitions) == 308_101


def test_wrong_declared_partition_rows_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["partitions"]["train"]["rows_per_scenario"] += 1

    with pytest.raises(SplitContractError, match="calculates"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_overlapping_partition_windows_fail(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        validation = value["partitions"]["validation"]
        validation["start"] = "2018-08-01T23:59:00"
        validation["rows_per_scenario"] = 64_801
        validation["total_rows"] = 1_360_821

    with pytest.raises(SplitContractError, match="overlap or gap"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_gap_between_partition_windows_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        validation = value["partitions"]["validation"]
        validation["start"] = "2018-08-02T00:01:00"
        validation["rows_per_scenario"] = 64_799
        validation["total_rows"] = 1_360_779

    with pytest.raises(SplitContractError, match="overlap or gap"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_wrong_common_total_rows_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["benchmark_universe"]["total_rows"] -= 1

    with pytest.raises(SplitContractError, match="total_rows"):
        load_split_contract(_write_contract(tmp_path, mutate))


def test_purge_gap_not_1440_fails(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["purge_gap_minutes"] = 60

    with pytest.raises(SplitContractError, match="purge_gap_minutes"):
        load_split_contract(_write_contract(tmp_path, mutate))


@pytest.mark.parametrize(
    ("timestamp", "expected"),
    [
        ("2018-04-01T01:00:00", "train"),
        ("2018-07-31T23:59:00", "train"),
        ("2018-08-01T00:00:00", "purge_train_validation"),
        ("2018-08-01T23:59:00", "purge_train_validation"),
        ("2018-08-02T00:00:00", "validation"),
        ("2018-09-15T23:59:00", "validation"),
        ("2018-09-16T00:00:00", "purge_validation_test"),
        ("2018-09-16T23:59:00", "purge_validation_test"),
        ("2018-09-17T00:00:00", "test"),
        ("2018-11-01T00:00:00", "test"),
        ("2018-04-01T00:59:00", "outside_primary_benchmark_scope"),
        ("2018-11-01T00:01:00", "outside_primary_benchmark_scope"),
    ],
)
def test_exact_boundary_assignment(
    contract: BenchmarkSplitContract, timestamp: str, expected: str
) -> None:
    assert assign_partition(timestamp, contract) == expected


@pytest.mark.parametrize("value", [pd.NaT, "not-a-timestamp"])
def test_invalid_single_timestamp_fails(
    contract: BenchmarkSplitContract, value: object
) -> None:
    with pytest.raises(SplitContractError):
        assign_partition(value, contract)


def test_timezone_aware_single_timestamp_fails(contract: BenchmarkSplitContract) -> None:
    with pytest.raises(SplitContractError, match="timezone-naive"):
        assign_partition(pd.Timestamp("2018-06-01", tz="UTC"), contract)


def test_vectorized_assignment_preserves_index_and_input(
    contract: BenchmarkSplitContract,
) -> None:
    values = pd.Series(
        [
            "2018-04-01T00:59:00",
            "2018-04-01T01:00:00",
            "2018-08-01T00:00:00",
            "2018-08-02T00:00:00",
            "2018-09-16T00:00:00",
            "2018-09-17T00:00:00",
            "2018-11-01T00:01:00",
        ],
        index=[11, 13, 17, 19, 23, 29, 31],
        name="Datetime",
    )
    original = values.copy(deep=True)
    result = assign_partitions(values, contract)
    pd.testing.assert_series_equal(values, original)
    assert result.index.equals(values.index)
    assert result.tolist() == [
        "outside_primary_benchmark_scope",
        "train",
        "purge_train_validation",
        "validation",
        "purge_validation_test",
        "test",
        "outside_primary_benchmark_scope",
    ]


@pytest.mark.parametrize("value", ["invalid", pd.NaT])
def test_vectorized_invalid_member_fails(
    contract: BenchmarkSplitContract, value: object
) -> None:
    values = pd.Series(["2018-05-01T00:00:00", value])
    with pytest.raises(SplitContractError):
        assign_partitions(values, contract)


def test_vectorized_timezone_aware_input_fails(
    contract: BenchmarkSplitContract,
) -> None:
    values = pd.Series(pd.date_range("2018-05-01", periods=2, freq="min", tz="UTC"))
    with pytest.raises(SplitContractError, match="timezone-naive"):
        assign_partitions(values, contract)


def test_full_common_timeline_counts(contract: BenchmarkSplitContract) -> None:
    timeline = pd.Series(
        pd.date_range(
            "2018-04-01T01:00:00",
            "2018-11-01T00:00:00",
            freq="min",
        )
    )
    counts = assign_partitions(timeline, contract).value_counts().to_dict()
    assert counts == {
        "train": 175_620,
        "purge_train_validation": 1_440,
        "validation": 64_800,
        "purge_validation_test": 1_440,
        "test": 64_801,
    }
    assert len(timeline) == 308_101
    assert len(timeline) * contract.logical_scenario_count == 6_470_121


def test_assignment_is_independent_of_scenario_identity(
    contract: BenchmarkSplitContract,
) -> None:
    timestamp = "2018-06-01T12:00:00"
    labels = {
        member: assign_partition(timestamp, contract)
        for group in contract.duplicate_groups
        for member in group.members
    }
    assert set(labels.values()) == {"train"}


def test_real_repository_evidence_passes() -> None:
    summary = validate_split_evidence(
        TRACKED_CONTRACT,
        TRACKED_MANIFEST,
        TRACKED_REGISTRY,
    )
    assert summary["status"] == "passed"
    assert summary["manifest_scenarios"] == 21
    assert summary["manifest_total_rows"] == 10_818_901
    assert summary["manifest_columns"] == 42
    assert summary["registry_scenarios"] == 21
    assert summary["scenario_sets_match"] is True


def test_valid_synthetic_evidence_returns_summary(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    summary = validate_split_evidence(case.contract, case.manifest, case.registry)
    assert summary["status"] == "passed"
    assert summary["short_exactly_defines_common_universe"] is True
    assert summary["duplicate_groups_match_registry"] is True
    assert summary["duplicate_weighting_status"] == "required_before_model_training"


def test_manifest_sha_mismatch_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    case.manifest.write_text(
        case.manifest.read_text(encoding="utf-8") + " ", encoding="utf-8"
    )
    with pytest.raises(SplitContractError, match="SHA-256"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_manifest_status_not_complete_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    _update_manifest(case, lambda value: value.update(dataset_status="failed"))
    with pytest.raises(SplitContractError, match="dataset_status"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_manifest_scenario_count_mismatch_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    _update_manifest(case, lambda value: value.update(scenario_count=20))
    with pytest.raises(SplitContractError, match="scenario_count"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_short_scenario_coverage_mismatch_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)

    def mutate(value: dict[str, Any]) -> None:
        short = next(
            item
            for item in value["scenarios"]
            if item["source_filename"] == "damper_stuck_100_annual_short.csv"
        )
        short["datetime_end"] = "2018-11-01T00:01:00"

    _update_manifest(case, mutate)
    with pytest.raises(SplitContractError, match="exactly define"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_scenario_lacking_common_overlap_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)

    def mutate(value: dict[str, Any]) -> None:
        value["scenarios"][0]["datetime_start"] = "2018-04-01T01:01:00"

    _update_manifest(case, mutate)
    with pytest.raises(SplitContractError, match="does not cover"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_duplicate_group_registry_mismatch_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    registry = _yaml_data(case.registry)
    member = next(
        item for item in registry["scenarios"] if item["filename"] == "oa_bias_-2_annual.csv"
    )
    member["byte_identical_group"] = None
    case.registry.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    with pytest.raises(SplitContractError, match="duplicate groups differ"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_distinguishable_flag_inside_duplicate_group_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    registry = _yaml_data(case.registry)
    member = next(
        item for item in registry["scenarios"] if item["filename"] == "oa_bias_-2_annual.csv"
    )
    member["severity_content_distinguishable"] = True
    case.registry.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    with pytest.raises(SplitContractError, match="severity-content-indistinguishable"):
        validate_split_evidence(case.contract, case.manifest, case.registry)


def test_manifest_registry_scenario_set_mismatch_fails(tmp_path: Path) -> None:
    case = _make_evidence(tmp_path)
    registry = _yaml_data(case.registry)
    registry["scenarios"][0]["filename"] = "renamed_fault_free.csv"
    case.registry.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    with pytest.raises(SplitContractError, match="filename sets differ"):
        validate_split_evidence(case.contract, case.manifest, case.registry)
