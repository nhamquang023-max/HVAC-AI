from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest
import yaml

import hvac_ai.data.lbnl_sd_ahu_weighting as weighting_module
from hvac_ai.data.lbnl_sd_ahu_split import (
    SplitContractError,
    load_split_contract,
)
from hvac_ai.data.lbnl_sd_ahu_view import (
    DatasetViewBatch,
    DatasetViewError,
    DatasetViewPlan,
    ScenarioViewSource,
    load_dataset_view_strategy,
)
from hvac_ai.data.lbnl_sd_ahu_weighting import (
    DuplicateWeightingError,
    DuplicateWeightPlan,
    build_duplicate_weight_plan,
    iter_duplicate_weighted_batches,
    load_duplicate_weighting_policy,
    weight_dataset_view_batch,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TRACKED_POLICY = (
    REPO_ROOT
    / "configs"
    / "benchmarks"
    / "lbnl_sd_ahu_duplicate_weighting_v1.yaml"
)
TRACKED_SPLIT = REPO_ROOT / "configs" / "benchmarks" / "lbnl_sd_ahu_split_v1.yaml"
TRACKED_VIEW = (
    REPO_ROOT / "configs" / "benchmarks" / "lbnl_sd_ahu_dataset_view_v1.yaml"
)
TRACKED_REGISTRY = REPO_ROOT / "configs" / "lbnl_sd_ahu_scenarios.yaml"


def _policy_data() -> dict[str, Any]:
    value = yaml.safe_load(TRACKED_POLICY.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_policy(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    value = copy.deepcopy(_policy_data())
    if mutate is not None:
        mutate(value)
    path = tmp_path / "policy.yaml"
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def _view_plan(tmp_path: Path, partition: str = "train") -> DatasetViewPlan:
    strategy = load_dataset_view_strategy(TRACKED_VIEW)
    contract = load_split_contract(TRACKED_SPLIT)
    window = next(item for item in contract.partitions if item.label == partition)
    registry = yaml.safe_load(TRACKED_REGISTRY.read_text(encoding="utf-8"))
    sources = tuple(
        ScenarioViewSource(
            order=order,
            scenario_id=item["scenario_id"],
            source_filename=item["filename"],
            output_filename=Path(item["filename"]).with_suffix(".parquet").name,
            path=tmp_path / Path(item["filename"]).with_suffix(".parquet").name,
            canonical_rows=item["row_count"],
        )
        for order, item in enumerate(registry["scenarios"], start=1)
    )
    return DatasetViewPlan(
        repo_root=tmp_path,
        strategy=strategy,
        split_contract=contract,
        partition=window,
        sources=sources,
        columns=("feature_a",),
        batch_size=100_000,
        expected_total_rows=window.total_rows,
    )


def _weight_plan(tmp_path: Path, partition: str = "train") -> DuplicateWeightPlan:
    return build_duplicate_weight_plan(
        _view_plan(tmp_path, partition),
        TRACKED_POLICY,
        "fault_detection",
    )


def _scenario(plan: DuplicateWeightPlan, scenario_id: str):
    return next(item for item in plan.scenario_weights if item.scenario_id == scenario_id)


def _batch(
    plan: DuplicateWeightPlan,
    scenario_id: str,
    rows: int = 3,
) -> DatasetViewBatch:
    scenario = _scenario(plan, scenario_id)
    record_batch = pa.record_batch(
        [pa.array(range(rows), type=pa.int64())],
        names=["feature_a"],
    )
    return DatasetViewBatch(
        scenario_order=scenario.scenario_order,
        scenario_id=scenario.scenario_id,
        source_filename=scenario.source_filename,
        output_filename=scenario.output_filename,
        partition=plan.partition,
        record_batch=record_batch,
    )


def test_tracked_policy_loads() -> None:
    policy = load_duplicate_weighting_policy(TRACKED_POLICY)
    assert policy.policy_id == "lbnl_sd_ahu_duplicate_weighting_v1"
    assert policy.policy_version == 1
    assert policy.dataset == "lbnl_sd_ahu"
    assert policy.tasks == ("fault_detection", "fault_family_diagnosis")
    assert policy.weight_method == "inverse_duplicate_group_size"
    assert policy.implementation_recorded_as_complete is False


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda value: value.update(policy_id="wrong"), "policy_id"),
        (lambda value: value.update(policy_version=2), "policy_version"),
        (lambda value: value.update(dataset="other"), "dataset"),
        (
            lambda value: value["weight_rule"].update(method="fixed"),
            "weight method",
        ),
        (
            lambda value: value["weight_rule"].update(nonduplicate_weight=0.5),
            "Nonduplicate weight",
        ),
        (
            lambda value: value["training"].update(
                physical_deduplication="allowed"
            ),
            "physical deduplication",
        ),
        (
            lambda value: value["training"].update(duplicate_resampling="allowed"),
            "duplicate resampling",
        ),
        (
            lambda value: value["materialization"].update(
                row_level_weight_artifact="allowed"
            ),
            "row-level weight artifact",
        ),
        (
            lambda value: value["class_imbalance"].update(
                solved_by_this_policy=True
            ),
            "class imbalance",
        ),
        (
            lambda value: value["severity"].update(
                primary_benchmark_status="primary"
            ),
            "severity status",
        ),
        (
            lambda value: value["materialization"].update(derive_at_runtime=False),
            "derived at runtime",
        ),
        (
            lambda value: value["implementation_status"].update(implemented=True),
            "pre-implementation status",
        ),
    ],
)
def test_policy_violation_is_rejected(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
    match: str,
) -> None:
    with pytest.raises(DuplicateWeightingError, match=match):
        load_duplicate_weighting_policy(_write_policy(tmp_path, mutate))


def test_duplicate_policy_yaml_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        TRACKED_POLICY.read_text(encoding="utf-8") + "\npolicy_id: duplicate\n",
        encoding="utf-8",
    )
    with pytest.raises(DuplicateWeightingError, match="Duplicate YAML mapping key"):
        load_duplicate_weighting_policy(path)


@pytest.mark.parametrize("path_value", ["C:/private/split.yaml", "/private/split.yaml"])
def test_absolute_policy_reference_is_rejected(
    tmp_path: Path, path_value: str
) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["source_of_truth"]["split_contract"] = path_value

    with pytest.raises(DuplicateWeightingError, match="repository-relative"):
        load_duplicate_weighting_policy(_write_policy(tmp_path, mutate))


def test_parent_traversal_policy_reference_is_rejected(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["source_of_truth"]["scenario_registry"] = "../registry.yaml"

    with pytest.raises(DuplicateWeightingError, match="parent directory"):
        load_duplicate_weighting_policy(_write_policy(tmp_path, mutate))


@pytest.mark.parametrize(
    "task",
    ["severity", "severity_classification", "energy_prediction", "unknown"],
)
def test_unsupported_task_is_rejected(tmp_path: Path, task: str) -> None:
    with pytest.raises(DuplicateWeightingError, match="Unsupported Primary V1 task"):
        build_duplicate_weight_plan(_view_plan(tmp_path), TRACKED_POLICY, task)


@pytest.mark.parametrize(
    ("partition", "raw_rows", "corrected_support"),
    [
        ("train", 3_688_020, 2_634_300),
        ("validation", 1_360_800, 972_000),
        ("test", 1_360_821, 972_015),
    ],
)
def test_valid_partition_weight_plan(
    tmp_path: Path,
    partition: str,
    raw_rows: int,
    corrected_support: int,
) -> None:
    plan = _weight_plan(tmp_path, partition)
    assert plan.partition == partition
    assert plan.expected_raw_rows == raw_rows
    assert plan.expected_duplicate_corrected_weight_sum == corrected_support
    assert plan.effective_content_units == 15.0
    assert len(plan.scenario_weights) == 21


def test_tracked_policy_and_split_produce_exact_scenario_weights(
    tmp_path: Path,
) -> None:
    plan = _weight_plan(tmp_path)
    duplicate = [item for item in plan.scenario_weights if item.duplicate_group]
    nonduplicate = [item for item in plan.scenario_weights if not item.duplicate_group]
    assert len(duplicate) == 8
    assert len(nonduplicate) == 13
    assert all(item.duplicate_weight == 0.25 for item in duplicate)
    assert all(item.duplicate_weight == 1.0 for item in nonduplicate)
    assert sum(item.duplicate_weight for item in plan.scenario_weights) == 15.0
    for group_name in ("duplicate_group_001", "duplicate_group_002"):
        members = [
            item for item in plan.scenario_weights if item.duplicate_group == group_name
        ]
        assert len(members) == 4
        assert {item.group_size for item in members} == {4}
        assert sum(item.duplicate_weight for item in members) == 1.0


def test_all_partition_corrected_support_sums_to_modeling_support(
    tmp_path: Path,
) -> None:
    plans = [_weight_plan(tmp_path, partition) for partition in ("train", "validation", "test")]
    assert sum(plan.expected_duplicate_corrected_weight_sum for plan in plans) == 4_578_315
    assert sum(plan.expected_raw_rows for plan in plans) == 6_409_641


def test_missing_duplicate_member_is_rejected(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    sources = list(view_plan.sources)
    index = next(
        index
        for index, source in enumerate(sources)
        if source.source_filename == "oa_bias_-2_annual.csv"
    )
    sources[index] = replace(
        sources[index],
        scenario_id="replacement_nonduplicate",
        source_filename="replacement_nonduplicate.csv",
        output_filename="replacement_nonduplicate.parquet",
    )
    with pytest.raises(DuplicateWeightingError, match="absent from Dataset View plan"):
        build_duplicate_weight_plan(
            replace(view_plan, sources=tuple(sources)),
            TRACKED_POLICY,
            "fault_detection",
        )


def test_unknown_contract_duplicate_member_is_rejected(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    groups = list(view_plan.split_contract.duplicate_groups)
    groups[0] = replace(groups[0], members=(*groups[0].members[:-1], "unknown.csv"))
    changed = replace(
        view_plan,
        split_contract=replace(
            view_plan.split_contract,
            duplicate_groups=tuple(groups),
        ),
    )
    with pytest.raises(SplitContractError, match="Duplicate group members differ"):
        build_duplicate_weight_plan(changed, TRACKED_POLICY, "fault_detection")


def test_overlapping_contract_membership_is_rejected(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    groups = list(view_plan.split_contract.duplicate_groups)
    groups[1] = replace(groups[1], members=(groups[0].members[0], *groups[1].members[1:]))
    changed = replace(
        view_plan,
        split_contract=replace(
            view_plan.split_contract,
            duplicate_groups=tuple(groups),
        ),
    )
    with pytest.raises(SplitContractError, match="Duplicate group members differ"):
        build_duplicate_weight_plan(changed, TRACKED_POLICY, "fault_detection")


def test_policy_group_size_mismatch_is_rejected(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        group = value["expected_groups"]["duplicate_group_001"]
        group["expected_size"] = 3
        group["member_weight"] = 1 / 3

    with pytest.raises(DuplicateWeightingError, match="size differs from policy"):
        build_duplicate_weight_plan(
            _view_plan(tmp_path),
            _write_policy(tmp_path, mutate),
            "fault_detection",
        )


def test_policy_source_of_truth_mismatch_is_rejected(tmp_path: Path) -> None:
    def mutate(value: dict[str, Any]) -> None:
        value["source_of_truth"]["split_contract"] = "configs/alternate_split.yaml"

    with pytest.raises(DuplicateWeightingError, match="source_of_truth.split_contract"):
        build_duplicate_weight_plan(
            _view_plan(tmp_path),
            _write_policy(tmp_path, mutate),
            "fault_detection",
        )


def test_dataset_view_source_of_truth_error_is_not_swallowed(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    changed = replace(
        view_plan,
        strategy=replace(view_plan.strategy, split_contract="configs/alternate_split.yaml"),
    )
    with pytest.raises(DatasetViewError, match="source paths differ from V1"):
        build_duplicate_weight_plan(changed, TRACKED_POLICY, "fault_detection")


def test_scenario_identity_mismatch_is_rejected(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    sources = list(view_plan.sources)
    sources[0] = replace(sources[0], output_filename="wrong.parquet")
    with pytest.raises(DuplicateWeightingError, match="Scenario identity mismatch"):
        build_duplicate_weight_plan(
            replace(view_plan, sources=tuple(sources)),
            TRACKED_POLICY,
            "fault_detection",
        )


def test_plan_must_have_exactly_21_scenarios(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    with pytest.raises(DuplicateWeightingError, match="21 scenarios"):
        build_duplicate_weight_plan(
            replace(view_plan, sources=view_plan.sources[:-1]),
            TRACKED_POLICY,
            "fault_detection",
        )


def test_purge_partition_is_rejected(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    purge = next(
        item
        for item in view_plan.split_contract.partitions
        if item.label == "purge_train_validation"
    )
    with pytest.raises(DuplicateWeightingError, match="Unsupported weighting partition"):
        build_duplicate_weight_plan(
            replace(view_plan, partition=purge, expected_total_rows=purge.total_rows),
            TRACKED_POLICY,
            "fault_detection",
        )


def test_filename_substring_does_not_assign_duplicate_weight(tmp_path: Path) -> None:
    view_plan = _view_plan(tmp_path)
    sources = list(view_plan.sources)
    sources[0] = replace(
        sources[0],
        scenario_id="oa_bias_decoy",
        source_filename="oa_bias_decoy.csv",
        output_filename="oa_bias_decoy.parquet",
    )
    weight_plan = build_duplicate_weight_plan(
        replace(view_plan, sources=tuple(sources)),
        TRACKED_POLICY,
        "fault_detection",
    )
    assert _scenario(weight_plan, "oa_bias_decoy").duplicate_group is None
    assert _scenario(weight_plan, "oa_bias_decoy").duplicate_weight == 1.0


@pytest.fixture
def train_plan(tmp_path: Path) -> DuplicateWeightPlan:
    return _weight_plan(tmp_path)


def test_duplicate_batch_receives_float64_side_vector(
    train_plan: DuplicateWeightPlan,
) -> None:
    batch = _batch(train_plan, "oa_bias_-2", rows=5)
    schema = batch.record_batch.schema
    weighted = weight_dataset_view_batch(batch, train_plan)
    assert weighted.original is batch
    assert weighted.original.record_batch is batch.record_batch
    assert weighted.duplicate_weight == 0.25
    assert isinstance(weighted.sample_weight, np.ndarray)
    assert weighted.sample_weight.dtype == np.float64
    assert weighted.sample_weight.shape == (5,)
    np.testing.assert_array_equal(weighted.sample_weight, np.full(5, 0.25))
    assert weighted.sample_weight.flags.writeable is False
    assert batch.record_batch.schema == schema
    assert batch.record_batch.schema.names == ["feature_a"]


def test_nonduplicate_batch_receives_unit_weights(
    train_plan: DuplicateWeightPlan,
) -> None:
    batch = _batch(train_plan, "fault_free", rows=4)
    weighted = weight_dataset_view_batch(batch, train_plan)
    assert weighted.duplicate_weight == 1.0
    np.testing.assert_array_equal(weighted.sample_weight, np.ones(4))


@pytest.mark.parametrize("rows", [1, 2, 50])
def test_batch_size_does_not_change_scalar_weight(
    train_plan: DuplicateWeightPlan, rows: int
) -> None:
    weighted = weight_dataset_view_batch(
        _batch(train_plan, "coi_leakage_010", rows=rows),
        train_plan,
    )
    assert weighted.sample_weight.shape == (rows,)
    assert np.all(weighted.sample_weight == 0.25)


def test_same_input_produces_identical_weight_vector(
    train_plan: DuplicateWeightPlan,
) -> None:
    batch = _batch(train_plan, "coi_leakage_025", rows=7)
    first = weight_dataset_view_batch(batch, train_plan)
    second = weight_dataset_view_batch(batch, train_plan)
    np.testing.assert_array_equal(first.sample_weight, second.sample_weight)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("scenario_order", 99, "scenario order"),
        ("source_filename", "wrong.csv", "source filename"),
        ("output_filename", "wrong.parquet", "output filename"),
        ("partition", "validation", "partition"),
    ],
)
def test_batch_identity_mismatch_is_rejected(
    train_plan: DuplicateWeightPlan,
    field: str,
    value: object,
    match: str,
) -> None:
    batch = replace(_batch(train_plan, "fault_free"), **{field: value})
    with pytest.raises(DuplicateWeightingError, match=match):
        weight_dataset_view_batch(batch, train_plan)


def test_wrong_known_scenario_id_is_rejected(train_plan: DuplicateWeightPlan) -> None:
    batch = replace(_batch(train_plan, "fault_free"), scenario_id="coi_bias_-2")
    with pytest.raises(DuplicateWeightingError, match="scenario order"):
        weight_dataset_view_batch(batch, train_plan)


def test_unknown_scenario_is_rejected(train_plan: DuplicateWeightPlan) -> None:
    batch = replace(_batch(train_plan, "fault_free"), scenario_id="unknown")
    with pytest.raises(DuplicateWeightingError, match="Unknown or ambiguous"):
        weight_dataset_view_batch(batch, train_plan)


def test_non_record_batch_payload_is_rejected(train_plan: DuplicateWeightPlan) -> None:
    batch = replace(_batch(train_plan, "fault_free"), record_batch="invalid")
    with pytest.raises(DuplicateWeightingError, match="pyarrow.RecordBatch"):
        weight_dataset_view_batch(batch, train_plan)


def test_iterator_preserves_one_to_one_order_and_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_plan = _view_plan(tmp_path)
    weight_plan = build_duplicate_weight_plan(
        view_plan, TRACKED_POLICY, "fault_family_diagnosis"
    )
    batches = [
        _batch(weight_plan, "fault_free", rows=1),
        _batch(weight_plan, "coi_bias_-2", rows=2),
        _batch(weight_plan, "oa_bias_-2", rows=3),
    ]
    monkeypatch.setattr(
        weighting_module,
        "iter_dataset_view_batches",
        lambda _: iter(batches),
    )
    weighted = list(iter_duplicate_weighted_batches(view_plan, weight_plan))
    assert len(weighted) == len(batches)
    assert [item.original for item in weighted] == batches
    assert [item.original.scenario_id for item in weighted] == [
        item.scenario_id for item in batches
    ]
    assert [item.original.record_batch for item in weighted] == [
        item.record_batch for item in batches
    ]
    assert [item.duplicate_weight for item in weighted] == [1.0, 1.0, 0.25]


def test_iterator_early_stop_does_not_consume_remaining_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_plan = _view_plan(tmp_path)
    weight_plan = build_duplicate_weight_plan(
        view_plan, TRACKED_POLICY, "fault_detection"
    )
    batches = [
        _batch(weight_plan, "fault_free"),
        _batch(weight_plan, "coi_bias_-2"),
        _batch(weight_plan, "oa_bias_-2"),
    ]
    consumed = []

    def stream(_: DatasetViewPlan):
        for batch in batches:
            consumed.append(batch.scenario_id)
            yield batch

    monkeypatch.setattr(weighting_module, "iter_dataset_view_batches", stream)
    iterator = iter_duplicate_weighted_batches(view_plan, weight_plan)
    first = next(iterator)
    assert first.original is batches[0]
    assert consumed == ["fault_free"]
    iterator.close()
    assert consumed == ["fault_free"]


def test_iterator_rejects_plan_identity_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_plan = _view_plan(tmp_path)
    weight_plan = build_duplicate_weight_plan(
        view_plan, TRACKED_POLICY, "fault_detection"
    )
    monkeypatch.setattr(
        weighting_module,
        "iter_dataset_view_batches",
        lambda _: iter(()),
    )
    changed = replace(view_plan, sources=tuple(reversed(view_plan.sources)))
    with pytest.raises(DuplicateWeightingError, match="scenario identities"):
        next(iter_duplicate_weighted_batches(changed, weight_plan))


def test_runtime_weighting_creates_no_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    view_plan = _view_plan(tmp_path)
    weight_plan = build_duplicate_weight_plan(
        view_plan, TRACKED_POLICY, "fault_detection"
    )
    batch = _batch(weight_plan, "fault_free")
    before = set(tmp_path.rglob("*"))
    monkeypatch.setattr(
        weighting_module,
        "iter_dataset_view_batches",
        lambda _: iter((batch,)),
    )
    assert len(list(iter_duplicate_weighted_batches(view_plan, weight_plan))) == 1
    assert set(tmp_path.rglob("*")) == before


def test_runtime_module_has_no_random_or_artifact_write_api() -> None:
    source = Path(weighting_module.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import random",
        "np.random",
        "np.save",
        "np.savez",
        "write_table",
        "to_parquet",
        "class_weight=",
    ):
        assert forbidden not in source
