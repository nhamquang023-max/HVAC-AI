from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from hvac_ai.data.lbnl_sd_ahu import (
    ScenarioRegistryError,
    get_scenario,
    get_severity_metadata,
    load_scenario_registry,
    parse_scenario_filename,
    summarize_registry,
    validate_scenario_registry,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = PROJECT_ROOT / "configs/lbnl_sd_ahu_scenarios.yaml"
ARCHIVE_MANIFEST_PATH = (
    PROJECT_ROOT / "docs/datasets/lbnl_sd_ahu_archive_manifest.json"
)
SCHEMA_PATH = PROJECT_ROOT / "configs/lbnl_sd_ahu_schema.yaml"


@pytest.fixture
def registry() -> dict[str, object]:
    return load_scenario_registry(REGISTRY_PATH)


def test_load_and_validate_registry(registry: dict[str, object]) -> None:
    validate_scenario_registry(registry, ARCHIVE_MANIFEST_PATH)
    assert summarize_registry(registry) == {
        "scenario_count": 21,
        "fault_family_counts": {
            "fault_free": 1,
            "coi_bias": 4,
            "coi_leakage": 4,
            "coi_stuck": 4,
            "damper_stuck": 4,
            "oa_bias": 4,
        },
        "fault_free_count": 1,
        "duplicate_group_count": 2,
        "timeline_group_count": 2,
    }


def test_scenario_ids_and_filenames_are_unique(registry: dict[str, object]) -> None:
    scenarios = registry["scenarios"]
    assert len({item["scenario_id"] for item in scenarios}) == 21
    assert len({item["filename"] for item in scenarios}) == 21
    assert sum(not item["fault_present"] for item in scenarios) == 1


@pytest.mark.parametrize(
    ("filename", "token", "value"),
    [
        ("coi_bias_-2_annual.csv", "-2", -2),
        ("coi_bias_4_annual.csv", "4", 4),
        ("coi_stuck_010_annual.csv", "010", 10),
        ("damper_stuck_100_annual_short.csv", "100", 100),
    ],
)
def test_severity_token_parsing(filename: str, token: str, value: int) -> None:
    parsed = parse_scenario_filename(filename)
    assert parsed["severity_token"] == token
    assert parsed["severity_value"] == value


def test_short_scenario_metadata(registry: dict[str, object]) -> None:
    short = get_scenario(registry, "damper_stuck_100_annual_short.csv")
    assert short["is_short"] is True
    assert short["row_count"] == 308101
    assert short["start_datetime"] == "2018-04-01T01:00:00"
    assert short["end_datetime"] == "2018-11-01T00:00:00"


def test_duplicate_group_metadata(registry: dict[str, object]) -> None:
    oa = get_scenario(registry, "oa_bias_-2_annual.csv")
    leakage = get_scenario(registry, "coi_leakage_010_annual.csv")
    assert oa["byte_identical_group"] == "duplicate_group_001"
    assert leakage["byte_identical_group"] == "duplicate_group_002"
    assert oa["byte_identical_group_size"] == 4
    assert leakage["byte_identical_group_size"] == 4
    assert get_severity_metadata(registry, oa["filename"])[
        "severity_content_distinguishable"
    ] is False
    assert get_severity_metadata(registry, leakage["filename"])[
        "severity_content_distinguishable"
    ] is False


@pytest.mark.parametrize("duplicated_field", ["filename", "scenario_id"])
def test_duplicate_filename_or_id_fails(
    registry: dict[str, object], duplicated_field: str
) -> None:
    invalid = copy.deepcopy(registry)
    invalid["scenarios"][1][duplicated_field] = invalid["scenarios"][0][duplicated_field]
    with pytest.raises(ScenarioRegistryError):
        validate_scenario_registry(invalid, ARCHIVE_MANIFEST_PATH)


def test_registry_filenames_match_archive_manifest(registry: dict[str, object]) -> None:
    archive = json.loads(ARCHIVE_MANIFEST_PATH.read_text(encoding="utf-8"))
    expected = {Path(item["name"]).name for item in archive["csv_members"]}
    actual = {item["filename"] for item in registry["scenarios"]}
    assert actual == expected


def test_raw_schema_contract() -> None:
    schema = yaml.safe_load(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert schema["timestamp_column"] == "Datetime"
    assert schema["feature_count"] == len(schema["feature_columns"]) == 30
    assert schema["total_csv_columns"] == 31
    assert "scenario_id" not in schema["feature_columns"]
