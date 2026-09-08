"""Canonical scenario registry helpers for the LBNL SD-AHU dataset."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

ALLOWED_FAULT_FAMILIES = frozenset(
    {
        "fault_free",
        "coi_bias",
        "coi_leakage",
        "coi_stuck",
        "damper_stuck",
        "oa_bias",
    }
)
EXPECTED_SCENARIO_COUNT = 21
EXPECTED_DUPLICATE_GROUP_COUNT = 2

_FAULT_FILENAME = re.compile(
    r"^(coi_bias|coi_leakage|coi_stuck|damper_stuck|oa_bias)_"
    r"(?P<severity>-?\d+)_annual(?P<short>_short)?\.csv$"
)
_REQUIRED_FIELDS = frozenset(
    {
        "scenario_id",
        "filename",
        "archive_member",
        "fault_family",
        "fault_present",
        "severity_token",
        "severity_value",
        "severity_unit",
        "is_short",
        "content_sha256",
        "byte_identical_group",
        "byte_identical_group_size",
        "severity_content_distinguishable",
        "timeline_group",
        "timeline_sha256",
        "row_count",
        "start_datetime",
        "end_datetime",
    }
)


class ScenarioRegistryError(ValueError):
    """Raised when the LBNL SD-AHU scenario contract is inconsistent."""


def parse_scenario_filename(filename: str) -> dict[str, Any]:
    """Mechanically parse canonical fault and severity tokens from a CSV basename."""
    basename = Path(filename).name
    if basename == "AHU_annual.csv":
        return {
            "fault_family": "fault_free",
            "fault_present": False,
            "severity_token": None,
            "severity_value": None,
            "is_short": False,
        }
    match = _FAULT_FILENAME.fullmatch(basename)
    if match is None:
        raise ScenarioRegistryError(f"Unrecognized LBNL SD-AHU filename: {basename}")
    severity_token = match.group("severity")
    return {
        "fault_family": match.group(1),
        "fault_present": True,
        "severity_token": severity_token,
        "severity_value": int(severity_token),
        "is_short": match.group("short") is not None,
    }


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream)
    if not isinstance(value, dict):
        raise ScenarioRegistryError(f"Expected a YAML mapping in {path}")
    return value


def _archive_members(path: Path) -> set[str]:
    with path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    members = manifest.get("csv_members")
    if not isinstance(members, list):
        raise ScenarioRegistryError("Archive manifest has no csv_members list")
    return {item["name"] if isinstance(item, dict) else item for item in members}


def validate_scenario_registry(
    registry: Mapping[str, Any], archive_manifest_path: Path
) -> None:
    """Validate registry structure, labels, duplicate groups, and archive coverage."""
    scenarios = registry.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != EXPECTED_SCENARIO_COUNT:
        raise ScenarioRegistryError(
            f"Expected {EXPECTED_SCENARIO_COUNT} scenarios, got "
            f"{len(scenarios) if isinstance(scenarios, list) else 'non-list'}"
        )
    if registry.get("dataset_id") != "lbnl_sd_ahu":
        raise ScenarioRegistryError("Unexpected dataset_id")

    scenario_ids = [scenario.get("scenario_id") for scenario in scenarios]
    filenames = [scenario.get("filename") for scenario in scenarios]
    archive_members = [scenario.get("archive_member") for scenario in scenarios]
    if len(set(scenario_ids)) != len(scenario_ids):
        raise ScenarioRegistryError("scenario_id values must be unique")
    if len(set(filenames)) != len(filenames):
        raise ScenarioRegistryError("filename values must be unique")
    if len(set(archive_members)) != len(archive_members):
        raise ScenarioRegistryError("archive_member values must be unique")

    expected_members = _archive_members(Path(archive_manifest_path))
    expected_filenames = {Path(member).name for member in expected_members}
    if set(filenames) != expected_filenames:
        missing = sorted(expected_filenames - set(filenames))
        extra = sorted(set(filenames) - expected_filenames)
        raise ScenarioRegistryError(
            f"Registry/archive filename mismatch; missing={missing}, extra={extra}"
        )
    if set(archive_members) != expected_members:
        raise ScenarioRegistryError("Registry archive_member paths differ from the manifest")

    duplicate_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    contents: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    fault_free_count = 0
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ScenarioRegistryError("Every scenario must be a mapping")
        missing_fields = _REQUIRED_FIELDS - set(scenario)
        if missing_fields:
            raise ScenarioRegistryError(
                f"{scenario.get('filename', '<unknown>')} lacks {sorted(missing_fields)}"
            )
        filename = scenario["filename"]
        if Path(scenario["archive_member"]).name != filename:
            raise ScenarioRegistryError(f"archive_member basename differs for {filename}")

        parsed = parse_scenario_filename(filename)
        for field, expected in parsed.items():
            if scenario[field] != expected:
                raise ScenarioRegistryError(
                    f"{filename} has {field}={scenario[field]!r}; expected {expected!r}"
                )
        if scenario["fault_family"] not in ALLOWED_FAULT_FAMILIES:
            raise ScenarioRegistryError(f"Invalid fault_family for {filename}")
        if scenario["fault_family"] == "fault_free":
            fault_free_count += 1
            if scenario["severity_unit"] is not None:
                raise ScenarioRegistryError("Fault-free severity_unit must be null")
        elif scenario["severity_unit"] not in {None, "unknown"}:
            raise ScenarioRegistryError(f"Unverified severity_unit for {filename}")

        content_sha256 = scenario["content_sha256"]
        timeline_sha256 = scenario["timeline_sha256"]
        if not isinstance(content_sha256, str) or len(content_sha256) != 64:
            raise ScenarioRegistryError(f"Invalid content_sha256 for {filename}")
        if not isinstance(timeline_sha256, str) or len(timeline_sha256) != 64:
            raise ScenarioRegistryError(f"Invalid timeline_sha256 for {filename}")
        if not isinstance(scenario["row_count"], int) or scenario["row_count"] <= 0:
            raise ScenarioRegistryError(f"Invalid row_count for {filename}")
        if not scenario["timeline_group"]:
            raise ScenarioRegistryError(f"Missing timeline_group for {filename}")

        group = scenario["byte_identical_group"]
        if group is None:
            if scenario["byte_identical_group_size"] != 1:
                raise ScenarioRegistryError(f"Singleton content size must be 1 for {filename}")
            if scenario["severity_content_distinguishable"] is not True:
                raise ScenarioRegistryError(
                    f"Singleton content must be distinguishable for {filename}"
                )
        else:
            duplicate_groups[group].append(scenario)
        contents[content_sha256].append(scenario)

    if fault_free_count != 1:
        raise ScenarioRegistryError("Registry must contain exactly one fault-free scenario")
    if len(duplicate_groups) != EXPECTED_DUPLICATE_GROUP_COUNT:
        raise ScenarioRegistryError(
            f"Expected {EXPECTED_DUPLICATE_GROUP_COUNT} duplicate groups"
        )

    for group_id, members in duplicate_groups.items():
        declared_sizes = {member["byte_identical_group_size"] for member in members}
        hashes = {member["content_sha256"] for member in members}
        if declared_sizes != {len(members)} or len(hashes) != 1:
            raise ScenarioRegistryError(f"Inconsistent duplicate group {group_id}")
        if any(member["severity_content_distinguishable"] is not False for member in members):
            raise ScenarioRegistryError(f"Duplicate group {group_id} must be constrained")

    for content_sha256, members in contents.items():
        if len(members) > 1 and any(
            member["byte_identical_group"] is None for member in members
        ):
            raise ScenarioRegistryError(
                f"Repeated content {content_sha256} lacks duplicate metadata"
            )


def load_scenario_registry(path: Path) -> dict[str, Any]:
    """Load and fully validate a scenario registry and its archive manifest."""
    registry_path = Path(path)
    registry = _load_yaml_mapping(registry_path)
    source = registry.get("source")
    if not isinstance(source, dict) or not source.get("archive_manifest"):
        raise ScenarioRegistryError("Registry source.archive_manifest is required")
    archive_manifest_path = (registry_path.parent / source["archive_manifest"]).resolve()
    validate_scenario_registry(registry, archive_manifest_path)
    return registry


def list_scenarios(
    registry: Mapping[str, Any], fault_family: str | None = None
) -> tuple[Mapping[str, Any], ...]:
    """Return all scenarios, optionally filtered by canonical fault family."""
    scenarios: Sequence[Mapping[str, Any]] = registry["scenarios"]
    if fault_family is None:
        return tuple(scenarios)
    if fault_family not in ALLOWED_FAULT_FAMILIES:
        raise ScenarioRegistryError(f"Unknown fault_family: {fault_family}")
    return tuple(
        scenario for scenario in scenarios if scenario["fault_family"] == fault_family
    )


def get_scenario(registry: Mapping[str, Any], filename: str) -> Mapping[str, Any]:
    """Return one scenario by basename or archive member path."""
    basename = Path(filename).name
    matches = [
        scenario for scenario in registry["scenarios"] if scenario["filename"] == basename
    ]
    if len(matches) != 1:
        raise ScenarioRegistryError(f"Expected one scenario for {basename}, got {len(matches)}")
    return matches[0]


def get_fault_family(registry: Mapping[str, Any], filename: str) -> str:
    """Return the canonical fault family for a source filename."""
    return str(get_scenario(registry, filename)["fault_family"])


def get_severity_metadata(registry: Mapping[str, Any], filename: str) -> dict[str, Any]:
    """Return source-token severity metadata without inferring a physical unit."""
    scenario = get_scenario(registry, filename)
    return {
        "severity_token": scenario["severity_token"],
        "severity_value": scenario["severity_value"],
        "severity_unit": scenario["severity_unit"],
        "severity_content_distinguishable": scenario[
            "severity_content_distinguishable"
        ],
    }


def get_duplicate_metadata(registry: Mapping[str, Any], filename: str) -> dict[str, Any]:
    """Return byte-identical content-group metadata for a source filename."""
    scenario = get_scenario(registry, filename)
    return {
        "content_sha256": scenario["content_sha256"],
        "byte_identical_group": scenario["byte_identical_group"],
        "byte_identical_group_size": scenario["byte_identical_group_size"],
    }


def get_timeline_metadata(registry: Mapping[str, Any], filename: str) -> dict[str, Any]:
    """Return observed timeline metadata for a source filename."""
    scenario = get_scenario(registry, filename)
    return {
        "timeline_group": scenario["timeline_group"],
        "timeline_sha256": scenario["timeline_sha256"],
        "row_count": scenario["row_count"],
        "start_datetime": scenario["start_datetime"],
        "end_datetime": scenario["end_datetime"],
        "is_short": scenario["is_short"],
    }


def summarize_registry(registry: Mapping[str, Any]) -> dict[str, Any]:
    """Return compact counts useful for validation and user interfaces."""
    scenarios = list_scenarios(registry)
    return {
        "scenario_count": len(scenarios),
        "fault_family_counts": dict(Counter(item["fault_family"] for item in scenarios)),
        "fault_free_count": sum(not item["fault_present"] for item in scenarios),
        "duplicate_group_count": len(
            {item["byte_identical_group"] for item in scenarios}
            - {None}
        ),
        "timeline_group_count": len({item["timeline_group"] for item in scenarios}),
    }
