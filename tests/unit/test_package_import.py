"""Tests for the public HVAC-AI package structure."""

import importlib

import hvac_ai


def test_package_is_importable() -> None:
    """The main package can be imported."""
    assert hvac_ai.__name__ == "hvac_ai"


def test_package_version() -> None:
    """The package exposes its declared version."""
    assert hvac_ai.__version__ == "0.1.0"


def test_subpackages_are_importable() -> None:
    """Every initial feature subpackage can be imported."""
    subpackages = (
        "anomaly",
        "data",
        "evaluation",
        "fdd",
        "features",
        "utils",
        "visualization",
    )

    for subpackage in subpackages:
        imported = importlib.import_module(f"hvac_ai.{subpackage}")
        assert imported.__name__ == f"hvac_ai.{subpackage}"
