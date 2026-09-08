from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "configs/lbnl_sd_ahu_schema.yaml"


def _schema() -> dict[str, object]:
    return yaml.safe_load(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_all_features_have_verified_table_2_metadata() -> None:
    schema = _schema()
    features = schema["feature_columns"]
    metadata = schema["feature_metadata"]
    assert schema["feature_count"] == len(features) == len(metadata) == 30
    assert schema["total_csv_columns"] == 31
    assert list(metadata) == features
    for column in features:
        field = metadata[column]
        assert field["description"]
        assert field["unit"]
        assert isinstance(field["basic_point"], bool)
        assert field["officially_verified"] is True
        assert field["official_source"] == {
            "document": "LBNL_FDD_Data_Sets_SDAHU.pdf",
            "table": 2,
        }


def test_timestamp_metadata_is_archive_verified() -> None:
    schema = _schema()
    timestamp = schema["timestamp_metadata"]
    assert schema["timestamp_column"] == timestamp["column"] == "Datetime"
    assert timestamp["role"] == "timestamp"
    assert timestamp["unit"] is None
    assert timestamp["timezone"] == "unspecified"
    assert timestamp["officially_verified"] is False
    assert timestamp["officially_verified_from_inventory"] is False
    assert timestamp["verified_from_archive_schema"] is True
    assert "Datetime" not in schema["feature_columns"]


def test_temperature_units() -> None:
    metadata = _schema()["feature_metadata"]
    columns = (
        "SA_TEMP",
        "SA_TEMPSPT",
        "OA_TEMP",
        "MA_TEMP",
        "RA_TEMP",
        "ZONE_TEMP_1",
        "ZONE_TEMP_2",
        "ZONE_TEMP_3",
        "ZONE_TEMP_4",
        "ZONE_TEMP_5",
    )
    assert {metadata[column]["unit"] for column in columns} == {"degF"}


def test_airflow_power_and_pressure_units() -> None:
    metadata = _schema()["feature_metadata"]
    assert {metadata[column]["unit"] for column in ("OA_CFM", "RA_CFM", "SA_CFM")} == {
        "CFM"
    }
    assert {metadata[column]["unit"] for column in ("SF_WAT", "RF_WAT")} == {"W"}
    assert {metadata[column]["unit"] for column in ("SA_SP", "SA_SPSPT")} == {
        "inH2O"
    }


def test_control_status_and_position_units_are_dimensionless() -> None:
    metadata = _schema()["feature_metadata"]
    columns = (
        "SF_SPD_DM",
        "RF_SPD_DM",
        "SF_CS",
        "SF_SPD",
        "RF_CS",
        "RF_SPD",
        "OA_DMPR_DM",
        "OA_DMPR",
        "RA_DMPR_DM",
        "RA_DMPR",
        "CHWC_VLV_DM",
        "CHWC_VLV",
        "SYS_CTL",
    )
    assert {metadata[column]["unit"] for column in columns} == {"dimensionless"}
