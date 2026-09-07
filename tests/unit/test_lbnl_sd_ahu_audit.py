from __future__ import annotations

import zipfile
from pathlib import Path

from hvac_ai.data.lbnl_sd_ahu_audit import audit_csv_member


def _audit_synthetic_csv(tmp_path: Path, csv_text: str) -> dict[str, object]:
    archive_path = tmp_path / "synthetic.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("synthetic.csv", csv_text)
    with zipfile.ZipFile(archive_path, "r") as archive:
        return audit_csv_member(archive, archive.getinfo("synthetic.csv"), chunk_size=2)


def test_normal_one_minute_timeline(tmp_path: Path) -> None:
    result = _audit_synthetic_csv(
        tmp_path,
        "Datetime,sensor\n"
        "2024-01-01 00:00:00,1\n"
        "2024-01-01 00:01:00,2\n"
        "2024-01-01 00:02:00,3\n",
    )

    assert result["is_strictly_increasing"] is True
    assert result["missing_grid_timestamp_count"] == 0
    assert result["duplicate_timestamp_count"] == 0
    assert result["non_one_minute_interval_count"] == 0


def test_duplicate_timestamp_and_two_minute_gap(tmp_path: Path) -> None:
    result = _audit_synthetic_csv(
        tmp_path,
        "Datetime,sensor\n"
        "2024-01-01 00:00:00,1\n"
        "2024-01-01 00:00:00,2\n"
        "2024-01-01 00:02:00,3\n",
    )

    assert result["duplicate_timestamp_count"] == 1
    assert result["zero_interval_count"] == 1
    assert result["non_one_minute_interval_count"] == 2
    assert result["missing_grid_timestamp_count"] == 1
    assert [item["delta_seconds"] for item in result["anomalous_intervals_first_20"]] == [
        0.0,
        120.0,
    ]


def test_numeric_missing_inf_and_constant_column(tmp_path: Path) -> None:
    result = _audit_synthetic_csv(
        tmp_path,
        "Datetime,value,constant,bad\n"
        "2024-01-01 00:00:00,1,7,1\n"
        "2024-01-01 00:01:00,,7,invalid\n"
        "2024-01-01 00:02:00,inf,7,3\n"
        "2024-01-01 00:03:00,-inf,,4\n"
        "2024-01-01 00:04:00,2,7,5\n",
    )

    value = result["numeric_columns"]["value"]
    assert value["missing_count"] == 1
    assert value["positive_inf_count"] == 1
    assert value["negative_inf_count"] == 1
    assert result["numeric_columns"]["bad"]["numeric_parse_failure_count"] == 1
    assert result["constant_non_missing_columns"] == {"constant": 7.0}
    assert result["total_missing_values"] == 2
    assert result["total_positive_inf"] == 1
    assert result["total_negative_inf"] == 1


def test_datetime_fingerprint_preserves_nat_position(tmp_path: Path) -> None:
    first = _audit_synthetic_csv(
        tmp_path,
        "Datetime,sensor\n2024-01-01 00:00:00,1\ninvalid,2\n2024-01-01 00:02:00,3\n",
    )
    second_path = tmp_path / "other"
    second_path.mkdir()
    second = _audit_synthetic_csv(
        second_path,
        "Datetime,sensor\n"
        "2024-01-01 00:00:00,1\n"
        "2024-01-01 00:01:00,2\n"
        "invalid,3\n",
    )

    assert first["datetime_parse_failure_count"] == 1
    assert second["datetime_parse_failure_count"] == 1
    assert first["datetime_sequence_sha256"] != second["datetime_sequence_sha256"]
