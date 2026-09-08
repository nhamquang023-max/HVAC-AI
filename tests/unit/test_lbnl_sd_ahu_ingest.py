from __future__ import annotations

import hashlib
import zipfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from hvac_ai.data.lbnl_sd_ahu_ingest import (
    FILE_METADATA_KEYS,
    METADATA_COLUMNS,
    IngestionError,
    ingest_scenario_to_parquet,
)

FEATURES = [f"FEATURE_{index:02d}" for index in range(30)]
RAW_COLUMNS = ["Datetime", *FEATURES]
FILENAME = "AHU_annual.csv"
MEMBER = f"LBNL_FDD_Dataset_SDAHU/{FILENAME}"


@dataclass(frozen=True)
class SyntheticCase:
    archive: Path
    schema: Path
    scenarios: Path
    output: Path
    frame: pd.DataFrame


def _frame(rows: int = 8) -> pd.DataFrame:
    values: dict[str, object] = {
        "Datetime": pd.date_range("2020-01-01", periods=rows, freq="min")
    }
    for index, column in enumerate(FEATURES):
        values[column] = np.arange(rows, dtype="float64") + index / 10
    return pd.DataFrame(values)


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def _make_case(
    tmp_path: Path,
    *,
    frame: pd.DataFrame | None = None,
    members: dict[str, bytes] | None = None,
) -> SyntheticCase:
    frame = _frame() if frame is None else frame
    csv_content = _csv_bytes(frame)
    members = {MEMBER: csv_content} if members is None else members
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for member, content in members.items():
            bundle.writestr(member, content)

    schema = tmp_path / "schema.yaml"
    schema.write_text(
        yaml.safe_dump(
            {
                "timestamp_column": "Datetime",
                "feature_count": 30,
                "total_csv_columns": 31,
                "feature_columns": FEATURES,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    scenarios = tmp_path / "scenarios.yaml"
    scenarios.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest().upper()
                },
                "scenarios": [
                    {
                        "scenario_id": "fault_free",
                        "filename": FILENAME,
                        "fault_present": False,
                        "fault_family": "fault_free",
                        "severity_token": None,
                        "severity_value": None,
                        "severity_unit": None,
                        "is_short": False,
                        "content_sha256": hashlib.sha256(csv_content).hexdigest().upper(),
                        "byte_identical_group": None,
                        "severity_content_distinguishable": True,
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return SyntheticCase(archive, schema, scenarios, tmp_path / "output.parquet", frame)


def _ingest(case: SyntheticCase, **kwargs: object) -> dict[str, object]:
    options: dict[str, object] = {"chunk_size": 2}
    options.update(kwargs)
    return ingest_scenario_to_parquet(
        case.archive,
        case.schema,
        case.scenarios,
        FILENAME,
        case.output,
        **options,
    )


@pytest.fixture
def ingested(tmp_path: Path) -> tuple[SyntheticCase, dict[str, object]]:
    case = _make_case(tmp_path)
    return case, _ingest(case, row_limit=5)


def test_valid_zip_member_writes_parquet(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, result = ingested
    assert case.output.is_file()
    assert result["rows"] == 5


def test_output_has_42_columns(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    assert pq.ParquetFile(case.output).metadata.num_columns == 42


def test_datetime_is_timestamp_ns(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    assert pq.read_schema(case.output).field("Datetime").type == pa.timestamp("ns")


def test_all_features_are_float64(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    schema = pq.read_schema(case.output)
    assert all(schema.field(column).type == pa.float64() for column in FEATURES)


def test_registry_metadata_values_are_written(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    table = pq.read_table(case.output, columns=list(METADATA_COLUMNS))
    assert table.column("scenario_id").to_pylist() == ["fault_free"] * 5
    assert table.column("fault_present").to_pylist() == [False] * 5
    assert table.column("fault_family").to_pylist() == ["fault_free"] * 5


def test_fault_free_severity_values_are_arrow_nulls(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    table = pq.read_table(
        case.output,
        columns=["severity_token", "severity_value", "severity_unit"],
    )
    assert all(table.column(column).null_count == 5 for column in table.column_names)


def test_row_limit_preserves_first_rows(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    _ingest(case, row_limit=3)
    actual = pq.read_table(case.output).to_pandas()[RAW_COLUMNS]
    expected = case.frame.iloc[:3].copy()
    for column in FEATURES:
        expected[column] = expected[column].astype("float64")
    pd.testing.assert_frame_equal(actual, expected)


def test_chunk_boundaries_accept_one_minute_intervals(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    result = _ingest(case, row_limit=7)
    assert result["row_groups"] == 4


def test_bad_raw_header_order_fails(tmp_path: Path) -> None:
    frame = _frame()[["Datetime", FEATURES[1], FEATURES[0], *FEATURES[2:]]]
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="column order"):
        _ingest(case)


def test_missing_feature_fails(tmp_path: Path) -> None:
    case = _make_case(tmp_path, frame=_frame().drop(columns=FEATURES[-1]))
    with pytest.raises(IngestionError, match="missing"):
        _ingest(case)


def test_extra_unknown_feature_fails(tmp_path: Path) -> None:
    frame = _frame().assign(UNKNOWN=1)
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="extra"):
        _ingest(case)


def test_invalid_numeric_string_fails(tmp_path: Path) -> None:
    frame = _frame()
    frame[FEATURES[0]] = frame[FEATURES[0]].astype("object")
    frame.loc[1, FEATURES[0]] = "invalid"
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="invalid numeric"):
        _ingest(case)


def test_nan_fails(tmp_path: Path) -> None:
    frame = _frame()
    frame.loc[1, FEATURES[0]] = np.nan
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="NaN"):
        _ingest(case)


def test_infinite_value_fails(tmp_path: Path) -> None:
    frame = _frame()
    frame.loc[1, FEATURES[0]] = np.inf
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="infinite"):
        _ingest(case)


def test_invalid_datetime_fails(tmp_path: Path) -> None:
    frame = _frame()
    frame["Datetime"] = frame["Datetime"].astype("object")
    frame.loc[1, "Datetime"] = "not-a-date"
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="Invalid Datetime"):
        _ingest(case)


@pytest.mark.parametrize(
    "timestamps",
    [
        ["2020-01-01 00:00", "2020-01-01 00:00", "2020-01-01 00:01"],
        ["2020-01-01 00:00", "2019-12-31 23:59", "2020-01-01 00:01"],
    ],
)
def test_duplicate_or_non_increasing_datetime_fails(
    tmp_path: Path, timestamps: list[str]
) -> None:
    frame = _frame(rows=3)
    frame["Datetime"] = timestamps
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="strictly increasing"):
        _ingest(case)


def test_non_one_minute_gap_fails(tmp_path: Path) -> None:
    frame = _frame(rows=4)
    frame.loc[2:, "Datetime"] += pd.Timedelta(minutes=1)
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError, match="one-minute"):
        _ingest(case)


def test_zip_basename_zero_matches_fails(tmp_path: Path) -> None:
    content = _csv_bytes(_frame())
    case = _make_case(tmp_path, members={"folder/other.csv": content})
    with pytest.raises(IngestionError, match="got 0"):
        _ingest(case)


def test_zip_basename_multiple_matches_fails(tmp_path: Path) -> None:
    content = _csv_bytes(_frame())
    case = _make_case(
        tmp_path,
        members={f"a/{FILENAME}": content, f"b/{FILENAME}": content},
    )
    with pytest.raises(IngestionError, match="got 2"):
        _ingest(case)


def test_zip_path_traversal_member_fails(tmp_path: Path) -> None:
    content = _csv_bytes(_frame())
    case = _make_case(tmp_path, members={f"../{FILENAME}": content})
    with pytest.raises(IngestionError, match="Unsafe"):
        _ingest(case)


def test_existing_output_requires_overwrite(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.output.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        _ingest(case)
    assert case.output.read_bytes() == b"keep"


def test_atomic_temp_is_cleaned_after_failure(tmp_path: Path) -> None:
    frame = _frame()
    frame[FEATURES[0]] = frame[FEATURES[0]].astype("object")
    frame.loc[3, FEATURES[0]] = "bad"
    case = _make_case(tmp_path, frame=frame)
    with pytest.raises(IngestionError):
        _ingest(case)
    assert not case.output.exists()
    assert list(tmp_path.glob(f"{case.output.name}.*.tmp")) == []


def test_readback_file_metadata_is_present(
    ingested: tuple[SyntheticCase, dict[str, object]],
) -> None:
    case, _ = ingested
    metadata = {
        key.decode(): value.decode()
        for key, value in (pq.read_schema(case.output).metadata or {}).items()
    }
    assert set(FILE_METADATA_KEYS) <= set(metadata)
    assert metadata["dataset"] == "LBNL_SD_AHU"
    assert metadata["pipeline_version"] == "1"
