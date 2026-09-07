"""Streaming data-quality audit for the LBNL SD-AHU archive."""

from __future__ import annotations

import copy
import hashlib
import json
import zipfile
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DATETIME_COLUMN = "Datetime"
EXPECTED_COLUMN_COUNT = 31
REFERENCE_ANNUAL_ROWS = 525_600
NANOSECONDS_PER_MINUTE = 60_000_000_000
NAT_SENTINEL = np.iinfo(np.int64).min


def sha256_file(path: Path, chunk_size_bytes: int = 1024 * 1024) -> str:
    """Return the uppercase SHA-256 digest of a file read in fixed-size chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(chunk_size_bytes):
            digest.update(block)
    return digest.hexdigest().upper()


def _timestamp_text(value_ns: int | None) -> str | None:
    if value_ns is None:
        return None
    return pd.Timestamp(value_ns).isoformat()


@dataclass
class TimestampAccumulator:
    """Incrementally collect timestamp quality statistics."""

    parse_success_count: int = 0
    parse_failure_count: int = 0
    first_ns: int | None = None
    last_ns: int | None = None
    previous_valid_ns: int | None = None
    unique_ns: set[int] = field(default_factory=set)
    off_minute_count: int = 0
    interval_count: int = 0
    one_minute_count: int = 0
    zero_interval_count: int = 0
    negative_interval_count: int = 0
    minimum_interval_ns: int | None = None
    maximum_interval_ns: int | None = None
    anomalous_intervals: list[dict[str, Any]] = field(default_factory=list)
    sequence_digest: Any = field(default_factory=hashlib.sha256)

    def update(self, raw_values: pd.Series) -> None:
        """Add one CSV chunk while preserving original row order and NaT positions."""
        cleaned = raw_values.astype("string").str.strip().mask(lambda values: values == "")
        parsed = pd.to_datetime(cleaned, errors="coerce")
        sequence_ns = np.asarray(parsed.array.asi8, dtype="<i8")
        self.sequence_digest.update(sequence_ns.tobytes())

        valid_mask = sequence_ns != NAT_SENTINEL
        valid_ns = sequence_ns[valid_mask]
        valid_count = int(valid_mask.sum())
        self.parse_success_count += valid_count
        self.parse_failure_count += len(sequence_ns) - valid_count

        if valid_count == 0:
            return

        valid_series = parsed[valid_mask]
        self.off_minute_count += int(
            ((valid_series.dt.second != 0) | (valid_series.dt.microsecond != 0)).sum()
        )

        integer_values = [int(value) for value in valid_ns]
        if self.first_ns is None:
            self.first_ns = integer_values[0]
        self.last_ns = integer_values[-1]
        self.unique_ns.update(integer_values)

        for current_ns in integer_values:
            if self.previous_valid_ns is not None:
                self._record_interval(self.previous_valid_ns, current_ns)
            self.previous_valid_ns = current_ns

    def _record_interval(self, from_ns: int, to_ns: int) -> None:
        delta_ns = to_ns - from_ns
        self.interval_count += 1
        if delta_ns == NANOSECONDS_PER_MINUTE:
            self.one_minute_count += 1
        else:
            if len(self.anomalous_intervals) < 20:
                self.anomalous_intervals.append(
                    {
                        "from": _timestamp_text(from_ns),
                        "to": _timestamp_text(to_ns),
                        "delta_seconds": delta_ns / 1_000_000_000,
                    }
                )
        if delta_ns == 0:
            self.zero_interval_count += 1
        if delta_ns < 0:
            self.negative_interval_count += 1
        if self.minimum_interval_ns is None or delta_ns < self.minimum_interval_ns:
            self.minimum_interval_ns = delta_ns
        if self.maximum_interval_ns is None or delta_ns > self.maximum_interval_ns:
            self.maximum_interval_ns = delta_ns

    def finalize(self) -> dict[str, Any]:
        """Return JSON-safe timestamp statistics."""
        unique_count = len(self.unique_ns)
        duplicate_count = self.parse_success_count - unique_count
        non_one_minute_count = self.interval_count - self.one_minute_count
        is_non_decreasing = self.negative_interval_count == 0
        is_strict = is_non_decreasing and self.zero_interval_count == 0

        expected_grid_rows: int | None = None
        missing_grid_count: int | None = None
        grid_reason: str | None = None
        if self.parse_success_count == 0:
            grid_reason = "no valid Datetime values"
        elif self.parse_failure_count:
            grid_reason = "Datetime parse failures prevent safe grid assessment"
        elif self.off_minute_count:
            grid_reason = "off-minute timestamps prevent safe grid assessment"
        elif not is_non_decreasing:
            grid_reason = "negative intervals prevent safe grid assessment"
        elif self.first_ns is not None and self.last_ns is not None:
            span_ns = self.last_ns - self.first_ns
            if span_ns % NANOSECONDS_PER_MINUTE:
                grid_reason = "first-to-last span is not an integer number of minutes"
            else:
                expected_grid_rows = span_ns // NANOSECONDS_PER_MINUTE + 1
                missing_grid_count = expected_grid_rows - unique_count

        span_days = None
        if self.first_ns is not None and self.last_ns is not None:
            span_days = (self.last_ns - self.first_ns) / 86_400_000_000_000

        return {
            "datetime_parse_success_count": self.parse_success_count,
            "datetime_parse_failure_count": self.parse_failure_count,
            "valid_datetime_count": self.parse_success_count,
            "unique_datetime_count": unique_count,
            "duplicate_timestamp_count": duplicate_count,
            "first_datetime": _timestamp_text(self.first_ns),
            "last_datetime": _timestamp_text(self.last_ns),
            "is_monotonic_non_decreasing": is_non_decreasing,
            "is_strictly_increasing": is_strict,
            "off_minute_timestamp_count": self.off_minute_count,
            "interval_count": self.interval_count,
            "one_minute_interval_count": self.one_minute_count,
            "non_one_minute_interval_count": non_one_minute_count,
            "zero_interval_count": self.zero_interval_count,
            "negative_interval_count": self.negative_interval_count,
            "minimum_interval_seconds": (
                None
                if self.minimum_interval_ns is None
                else self.minimum_interval_ns / 1_000_000_000
            ),
            "maximum_interval_seconds": (
                None
                if self.maximum_interval_ns is None
                else self.maximum_interval_ns / 1_000_000_000
            ),
            "one_minute_interval_fraction": (
                None if self.interval_count == 0 else self.one_minute_count / self.interval_count
            ),
            "anomalous_intervals_first_20": self.anomalous_intervals,
            "expected_grid_rows": expected_grid_rows,
            "missing_grid_timestamp_count": missing_grid_count,
            "grid_assessment_unavailable_reason": grid_reason,
            "coverage_span_days": span_days,
            "datetime_sequence_sha256": self.sequence_digest.hexdigest().upper(),
        }


@dataclass
class NumericColumnAccumulator:
    """Incrementally collect one numeric column's integrity statistics."""

    valid_numeric_count: int = 0
    missing_count: int = 0
    numeric_parse_failure_count: int = 0
    positive_inf_count: int = 0
    negative_inf_count: int = 0
    minimum: float | None = None
    maximum: float | None = None

    def update(self, raw_values: pd.Series) -> None:
        cleaned = raw_values.astype("string").str.strip().mask(lambda values: values == "")
        missing = cleaned.isna().to_numpy()
        numeric = pd.to_numeric(cleaned, errors="coerce").to_numpy(
            dtype=np.float64, na_value=np.nan
        )
        is_nan = np.isnan(numeric)
        is_positive_inf = np.isposinf(numeric)
        is_negative_inf = np.isneginf(numeric)
        is_finite = np.isfinite(numeric)

        self.missing_count += int(missing.sum())
        self.numeric_parse_failure_count += int((~missing & is_nan).sum())
        self.positive_inf_count += int(is_positive_inf.sum())
        self.negative_inf_count += int(is_negative_inf.sum())
        self.valid_numeric_count += int(is_finite.sum())

        if is_finite.any():
            chunk_min = float(numeric[is_finite].min())
            chunk_max = float(numeric[is_finite].max())
            self.minimum = chunk_min if self.minimum is None else min(self.minimum, chunk_min)
            self.maximum = chunk_max if self.maximum is None else max(self.maximum, chunk_max)

    def finalize(self) -> dict[str, Any]:
        constant = (
            self.valid_numeric_count > 0
            and self.minimum is not None
            and self.maximum is not None
            and self.minimum == self.maximum
        )
        return {
            "valid_numeric_count": self.valid_numeric_count,
            "missing_count": self.missing_count,
            "numeric_parse_failure_count": self.numeric_parse_failure_count,
            "positive_inf_count": self.positive_inf_count,
            "negative_inf_count": self.negative_inf_count,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "is_constant_non_missing": constant,
            "constant_value": self.minimum if constant else None,
        }


def summarize_timestamps(
    values: pd.Series, accumulator: TimestampAccumulator
) -> TimestampAccumulator:
    """Update timestamp statistics for one chunk and return the accumulator."""
    accumulator.update(values)
    return accumulator


def summarize_numeric_columns(
    chunk: pd.DataFrame,
    accumulators: dict[str, NumericColumnAccumulator],
) -> dict[str, NumericColumnAccumulator]:
    """Update numeric statistics for every expected data column in one chunk."""
    for column, accumulator in accumulators.items():
        accumulator.update(chunk[column])
    return accumulators


def audit_csv_member(
    archive: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    chunk_size: int = 50_000,
) -> dict[str, Any]:
    """Audit one CSV member by streaming pandas chunks directly from the ZIP."""
    with archive.open(member, "r") as stream:
        header = pd.read_csv(
            stream,
            nrows=0,
            encoding="utf-8",
            on_bad_lines="error",
        )
    columns = list(header.columns)
    if DATETIME_COLUMN not in columns:
        raise ValueError(f"{member.filename} has no {DATETIME_COLUMN!r} column")

    numeric_columns = [column for column in columns if column != DATETIME_COLUMN]
    timestamp_accumulator = TimestampAccumulator()
    numeric_accumulators = {
        column: NumericColumnAccumulator() for column in numeric_columns
    }
    row_count = 0

    with archive.open(member, "r") as stream:
        chunks = pd.read_csv(
            stream,
            chunksize=chunk_size,
            dtype="string",
            encoding="utf-8",
            on_bad_lines="error",
        )
        for chunk in chunks:
            if list(chunk.columns) != columns:
                raise ValueError(f"Column layout changed while reading {member.filename}")
            row_count += len(chunk)
            summarize_timestamps(chunk[DATETIME_COLUMN], timestamp_accumulator)
            summarize_numeric_columns(chunk, numeric_accumulators)

    timestamp_summary = timestamp_accumulator.finalize()
    column_summaries = {
        column: accumulator.finalize()
        for column, accumulator in numeric_accumulators.items()
    }
    all_missing_columns = [
        column
        for column, summary in column_summaries.items()
        if row_count > 0 and summary["missing_count"] == row_count
    ]
    constant_columns = {
        column: summary["constant_value"]
        for column, summary in column_summaries.items()
        if summary["is_constant_non_missing"]
    }
    columns_with_missing = [
        column for column, summary in column_summaries.items() if summary["missing_count"]
    ]
    columns_with_parse_failures = [
        column
        for column, summary in column_summaries.items()
        if summary["numeric_parse_failure_count"]
    ]
    columns_with_inf = [
        column
        for column, summary in column_summaries.items()
        if summary["positive_inf_count"] or summary["negative_inf_count"]
    ]

    return {
        "header_column_count": len(columns),
        "columns": columns,
        "numeric_column_count": len(numeric_columns),
        "row_count": row_count,
        "row_count_minus_525600": row_count - REFERENCE_ANNUAL_ROWS,
        **timestamp_summary,
        "numeric_columns": column_summaries,
        "total_missing_values": sum(
            summary["missing_count"] for summary in column_summaries.values()
        ),
        "total_numeric_parse_failures": sum(
            summary["numeric_parse_failure_count"] for summary in column_summaries.values()
        ),
        "total_positive_inf": sum(
            summary["positive_inf_count"] for summary in column_summaries.values()
        ),
        "total_negative_inf": sum(
            summary["negative_inf_count"] for summary in column_summaries.values()
        ),
        "columns_with_missing_values": columns_with_missing,
        "columns_with_parse_failures": columns_with_parse_failures,
        "columns_with_inf": columns_with_inf,
        "all_missing_columns": all_missing_columns,
        "constant_non_missing_columns": constant_columns,
    }


def _load_member_hashes(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    if manifest.get("csv_count") != 21:
        raise ValueError("Member hash manifest does not contain 21 CSV files")
    return manifest


def _timeline_groups(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for file_audit in files:
        grouped[file_audit["datetime_sequence_sha256"]].append(file_audit["name"])
    return [
        {"datetime_sequence_sha256": digest, "count": len(names), "members": names}
        for digest, names in sorted(grouped.items())
    ]


def _file_names_where(
    files: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool]
) -> list[str]:
    return [file_audit["name"] for file_audit in files if predicate(file_audit)]


def _baseline_summary(file_audit: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "name",
        "row_count",
        "first_datetime",
        "last_datetime",
        "expected_grid_rows",
        "missing_grid_timestamp_count",
        "duplicate_timestamp_count",
        "non_one_minute_interval_count",
        "total_missing_values",
        "total_positive_inf",
        "total_negative_inf",
        "constant_non_missing_columns",
    ]
    return {key: copy.deepcopy(file_audit[key]) for key in keys}


def _short_file_comparison(
    baseline: dict[str, Any], short_file: dict[str, Any]
) -> dict[str, Any]:
    short_contiguous = bool(
        short_file["datetime_parse_failure_count"] == 0
        and short_file["off_minute_timestamp_count"] == 0
        and short_file["is_strictly_increasing"]
        and short_file["non_one_minute_interval_count"] == 0
        and short_file["expected_grid_rows"] == short_file["unique_datetime_count"]
    )
    baseline_contiguous = bool(
        baseline["datetime_parse_failure_count"] == 0
        and baseline["off_minute_timestamp_count"] == 0
        and baseline["is_strictly_increasing"]
        and baseline["non_one_minute_interval_count"] == 0
        and baseline["expected_grid_rows"] == baseline["unique_datetime_count"]
    )
    datetime_values = [
        baseline["first_datetime"],
        baseline["last_datetime"],
        short_file["first_datetime"],
        short_file["last_datetime"],
    ]
    start_offset_minutes: float | None = None
    end_offset_minutes: float | None = None
    subset: bool | None = None
    if all(value is not None for value in datetime_values):
        baseline_start, baseline_end, short_start, short_end = map(
            pd.Timestamp, datetime_values
        )
        start_offset_minutes = (short_start - baseline_start).total_seconds() / 60
        end_offset_minutes = (short_end - baseline_end).total_seconds() / 60
        if short_contiguous and baseline_contiguous:
            subset = bool(baseline_start <= short_start <= short_end <= baseline_end)

    return {
        "baseline_file": baseline["basename"],
        "short_file": short_file["basename"],
        "baseline_rows": baseline["row_count"],
        "short_rows": short_file["row_count"],
        "row_difference_short_minus_baseline": (
            short_file["row_count"] - baseline["row_count"]
        ),
        "baseline_start": baseline["first_datetime"],
        "baseline_end": baseline["last_datetime"],
        "short_start": short_file["first_datetime"],
        "short_end": short_file["last_datetime"],
        "start_offset_minutes": start_offset_minutes,
        "end_offset_minutes": end_offset_minutes,
        "short_coverage_span_days": short_file["coverage_span_days"],
        "short_is_contiguous_1min_grid": short_contiguous,
        "short_timeline_is_contiguous_subset_of_baseline": subset,
    }


def audit_archive(
    archive_path: Path,
    member_hashes_path: Path,
    chunk_size: int = 50_000,
    progress: Callable[[str], None] | None = print,
) -> dict[str, Any]:
    """Audit all logical CSVs, scanning each verified unique content once."""
    archive_path = Path(archive_path)
    member_hashes_path = Path(member_hashes_path)
    if not zipfile.is_zipfile(archive_path):
        raise ValueError(f"Not a ZIP archive: {archive_path}")

    archive_sha256 = sha256_file(archive_path)
    hash_manifest = _load_member_hashes(member_hashes_path)
    if hash_manifest.get("archive_sha256") != archive_sha256:
        raise ValueError("Archive SHA-256 differs from the member hash manifest")

    hash_members = hash_manifest["members"]
    hashes_by_name = {member["name"]: member for member in hash_members}
    unique_hashes = {member["sha256"] for member in hash_members}
    if len(unique_hashes) != 15:
        raise ValueError("Member hash manifest does not contain 15 unique contents")

    with zipfile.ZipFile(archive_path, "r") as archive:
        csv_members = {
            info.filename: info
            for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".csv")
        }
        if len(csv_members) != 21 or set(csv_members) != set(hashes_by_name):
            raise ValueError("ZIP members differ from the member hash manifest")

        representatives: dict[str, str] = {}
        for member in hash_members:
            representatives.setdefault(member["sha256"], member["name"])

        scanned_by_hash: dict[str, dict[str, Any]] = {}
        ordered_representatives = sorted(representatives.items(), key=lambda item: item[1])
        for index, (content_sha256, member_name) in enumerate(ordered_representatives, start=1):
            if progress:
                progress(
                    f"[{index}/{len(ordered_representatives)}] auditing "
                    f"{Path(member_name).name}"
                )
            info = csv_members[member_name]
            expected = hashes_by_name[member_name]
            if info.file_size != expected["size_bytes"]:
                raise ValueError(f"Size mismatch for {member_name}")
            scanned_by_hash[content_sha256] = audit_csv_member(archive, info, chunk_size)

    files = []
    for member in hash_members:
        content_sha256 = member["sha256"]
        representative = representatives[content_sha256]
        file_audit = copy.deepcopy(scanned_by_hash[content_sha256])
        file_audit.update(
            {
                "name": member["name"],
                "basename": member["basename"],
                "content_sha256": content_sha256,
                "audit_reused_from_identical_content": member["name"] != representative,
                "reused_from_member": (
                    representative if member["name"] != representative else None
                ),
            }
        )
        files.append(file_audit)

    timeline_groups = _timeline_groups(files)
    baseline = next(file_audit for file_audit in files if file_audit["basename"] == "AHU_annual.csv")
    short_file = next(
        file_audit
        for file_audit in files
        if file_audit["basename"] == "damper_stuck_100_annual_short.csv"
    )

    datetime_issue_files = _file_names_where(
        files,
        lambda item: bool(
            item["datetime_parse_failure_count"]
            or item["duplicate_timestamp_count"]
            or item["non_one_minute_interval_count"]
            or item["off_minute_timestamp_count"]
            or (item["missing_grid_timestamp_count"] or 0)
        ),
    )
    summary = {
        "total_logical_rows": sum(item["row_count"] for item in files),
        "files_with_datetime_issues": datetime_issue_files,
        "files_with_datetime_parse_failures": _file_names_where(
            files, lambda item: bool(item["datetime_parse_failure_count"])
        ),
        "files_with_duplicate_timestamps": _file_names_where(
            files, lambda item: bool(item["duplicate_timestamp_count"])
        ),
        "files_with_non_one_minute_intervals": _file_names_where(
            files, lambda item: bool(item["non_one_minute_interval_count"])
        ),
        "files_with_missing_grid_timestamps": _file_names_where(
            files, lambda item: bool(item["missing_grid_timestamp_count"])
        ),
        "files_with_missing_values": _file_names_where(
            files, lambda item: bool(item["total_missing_values"])
        ),
        "files_with_numeric_parse_failures": _file_names_where(
            files, lambda item: bool(item["total_numeric_parse_failures"])
        ),
        "files_with_inf": _file_names_where(
            files,
            lambda item: bool(item["total_positive_inf"] or item["total_negative_inf"]),
        ),
        "files_with_all_missing_columns": _file_names_where(
            files, lambda item: bool(item["all_missing_columns"])
        ),
        "files_with_constant_columns": _file_names_where(
            files, lambda item: bool(item["constant_non_missing_columns"])
        ),
        "total_missing_values": sum(item["total_missing_values"] for item in files),
        "total_numeric_parse_failures": sum(
            item["total_numeric_parse_failures"] for item in files
        ),
        "total_positive_inf": sum(item["total_positive_inf"] for item in files),
        "total_negative_inf": sum(item["total_negative_inf"] for item in files),
    }

    if progress:
        progress("Audit completed.")
    return {
        "archive_sha256": archive_sha256,
        "audit_timestamp": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "chunk_size": chunk_size,
        "logical_csv_count": len(files),
        "unique_content_scans": len(scanned_by_hash),
        "unique_timeline_count": len(timeline_groups),
        "reference_annual_rows": REFERENCE_ANNUAL_ROWS,
        "files": files,
        "summary": summary,
        "timeline_groups": timeline_groups,
        "baseline_summary": _baseline_summary(baseline),
        "short_file_comparison": _short_file_comparison(baseline, short_file),
    }


def write_audit_manifest(audit: dict[str, Any], output_path: Path) -> None:
    """Write an audit result as deterministic, human-readable JSON."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
