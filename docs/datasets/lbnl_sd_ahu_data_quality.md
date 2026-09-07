# LBNL SD-AHU Data Quality Audit

## Scope

This audit covers all 21 logical CSV members in the pinned LBNL SD-AHU archive.
Every file has 31 columns: `Datetime` plus 30 numerical data columns.

- Archive SHA-256: `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`
- Audit manifest: [lbnl_sd_ahu_data_quality.json](lbnl_sd_ahu_data_quality.json)
- Reference annual resolution: 365 days at one-minute intervals, or 525,600 rows

The 525,600-row value is a comparison reference. Observed row counts and timestamp
bounds below are reported independently.

## Method

The audit streamed each CSV directly from the ZIP with `ZipFile.open()` and
`pandas.read_csv(..., chunksize=50000)`. It did not extract CSV files or modify the
raw archive. Datetimes were parsed with coercion so every failure could be counted,
and adjacent valid timestamps were evaluated in original row order. A SHA-256
fingerprint was calculated from each complete parsed datetime sequence using a
stable int64 nanosecond representation that preserves `NaT` positions.

Numerical values were assessed separately as finite values, missing values, parse
failures, positive infinity, and negative infinity. A column was classified as
constant when it contained at least one finite value and all finite values had the
same minimum and maximum.

The prior member-hash manifest proves that 21 logical files contain 15 unique byte
contents. Full scanning was therefore performed once per unique SHA-256 content;
statistics were reused for the other six byte-identical logical members and each
reuse is marked in the JSON manifest.

## Dataset-wide summary

| Measure | Observed result |
| --- | ---: |
| Logical CSV files | 21 |
| Unique contents physically scanned | 15 |
| Total logical rows | 10,818,901 |
| Files with datetime parse failures | 0 |
| Files with duplicate timestamps | 0 |
| Files with non-one-minute intervals | 0 |
| Files with missing grid timestamps within observed bounds | 0 |
| Files with missing numerical values | 0 |
| Files with numerical parse failures | 0 |
| Files with positive or negative infinity | 0 |
| Files with all-missing columns | 0 |
| Files with constant columns | 21 |
| Unique datetime sequences | 2 |

## Timeline audit

Twenty files share the exact datetime sequence with fingerprint
`60FE93705236C6711CE89837C4372CB3D1FA9833E05BE4B3A7DF929047D090D8`.
Each has 525,540 rows from `2018-01-01T01:00:00` through
`2018-12-31T23:59:00`. The sequence is strictly increasing, contains only
one-minute intervals, and has no duplicate or missing grid timestamps within its
observed bounds. It contains 60 fewer rows than the 525,600-row reference because
the observed sequence begins at 01:00 rather than 00:00 on 1 January.

The short file has its own datetime fingerprint,
`ECE9D3B216DE013AC778412D842F43F7E13C212C4949D93CAB99CB412A0E103B`.
Its coverage is detailed below. No file contains an off-minute timestamp or a
Datetime parse failure.

## Numerical integrity

Across the 10,818,901 logical rows, the 30 numerical columns contain no missing
values, numerical parse failures, positive infinity, or negative infinity. No
column is entirely missing.

Every file contains constant columns. `OA_CFM`, `SA_SPSPT`, `SA_TEMPSPT`, and
`SF_SPD` are constant in all 21 files. `CHWC_VLV` is additionally constant in the
four `coi_stuck` files. `OA_DMPR` and `RA_DMPR` are additionally constant in the
three full-length `damper_stuck` files and the short file. Constant values and
per-column counts are preserved in the JSON manifest. These columns require
downstream handling when a method assumes nonzero within-file variance.

## Baseline file

`AHU_annual.csv` contains 525,540 rows from `2018-01-01T01:00:00` through
`2018-12-31T23:59:00`, covering 364.957639 elapsed days. Its observed timeline is
a complete, strictly increasing one-minute grid:

- Expected rows from its own first and last timestamps: 525,540
- Missing grid timestamps: 0
- Duplicate timestamps: 0
- Non-one-minute intervals: 0
- Missing numerical values: 0
- Positive or negative infinity: 0
- Constant columns: `OA_CFM`, `SA_SPSPT`, `SA_TEMPSPT`, `SF_SPD`

This observed sequence is the baseline timeline reference for the comparison
below. Its 60-row difference from the standard calendar-year reference reflects
the observed 01:00 start and is not counted as an internal grid gap.

## Short file

`damper_stuck_100_annual_short.csv` contains 308,101 rows from
`2018-04-01T01:00:00` through `2018-11-01T00:00:00`, covering 213.958333 elapsed
days. It is a complete, strictly increasing one-minute grid and is a contiguous
subset of the observed `AHU_annual.csv` timeline.

Compared with the baseline, the short file has 217,439 fewer rows. Its start is
129,600 minutes later and its end is 87,839 minutes earlier. Relative to the
baseline it does not cover these two timestamp ranges:

- `2018-01-01T01:00:00` through `2018-04-01T00:59:00`
- `2018-11-01T00:01:00` through `2018-12-31T23:59:00`

The audit records this observed coverage without inferring why the published file
is shorter.

## File summary

| Filename | Rows | Start | End | Duplicate timestamps | Non-1min intervals | Missing grid | Missing values | Inf | Constants |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `AHU_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_bias_-2_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_bias_-4_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_bias_2_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_bias_4_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_leakage_010_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_leakage_025_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_leakage_040_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_leakage_050_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `coi_stuck_010_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 5 |
| `coi_stuck_025_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 5 |
| `coi_stuck_050_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 5 |
| `coi_stuck_075_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 5 |
| `damper_stuck_010_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 6 |
| `damper_stuck_025_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 6 |
| `damper_stuck_075_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 6 |
| `damper_stuck_100_annual_short.csv` | 308,101 | 2018-04-01 01:00 | 2018-11-01 00:00 | 0 | 0 | 0 | 0 | 0 | 6 |
| `oa_bias_-2_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `oa_bias_-4_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `oa_bias_2_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |
| `oa_bias_4_annual.csv` | 525,540 | 2018-01-01 01:00 | 2018-12-31 23:59 | 0 | 0 | 0 | 0 | 0 | 4 |

## Duplicate-content handling

The two verified byte-identical groups are documented in the
[LBNL SD-AHU byte-identity audit](lbnl_sd_ahu_duplicate_audit.md). This audit
reused computation only after matching complete member SHA-256 values. It did not
modify, delete, deduplicate, or rewrite any raw file.

## Benchmark implications

The short file's distinct coverage and the 60-row difference between the observed
full-length timelines and the standard annual reference require downstream
handling when aligning cases. Constant columns require downstream handling for
methods that assume within-file variance. The byte-identical scenario files must
also remain accounted for when constructing evaluations. This report records
observed data properties and does not prescribe a final machine-learning strategy.
