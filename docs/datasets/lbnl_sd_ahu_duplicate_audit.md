# LBNL SD-AHU Byte-Identity Audit

## Scope

This audit covers all 21 regular CSV members in the pinned LBNL SD-AHU archive.

- Archive SHA-256: `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`
- Member hash manifest: [lbnl_sd_ahu_member_hashes.json](lbnl_sd_ahu_member_hashes.json)

## Method

Each member's SHA-256 digest was computed by streaming its uncompressed bytes
directly from the ZIP with `ZipFile.open()` in 1 MiB chunks. The byte count read
for every member was checked against its `ZipInfo.file_size`. No CSV was extracted
and the raw archive was not modified.

Only matching SHA-256 digests are classified as byte-identical. CRC32, file size,
and compressed size were not used to establish byte identity.

## Summary

| Measure | Result |
| --- | ---: |
| CSV members audited | 21 |
| Unique CSV contents | 15 |
| Duplicate content groups | 2 |
| Files belonging to duplicate groups | 8 |
| Other duplicate groups | 0 |

## Duplicate groups

Different filenames and severity tokens map to identical file content in each
group below.

### Group 1: OA bias

- SHA-256: `07302CF907B48C495609C2DB44C5E82CBB62290D1B4D9C05F8012FC00A5388E8`

| Filename | Scenario token | Uncompressed size (bytes) |
| --- | --- | ---: |
| `oa_bias_-2_annual.csv` | `oa_bias_-2` | 143,313,007 |
| `oa_bias_-4_annual.csv` | `oa_bias_-4` | 143,313,007 |
| `oa_bias_2_annual.csv` | `oa_bias_2` | 143,313,007 |
| `oa_bias_4_annual.csv` | `oa_bias_4` | 143,313,007 |

### Group 2: COI leakage

- SHA-256: `E520275F67027BBED69F84E6966BCEF206AB0F7B2090D265934793E8D7AF45FF`

| Filename | Scenario token | Uncompressed size (bytes) |
| --- | --- | ---: |
| `coi_leakage_010_annual.csv` | `coi_leakage_010` | 139,023,633 |
| `coi_leakage_025_annual.csv` | `coi_leakage_025` | 139,023,633 |
| `coi_leakage_040_annual.csv` | `coi_leakage_040` | 139,023,633 |
| `coi_leakage_050_annual.csv` | `coi_leakage_050` | 139,023,633 |

No other byte-identical groups were found among the 21 CSV members.

## Benchmark implications

Byte-identical files with different names or severity tokens must not be treated
as independent distinguishable severity classes without further methodological
justification. Dataset splits and metrics should account for content identity to
avoid assigning the same byte content to nominally different benchmark cases.
This audit records the observed identity and does not infer an HVAC mechanism or
assert an error by the dataset publisher.

## Raw-data policy

No source file was deleted, changed, deduplicated, or rewritten. No CSV was
extracted to disk. The ZIP and its CSV members remain ignored by Git.
