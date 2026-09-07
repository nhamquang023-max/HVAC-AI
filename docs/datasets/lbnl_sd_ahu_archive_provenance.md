# LBNL SD-AHU Archive Provenance

## Source archive

Local path:
`data/raw/lbnl_sd_ahu/archive/LBNL_FDD_Data_Sets_SDAHU.zip`

The archive was obtained through the official
[LBNL SD-AHU dataset page](https://faultdetection.lbl.gov/dataset/simulated-sd-ahu/).
No direct-download URL is recorded because the download was completed through the
official browser workflow.

The archive is intentionally excluded from Git.

## Integrity

- Audit date: 2026-09-07
- Archive filename: `LBNL_FDD_Data_Sets_SDAHU.zip`
- Size: 607666899 bytes
- SHA-256: `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`
- ZIP validation: passed (`zipfile.is_zipfile` returned `True`)
- Magic bytes: `50 4B 03 04`
- CRC integrity: passed (`ZipFile.testzip()` returned `None`)
- Total compressed member bytes: 607662409
- Total uncompressed member bytes: 2894503088
- Uncompressed-to-compressed ratio: 4.763341
- Compression methods: DEFLATE for regular files; stored for the directory entry

The integrity test streamed member contents for CRC verification without extracting
files to disk.

## Safety audit

- Unsafe member count: 0
- Duplicate member count: 0
- Portable path collision count: 0
- Encrypted member count: 0
- Unsupported compression member count: 0

The archive contains 22 members: one directory entry and 21 regular files. Every
regular file is a CSV member.

## CSV inventory

- Website reported: 20
- Inventory PDF listed: 21
- Actual downloaded archive: 21

The actual downloaded archive contents are treated as the authoritative file-count
observation for this pinned archive hash. The historical difference between the
website and inventory PDF remains documented.

## CSV file list

1. `LBNL_FDD_Dataset_SDAHU/AHU_annual.csv`
2. `LBNL_FDD_Dataset_SDAHU/coi_bias_-2_annual.csv`
3. `LBNL_FDD_Dataset_SDAHU/coi_bias_-4_annual.csv`
4. `LBNL_FDD_Dataset_SDAHU/coi_bias_2_annual.csv`
5. `LBNL_FDD_Dataset_SDAHU/coi_bias_4_annual.csv`
6. `LBNL_FDD_Dataset_SDAHU/coi_leakage_010_annual.csv`
7. `LBNL_FDD_Dataset_SDAHU/coi_leakage_025_annual.csv`
8. `LBNL_FDD_Dataset_SDAHU/coi_leakage_040_annual.csv`
9. `LBNL_FDD_Dataset_SDAHU/coi_leakage_050_annual.csv`
10. `LBNL_FDD_Dataset_SDAHU/coi_stuck_010_annual.csv`
11. `LBNL_FDD_Dataset_SDAHU/coi_stuck_025_annual.csv`
12. `LBNL_FDD_Dataset_SDAHU/coi_stuck_050_annual.csv`
13. `LBNL_FDD_Dataset_SDAHU/coi_stuck_075_annual.csv`
14. `LBNL_FDD_Dataset_SDAHU/damper_stuck_010_annual.csv`
15. `LBNL_FDD_Dataset_SDAHU/damper_stuck_025_annual.csv`
16. `LBNL_FDD_Dataset_SDAHU/damper_stuck_075_annual.csv`
17. `LBNL_FDD_Dataset_SDAHU/damper_stuck_100_annual_short.csv`
18. `LBNL_FDD_Dataset_SDAHU/oa_bias_-2_annual.csv`
19. `LBNL_FDD_Dataset_SDAHU/oa_bias_-4_annual.csv`
20. `LBNL_FDD_Dataset_SDAHU/oa_bias_2_annual.csv`
21. `LBNL_FDD_Dataset_SDAHU/oa_bias_4_annual.csv`

## Schema summary

- Unique schema count: 1
- Column-count distribution: 31 columns in all 21 CSV files
- Encoding observed in the header and first two records: UTF-8 for all files
- Delimiter: comma for all files
- Sample row field counts: matched the 31-column header for all files
- Schema anomaly: none observed in the inspected headers

The common columns are:

```text
Datetime
CHWC_VLV
CHWC_VLV_DM
MA_TEMP
OA_CFM
OA_DMPR
OA_DMPR_DM
OA_TEMP
RA_CFM
RA_DMPR
RA_DMPR_DM
RA_TEMP
RF_CS
RF_SPD
RF_SPD_DM
RF_WAT
SA_CFM
SA_SP
SA_SPSPT
SA_TEMP
SA_TEMPSPT
SF_CS
SF_SPD
SF_SPD_DM
SF_WAT
SYS_CTL
ZONE_TEMP_1
ZONE_TEMP_2
ZONE_TEMP_3
ZONE_TEMP_4
ZONE_TEMP_5
```

The 31 columns comprise the `Datetime` field and the 30 monitored data points.

## Filename scenario audit

Mechanical filename-token rules identify one fault-free candidate,
`LBNL_FDD_Dataset_SDAHU/AHU_annual.csv`, and 20 faulted candidates. No files remain
unclassified. Observed fault tokens are `coi_bias`, `coi_leakage`, `coi_stuck`,
`damper_stuck`, and `oa_bias`; numeric suffixes are recorded as severity tokens in
the machine-readable manifest.

Four archive members use the filename token `coi_bias`, whereas the inventory PDF
lists `sa_bias` filenames. This difference is preserved as an observation; no
semantic expansion of `coi_bias` is inferred from the filename alone.

The complete central-directory metadata and per-CSV header audit are stored in
[the archive manifest](lbnl_sd_ahu_archive_manifest.json).
