# LBNL SD-AHU Sample Provenance

## Source

- Source archive SHA-256: `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`
- Source member: `LBNL_FDD_Dataset_SDAHU/AHU_annual.csv`

## Sampling rule

The local sample contains the header and first 1,440 data rows from the source
member. Raw byte order, row order, column order, and values were preserved. No
labels were added.

## Verified sample

- Local path: `data/interim/lbnl_sd_ahu/sample/AHU_annual_first_24h.csv`
- Rows: 1,440, excluding the header
- Columns: 31
- Datetime range: `2018-01-01T01:00:00` through `2018-01-02T00:59:00`
- Timeline: strictly increasing, one-minute intervals
- Missing values: 0
- Sample SHA-256: `473A4F8490C323215A7B864F6C8C9871AA45E1CA5FE20C8E352D35117AB5EC72`
- Git tracked: false

The sample is ignored by Git and is intended only for local development and smoke
testing. It is not a replacement for the pinned source archive and must not be
used as a complete benchmark dataset.
