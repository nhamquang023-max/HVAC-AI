# LBNL SD-AHU Data Dictionary

## Source and provenance

The dataset identity, official sources, archive hash, and source discrepancies are
documented in the [LBNL SD-AHU dataset record](lbnl_sd_ahu.md). The column order
below is verified from the common header of all 21 CSV members in the pinned
archive. The corresponding machine-readable schema is
[`configs/lbnl_sd_ahu_schema.yaml`](../../configs/lbnl_sd_ahu_schema.yaml).

The local environment does not contain a reliable PDF text-extraction tool for the
official inventory PDF. No dependency was installed for this task. Consequently,
this document does not expand abbreviations or assign physical units from memory.
Detailed descriptions and units remain pending official inventory verification.

## Raw schema

`Datetime` is the sole raw timestamp column. The other 30 columns are raw monitored
points. Scenario IDs, fault labels, and severity metadata are not raw CSV features;
the ingestion pipeline will add them from the scenario registry.

| Column | Role | Description | Unit | Officially verified | Notes |
| --- | --- | --- | --- | --- | --- |
| `Datetime` | timestamp | pending official inventory verification | unknown | false | Column identity and timestamp role are verified from the raw schema. |
| `CHWC_VLV` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `CHWC_VLV_DM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `MA_TEMP` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `OA_CFM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `OA_DMPR` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `OA_DMPR_DM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `OA_TEMP` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RA_CFM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RA_DMPR` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RA_DMPR_DM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RA_TEMP` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RF_CS` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RF_SPD` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RF_SPD_DM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `RF_WAT` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SA_CFM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SA_SP` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SA_SPSPT` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SA_TEMP` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SA_TEMPSPT` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SF_CS` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SF_SPD` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SF_SPD_DM` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SF_WAT` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `SYS_CTL` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `ZONE_TEMP_1` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `ZONE_TEMP_2` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `ZONE_TEMP_3` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `ZONE_TEMP_4` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |
| `ZONE_TEMP_5` | raw monitored point | pending official inventory verification | unknown | false | Abbreviation is preserved verbatim. |

## Verification status

- Raw column identifiers verified from the pinned archive: 31 of 31
- Official semantic descriptions verified from the inventory PDF: 0 of 31
- Physical units verified from the inventory PDF: 0 of 31
- Fields awaiting reliable official description and unit verification: 31

These pending fields must remain unknown until a reliable extraction or manual
source review can cite the official inventory text.
