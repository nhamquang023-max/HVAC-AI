# LBNL SD-AHU Data Dictionary

## Source and provenance

The dataset identity, official sources, archive hash, and source discrepancies are
documented in the [LBNL SD-AHU dataset record](lbnl_sd_ahu.md). The raw column
names and order are verified from the common header of all 21 CSV members in the
pinned archive. Field descriptions, units, ranges or states, and Basic Point flags
for the 30 monitored features are verified from Table 2 of the official
`LBNL_FDD_Data_Sets_SDAHU.pdf` inventory.

The corresponding machine-readable schema is
[`configs/lbnl_sd_ahu_schema.yaml`](../../configs/lbnl_sd_ahu_schema.yaml).

## Raw schema

`Datetime` is the sole timestamp column and is verified from the pinned archive
schema. It is not one of the 30 monitored points in inventory Table 2. Its timezone
is unspecified and no timezone is inferred. Scenario IDs, fault labels, and severity
metadata are added later from the scenario registry and are not raw CSV features.

| Column | Role | Official description | Unit | Range / states | Basic point | Verification source | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Datetime` | timestamp | Source timestamp column; no Table-2 monitored-point description | — | — | — | Pinned archive schema | Timezone unspecified; not a Table-2 monitored point. |
| `CHWC_VLV` | raw monitored point | AHU cooling-coil valve position | `dimensionless` | 0-1 open fraction | false | Official inventory Table 2 | Raw identifier preserved. |
| `CHWC_VLV_DM` | raw monitored point | AHU cooling-coil valve control signal | `dimensionless` | 0-1 open fraction | true | Official inventory Table 2 | Raw identifier preserved. |
| `MA_TEMP` | raw monitored point | AHU mixed-air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `OA_CFM` | raw monitored point | AHU outdoor-air volumetric airflow | `CFM` | — | false | Official inventory Table 2 | Raw identifier preserved. |
| `OA_DMPR` | raw monitored point | AHU outdoor-air damper position | `dimensionless` | 0-1 open fraction | false | Official inventory Table 2 | Raw identifier preserved. |
| `OA_DMPR_DM` | raw monitored point | AHU outdoor-air damper control signal | `dimensionless` | 0-1 open fraction | true | Official inventory Table 2 | Raw identifier preserved. |
| `OA_TEMP` | raw monitored point | AHU outdoor-air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `RA_CFM` | raw monitored point | AHU return-air volumetric airflow | `CFM` | — | false | Official inventory Table 2 | Raw identifier preserved. |
| `RA_DMPR` | raw monitored point | AHU return-air damper position | `dimensionless` | 0-1 open fraction | false | Official inventory Table 2 | Raw identifier preserved. |
| `RA_DMPR_DM` | raw monitored point | AHU return-air damper control signal | `dimensionless` | 0-1 open fraction | true | Official inventory Table 2 | Raw identifier preserved. |
| `RA_TEMP` | raw monitored point | AHU return-air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `RF_CS` | raw monitored point | AHU return-fan speed control signal | `dimensionless` | 0-1 | true | Official inventory Table 2 | Raw identifier preserved. |
| `RF_SPD` | raw monitored point | AHU return-fan speed position | `dimensionless` | 0-1 | false | Official inventory Table 2 | Raw identifier preserved. |
| `RF_SPD_DM` | raw monitored point | AHU return-fan on/off status | `dimensionless` | 0=off, 1=on | true | Official inventory Table 2 | Raw identifier preserved. |
| `RF_WAT` | raw monitored point | AHU return-fan power | `W` | — | false | Official inventory Table 2 | Raw identifier preserved. |
| `SA_CFM` | raw monitored point | AHU supply-air volumetric airflow | `CFM` | — | false | Official inventory Table 2 | Raw identifier preserved. |
| `SA_SP` | raw monitored point | AHU supply-air duct static pressure | `inH2O` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `SA_SPSPT` | raw monitored point | AHU supply-air duct static-pressure setpoint | `inH2O` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `SA_TEMP` | raw monitored point | AHU supply-air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `SA_TEMPSPT` | raw monitored point | AHU supply-air temperature setpoint | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `SF_CS` | raw monitored point | AHU supply-fan speed control signal | `dimensionless` | 0-1 | true | Official inventory Table 2 | Raw identifier preserved. |
| `SF_SPD` | raw monitored point | AHU supply-fan speed position | `dimensionless` | 0-1 | false | Official inventory Table 2 | Raw identifier preserved. |
| `SF_SPD_DM` | raw monitored point | AHU supply-fan on/off status | `dimensionless` | 0=off, 1=on | true | Official inventory Table 2 | Raw identifier preserved. |
| `SF_WAT` | raw monitored point | AHU supply-fan power | `W` | — | false | Official inventory Table 2 | Raw identifier preserved. |
| `SYS_CTL` | raw monitored point | AHU occupied/unoccupied mode indicator | `dimensionless` | 0=unoccupied, 1=occupied | false | Official inventory Table 2 | Raw identifier preserved. |
| `ZONE_TEMP_1` | raw monitored point | Zone 1 air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `ZONE_TEMP_2` | raw monitored point | Zone 2 air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `ZONE_TEMP_3` | raw monitored point | Zone 3 air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `ZONE_TEMP_4` | raw monitored point | Zone 4 air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |
| `ZONE_TEMP_5` | raw monitored point | Zone 5 air temperature | `degF` | — | true | Official inventory Table 2 | Raw identifier preserved. |

## Verification status

- Raw column identifiers verified from the pinned archive: 31 of 31
- Official monitored-feature descriptions verified from inventory Table 2: 30 of 30
- Official monitored-feature units verified from inventory Table 2: 30 of 30
- Basic Point flags represented for monitored features: 30 of 30
- Timestamp metadata: archive-schema verified; inventory verification false; timezone unspecified

Descriptions are concise technical summaries of Table 2 metadata rather than long
extracts from the source document.
