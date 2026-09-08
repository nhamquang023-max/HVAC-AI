# LBNL SD-AHU Inventory Reconciliation

## Official inventory

The official source document is `LBNL_FDD_Data_Sets_SDAHU.pdf`.

- Table 2 provides the data-point summary for 30 monitored features.
- Tables 3–4 provide fault scenarios, intensity metadata, and the file inventory.

The local PDF identity and hash are recorded in the
[inventory provenance](lbnl_sd_ahu_inventory_provenance.md).

## Verified field semantics

All 30 monitored features now have official descriptions, units, and Basic Point
flags recorded from inventory Table 2 in the
[data dictionary](lbnl_sd_ahu_data_dictionary.md) and machine-readable schema.

`Datetime` is verified as the timestamp column from the pinned archive schema. It
is not one of the 30 Table-2 monitored points, its inventory-verification flag is
false, and its timezone remains unspecified.

## Scenario naming discrepancy

Official inventory Table 4 lists:

- `sa_bias_-2_annual.csv`
- `sa_bias_-4_annual.csv`
- `sa_bias_2_annual.csv`
- `sa_bias_4_annual.csv`

The pinned downloaded archive instead contains:

- `coi_bias_-2_annual.csv`
- `coi_bias_-4_annual.csv`
- `coi_bias_2_annual.csv`
- `coi_bias_4_annual.csv`

The archive contains no `sa_bias` CSV members. No archive member is renamed, and
this project does not claim a publisher error or assert that `coi_bias` and
`sa_bias` are equivalent. The status is **unresolved source naming discrepancy**.

Table 3 associates `sa_bias` with supply-air temperature sensor bias at ±2°C and
±4°C. The registry retains this as candidate inventory metadata for the four
`coi_bias` files while leaving their canonical `severity_unit` null pending source
reconciliation.

## Severity metadata

Tables 3–4 reliably confirm the following metadata where inventory and archive
family tokens match:

| Archive fault family | Verified severity meaning | Registry unit |
| --- | --- | --- |
| `oa_bias` | degree Celsius bias | `degC` |
| `coi_leakage` | percent leakage | `percent` |
| `coi_stuck` | percent stuck position | `percent` |
| `damper_stuck` | percent stuck/open position | `percent` |
| `coi_bias` | pending reconciliation with inventory `sa_bias` | null |

The reconciliation status is partial because field semantics are complete while
one scenario-family naming discrepancy remains unresolved.
