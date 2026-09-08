# LBNL SD-AHU Label Contract

## Contract source

The canonical machine-readable source is
[`configs/lbnl_sd_ahu_scenarios.yaml`](../../configs/lbnl_sd_ahu_scenarios.yaml).
It covers all 21 filenames in the verified archive. Labels are scenario metadata
derived mechanically from source filenames; they are not columns in the raw CSVs.

## Label levels

### Level 0: fault presence

`fault_present` is binary:

- `false` for `AHU_annual.csv`
- `true` for every other source scenario

### Level 1: fault family

The canonical `fault_family` values are:

- `fault_free`
- `coi_bias`
- `coi_leakage`
- `coi_stuck`
- `damper_stuck`
- `oa_bias`

`fault_free` applies only to `AHU_annual.csv`.

### Level 2: severity token

`severity_token` preserves the exact token embedded in the source filename. For
example, `-2`, `010`, and `100` remain distinct source strings. `severity_value`
is their direct integer conversion for ordering and filtering. Official inventory
Tables 3–4 verify these units where the inventory and archive family tokens match:

- `oa_bias`: `degC`
- `coi_leakage`: `percent`
- `coi_stuck`: `percent`
- `damper_stuck`: `percent`

The fault-free scenario has null token, value, and unit. The four `coi_bias`
scenarios also retain a null `severity_unit` because their archive token does not
match the inventory's `sa_bias` token. Their candidate inventory unit is recorded
separately as `degC` with `semantic_mapping_status: unresolved_filename_mismatch`.

Severity labels are source-scenario metadata. They do not by themselves establish
a physical interpretation beyond the explicitly reconciled official metadata.

## Source naming discrepancy

The actual archive contains `coi_bias_-2_annual.csv`, `coi_bias_-4_annual.csv`,
`coi_bias_2_annual.csv`, and `coi_bias_4_annual.csv`. Official inventory Table 4
instead lists the corresponding filename positions with the `sa_bias` token, and
Table 3 describes `sa_bias` as supply-air temperature sensor bias at ±2°C and
±4°C. The archive contains no `sa_bias` CSV member.

The registry preserves `coi_bias` because the archive filename is the canonical
machine-readable source. Current evidence does not establish that `coi_bias` and
`sa_bias` have fully equivalent business or physical meanings, so this project
makes no equivalence assertion. The status is `unresolved_filename_mismatch`; see
the [inventory reconciliation](lbnl_sd_ahu_inventory_reconciliation.md).

## Severity benchmark limitations

For byte-identical source files carrying different severity tokens, the raw
feature content is not sufficient to distinguish those severity tokens.

- `duplicate_group_001`: all four `oa_bias` severity files are byte-identical.
- `duplicate_group_002`: all four `coi_leakage` severity files are byte-identical.

For these two families, severity-level distinguishability is constrained and
requires benchmark policy. This contract does not decide whether to drop, merge,
or retain one or more scenarios. All 21 scenarios remain represented.

The underlying hashes and filenames are recorded in the
[byte-identity audit](lbnl_sd_ahu_duplicate_audit.md).

## Benchmark task separation

Fault-family classification and fault-severity classification are different
tasks. The planned V1.0 primary benchmark should prioritize fault detection and
fault-family diagnosis. Severity prediction must not become a default primary
benchmark while byte-identical severity scenarios remain unresolved by an
explicit benchmark policy.

## Constant-column handling

A variable must not be removed mechanically because its variance is zero within a
single scenario file. `CHWC_VLV` is constant within the `coi_stuck` scenarios, and
`OA_DMPR` and `RA_DMPR` are constant within the `damper_stuck` scenarios.
Scenario-specific constancy may itself carry fault information.

Any later feature pruning must be based on the training partition together with
cross-scenario analysis. A single-file constant-column rule is not permitted.
