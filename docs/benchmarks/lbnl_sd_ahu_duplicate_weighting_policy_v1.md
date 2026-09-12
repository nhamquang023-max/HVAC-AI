# LBNL SD-AHU Duplicate-Content Weighting Policy V1

## 1. Purpose

This policy freezes how the LBNL SD-AHU Primary Benchmark V1 controls the
statistical contribution of known byte-identical scenario content. It applies to
the primary `fault_detection` and `fault_family_diagnosis` tasks. It defines
weighting, sampling, and evaluation semantics; it does not implement weights or
authorize model training.

## 2. Evidence and problem

The frozen evidence is the Split V1 contract, scenario registry, and
duplicate-content audit. They establish 21 logical scenarios, 15 unique byte
contents, and two duplicate groups containing eight logical scenarios. The
weighting implementation must read canonical group membership from the split
contract and scenario registry. Filename substring guessing is prohibited.

## 3. Logical scenarios and effective content units

The benchmark contains 13 nonduplicate logical scenarios and two independent
duplicate-content groups:

```text
13 nonduplicate scenarios + 2 duplicate groups = 15 effective content units
```

Thus, 21 logical scenarios do not represent 21 independent physical-content
scenarios.

## 4. Why equal scenario contribution is invalid

Equal contribution from all 21 logical scenarios would count each byte-identical
`oa_bias` and `coi_leakage` content group four times. This is structural
duplicate-content overrepresentation. It is distinct from ordinary class
imbalance.

## 5. Selected inverse-group-size rule

For scenario \(s\), the duplicate weight is

```text
duplicate_weight(s) = 1.0                         if s is nonduplicate
duplicate_weight(s) = 1 / duplicate_group_size   otherwise
```

The general method is `inverse_duplicate_group_size`. Each current group has
size four, so each member receives `0.25`; that value is a consequence of group
size rather than an independently hard-coded decision.

## 6. Exact duplicate groups

| Group | Logical scenario IDs | Size | Member weight | Group total weight |
| --- | --- | ---: | ---: | ---: |
| `duplicate_group_001` | `oa_bias_-2`, `oa_bias_-4`, `oa_bias_2`, `oa_bias_4` | 4 | 0.25 | 1.0 |
| `duplicate_group_002` | `coi_leakage_010`, `coi_leakage_025`, `coi_leakage_040`, `coi_leakage_050` | 4 | 0.25 | 1.0 |

Every other logical scenario receives weight `1.0`.

## 7. Training semantics

For both primary tasks, every training row inherits the duplicate weight of its
logical scenario. Primary V1-compliant training must apply these weights. The
preferred mechanism is estimator or framework `sample_weight`, or its verified
equivalent.

**All 21 logical scenarios remain present in train, validation, and test views.**

## 8. Why physical deduplication is rejected

Physical deduplication would discard logical identities and provenance. It would
also require changing canonical Parquet or Dataset View membership. Both actions
are prohibited. Weighting changes statistical contribution while preserving the
21-scenario benchmark population.

## 9. Why random duplicate resampling is rejected

Random undersampling, random duplicate dropping, and choosing one severity member
per epoch are prohibited as the Primary V1 default. Those methods make the
correction sample- or epoch-dependent. Deterministic sample weights preserve every
logical scenario and give each independent content unit stable contribution.

## 10. Estimator requirement

An estimator that cannot correctly consume per-sample weights is unsupported for
Primary V1 weighted training. The weights must never be silently ignored. Such an
estimator is noncompliant unless a separate deterministic adapter contract is
defined and validated later.

## 11. Validation and test population preservation

Validation and test retain every contract-defined row from all 21 scenarios.
Neither partition may be under-sampled, over-sampled, balanced, or physically
deduplicated. Their raw populations remain 1,360,800 and 1,360,821 rows,
respectively.

## 12. Raw and duplicate-corrected metrics

Every validation and test report must include both views:

- `raw_logical_population`: unweighted metrics over all 21 logical scenarios;
- `duplicate_corrected_population`: metrics over the same rows using the scenario
  duplicate weights.

Duplicate-corrected evaluation changes metric contribution, not dataset
membership. Raw metrics remain required for transparency.

## 13. Primary aggregate comparison

The duplicate-corrected validation aggregate is the duplicate-aware view used for
Primary Benchmark model selection and comparison. The later Metric Contract will
choose applicable metrics such as F1, balanced accuracy, or AUROC. Whatever metric
is selected must preserve this duplicate-correction policy. The raw logical
aggregate is a required secondary transparency metric.

## 14. Per-scenario reporting

Per-scenario metrics describe performance within one logical scenario and require
no duplicate weighting. When metrics are aggregated across scenarios, a member of
a size-four duplicate group contributes `0.25`, so the group cannot receive four
times the influence of an ordinary scenario.

## 15. Exact weight-sum invariants

| Partition | Raw rows | Rows per scenario | Effective units | Duplicate-corrected weight sum |
| --- | ---: | ---: | ---: | ---: |
| Train | 3,688,020 | 175,620 | 15 | 2,634,300 |
| Validation | 1,360,800 | 64,800 | 15 | 972,000 |
| Test | 1,360,821 | 64,801 | 15 | 972,015 |
| **Modeling total** | **6,409,641** | — | — | **4,578,315** |

For either duplicate group, its partition weight sums are:

```text
train:      4 × 175620 × 0.25 = 175620
validation: 4 ×  64800 × 0.25 =  64800
test:       4 ×  64801 × 0.25 =  64801
```

The corrected values are weight sums, or effective weighted support. They are not
physical row counts; all 6,409,641 modeling rows remain available.

## 16. Fault-detection effective support

After duplicate correction, train has one `fault_free` scenario-equivalent and
14 fault scenario-equivalents. The effective fault-to-fault-free ratio remains
approximately `14:1`.

## 17. Fault-family effective support

| Fault family | Effective train scenario units |
| --- | ---: |
| `fault_free` | 1 |
| `coi_bias` | 4 |
| `coi_leakage` | 1 |
| `coi_stuck` | 4 |
| `damper_stuck` | 4 |
| `oa_bias` | 1 |
| **Total** | **15** |

These values remove known byte-identical repetition. They are not targets for
equalizing fault families.

## 18. Separation from class imbalance

**Duplicate weighting corrects known byte-identical replication; it does not balance classes.**

The remaining `14:1` effective fault imbalance requires a separate Class
Imbalance Contract. Duplicate weights must not be altered to solve that separate
problem.

## 19. Future class-weight composition

If a later contract introduces task-specific class weights, the training rule is

```text
final_sample_weight = duplicate_weight × future_task_class_weight
```

The duplicate component is fixed here. The class component may be derived only
from the train partition. Validation, test, or full-dataset statistics must not be
used to fit it.

Training class-balance weights are prohibited for default validation/test
aggregate evaluation. Duplicate correction remains allowed there because it
corrects verified content replication rather than manufacturing a balanced test
population.

## 20. Severity limitation

Severity classification remains `not_primary`. Weighting cannot recover severity
information absent from byte-identical content, and it does not make severity
members distinguishable. Naive severity classification for affected groups
remains prohibited.

## 21. Runtime provenance and feature boundary

Weights must derive from scenario-level duplicate metadata in the Split V1
contract and scenario registry. They must not be inferred from feature values.
Scenario ID, duplicate group, and severity token may support weight provenance but
must not automatically enter the ML feature matrix. A later Feature Contract will
define feature eligibility.

## 22. Determinism and batch independence

The same Split V1 contract, scenario registry, and Duplicate Policy V1 must always
produce the same weight vector. Results cannot depend on random seed, epoch, model,
machine, filesystem order, streaming batch order, or batch size. Changing Dataset
View `batch_size` from 50,000 to 100,000 cannot change any row's scenario weight.

## 23. No row-level weight artifact

V1 weights are derived deterministically at runtime. This policy creates no
6.4-million-row weight file, row-level manifest, weighted Parquet copy, or cached
dataset. Duplicate weights must not be appended to canonical Parquet or used to
rewrite its schema.

## 24. Known limitations

- The runtime weighting adapter is not implemented.
- Model-specific `sample_weight` compatibility is not validated.
- Class imbalance remains unresolved.
- The Metric Contract has not selected final aggregate metrics.
- Severity distinguishability remains unavailable for the byte-identical groups.

Formal Primary V1 model training remains prohibited until the runtime duplicate
weight implementation is complete and verified.

## 25. Next implementation step

Implement and test a lightweight runtime adapter that derives scenario weights
from the frozen split contract and scenario registry, composes them with Dataset
View batches without changing source columns, and fails closed for unsupported
estimators. That implementation requires a separate reviewed step.

## References

- [Split Contract V1](lbnl_sd_ahu_split_contract_v1.md)
- [Dataset View Strategy V1](lbnl_sd_ahu_dataset_view_strategy_v1.md)
- [Scenario Registry](../../configs/lbnl_sd_ahu_scenarios.yaml)
- [Duplicate-Content Audit](../datasets/lbnl_sd_ahu_duplicate_audit.md)
- [Data Audit](../data_audit.md)
