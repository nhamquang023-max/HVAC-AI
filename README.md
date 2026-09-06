# HVAC-AI

An open-source, reproducible AI toolkit for HVAC anomaly detection and fault detection and diagnosis (FDD).

## Project Status

Early development / Phase 1.

This repository currently contains the initial project structure and basic configuration. Core models, benchmarks, and data pipelines have not yet been released.

## Scope

The primary goals for V1.0 are:

1. AHU anomaly detection
2. AHU fault detection and diagnosis
3. Reproducible benchmarking on public HVAC datasets
4. Interpretable model evaluation
5. Engineering-oriented visualization and reporting

Energy forecasting is considered a secondary extension rather than the primary V1.0 objective.

## Planned Data

Public HVAC datasets will be used.
LBNL HVAC FDD datasets are currently planned as primary benchmark candidates.

## Repository Structure

| Directory | Intended purpose |
| --- | --- |
| `src/hvac_ai` | Python package and planned toolkit modules |
| `tests` | Unit and integration tests |
| `notebooks` | Exploratory analysis and examples |
| `data` | Raw, interim, processed, and external data |
| `configs` | Experiment and application configuration |
| `scripts` | Data preparation and workflow scripts |
| `docs` | Project documentation |
| `reports` | Generated figures, tables, and reports |

## Reproducibility

The project will use Python 3.11, `environment.yml` for the Conda environment definition, `pyproject.toml` for package metadata and configuration, and automated tests.

The current environment definition includes only Python and pip. Scientific dependencies and automated tests will be added in later phases.

## License

MIT License. See [LICENSE](LICENSE).
