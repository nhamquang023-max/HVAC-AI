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

## Data

The primary planned benchmark is the LBNL Single-Duct AHU FDD dataset. The dataset is
not bundled with this repository; users should obtain it from the
[official LBNL source](https://faultdetection.lbl.gov/dataset/simulated-sd-ahu/).
It is published under the Creative Commons Attribution 4.0 International license
(CC BY 4.0) with DOI [10.25984/1881324](https://doi.org/10.25984/1881324).

See the [dataset documentation](docs/datasets/lbnl_sd_ahu.md) and
[data license summary](DATA_LICENSES.md) for source, attribution, and repository policy.
Known discrepancies between official metadata sources are preserved in the dataset
documentation pending inspection of the downloaded archive.

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

Scientific and development dependencies are declared in `pyproject.toml`, with resolved versions
recorded in `requirements-lock.txt`.

## License

MIT License. See [LICENSE](LICENSE).
