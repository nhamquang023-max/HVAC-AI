# Data Directory

The data directories separate source data from intermediate and model-ready
artifacts. Dataset contents are kept outside Git.

## Directory structure

- `data/raw/`: Original downloaded datasets. Ignored by Git.
- `data/interim/`: Intermediate transformations. Ignored by Git.
- `data/processed/`: Model-ready processed datasets. Ignored by Git.
- `data/external/`: External metadata or documentation where appropriate. Raw
  third-party datasets should not be committed.

## Primary planned data source

The primary planned data source is the LBNL Single-Duct AHU FDD dataset.

- DOI: [10.25984/1881324](https://doi.org/10.25984/1881324)
- Official page: <https://faultdetection.lbl.gov/dataset/simulated-sd-ahu/>

See the [dataset documentation](../docs/datasets/lbnl_sd_ahu.md) for known
discrepancies between official CSV-count and DOI metadata.

Obtain the dataset from the official LBNL distribution. Do not commit downloaded
archives, CSV files, or derived datasets to this repository.
