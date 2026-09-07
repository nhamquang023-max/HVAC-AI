# LBNL Single-Duct AHU Dataset

## Dataset identity

The LBNL Fault Detection and Diagnostics Data Sets include the Single-Duct Air
Handling Unit (SD-AHU) dataset. It is distributed by Lawrence Berkeley National
Laboratory. This project currently uses
[10.25984/1881324](https://doi.org/10.25984/1881324) as the canonical dataset DOI;
the source discrepancy affecting this identifier is documented below.

## Official source

The official dataset page is:
<https://faultdetection.lbl.gov/dataset/simulated-sd-ahu/>

The official SD-AHU inventory PDF is:
<https://fdddata.lbl.gov/data/Simulated_LBNL_FDD_Data_Sets_SDAHU/LBNL_FDD_Data_Sets_SDAHU.pdf>

## System description

The simulated system is a single-duct AHU serving five variable air volume (VAV)
zones. The AHU includes a chilled-water cooling coil and variable-speed supply and
return fans. The terminal units regulate zone airflow and provide hydronic terminal
reheat when required.

## Data generation

The dataset was generated through EnergyPlus and Modelica co-simulation.

## Dataset scope

- Time span: 365 days
- Sample interval: 1 minute
- Monitored data points: 30
- CSV files: 21 in the downloaded archive identified below
- Fault types: 5
- Cases: multiple fault severity levels and one fault-free case
- Official downloadable ZIP size: approximately 593 MB
- Official uncompressed data size: approximately 2.69 GB

## Known source discrepancies

### CSV file count

The current LBNL dataset webpage reports 20 CSV files. The official SD-AHU
inventory PDF lists 21 entries in Table 4: 20 faulted-case files and one
fault-free file. The downloaded archive contains 21 regular CSV members and
therefore agrees with the inventory PDF for this pinned archive:

- SHA-256: `8295FCF0F55BC955937CB4EC0198512C28E5EDE32E6BBF735257B0DF55426471`
- Archive audit: [LBNL SD-AHU archive provenance](lbnl_sd_ahu_archive_provenance.md)

The website's reported count of 20 remains preserved as an official-source
discrepancy.

### DOI

The current LBNL landing page and DOE/Data.gov catalog report DOI
`10.25984/1881324`. The printed citation in the SD-AHU inventory PDF instead
contains DOI `10.25984/1881321`.

HVAC-AI currently uses `10.25984/1881324` as the canonical dataset DOI because
the two current official online metadata sources agree on that value. The
different DOI printed in the inventory PDF is retained here as a provenance
discrepancy. This project does not assert that either identifier is erroneous or
that `10.25984/1881321` is an SD-AHU-specific DOI.

## License

The dataset is licensed under the Creative Commons Attribution 4.0 International
(CC BY 4.0) license.

The dataset may be used and redistributed subject to the attribution requirements
of CC BY 4.0.

This dataset license is separate from the MIT License that applies to HVAC-AI source
code.

## Citation

Lawrence Berkeley National Laboratory, *LBNL Fault Detection and Diagnostics Data
Sets*. DOI: [10.25984/1881324](https://doi.org/10.25984/1881324).

This is the project's current canonical citation. See
[Known source discrepancies](#known-source-discrepancies) for the different DOI
printed in the SD-AHU inventory PDF.

## Repository policy

Raw LBNL data files are NOT committed to this Git repository.

Users should obtain the source data from the official LBNL/DOE distribution.

## Byte-identity audit

A streamed SHA-256 audit found two groups of byte-identical CSV members in the
pinned archive, covering eight filenames and 15 unique CSV contents overall.
See the [LBNL SD-AHU byte-identity audit](lbnl_sd_ahu_duplicate_audit.md) for the
member hashes, duplicate groups, and benchmark implications.

## Current project usage

The SD-AHU dataset is planned as the primary V1.0 benchmark for AHU anomaly
detection and fault detection and diagnosis. Benchmark preparation and evaluation
have not yet been completed.
