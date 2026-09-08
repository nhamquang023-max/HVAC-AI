"""Stream one registered LBNL SD-AHU ZIP member into canonical Parquet."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hvac_ai.data.lbnl_sd_ahu_ingest import ingest_scenario_to_parquet


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--scenarios", required=True, type=Path)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--row-limit", type=int)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    """Run one scenario ingestion and print its result as JSON."""
    args = parse_args()
    result = ingest_scenario_to_parquet(
        archive_path=args.archive,
        schema_path=args.schema,
        scenarios_path=args.scenarios,
        scenario=args.scenario,
        output_path=args.output,
        chunk_size=args.chunk_size,
        row_limit=args.row_limit,
        overwrite=args.overwrite,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
