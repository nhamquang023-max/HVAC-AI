"""Preflight or run canonical full-batch LBNL SD-AHU ingestion."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from hvac_ai.data.lbnl_sd_ahu_batch import (
    EXPECTED_SCENARIO_COUNT,
    EXPECTED_TOTAL_ROWS,
    ingest_all_scenarios,
    preflight_full_ingestion,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--schema", required=True, type=Path)
    parser.add_argument("--scenarios", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    """Run a side-effect-free preflight or the protected full batch."""
    args = parse_args()
    smoke_manifest = Path("docs/datasets/lbnl_sd_ahu_ingestion_smoke.json")
    common = {
        "archive_path": args.archive,
        "schema_path": args.schema,
        "scenarios_path": args.scenarios,
        "output_dir": args.output_dir,
        "smoke_manifest_path": smoke_manifest,
        "manifest_path": args.manifest,
        "expected_scenario_count": EXPECTED_SCENARIO_COUNT,
        "expected_total_rows": EXPECTED_TOTAL_ROWS,
        "git_root": Path.cwd(),
    }
    if args.preflight_only:
        result = preflight_full_ingestion(**common)
    else:
        result = ingest_all_scenarios(
            chunk_size=args.chunk_size,
            **common,
        )
    print(json.dumps(result, indent=2, sort_keys=False))


if __name__ == "__main__":
    main()
