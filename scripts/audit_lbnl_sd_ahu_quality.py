"""Command-line wrapper for the LBNL SD-AHU data-quality audit."""

from __future__ import annotations

import argparse
from pathlib import Path

from hvac_ai.data.lbnl_sd_ahu_audit import audit_archive, write_audit_manifest


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--member-hashes", required=True, type=Path)
    parser.add_argument("--output-json", required=True, type=Path)
    parser.add_argument("--chunk-size", type=int, default=50_000)
    return parser.parse_args()


def main() -> None:
    """Run the audit and write its JSON manifest."""
    args = parse_args()
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive")
    audit = audit_archive(
        args.archive,
        args.member_hashes,
        args.chunk_size,
        progress=lambda message: print(message, flush=True),
    )
    write_audit_manifest(audit, args.output_json)


if __name__ == "__main__":
    main()
