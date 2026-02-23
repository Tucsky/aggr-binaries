#!/usr/bin/env python3

import shutil
import argparse
from pathlib import Path
import subprocess
import sys


EXTERNAL_ROOT = Path("/Volumes/AGGR/input")
LOCAL_ROOT = Path("/Users/kevinrostagni/Documents/aggr-data")


def find_source_folders(collector: str, exchange: str, market: str):

    external_base = EXTERNAL_ROOT / collector

    if not external_base.exists():
        print(f"ERROR: External base not found: {external_base}")
        sys.exit(1)

    results = []

    for year_dir in external_base.iterdir():

        candidate = year_dir / exchange / market

        if candidate.exists():
            results.append((year_dir.name, candidate))

    results.sort()

    return results


def rsync_copy(src: Path, dst: Path):

    dst.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "rsync",
        "-a",
        "--delete",
        "--info=progress2",
        str(src) + "/",
        str(dst)
    ]

    subprocess.run(cmd, check=True)


def refresh(collector: str, exchange: str, market: str):

    sources = find_source_folders(collector, exchange, market)

    if not sources:
        print("No source folders found.")
        return

    print(f"Collector: {collector}")
    print(f"Found {len(sources)} folders:")

    for year_name, src in sources:

        dst = LOCAL_ROOT / collector / year_name / exchange / market

        print("\n----------------------------")
        print(f"Year: {year_name}")
        print(f"SRC: {src}")
        print(f"DST: {dst}")

        if dst.exists():
            print("Deleting local copy...")
            shutil.rmtree(dst)

        print("Copying fresh data with rsync...")
        rsync_copy(src, dst)

        print("Done.")


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--collector",
        default="PI",
        help="Collector name (PI, RAM, etc) default=PI"
    )

    parser.add_argument(
        "--exchange",
        required=True,
        help="Exchange name (example: KRAKEN)"
    )

    parser.add_argument(
        "--market",
        required=True,
        help="Market name (example: XBT-USD)"
    )

    args = parser.parse_args()

    refresh(
        collector=args.collector,
        exchange=args.exchange,
        market=args.market
    )


if __name__ == "__main__":
    main()