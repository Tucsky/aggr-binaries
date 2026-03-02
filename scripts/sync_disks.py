#!/usr/bin/env python3

import argparse
import subprocess
import platform
from pathlib import Path
import sys
from datetime import datetime


# surgical transfer of a whole serie from disk A to disk B
#
# supports optional --start YYYY-MM-DD --end YYYY-MM-DD
# filters files based on filename date prefix


def file_in_range(file: Path, start, end):

    name = file.name.replace(".gz", "")

    try:

        parts = name.split("-")

        year = int(parts[0])
        month = int(parts[1])
        day = int(parts[2])

        hour = int(parts[3]) if len(parts) >= 4 else 0

        dt = datetime(year, month, day, hour)

    except Exception:

        return False

    if start and dt < start:
        return False

    if end and dt >= end:
        return False

    return True


def sync_folder(src: Path, dst: Path, start, end):

    print(f"SRC: {src}")
    print(f"DST: {dst}")

    if not src.exists():
        print("Source does not exist, skipping.")
        return

    dst.mkdir(parents=True, exist_ok=True)

    files = [
        f for f in sorted(src.iterdir())
        if f.is_file() and file_in_range(f, start, end)
    ]

    print(f"Files selected: {len(files)}")

    if not files:
        print("Nothing to transfer.")
        return

    if platform.system() == "Windows":

        for f in files:

            cmd = [
                "robocopy",
                str(src),
                str(dst),
                f.name,
                "/MT:32",
                "/J",
                "/R:2",
                "/W:2",
                "/FFT",
                "/COPY:DAT",
                "/NP"
            ]

            result = subprocess.run(cmd)

            if result.returncode >= 8:
                print(f"Robocopy failed with code {result.returncode}")
                sys.exit(result.returncode)

    else:

        for f in files:

            cmd = [
                "rsync",
                "-a",
                "--progress",
                str(f),
                str(dst)
            ]

            subprocess.run(cmd, check=True)

    print("Done.")


def refresh(root_src, root_dst, collector, exchange, market, start, end):

    collector_src = root_src / collector

    if not collector_src.exists():

        print("Collector not found in source.")
        sys.exit(1)

    for year_dir in sorted(collector_src.iterdir()):

        src = year_dir / exchange / market

        if src.exists():

            dst = root_dst / collector / year_dir.name / exchange / market

            print("\n----------------------------")
            print(f"Year: {year_dir.name}")

            sync_folder(src, dst, start, end)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--src-root", required=True)
    parser.add_argument("--dst-root", required=True)

    parser.add_argument("--collector", required=True)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--market", required=True)

    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")

    args = parser.parse_args()

    start = datetime.fromisoformat(args.start) if args.start else None
    end = datetime.fromisoformat(args.end) if args.end else None

    refresh(
        Path(args.src_root),
        Path(args.dst_root),
        args.collector,
        args.exchange,
        args.market,
        start,
        end
    )


if __name__ == "__main__":

    main()