#!/usr/bin/env python3

import argparse
import subprocess
import platform
from pathlib import Path
import sys

# surgical transfer of a whole serie from disk A to disk B
#
# usage on windows:
# ```
# python sync_aggr.py ^
#   --src-root D:\AGGR ^
#   --dst-root F:\AGGR ^
#   --collector RAM ^
#   --exchange BITMEX ^
#   --market XBTUSD
# ```
#
# usage on mac:
# ```
# python sync_aggr.py \
#   --src-root /Volumes/AGGR/input \
#   --dst-root /Users/me/Documents/aggr-data \
#   --collector RAM \
#   --exchange BITMEX \
#   --market XBTUSD
# ```

def sync_folder(src: Path, dst: Path):

    print(f"SRC: {src}")
    print(f"DST: {dst}")

    if not src.exists():
        print("Source does not exist, skipping.")
        return

    dst.parent.mkdir(parents=True, exist_ok=True)

    if platform.system() == "Windows":

        cmd = [
            "robocopy",
            str(src),
            str(dst),
            "/MIR",
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

        cmd = [
            "rsync",
            "-a",
            "--delete",
            "--info=progress2",
            str(src) + "/",
            str(dst)
        ]

        subprocess.run(cmd, check=True)

    print("Done.")


def refresh(root_src: Path, root_dst: Path, collector: str, exchange: str, market: str):

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

            sync_folder(src, dst)


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--src-root", required=True)
    parser.add_argument("--dst-root", required=True)

    parser.add_argument("--collector", required=True)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--market", required=True)

    args = parser.parse_args()

    refresh(
        Path(args.src_root),
        Path(args.dst_root),
        args.collector,
        args.exchange,
        args.market
    )


if __name__ == "__main__":
    main()