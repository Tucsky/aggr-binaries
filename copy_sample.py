#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
import sys

def main():
    # Source and destination roots (can be overridden by CLI args)
    src_root = Path("/Volumes/AGGR/input")
    dst_root = Path("./samples/input")

    if len(sys.argv) >= 2:
        src_root = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        dst_root = Path(sys.argv[2])

    if not src_root.is_dir():
        print(f"Source root does not exist or is not a directory: {src_root}")
        sys.exit(1)

    print(f"Source:      {src_root}")
    print(f"Destination: {dst_root}")
    print("Walking filesystem…")

    copied_files = 0
    visited_dirs = 0

    for dirpath, dirnames, filenames in os.walk(src_root):
        visited_dirs += 1

        # Only regular files, ignore weird entries / symlinks
        full_files = []
        for name in filenames:
            p = Path(dirpath) / name
            # skip if not a real file (just in case)
            if p.is_file():
                full_files.append(name)

        if not full_files:
            continue

        # Take up to 2 files; sort for determinism in large dirs
        if len(full_files) > 2:
            chosen = sorted(full_files)[:2]
        else:
            chosen = full_files

        for name in chosen:
            src_file = Path(dirpath) / name
            rel_path = src_file.relative_to(src_root)
            dst_file = dst_root / rel_path

            dst_file.parent.mkdir(parents=True, exist_ok=True)
            # copy2 keeps timestamps etc., but still simple enough
            shutil.copy2(src_file, dst_file)
            copied_files += 1

            if copied_files % 1000 == 0:
                print(f"Copied {copied_files} files so far…")

    print(f"Done. Visited {visited_dirs} directories.")
    print(f"Copied {copied_files} files in total.")

if __name__ == "__main__":
    main()
