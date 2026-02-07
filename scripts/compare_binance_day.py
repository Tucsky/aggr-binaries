#!/usr/bin/env python3
import argparse
import gzip
import io
import os
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Iterable, Optional, Tuple


DATASET_URLS = {
    "spot-trades": "https://data.binance.vision/data/spot/daily/trades/{symbol}/{symbol}-trades-{date}.zip",
    "futures-um-trades": "https://data.binance.vision/data/futures/um/daily/trades/{symbol}/{symbol}-trades-{date}.zip",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare local raw trade counts for a day against Binance Vision raw trades.",
    )
    parser.add_argument("--file", required=True, help="Path to a local file (YYYY-MM-DD-HH(.gz)).")
    parser.add_argument(
        "--binance-symbol",
        default=None,
        help="Binance Vision symbol override (e.g. BTCUSDT). Defaults to symbol dir name.",
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASET_URLS.keys()),
        default="spot-trades",
        help="Binance Vision dataset.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Cache directory for Binance Vision zips (defaults to OS temp).",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List the local files included in the day.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run deterministic path parsing checks and exit.",
    )
    return parser.parse_args()


def ensure_cache_dir(path: Optional[str]) -> Path:
    if path:
        cache = Path(path)
    else:
        cache = Path(tempfile.gettempdir()) / "aggr-binaries-binance-vision"
    cache.mkdir(parents=True, exist_ok=True)
    return cache


def parse_day_from_name(name: str) -> Optional[str]:
    # Expected prefix: YYYY-MM-DD-...
    if len(name) < 10:
        return None
    prefix = name[:10]
    parts = prefix.split("-")
    if len(parts) != 3:
        return None
    if not all(p.isdigit() for p in parts):
        return None
    return prefix


def iter_day_files(directory: Path, day: str) -> Iterable[Path]:
    prefix = f"{day}-"
    for entry in sorted(directory.iterdir()):
        if not entry.is_file():
            continue
        name = entry.name
        if not name.startswith(prefix):
            continue
        if entry.suffix == ".gz" or entry.suffix == "":
            yield entry


def count_lines_stream(stream: io.BufferedReader) -> int:
    total = 0
    last_chunk = b""
    while True:
        chunk = stream.read(1024 * 1024)
        if not chunk:
            break
        total += chunk.count(b"\n")
        last_chunk = chunk
    if last_chunk and not last_chunk.endswith(b"\n"):
        total += 1
    return total


def count_lines_file(path: Path) -> int:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as handle:
        return count_lines_stream(handle)


def download_zip(url: str, cache_dir: Path) -> Optional[Path]:
    name = url.rsplit("/", 1)[-1]
    target = cache_dir / name
    if target.exists():
        return target
    try:
        with urllib.request.urlopen(url) as resp, open(target, "wb") as out:
            out.write(resp.read())
        return target
    except Exception as exc:  # noqa: BLE001
        print(f"  download failed: {url} ({exc})")
        return None


def count_binance_trades(zip_path: Path) -> Tuple[int, Optional[str]]:
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if not names:
            return 0, None
        name = names[0]
        with zf.open(name, "r") as raw:
            total = count_lines_stream(raw)  # type: ignore[arg-type]
        return total, name


def main() -> int:
    args = parse_args()
    if args.self_test:
        assert parse_day_from_name("2021-08-13-04.gz") == "2021-08-13"
        assert parse_day_from_name("2021-08-13-04") == "2021-08-13"
        assert parse_day_from_name("bad-name.gz") is None
        print("self-test ok")
        return 0

    file_path = Path(args.file).expanduser()
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return 2

    day = parse_day_from_name(file_path.name)
    if not day:
        print(f"Failed to parse day from filename: {file_path.name}")
        return 2

    directory = file_path.parent
    day_files = list(iter_day_files(directory, day))
    if not day_files:
        print(f"No day files found in {directory} for {day}")
        return 2

    local_total = 0
    for path in day_files:
        local_total += count_lines_file(path)

    symbol_dir = directory.name
    binance_symbol = args.binance_symbol or symbol_dir.upper()
    url_template = DATASET_URLS[args.dataset]
    url = url_template.format(symbol=binance_symbol, date=day)
    cache_dir = ensure_cache_dir(args.cache_dir)
    zip_path = download_zip(url, cache_dir)
    if not zip_path:
        print("Binance Vision download failed.")
        return 2

    binance_total, zip_name = count_binance_trades(zip_path)
    ratio = "n/a" if local_total == 0 else f"{(binance_total / local_total):.4f}x"

    print(f"day={day} dataset={args.dataset} symbol={binance_symbol}")
    print(f"local_files={len(day_files)} local_lines={local_total}")
    print(f"binance_zip={zip_name} binance_lines={binance_total}")
    print(f"ratio={ratio}")

    if args.list_files:
        for path in day_files:
            print(f"  {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
