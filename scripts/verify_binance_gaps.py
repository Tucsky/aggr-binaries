#!/usr/bin/env python3
import argparse
import datetime as dt
import gzip
import io
import os
import random
import sqlite3
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Iterable, Optional, Tuple


DATASET_URLS = {
    "spot-trades": "https://data.binance.vision/data/spot/daily/trades/{symbol}/{symbol}-trades-{date}.zip",
    "futures-um-trades": "https://data.binance.vision/data/futures/um/daily/trades/{symbol}/{symbol}-trades-{date}.zip",
    "spot-agg": "https://data.binance.vision/data/spot/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip",
    "futures-um-agg": "https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip",
}

DATASET_TS_INDEX = {
    "spot-trades": 4,
    "futures-um-trades": 4,
    "spot-agg": 5,
    "futures-um-agg": 5,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample gaps from the events table and validate against Binance Vision data.",
    )
    parser.add_argument("--db", default="index.sqlite", help="Path to the sqlite index db.")
    parser.add_argument("--collector", default=None, help="Collector filter (optional).")
    parser.add_argument("--exchange", default=None, help="Exchange filter (optional).")
    parser.add_argument("--symbol", default=None, help="Symbol filter (optional).")
    parser.add_argument(
        "--binance-symbol",
        default=None,
        help="Binance Vision symbol override (e.g. BTCUSDT). Defaults to --symbol.",
    )
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASET_URLS.keys()),
        default="spot-trades",
        help="Binance Vision dataset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of gaps to inspect (0 means no limit).",
    )
    parser.add_argument("--min-miss", type=int, default=0, help="Minimum gap_miss filter.")
    parser.add_argument("--min-gap-ms", type=int, default=0, help="Minimum gap_ms filter.")
    parser.add_argument(
        "--order",
        choices=["random", "gap_miss", "gap_ms", "hybrid"],
        default="random",
        help="How to choose gaps.",
    )
    parser.add_argument(
        "--offset",
        "--start",
        dest="offset",
        type=int,
        default=0,
        help="Skip the first N rows after ordering (useful for jumping into the list).",
    )
    parser.add_argument(
        "--pool",
        type=int,
        default=1000,
        help="Pool size for random selection (top by gap_miss).",
    )
    parser.add_argument("--seed", type=int, default=1337, help="Random seed for sampling.")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Cache directory for Binance Vision zips (defaults to OS temp).",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run a deterministic offset/selection check and exit.",
    )
    return parser.parse_args()


def fmt_ts(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def fmt_ratio(actual: int, estimated: int) -> str:
    if estimated <= 0:
        return "n/a"
    return f"{actual / estimated:.4f}x"


def format_gap_summary(gap_id: int, gap_miss: int, total: int, ratio: str) -> str:
    return f"[id={gap_id}] est_miss={gap_miss} binance_trades={total} ratio={ratio}"


def format_window_summary(
    start_ms: int,
    end_ms: int,
    gap_ms: int,
    first_hit: Optional[int],
    last_hit: Optional[int],
) -> str:
    first = fmt_ts(first_hit) if first_hit is not None else "n/a"
    last = fmt_ts(last_hit) if last_hit is not None else "n/a"
    return (
        f"  window={fmt_ts(start_ms)} -> {fmt_ts(end_ms)} UTC "
        f"gap_ms={gap_ms} first={first} last={last}"
    )


def iter_gap_rows(conn: sqlite3.Connection, args: argparse.Namespace) -> Iterable[sqlite3.Row]:
    filters = ["e.event_type = 'gap'", "e.gap_ms IS NOT NULL", "e.gap_miss IS NOT NULL"]
    params = {}
    if args.collector:
        filters.append("e.collector = :collector")
        params["collector"] = args.collector
    if args.exchange:
        filters.append("e.exchange = :exchange")
        params["exchange"] = args.exchange
    if args.symbol:
        filters.append("e.symbol = :symbol")
        params["symbol"] = args.symbol
    if args.min_miss:
        filters.append("e.gap_miss >= :min_miss")
        params["min_miss"] = args.min_miss
    if args.min_gap_ms:
        filters.append("e.gap_ms >= :min_gap_ms")
        params["min_gap_ms"] = args.min_gap_ms

    where_clause = " AND ".join(filters)
    order_by = "e.id ASC"
    if args.order == "gap_miss":
        order_by = "e.gap_miss DESC"
    elif args.order == "gap_ms":
        order_by = "e.gap_ms DESC"
    elif args.order == "hybrid":
        order_by = "(e.gap_ms * e.gap_miss) DESC"

    sql = f"""
      SELECT e.id, e.root_id, r.path AS root_path, e.relative_path,
             e.collector, e.exchange, e.symbol,
             e.start_line, e.end_line, e.gap_ms, e.gap_miss, e.gap_end_ts
        FROM events e
        JOIN roots r ON r.id = e.root_id
       WHERE {where_clause}
       ORDER BY {order_by}
       LIMIT :limit
       OFFSET :offset;
    """

    if args.order == "random":
        pool_sql = f"""
          SELECT e.id, e.root_id, r.path AS root_path, e.relative_path,
                 e.collector, e.exchange, e.symbol,
                 e.start_line, e.end_line, e.gap_ms, e.gap_miss, e.gap_end_ts
            FROM events e
            JOIN roots r ON r.id = e.root_id
           WHERE {where_clause}
           ORDER BY e.gap_miss DESC
           LIMIT :pool;
        """
        pool_params = dict(params)
        pool_params["pool"] = args.pool
        rows = conn.execute(pool_sql, pool_params).fetchall()
        if not rows:
            return []
        if args.offset:
            rows = rows[args.offset :]
            if not rows:
                return []
        if args.limit <= 0:
            return rows
        rng = random.Random(args.seed)
        sample = rng.sample(rows, k=min(args.limit, len(rows)))
        return sample

    params["limit"] = -1 if args.limit <= 0 else args.limit
    params["offset"] = args.offset
    return conn.execute(sql, params).fetchall()


def resolve_file_path(root_path: str, relative_path: str) -> Path:
    rel = Path(relative_path)
    return Path(root_path) / rel


def read_line_timestamp(file_path: Path, line_number: int) -> Optional[int]:
  opener = gzip.open if file_path.suffix == ".gz" else open
  try:
    with opener(file_path, "rt", encoding="utf-8", errors="ignore") as handle:
      for idx, line in enumerate(handle, start=1):
        if idx == line_number:
          parts = line.strip().split()
          if not parts:
            return None
          try:
            return int(parts[0])
          except ValueError:
            return None
  except FileNotFoundError:
    return None
  return None


def ensure_cache_dir(path: Optional[str]) -> Path:
    if path:
        cache = Path(path)
    else:
        cache = Path(tempfile.gettempdir()) / "aggr-binaries-binance-vision"
    cache.mkdir(parents=True, exist_ok=True)
    return cache


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


def scan_zip_for_window(
    zip_path: Path,
    start_ms: int,
    end_ms: int,
    ts_index: int,
) -> Tuple[int, Optional[int], Optional[int]]:
    count = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if not names:
            return 0, None, None
        with zf.open(names[0], "r") as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="ignore")
            for line in text:
                parts = line.rstrip().split(",")
                if len(parts) <= ts_index:
                    continue
                try:
                    ts = int(parts[ts_index])
                except ValueError:
                    continue
                if ts < start_ms:
                    continue
                if ts > end_ms:
                    break
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
                count += 1
    return count, first_ts, last_ts


def iter_dates(start_ms: int, end_ms: int) -> Iterable[str]:
    start_date = dt.datetime.utcfromtimestamp(start_ms / 1000).date()
    end_date = dt.datetime.utcfromtimestamp(end_ms / 1000).date()
    current = start_date
    while current <= end_date:
        yield current.strftime("%Y-%m-%d")
        current += dt.timedelta(days=1)


def main() -> int:
    args = parse_args()
    if args.self_test:
        sample = list(range(10))
        offset = 3
        limit = 4
        expected = sample[offset : offset + limit]
        actual = sample[offset : offset + limit]
        assert actual == expected, "offset selection failed"
        summary = format_gap_summary(1, 10, 5, "0.5000x")
        expected_summary = "[id=1] est_miss=10 binance_trades=5 ratio=0.5000x"
        assert summary == expected_summary, "summary formatting failed"
        window_summary = format_window_summary(1000, 2000, 1000, 11000, 12000)
        expected_window = (
            "  window=1970-01-01 00:00:01 -> 1970-01-01 00:00:02 UTC "
            "gap_ms=1000 first=1970-01-01 00:00:11 last=1970-01-01 00:00:12"
        )
        assert window_summary == expected_window, "window summary formatting failed"
        print("self-test ok")
        return 0
    binance_symbol = args.binance_symbol or args.symbol
    if not binance_symbol:
        print("Missing --symbol or --binance-symbol.")
        return 2
    url_template = DATASET_URLS[args.dataset]
    ts_index = DATASET_TS_INDEX[args.dataset]
    cache_dir = ensure_cache_dir(args.cache_dir)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = iter_gap_rows(conn, args)
    if not rows:
        print("No gap events matched the filters.")
        return 0

    inspected = 0
    skipped = 0
    total_est_miss = 0
    total_binance_trades = 0

    for row in rows:
        gap_end_ts = row["gap_end_ts"]
        file_path = None
        ts = gap_end_ts
        if ts is None:
            file_path = resolve_file_path(row["root_path"], row["relative_path"])
            ts = read_line_timestamp(file_path, row["start_line"])
        if ts is None:
            path_desc = file_path or resolve_file_path(row["root_path"], row["relative_path"])
            print(f"[id={row['id']}] missing or unreadable line {row['start_line']} in {path_desc}")
            continue

        gap_ms = int(row["gap_ms"])
        gap_miss = int(row["gap_miss"])
        start_ms = ts - gap_ms
        end_ms = ts
        dates = list(iter_dates(start_ms, end_ms))
        if len(dates) > 3:
            print(format_gap_summary(row["id"], gap_miss, 0, "n/a"))
            print(f"  binance_trades=skip days={len(dates)}")
            skipped += 1
            continue

        total = 0
        first_hit: Optional[int] = None
        last_hit: Optional[int] = None
        missing_days = 0
        for day in dates:
            url = url_template.format(symbol=binance_symbol, date=day)
            zip_path = download_zip(url, cache_dir)
            if not zip_path:
                missing_days += 1
                continue
            count, first_ts, last_ts = scan_zip_for_window(zip_path, start_ms, end_ms, ts_index)
            if count:
                if first_hit is None or (first_ts is not None and first_ts < first_hit):
                    first_hit = first_ts
                if last_hit is None or (last_ts is not None and last_ts > last_hit):
                    last_hit = last_ts
            total += count

        if missing_days:
            print(format_gap_summary(row["id"], gap_miss, 0, "n/a"))
            print(f"  binance_trades=skip missing_days={missing_days}")
            skipped += 1
            continue

        ratio = fmt_ratio(total, gap_miss)
        print(format_gap_summary(row["id"], gap_miss, total, ratio))
        if gap_end_ts is None and file_path is not None:
            print(f"  source=file line={row['start_line']} file={file_path}")
        print(format_window_summary(start_ms, end_ms, gap_ms, first_hit, last_hit))
        inspected += 1
        total_est_miss += gap_miss
        total_binance_trades += total

    summary_ratio = fmt_ratio(total_binance_trades, total_est_miss)
    print(
        "summary "
        f"gaps={len(rows)} inspected={inspected} skipped={skipped} "
        f"total_est_miss={total_est_miss} total_binance_trades={total_binance_trades} "
        f"ratio={summary_ratio}",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
