#!/usr/bin/env python3

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


LINE_RE = re.compile(
    r"^\[BookImbalanceDiff:(?P<symbol>[^\s\]]+)\s+(?P<hh>\d\d):(?P<mm>\d\d):(?P<ss>\d\d)\.(?P<ms>\d+)\]\s+"
    r"(?P<bid>[-+0-9.eE]+),(?P<ask>[-+0-9.eE]+),(?P<imb>[-+0-9.eE]+)\s*$"
)


@dataclass
class Row:
    t: float
    imb_diff: float


def _time_to_seconds(hh: int, mm: int, ss: int, ms: int) -> float:
    return hh * 3600.0 + mm * 60.0 + ss + ms / 1000.0


def parse_logdiff(path: Path) -> Dict[str, List[Row]]:
    # Keep the *last* value for each (symbol, time) since the diff node can publish twice
    # (once when A arrives, once when B arrives). The later one is typically the aligned pair.
    last_by_key: Dict[Tuple[str, float], float] = {}

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = LINE_RE.match(line.strip())
            if not m:
                continue

            symbol = m.group("symbol")
            hh = int(m.group("hh"))
            mm = int(m.group("mm"))
            ss = int(m.group("ss"))
            ms_str = m.group("ms")
            ms = int(ms_str[:3].ljust(3, "0"))  # normalize to milliseconds

            t = _time_to_seconds(hh, mm, ss, ms)
            imb = float(m.group("imb"))

            last_by_key[(symbol, t)] = imb

    # Convert to per-symbol sorted series, and make time relative.
    series: Dict[str, List[Row]] = {}
    for (symbol, t), imb in last_by_key.items():
        series.setdefault(symbol, []).append(Row(t=t, imb_diff=imb))

    for symbol in list(series.keys()):
        series[symbol].sort(key=lambda r: r.t)

        # Handle possible midnight wrap by enforcing monotonic time via +24h jumps.
        fixed: List[Row] = []
        prev_t = None
        day_offset = 0.0
        for r in series[symbol]:
            cur = r.t + day_offset
            if prev_t is not None and cur < prev_t - 1.0:  # tolerate minor disorder
                day_offset += 24.0 * 3600.0
                cur = r.t + day_offset
            fixed.append(Row(t=cur, imb_diff=r.imb_diff))
            prev_t = cur

        t0 = fixed[0].t if fixed else 0.0
        series[symbol] = [Row(t=row.t - t0, imb_diff=row.imb_diff) for row in fixed]

    return series


def plot_series(series: Dict[str, List[Row]], out_png: Path, title: str) -> None:
    import matplotlib.pyplot as plt

    symbols = sorted(series.keys())
    if not symbols:
        raise SystemExit("No BookImbalanceDiff lines found.")

    fig, ax = plt.subplots(figsize=(12, 6))

    for sym in symbols:
        xs = [r.t for r in series[sym]]
        ys = [r.imb_diff for r in series[sym]]
        ax.plot(xs, ys, label=sym, linewidth=1.0)

    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.set_xlabel("Time since first sample (s)")
    ax.set_ylabel("ImbalanceDiff (Inc - Cg)")
    ax.set_title(title)
    ax.legend(loc="best", ncol=min(4, len(symbols)))
    ax.grid(True, linestyle=":", linewidth=0.7)

    fig.tight_layout()
    fig.savefig(out_png)


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Muse2 logdiff and plot BookImbalanceDiff (imbalance) vs time")
    ap.add_argument("logfile", type=Path, help="Path to logdiff file")
    ap.add_argument("--out", type=Path, default=None, help="Output PNG path (default: <logfile>.png)")
    ap.add_argument("--title", type=str, default="BookImbalanceDiff vs time", help="Plot title")
    args = ap.parse_args()

    log_path: Path = args.logfile
    out_png: Path = args.out if args.out is not None else log_path.with_suffix(log_path.suffix + ".png")

    series = parse_logdiff(log_path)
    plot_series(series, out_png, args.title)

    total = sum(len(v) for v in series.values())
    symbols = ",".join(sorted(series.keys()))
    print(f"Wrote {out_png} ({total} points; symbols={symbols})")


if __name__ == "__main__":
    main()
