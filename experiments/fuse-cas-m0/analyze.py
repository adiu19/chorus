#!/usr/bin/env python3
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main(jsonl_paths: list[str]) -> None:
    rows = []
    for path in jsonl_paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))

    # grouped[op][concurrency][label] -> list of trial p99s (ns)
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in rows:
        p99 = r.get("percentiles_ns", {}).get("p99")
        if p99 is None:
            continue
        grouped[r["op"]][r["concurrency"]][r["label"]].append(p99)

    for op in sorted(grouped):
        print(f"\n=== {op} ===")
        header = f"{'c':>4} {'tmpfs med (us)':>16} {'fuse med (us)':>16} {'fuse min..max (us)':>24} {'overhead med (us)':>20} {'n':>4}"
        print(header)
        for c in sorted(grouped[op]):
            tmpfs_runs = grouped[op][c].get("tmpfs", [])
            fuse_runs = grouped[op][c].get("fuse", [])
            if not tmpfs_runs or not fuse_runs:
                continue
            t_med = statistics.median(tmpfs_runs) / 1000.0
            f_med = statistics.median(fuse_runs) / 1000.0
            f_min = min(fuse_runs) / 1000.0
            f_max = max(fuse_runs) / 1000.0
            overhead = f_med - t_med
            n = len(fuse_runs)
            print(f"{c:>4} {t_med:>16.2f} {f_med:>16.2f} {f_min:>10.1f}..{f_max:>10.1f} {overhead:>20.2f} {n:>4}")

    out_png = Path(jsonl_paths[-1]).with_suffix(".png")
    try_plot(grouped, out_png)


def try_plot(grouped, png_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n[skipping chart: matplotlib not installed]")
        return

    fig, axes = plt.subplots(1, len(grouped), figsize=(6 * len(grouped), 5), squeeze=False)
    for ax_idx, op in enumerate(sorted(grouped)):
        ax = axes[0][ax_idx]
        concurrencies = sorted(grouped[op])
        for label in ("tmpfs", "fuse"):
            meds, mins, maxs = [], [], []
            for c in concurrencies:
                runs = grouped[op][c].get(label, [])
                if runs:
                    meds.append(statistics.median(runs) / 1000.0)
                    mins.append(min(runs) / 1000.0)
                    maxs.append(max(runs) / 1000.0)
                else:
                    meds.append(None)
                    mins.append(None)
                    maxs.append(None)
            ax.plot(concurrencies, meds, marker="o", label=label)
            xs = [c for c, m in zip(concurrencies, meds) if m is not None]
            lo = [v for v in mins if v is not None]
            hi = [v for v in maxs if v is not None]
            ax.fill_between(xs, lo, hi, alpha=0.15)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("concurrency")
        ax.set_ylabel("p99 latency (µs)")
        ax.set_title(f"{op} p99 (median of trials; band = min..max)")
        ax.legend()
        ax.grid(True, which="both", alpha=0.3)
    fig.suptitle("FUSE overhead vs tmpfs baseline")
    fig.tight_layout()
    fig.savefig(png_path, dpi=120)
    print(f"\n[chart written to {png_path}]")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <results.jsonl> [more.jsonl ...]", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1:])
