#!/usr/bin/env python3
import json
import sys
from collections import defaultdict
from pathlib import Path


def main(jsonl_path: str) -> None:
    rows = []
    with open(jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    grouped = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        grouped[r["op"]][r["concurrency"]][r["label"]] = r

    for op in sorted(grouped):
        print(f"\n=== {op} ===")
        print(f"{'concurrency':>12} {'tmpfs_p99(us)':>15} {'fuse_p99(us)':>15} {'overhead(us)':>15} {'overhead_x':>12}")
        for c in sorted(grouped[op]):
            tmpfs = grouped[op][c].get("tmpfs", {}).get("percentiles_ns", {}).get("p99")
            fuse = grouped[op][c].get("fuse", {}).get("percentiles_ns", {}).get("p99")
            if tmpfs is None or fuse is None:
                continue
            tmpfs_us = tmpfs / 1000.0
            fuse_us = fuse / 1000.0
            overhead_us = fuse_us - tmpfs_us
            ratio = fuse / tmpfs if tmpfs else float("inf")
            print(f"{c:>12} {tmpfs_us:>15.2f} {fuse_us:>15.2f} {overhead_us:>15.2f} {ratio:>11.2f}x")

    try_plot(grouped, Path(jsonl_path).with_suffix(".png"))


def try_plot(grouped, png_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print(f"\n[skipping chart: matplotlib not installed]")
        return

    fig, axes = plt.subplots(1, len(grouped), figsize=(6 * len(grouped), 5), squeeze=False)
    for ax_idx, op in enumerate(sorted(grouped)):
        ax = axes[0][ax_idx]
        concurrencies = sorted(grouped[op])
        for label in ("tmpfs", "fuse"):
            ys = []
            for c in concurrencies:
                r = grouped[op][c].get(label)
                ys.append((r["percentiles_ns"]["p99"] / 1000.0) if r else None)
            ax.plot(concurrencies, ys, marker="o", label=label)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("concurrency")
        ax.set_ylabel("p99 latency (µs)")
        ax.set_title(f"{op} p99")
        ax.legend()
        ax.grid(True, which="both", alpha=0.3)
    fig.suptitle("FUSE overhead vs tmpfs baseline")
    fig.tight_layout()
    fig.savefig(png_path, dpi=120)
    print(f"\n[chart written to {png_path}]")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <results.jsonl>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
