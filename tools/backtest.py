#!/usr/bin/env python3
"""
Backtest Tool — aggregates closed-out snapshot trades into an edge report.

Usage:
    python tools/backtest.py                    # default snapshots/ dir
    python tools/backtest.py --dir snapshots    # explicit dir
    python tools/backtest.py --json             # emit raw JSON

Reads every `snapshots/YYYY-MM-DD.json` that has been closed out by
`tools/compare.py`, computes per-feature hit rates / ROI, and prints
a plain-text report to stdout.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.performance import analyze_performance


DEFAULT_SNAPSHOT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "snapshots",
)

SEP = "=" * 78
SUB = "-" * 78


def _fmt_pct(v):
    return f"{v:>6.1f}%" if isinstance(v, (int, float)) else "   n/a"


def _fmt_money(v):
    if not isinstance(v, (int, float)):
        return "    n/a"
    return f"${v:>8.2f}"


def _print_stats_row(label: str, s: dict) -> None:
    print(
        f"  {label:<22} "
        f"n={s['n']:>3}  "
        f"wins={s['wins']:>3}  "
        f"losses={s['losses']:>3}  "
        f"hit={_fmt_pct(s.get('hit_rate'))}  "
        f"ROI={_fmt_pct(s.get('roi_pct'))}  "
        f"avgW={_fmt_money(s.get('avg_win'))}  "
        f"avgL={_fmt_money(s.get('avg_loss'))}"
    )


def _print_bucket(title: str, bucket: dict) -> None:
    if not bucket:
        return
    print()
    print(f"{title}")
    print(SUB)
    for key, stats in bucket.items():
        _print_stats_row(str(key), stats)


def _print_trade_list(title: str, trades: list) -> None:
    if not trades:
        return
    print()
    print(f"{title}")
    print(SUB)
    for t in trades:
        pnl = t.get("pnl")
        pnl_str = f"${pnl:>+8.2f}" if isinstance(pnl, (int, float)) else "  n/a"
        print(
            f"  {t.get('date',''):<12} "
            f"{(t.get('symbol') or ''):<6} "
            f"{(t.get('type') or ''):<5} "
            f"${t.get('strike') or 0:>7.2f}  "
            f"exp {t.get('expiry') or '':<10}  "
            f"score {t.get('score') or 0:>5.1f}  "
            f"entry ${t.get('entry') or 0:>6.2f}  "
            f"close ${t.get('close') or 0:>6.2f}  "
            f"pnl {pnl_str}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description="Backtest closed-out snapshots.")
    ap.add_argument("--dir", default=DEFAULT_SNAPSHOT_DIR,
                    help="snapshots directory (default: %(default)s)")
    ap.add_argument("--json", action="store_true",
                    help="emit raw JSON instead of pretty report")
    args = ap.parse_args()

    if not os.path.isdir(args.dir):
        print(f"Error: snapshots directory not found: {args.dir}", file=sys.stderr)
        return 2

    report = analyze_performance(args.dir)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return 0

    print(SEP)
    print(f"  BACKTEST REPORT  —  {args.dir}")
    print(SEP)
    print(f"  Snapshots parsed:   {report.get('n_snapshots_parsed', 0)}")
    print(f"  Closed trades:      {report.get('n_closed_trades', 0)}")

    warnings = report.get("warnings") or []
    if warnings:
        print()
        print("Warnings:")
        for w in warnings:
            print(f"  ! {w}")

    n_trades = report.get("n_closed_trades", 0)
    if n_trades == 0:
        print()
        print("No closed trades to analyze. Run tools/compare.py after market close first.")
        return 0

    print()
    print("OVERALL")
    print(SUB)
    _print_stats_row("all trades", report["overall"])

    _print_bucket("BY VOL SIGNAL",        report.get("by_vol_signal"))
    _print_bucket("BY SCORE BUCKET",      report.get("by_score_bucket"))
    _print_bucket("BY OPTION TYPE",       report.get("by_option_type"))
    _print_bucket("BY DTE BUCKET",        report.get("by_dte_bucket"))
    _print_bucket("BY IV RANK LABEL",     report.get("by_iv_rank_label"))
    _print_bucket("BY GEX SIGNAL",        report.get("by_gex_signal"))
    _print_bucket("BY SKEW SIGNAL",       report.get("by_skew_signal"))
    _print_bucket("BY SECTOR",            report.get("by_sector"))

    _print_trade_list("TOP WINS",   report.get("top_wins"))
    _print_trade_list("TOP LOSSES", report.get("top_losses"))

    print()
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
