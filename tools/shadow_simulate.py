"""
Shadow simulation — full buy/evaluate/sell lifecycle for hypothetical
trades we couldn't take. Built 2026-05-26 in response to the PDT
lockout blocking 6 strategy_v1.2-qualifying entries that day.

Two modes:

  --plant-today   Seed today's qualifying-but-PDT-blocked contracts as
                   shadow positions in logs/shadow_positions.json.
                   Each subsequent run advances them through chain_surface
                   until SL hits or max_hold reached.

  --replay        For each tracked OCC, also run a historical replay:
                   find every prior day where the same contract met
                   strategy_v1.2 criteria, simulate the 5-day lifecycle,
                   show the distribution of outcomes. Gives a concrete
                   "what would similar setups have done" answer.

Once planted, the simulation runs idempotently each time it's invoked.
Closed positions are kept in the ledger for cumulative P&L tracking.

Usage:
    python -m tools.shadow_simulate --plant-today --replay
    python -m tools.shadow_simulate                # just advance existing
    python -m tools.shadow_simulate --replay       # historical-only
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

LOG_DIR = REPO_ROOT / "logs"
SNAP_DIR = REPO_ROOT / "snapshots"
DB_PATH = REPO_ROOT / "engine_state.db"
LEDGER_PATH = LOG_DIR / "shadow_positions.json"
REPORT_PATH = LOG_DIR / "shadow_ledger.md"

SL_MID_PCT = -0.12
MAX_HOLD_DAYS = 5
CAPITAL_PER_TRADE = 200.0


def _trading_days_after(d: str, n: int) -> str:
    dt = datetime.strptime(d, "%Y-%m-%d").date()
    added = 0
    while added < n:
        dt += timedelta(days=1)
        if dt.weekday() < 5:
            added += 1
    return dt.isoformat()


def _occ_key(symbol: str, opt_type: str, strike: float, expiry: str) -> str:
    return f"{symbol}|{(opt_type or '').lower()[:1]}|{float(strike):.2f}|{expiry}"


def _parse_snapshot_ts(filename: str) -> str | None:
    """Parse '2026-05-26_auto-1336.json' → '2026-05-26 13:36'."""
    import re
    m = re.match(r"(\d{4}-\d{2}-\d{2})_auto-(\d{2})(\d{2})\.json$", filename)
    if not m:
        return None
    d, hh, mm = m.groups()
    return f"{d} {hh}:{mm}"


def _snapshot_dt(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y-%m-%d %H:%M")


# Build a chronological index of every snapshot's contracts. Used to walk
# trades minute-by-minute through entry and exit.
_TICK_INDEX_CACHE: list[tuple[str, dict]] | None = None


def build_tick_index() -> list[tuple[str, dict]]:
    """Return [(timestamp_str, snapshot_dict), ...] sorted chronologically.
    snapshot_dict maps occ_key -> contract row (with bid/ask/mid/skew/etc.)"""
    global _TICK_INDEX_CACHE
    if _TICK_INDEX_CACHE is not None:
        return _TICK_INDEX_CACHE
    ticks: list[tuple[str, dict]] = []
    for f in sorted(SNAP_DIR.glob("*_auto-*.json")):
        ts = _parse_snapshot_ts(f.name)
        if not ts:
            continue
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        by_occ: dict[str, dict] = {}
        for t in (d.get("universe") or d.get("trades") or []):
            sym = t.get("symbol")
            opt_type = (t.get("type") or t.get("option_type") or "").lower()
            strike = t.get("strike")
            exp = t.get("expiry")
            if not all([sym, opt_type, strike, exp]):
                continue
            by_occ[_occ_key(sym, opt_type, strike, exp)] = t
        ticks.append((ts, by_occ))
    _TICK_INDEX_CACHE = ticks
    return ticks


# ── Find today's qualifying-but-PDT-blocked contracts ───────────────────────

def find_todays_qualifying(today_iso: str = None) -> list[dict]:
    """For today's snapshots in chronological order, find the FIRST tick
    where each unique contract qualifies under strategy_v1.2. That tick's
    timestamp becomes the entry time."""
    today_iso = today_iso or date.today().isoformat()
    seen: dict[tuple, dict] = {}
    for f in sorted(SNAP_DIR.glob(f"{today_iso}_*.json")):
        ts = _parse_snapshot_ts(f.name)
        if not ts:
            continue
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        for t in (d.get("universe") or d.get("trades") or []):
            opt_type = (t.get("type") or t.get("option_type") or "").lower()
            if opt_type != "call":
                continue
            if t.get("skew_signal") != "BULLISH":
                continue
            if t.get("vol_signal") != "BUY VOL":
                continue
            bid = float(t.get("bid") or 0)
            ask = float(t.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                continue
            mid = (bid + ask) / 2.0
            if mid <= 0:
                continue
            spread = (ask - bid) / mid
            if spread > 0.10:
                continue
            dte = int(t.get("dte") or 0)
            if not (14 <= dte <= 45):
                continue
            sym = t.get("symbol")
            strike = float(t.get("strike"))
            exp = t.get("expiry")
            key = (sym, strike, exp)
            if key in seen:
                continue   # keep the FIRST qualifying tick of the day
            seen[key] = {
                "symbol": sym, "strike": strike, "expiry": exp,
                "option_type": opt_type,
                "occ_key": _occ_key(sym, opt_type, strike, exp),
                "entry_date": today_iso,
                "entry_ts": ts,                      # YYYY-MM-DD HH:MM
                "entry_snapshot": f.name,
                "entry_ask": ask, "entry_bid": bid, "entry_mid": mid,
                "spread_pct_at_entry": spread,
                "score": t.get("score"),
                "stock_price_at_entry": float(t.get("stock_price") or 0),
                "dte_at_entry": dte,
                "qty": max(1, int(CAPITAL_PER_TRADE // (ask * 100))),
                "status": "open",
            }
    return list(seen.values())


# ── Walk-forward simulation ──────────────────────────────────────────────────

def _surface_lookup(occ_key: str) -> dict[str, dict]:
    """Return {snapshot_date: {bid, ask, mid, last}} for an OCC across all
    available dates. chain_surface stores option_type as full word
    ('call'/'put'); our occ_key abbreviates to 'c'/'p' so we match by
    first letter."""
    out: dict[str, dict] = {}
    if not DB_PATH.exists():
        return out
    sym, opt_abbr, strike, expiry = occ_key.split("|")
    with sqlite3.connect(DB_PATH) as c:
        c.row_factory = sqlite3.Row
        for r in c.execute(
            "SELECT snapshot_date, bid, ask, last_price FROM chain_surface "
            "WHERE symbol=? AND substr(option_type,1,1)=? "
            "  AND strike=? AND expiry=? "
            "ORDER BY snapshot_date",
            (sym, opt_abbr, float(strike), expiry),
        ):
            bid = float(r["bid"] or 0); ask = float(r["ask"] or 0)
            last = float(r["last_price"] or 0)
            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else (last or 0)
            if mid <= 0:
                continue
            out[r["snapshot_date"]] = {
                "bid": bid, "ask": ask, "mid": mid, "last": last,
            }
    return out


def _chain_surface_walk(occ_key: str, after_dt: datetime,
                         until_dt: datetime) -> list[tuple[str, dict]]:
    """Fallback walker: query chain_surface for daily bid/ask history of
    an OCC between two timestamps. Used when the snapshot tick index
    has no forward data for a contract (i.e., it dropped out of the
    picker's scored universe). Each chain_surface row becomes a
    pseudo-tick at 'YYYY-MM-DD 16:00' (end-of-day).

    2026-06-14 fix: shadow ledger was frozen 17 days because original
    advance_position_intraday only saw snapshot universe ticks. Many
    OCCs (PLUG/BBAI/SG) stop being scored after entry day so never
    appeared in subsequent snapshots — walker had no data to advance
    them. chain_surface tracks all listed contracts daily."""
    out: list[tuple[str, dict]] = []
    if not DB_PATH.exists():
        return out
    sym, opt_abbr, strike, expiry = occ_key.split("|")
    after_date = after_dt.date().isoformat()
    until_date = until_dt.date().isoformat()
    try:
        with sqlite3.connect(DB_PATH) as c:
            c.row_factory = sqlite3.Row
            for r in c.execute(
                "SELECT snapshot_date, bid, ask, last_price FROM chain_surface "
                "WHERE symbol=? AND substr(option_type,1,1)=? "
                "  AND strike=? AND expiry=? "
                "  AND snapshot_date > ? AND snapshot_date <= ? "
                "ORDER BY snapshot_date",
                (sym, opt_abbr, float(strike), expiry,
                 after_date, until_date),
            ):
                bid = float(r["bid"] or 0); ask = float(r["ask"] or 0)
                last = float(r["last_price"] or 0)
                mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else (last or 0)
                if mid <= 0:
                    continue
                ts = f"{r['snapshot_date']} 16:00"
                out.append((ts, {
                    "bid": bid, "ask": ask, "mid": mid, "last": last,
                    "_source": "chain_surface",
                }))
    except Exception:
        pass
    return out


def advance_position_intraday(pos: dict, ticks: list[tuple[str, dict]]) -> dict:
    """Walk this position forward through every snapshot tick (intraday
    granularity) starting AFTER entry_ts. Returns updated position with
    exact entry/exit timestamps + realized P&L.

    Each tick is a (timestamp_str, occ_dict) pair from build_tick_index().
    We evaluate strategy_v1.2's exit rules on each tick:
      - SL hit (mid_pnl <= -12%) → close at this tick's bid + timestamp
      - max_hold reached (5 trading days after entry) → close at last
        available bid + timestamp on day 5
      - When snapshot ticks run out before max_hold (contract dropped
        from picker universe), fall back to chain_surface daily bid"""
    if pos["status"] != "open":
        return pos
    entry_ts = pos.get("entry_ts") or f"{pos['entry_date']} 09:35"
    entry_dt = _snapshot_dt(entry_ts)
    entry_mid = pos["entry_mid"]
    entry_ask = pos["entry_ask"]
    qty = pos["qty"]
    occ = pos["occ_key"]

    # max_hold cutoff: 5 trading days after entry_date, at session close
    max_hold_date = _trading_days_after(pos["entry_date"], MAX_HOLD_DAYS)
    max_hold_cutoff = _snapshot_dt(f"{max_hold_date} 16:00")

    pos["walk"] = []
    last_tick_bid: float | None = None
    last_tick_ts: str | None = None
    last_seen_ts: str | None = None

    # Build the walk: snapshot ticks for the OCC + chain_surface fallback
    # for days where the contract didn't appear in any snapshot. This
    # was the 2026-06-14 fix that unfroze the ledger.
    snapshot_walk: list[tuple[str, dict]] = []
    snapshot_dates: set[str] = set()
    for ts, by_occ in ticks:
        tick_dt = _snapshot_dt(ts)
        if tick_dt <= entry_dt:
            continue
        if tick_dt > max_hold_cutoff:
            break
        c = by_occ.get(occ)
        if not c:
            continue
        snapshot_walk.append((ts, c))
        snapshot_dates.add(ts.split(" ")[0])

    chain_walk = _chain_surface_walk(occ, entry_dt, max_hold_cutoff)
    # Only add chain_surface days that aren't already covered by snapshots
    chain_walk = [(ts, c) for ts, c in chain_walk
                  if ts.split(" ")[0] not in snapshot_dates]

    combined = sorted(snapshot_walk + chain_walk, key=lambda x: x[0])

    for ts, c in combined:
        bid = float(c.get("bid") or 0)
        ask = float(c.get("ask") or 0)
        if bid <= 0 or ask <= 0:
            continue
        mid = (bid + ask) / 2.0
        if mid <= 0:
            continue
        last_seen_ts = ts
        last_tick_bid = bid
        last_tick_ts = ts
        mid_pnl = (mid / entry_mid) - 1
        bid_pnl_vs_paid = (bid / entry_ask) - 1
        pos["walk"].append({
            "ts": ts, "bid": bid, "ask": ask, "mid": mid,
            "mid_pnl": round(mid_pnl, 4),
            "bid_pnl_vs_paid": round(bid_pnl_vs_paid, 4),
            "source": c.get("_source", "snapshot"),
        })
        if mid_pnl <= SL_MID_PCT:
            held_hours = (_snapshot_dt(ts) - entry_dt).total_seconds() / 3600.0
            pos["status"] = "closed"
            pos["exit_ts"] = ts
            pos["exit_date"] = ts.split(" ")[0]
            pos["exit_bid"] = bid
            pos["hold_hours"] = round(held_hours, 1)
            pos["exit_reason"] = (f"SL {SL_MID_PCT*100:+.0f}% hit "
                                   f"(mid_pnl={mid_pnl*100:+.1f}%)")
            pos["pnl_pct"] = round(bid_pnl_vs_paid, 4)
            pos["realized_dollar"] = round(qty * (bid - entry_ask) * 100, 2)
            return pos
    # Did we reach the max_hold cutoff?
    now = datetime.now()
    if now >= max_hold_cutoff:
        # max_hold elapsed — close at last seen bid
        if last_tick_bid is not None:
            pos["status"] = "closed"
            pos["exit_ts"] = last_tick_ts
            pos["exit_date"] = last_tick_ts.split(" ")[0]
            pos["exit_bid"] = last_tick_bid
            held_hours = (_snapshot_dt(last_tick_ts) - entry_dt).total_seconds() / 3600.0
            pos["hold_hours"] = round(held_hours, 1)
            pos["exit_reason"] = f"max_hold ({MAX_HOLD_DAYS}d) elapsed"
            final_pnl = (last_tick_bid / entry_ask) - 1
            pos["pnl_pct"] = round(final_pnl, 4)
            pos["realized_dollar"] = round(qty * (last_tick_bid - entry_ask) * 100, 2)
            return pos
    # Still open
    pos["ticks_evaluated"] = len(pos["walk"])
    pos["last_seen_ts"] = last_seen_ts
    return pos


# Backwards-compatible alias (older callers use the daily name)
def advance_position(pos: dict, surface: dict[str, dict]) -> dict:
    """Daily-granularity advance, kept as a fallback for replays that
    need it. Most callers should use advance_position_intraday."""
    if pos["status"] != "open":
        return pos
    ticks = build_tick_index()
    return advance_position_intraday(pos, ticks)


def advance_all(ledger: list[dict]) -> list[dict]:
    """Re-walk every open position with the intraday tick index."""
    ticks = build_tick_index()
    for pos in ledger:
        if pos["status"] != "open":
            continue
        advance_position_intraday(pos, ticks)
    return ledger


# ── Historical replay (find past qualifying entries for these OCCs) ─────────

def historical_replays(occ_keys: list[str]) -> list[dict]:
    """For each OCC, find every HISTORICAL FIRST-QUALIFYING tick (across
    snapshot files, intraday granularity). Each new qualifying entry on
    a different trading day generates one replay — walking forward
    through subsequent ticks until SL or max_hold. Returns completed
    + still-open replays with exact entry/exit timestamps."""
    ticks = build_tick_index()
    today_iso = date.today().isoformat()
    out: list[dict] = []

    # Per-OCC: track which date-of-entry we've already simulated to avoid
    # re-entering the same OCC multiple times within a single day
    seen_date_by_occ: dict[str, set[str]] = {k: set() for k in occ_keys}

    for ts, by_occ in ticks:
        tdate = ts.split(" ")[0]
        if tdate >= today_iso:
            break   # only HISTORICAL replays; today is handled separately
        for key in occ_keys:
            if tdate in seen_date_by_occ[key]:
                continue
            t = by_occ.get(key)
            if t is None:
                continue
            opt_type = (t.get("type") or t.get("option_type") or "").lower()
            if opt_type != "call":
                continue
            if t.get("skew_signal") != "BULLISH":
                continue
            if t.get("vol_signal") != "BUY VOL":
                continue
            bid = float(t.get("bid") or 0); ask = float(t.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                continue
            mid = (bid + ask) / 2.0
            if mid <= 0:
                continue
            spread = (ask - bid) / mid
            if spread > 0.10:
                continue
            dte = int(t.get("dte") or 0)
            if not (14 <= dte <= 45):
                continue
            # First qualifying tick for this OCC on this date → entry
            pos = {
                "symbol": t["symbol"], "strike": float(t["strike"]),
                "expiry": t["expiry"], "option_type": opt_type,
                "occ_key": key, "entry_date": tdate, "entry_ts": ts,
                "entry_ask": ask, "entry_bid": bid, "entry_mid": mid,
                "spread_pct_at_entry": spread,
                "score": t.get("score"),
                "stock_price_at_entry": float(t.get("stock_price") or 0),
                "dte_at_entry": dte,
                "qty": max(1, int(CAPITAL_PER_TRADE // (ask * 100))),
                "status": "open",
            }
            seen_date_by_occ[key].add(tdate)
            advance_position_intraday(pos, ticks)
            out.append(pos)
    return out


# ── Ledger persistence ──────────────────────────────────────────────────────

def load_ledger() -> list[dict]:
    if not LEDGER_PATH.exists():
        return []
    try:
        return json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_ledger(ledger: list[dict]) -> None:
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEDGER_PATH.write_text(json.dumps(ledger, indent=2, default=str),
                            encoding="utf-8")


def trailing_history(occ_key: str, days: int = 10) -> list[dict]:
    """Pull the last N trading days of chain_surface entries for this OCC.
    Shows the contract's recent price trajectory leading up to today —
    a quick proxy for 'has this contract been moving favorably?'"""
    surface = _surface_lookup(occ_key)
    if not surface:
        return []
    sorted_dates = sorted(surface.keys())[-days:]
    return [
        {"date": d, "bid": surface[d]["bid"], "ask": surface[d]["ask"],
         "mid": surface[d]["mid"]}
        for d in sorted_dates
    ]


def render_report(ledger: list[dict], replays: list[dict]) -> str:
    lines = []
    lines.append(f"# Shadow simulation — `strategy_v1.2`")
    lines.append("")
    lines.append(f"_Generated {datetime.now().isoformat(timespec='seconds')}_")
    lines.append("")

    if ledger:
        lines.append(f"## Current shadow ledger ({len(ledger)} positions)")
        lines.append("")
        open_count = sum(1 for p in ledger if p["status"] == "open")
        closed_count = sum(1 for p in ledger if p["status"] == "closed")
        realized = sum(p.get("realized_dollar", 0) for p in ledger
                       if p["status"] == "closed")
        lines.append(f"- Open: {open_count}   |   Closed: {closed_count}")
        lines.append(f"- Realized so far (closed shadows): **${realized:+,.2f}**")
        lines.append("")
        lines.append("| OCC | qty | entry_ts | ask paid | exit_ts | hold | bid_pnl | $real | reason |")
        lines.append("|---|---:|---|---:|---|---:|---:|---:|---|")
        for p in ledger:
            occ = f"{p['symbol']} {p['option_type'][0].upper()} ${p['strike']:.2f} {p['expiry'][-5:]}"
            entry_ts = p.get("entry_ts", p["entry_date"])
            if p["status"] == "open":
                n_ticks = len(p.get("walk", []))
                last_seen = p.get("last_seen_ts", "—")
                lines.append(f"| {occ} | {p['qty']} | {entry_ts} | "
                             f"${p['entry_ask']:.2f} | open | "
                             f"{n_ticks} ticks | — | — | "
                             f"in flight (last tick {last_seen}) |")
            else:
                exit_ts = p.get("exit_ts", p.get("exit_date", "?"))
                hold = f"{p.get('hold_hours', 0):.1f}h"
                lines.append(f"| {occ} | {p['qty']} | {entry_ts} | "
                             f"${p['entry_ask']:.2f} | {exit_ts} | "
                             f"{hold} | {p['pnl_pct']*100:+.1f}% | "
                             f"${p['realized_dollar']:+,.2f} | "
                             f"{p['exit_reason']} |")
        lines.append("")

    # Trailing price history for each open position
    if ledger:
        open_positions = [p for p in ledger if p["status"] == "open"]
        if open_positions:
            lines.append(f"## Trailing price path (10 trading days) for open shadows")
            lines.append("")
            for p in open_positions:
                history = trailing_history(p["occ_key"], days=10)
                if not history:
                    continue
                occ = f"{p['symbol']} {p['option_type'][0].upper()} ${p['strike']:.2f} {p['expiry'][-5:]}"
                lines.append(f"### {occ}  (entered today at ask=${p['entry_ask']:.2f})")
                lines.append("")
                lines.append("| date | bid | ask | mid |")
                lines.append("|---|---:|---:|---:|")
                for h in history:
                    lines.append(f"| {h['date']} | ${h['bid']:.2f} | "
                                 f"${h['ask']:.2f} | ${h['mid']:.2f} |")
                # Quick directional read
                first = history[0]["mid"]; last = history[-1]["mid"]
                if first > 0:
                    move = (last / first - 1) * 100
                    direction = ("rising" if move > 5
                                 else "falling" if move < -5
                                 else "flat")
                    lines.append(f"")
                    lines.append(f"_10-day path: mid ${first:.2f} → ${last:.2f}  "
                                 f"({move:+.1f}%, **{direction}**)_")
                lines.append("")

    if replays:
        closed = [r for r in replays if r["status"] == "closed"]
        still_open = [r for r in replays if r["status"] == "open"]
        lines.append(f"## Historical replays ({len(replays)} prior qualifying entries: "
                     f"{len(closed)} closed, {len(still_open)} still in flight)")
        lines.append("")
        if closed:
            wins = [r for r in closed if r["pnl_pct"] > 0]
            total = sum(r["realized_dollar"] for r in closed)
            avg_pnl_pct = sum(r["pnl_pct"] for r in closed) / len(closed)
            lines.append(f"- Closed: n = {len(closed)}, win rate = "
                         f"{len(wins)/len(closed)*100:.0f}%, "
                         f"avg = {avg_pnl_pct*100:+.1f}%, "
                         f"total $: **${total:+,.2f}**")
            lines.append("")
        lines.append("| ticker | strike | exp | entry_ts | exit_ts | hold | bid_pnl | $real | reason |")
        lines.append("|---|---:|---|---|---|---:|---:|---:|---|")
        for r in replays:
            occ = f"{r['symbol']}"
            entry_ts = r.get("entry_ts", r["entry_date"])
            if r["status"] == "closed":
                exit_ts = r.get("exit_ts", r.get("exit_date", "?"))
                hold = f"{r.get('hold_hours', 0):.1f}h"
                lines.append(f"| {occ} | ${r['strike']:.2f} | {r['expiry'][-5:]} | "
                             f"{entry_ts} | {exit_ts} | {hold} | "
                             f"{r['pnl_pct']*100:+.1f}% | "
                             f"${r['realized_dollar']:+,.2f} | "
                             f"{r['exit_reason']} |")
            else:
                last_seen = r.get("last_seen_ts", "—")
                lines.append(f"| {occ} | ${r['strike']:.2f} | {r['expiry'][-5:]} | "
                             f"{entry_ts} | open | "
                             f"{r.get('ticks_evaluated', 0)} ticks | "
                             f"— | — | OPEN (last tick {last_seen}) |")
        lines.append("")

    return "\n".join(lines)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plant-today", action="store_true",
                    help="seed today's qualifying-but-PDT-blocked contracts")
    ap.add_argument("--replay", action="store_true",
                    help="also run historical replays for tracked OCCs")
    ap.add_argument("--reset", action="store_true",
                    help="wipe the shadow ledger and start fresh")
    args = ap.parse_args()

    if args.reset:
        if LEDGER_PATH.exists():
            LEDGER_PATH.unlink()
        print(f"reset {LEDGER_PATH.name}")

    ledger = load_ledger()
    print(f"Loaded ledger with {len(ledger)} positions")

    if args.plant_today:
        existing_keys = {(p["symbol"], p["strike"], p["expiry"],
                           p["entry_date"]) for p in ledger}
        new = find_todays_qualifying()
        added = 0
        for n in new:
            key = (n["symbol"], n["strike"], n["expiry"], n["entry_date"])
            if key in existing_keys:
                continue
            ledger.append(n)
            added += 1
        print(f"Planted {added} new shadow position(s) for {date.today()}")

    # Advance every open position
    print("Advancing open positions through chain_surface data...")
    ledger = advance_all(ledger)
    save_ledger(ledger)

    replays: list[dict] = []
    if args.replay:
        occ_keys = list({p["occ_key"] for p in ledger})
        if occ_keys:
            print(f"Running historical replays for {len(occ_keys)} OCCs...")
            replays = historical_replays(occ_keys)
            print(f"  found {len(replays)} historical qualifying entries")

    md = render_report(ledger, replays)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(md, encoding="utf-8")
    print(f"Wrote {REPORT_PATH}")
    print()
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
