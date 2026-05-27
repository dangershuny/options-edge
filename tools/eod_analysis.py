"""
End-of-day analysis + proposals — runs at 16:45 ET, after EODSession,
SurfaceSnapshot, and HealthSummary have all written their data.

Produces:
  logs/eod-analysis-{date}.md         — human-readable report
  logs/eod-proposals-{date}.json      — structured proposals (LOW/MED/HIGH risk)
  Telegram top-line summary           — top 3 proposals + headline P&L

The user reviews the report when they get home. Each proposal is tagged with
a risk tier so they can quickly approve LOW changes (log/text/threshold tweaks),
deliberate on MED (config or filter changes), and pause on HIGH (code structure
or trade-mechanism changes).

Until the Tier-3 Claude-in-loop agent is built, proposals are text-only.
The user manually applies approved ones in the next session.

Usage:
    python -m tools.eod_analysis
    python -m tools.eod_analysis --date 2026-04-30   # analyze a past day
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import statistics
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config_loader  # noqa: F401

LOG_DIR = REPO_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def _isnum(x: Any) -> bool:
    try:
        v = float(x)
        return v == v and v not in (float("inf"), float("-inf"))
    except Exception:
        return False


# ── Today's trade results ───────────────────────────────────────────────────

def collect_today_results(today: date) -> dict:
    iso = today.isoformat()
    out = {
        "today": iso,
        "entries": [],
        "exits_filled": [],
        "still_open": [],
        "queued_exits": [],
        "realized_pl": 0.0,
        "unrealized_pl": 0.0,
        "equity": None,
        "cash": None,
    }

    # Entries from paper_trades.jsonl
    pt = LOG_DIR / "paper_trades.jsonl"
    if pt.exists():
        try:
            for line in pt.read_text(encoding="utf-8").splitlines():
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if not rec.get("timestamp", "").startswith(iso):
                    continue
                if rec.get("status") != "submitted":
                    continue
                out["entries"].append({
                    "ts": rec["timestamp"][11:19],
                    "occ": rec.get("occ"),
                    "score": rec.get("score"),
                    "signal": rec.get("signal"),
                    "qty": rec.get("qty"),
                    "cost": rec.get("total_cost"),
                    "tag": rec.get("tag", ""),
                })
        except Exception:
            pass

    # Exits filled today + still-open from engine_state.db
    db = REPO_ROOT / "engine_state.db"
    if db.exists():
        try:
            with sqlite3.connect(db) as c:
                for r in c.execute(
                    "SELECT occ_symbol,entry_date,entry_price,exit_price,"
                    "realized_pl,exit_reason,score,vol_signal,qty "
                    "FROM positions WHERE status='closed' AND exit_date=?",
                    (iso,),
                ):
                    occ, ed, ep, xp, pl, rs, sc, sg, qty = r
                    out["exits_filled"].append({
                        "occ": occ, "entry_date": ed, "entry": ep, "exit": xp,
                        "pl": pl, "reason": rs, "score": sc, "qty": qty,
                    })
                    if _isnum(pl):
                        out["realized_pl"] += float(pl)
                for r in c.execute(
                    "SELECT occ_symbol,entry_date,entry_price,score,qty,status "
                    "FROM positions WHERE status IN ('open','closing')",
                ):
                    occ, ed, ep, sc, qty, st = r
                    out["still_open"].append({
                        "occ": occ, "entry_date": ed, "entry": ep,
                        "score": sc, "qty": qty, "status": st,
                    })
        except Exception:
            pass

    # Queued exits awaiting tomorrow's flush
    try:
        from engine.state import init_db, list_queued_exits
        init_db()
        for q in list_queued_exits():
            out["queued_exits"].append({
                "occ": q["occ_symbol"], "entry_date": q["entry_date"],
                "reason": q.get("exit_reason"),
            })
    except Exception:
        pass

    # Live broker snapshot (most recent unrealized)
    try:
        from broker import alpaca
        positions = alpaca.get_positions()
        out["unrealized_pl"] = sum(p.unrealized_pl for p in positions)
        acct = alpaca.get_account()
        out["equity"] = acct.equity
        out["cash"] = acct.cash
    except Exception:
        pass

    return out


def split_by_pipeline(entries: list[dict], exits: list[dict]) -> dict:
    """Bucket trades by tag prefix — divergence_picker uses 'divergence-{date}'
    while paper_trade uses tier tags like 'sim500', 'sim1000', etc."""
    div_entries = [e for e in entries if (e.get("tag") or "").startswith("divergence")]
    scorer_entries = [e for e in entries if not (e.get("tag") or "").startswith("divergence")]

    # Map exits back to their entry's pipeline by OCC
    div_occs = {e["occ"] for e in div_entries}
    div_exits = [x for x in exits if x.get("occ") in div_occs]
    scorer_exits = [x for x in exits if x.get("occ") not in div_occs]

    return {
        "divergence": {
            "entries": len(div_entries), "exits": len(div_exits),
            "realized_pl": sum(float(x["pl"]) for x in div_exits if _isnum(x.get("pl"))),
        },
        "scorer": {
            "entries": len(scorer_entries), "exits": len(scorer_exits),
            "realized_pl": sum(float(x["pl"]) for x in scorer_exits if _isnum(x.get("pl"))),
        },
    }


# ── Filter effectiveness ────────────────────────────────────────────────────

def filter_effectiveness(today: date) -> dict:
    """Read morning_auto_run log + intraday logs for filter-skip events."""
    iso = today.isoformat()
    skips: list[dict] = []
    for log_name in [f"morning_auto_run_{iso}.log",
                     f"morning_auto_run_{iso}_intraday-1100.log",
                     f"morning_auto_run_{iso}_intraday-1230.log",
                     f"morning_auto_run_{iso}_intraday-1400.log"]:
        p = LOG_DIR / log_name
        if not p.exists():
            continue
        try:
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                m = re.search(r"filter-skip\s+(\S+):\s+(.+)", line)
                if m:
                    skips.append({"sym": m.group(1), "reason": m.group(2).strip()})
        except Exception:
            continue

    # Categorize
    by_reason = {"underlying_price": 0, "trend_call": 0, "trend_put": 0, "other": 0}
    for s in skips:
        r = s["reason"]
        if "min_underlying_price" in r:
            by_reason["underlying_price"] += 1
        elif "CALL into downtrend" in r:
            by_reason["trend_call"] += 1
        elif "PUT into uptrend" in r:
            by_reason["trend_put"] += 1
        else:
            by_reason["other"] += 1

    # Per-symbol counts (so we can see if same name is being repeatedly blocked)
    sym_counts: dict[str, int] = {}
    for s in skips:
        sym_counts[s["sym"]] = sym_counts.get(s["sym"], 0) + 1

    return {
        "total_skips": len(skips),
        "by_reason": by_reason,
        "by_symbol": dict(sorted(sym_counts.items(), key=lambda x: -x[1])[:10]),
    }


# ── Cumulative backtest refresh ─────────────────────────────────────────────

def refresh_strategy_ab() -> dict:
    """Re-run strategy_backtest with all accumulated data and capture
    A/B comparison vs the current production baseline. Surfaces any
    variant that beats production by a significance threshold so the
    EOD proposal pass can flag it for human approval.

    Significance threshold (must satisfy BOTH):
      - n_trades >= 12 (enough samples)
      - ending_equity delta >= $200 (material improvement)

    Returns:
      {
        baseline: {name, n, win_rate, avg_return, ending_equity, ...},
        challengers: [ {...}, ... ],
        candidates_for_proposal: [ {name, delta, ...}, ... ],
      }
    """
    out = {"baseline": {}, "challengers": [], "candidates_for_proposal": []}
    try:
        from tools.strategy_backtest import (
            load_snapshots, load_chain_surface, run_strategy, STRATEGIES,
        )
    except Exception as e:
        out["error"] = f"import failed: {e}"
        return out
    try:
        rows = load_snapshots()
        surface = load_chain_surface()
    except Exception as e:
        out["error"] = f"data load failed: {e}"
        return out

    # 2026-05-26: baseline updated to T12 (v1.2 = + DTE 14-45 window).
    # If we ship v1.3+ via another gate change, update this name to match.
    baseline_name = "T12_bullskew_buyvol_tight10_dte_window"
    baseline_tuple = next((s for s in STRATEGIES if s[0] == baseline_name), None)
    if baseline_tuple is None:
        out["error"] = f"baseline {baseline_name} not in STRATEGIES"
        return out

    try:
        baseline = run_strategy(*baseline_tuple, rows=rows, surface=surface)
    except Exception as e:
        out["error"] = f"baseline run failed: {e}"
        return out
    out["baseline"] = {
        "name": baseline["name"], "n": baseline["n"],
        "win_rate": baseline["win_rate"], "avg_return": baseline["avg_return"],
        "ending_equity": baseline["ending_equity"],
        "max_drawdown": baseline["max_drawdown_pct"],
        "sharpe": baseline["sharpe"],
    }

    # Run T-series challengers (the candidate tweaks)
    for name, sel, ext in STRATEGIES:
        if not name.startswith("T"):
            continue
        try:
            r = run_strategy(name, sel, ext, rows, surface)
        except Exception:
            continue
        c = {
            "name": r["name"], "n": r["n"],
            "win_rate": r["win_rate"], "avg_return": r["avg_return"],
            "ending_equity": r["ending_equity"],
            "max_drawdown": r["max_drawdown_pct"],
            "sharpe": r["sharpe"],
            "delta_vs_baseline": r["ending_equity"] - baseline["ending_equity"],
        }
        out["challengers"].append(c)
        # Significance gate
        if (r["n"] >= 12 and
            r["ending_equity"] - baseline["ending_equity"] >= 200):
            out["candidates_for_proposal"].append(c)

    out["challengers"].sort(key=lambda c: -c["delta_vs_baseline"])
    out["candidates_for_proposal"].sort(key=lambda c: -c["delta_vs_baseline"])
    return out


def refresh_correlation_findings(horizon: int = 1) -> dict:
    """Run correlation_miner and capture top combinations beating baseline
    win rate by a material margin. EOD pass uses this to flag emergent
    patterns before they show up in our strategies."""
    out = {"horizon": horizon, "top_combos": []}
    try:
        from tools.correlation_miner import (
            build_trade_pairs, _wr, _avg,
            pairwise_numeric_x_categorical, univariate_categorical,
            univariate_numeric,
        )
        from tools.strategy_backtest import load_snapshots, load_chain_surface
    except Exception as e:
        out["error"] = f"import failed: {e}"
        return out
    try:
        rows = load_snapshots()
        surface = load_chain_surface()
        pairs = build_trade_pairs(rows, surface, horizon=horizon)
    except Exception as e:
        out["error"] = f"data load failed: {e}"
        return out
    if len(pairs) < 30:
        out["note"] = f"only {len(pairs)} pairs — too few for confidence"
        return out

    baseline_wr = _wr([p["pnl"] for p in pairs])
    out["baseline_wr"] = baseline_wr
    out["n_pairs"] = len(pairs)

    # Combine all finding kinds and pick top lifts with adequate n
    all_findings = []
    all_findings += univariate_numeric(pairs, baseline_wr)
    all_findings += univariate_categorical(pairs, baseline_wr)
    all_findings += pairwise_numeric_x_categorical(pairs, baseline_wr)
    # Require n >= 15 to take the lift seriously
    sig = [f for f in all_findings if f["n"] >= 15 and abs(f.get("lift", 0)) >= 0.10]
    sig.sort(key=lambda f: -abs(f.get("lift", 0)))
    out["top_combos"] = sig[:10]
    return out


def refresh_shadow_simulate() -> dict:
    """Advance the shadow ledger every EOD: plant any new strategy_v1.2
    qualifying contracts from today's snapshots, then walk every open
    position one more tick forward using today's intraday data.

    The shadow ledger tracks contracts the picker WOULD HAVE entered
    if PDT/other constraints hadn't blocked them. Provides a stream of
    'what could have been' realized P&L for the production rule.

    Wired into EOD on 2026-05-27 — runs nightly without operator
    intervention. Same pattern: closed positions left alone, open
    positions advanced as new chain_surface/snapshot data lands."""
    out = {"planted_today": 0, "open_after": 0, "closed_after": 0,
           "realized_so_far": 0.0}
    try:
        from tools.shadow_simulate import (
            load_ledger, save_ledger, advance_all, find_todays_qualifying,
        )
    except Exception as e:
        out["error"] = f"import failed: {e}"
        return out
    try:
        ledger = load_ledger()
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
        out["planted_today"] = added
        ledger = advance_all(ledger)
        save_ledger(ledger)
        out["open_after"] = sum(1 for p in ledger if p["status"] == "open")
        out["closed_after"] = sum(1 for p in ledger if p["status"] == "closed")
        out["realized_so_far"] = sum(p.get("realized_dollar", 0)
                                       for p in ledger
                                       if p["status"] == "closed")
        # Today's newly-closed (if any)
        today_iso = date.today().isoformat()
        closed_today = [p for p in ledger
                          if p["status"] == "closed"
                          and p.get("exit_date", "")[:10] == today_iso]
        out["closed_today"] = len(closed_today)
        out["realized_today_dollar"] = sum(p.get("realized_dollar", 0)
                                              for p in closed_today)
    except Exception as e:
        out["error"] = f"shadow_simulate run failed: {e}"
    return out


def refresh_backtests() -> dict:
    """Re-run the two backtests against current snapshot/sentinel data, capture
    headline metrics so we can see if they're trending."""
    out = {"scorer": {}, "sentinel_events": {}}

    try:
        py = sys.executable
        r = subprocess.run(
            [py, "-m", "tools.scorer_backtest", "--hold-days", "5"],
            capture_output=True, text=True, timeout=180, cwd=str(REPO_ROOT),
        )
        # Find the most recent backtest-*.json
        bt_files = sorted(LOG_DIR.glob("backtest-*.json"))
        if bt_files:
            data = json.loads(bt_files[-1].read_text(encoding="utf-8"))
            out["scorer"] = {
                "n_trades": data.get("n_trades"),
                "win_rate": data.get("win_rate"),
                "mean_dir_return_pct": data.get("mean_dir_return_pct"),
                "spearman_score_vs_return": data.get("spearman_score_vs_return"),
            }
    except Exception as e:
        out["scorer"]["error"] = str(e)

    try:
        py = sys.executable
        subprocess.run(
            [py, "-m", "tools.sentinel_event_backtest",
             "--since-days", "90", "--hold-days", "5"],
            capture_output=True, text=True, timeout=300, cwd=str(REPO_ROOT),
        )
        sb_files = sorted(LOG_DIR.glob("sentinel-event-backtest-*.json"))
        if sb_files:
            data = json.loads(sb_files[-1].read_text(encoding="utf-8"))
            divs = data.get("divergence_events", [])
            news = data.get("news_events", [])
            soc = data.get("social_events", [])
            def _stats(items):
                if not items:
                    return {"n": 0}
                wins = sum(1 for x in items if x.get("won"))
                rets = [x.get("directional_return_pct", 0) for x in items]
                return {
                    "n": len(items),
                    "win_rate": wins / len(items),
                    "mean": statistics.fmean(rets) if rets else 0,
                }
            out["sentinel_events"] = {
                "divergence": _stats(divs),
                "news": _stats(news),
                "social": _stats(soc),
            }
    except Exception as e:
        out["sentinel_events"]["error"] = str(e)

    return out


# ── Health monitor activity summary ─────────────────────────────────────────

def health_activity(today: date) -> dict:
    iso = today.isoformat()
    rem_log = LOG_DIR / f"remediations-{iso}.jsonl"
    n_remediations = 0
    by_action: dict[str, int] = {}
    if rem_log.exists():
        for line in rem_log.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
                action = rec.get("action", "?")
                by_action[action] = by_action.get(action, 0) + 1
                n_remediations += 1
            except Exception:
                continue
    # Open proposals (from anomaly_classifier)
    pp = LOG_DIR / "proposals-current.json"
    open_props = 0
    if pp.exists():
        try:
            open_props = len(json.loads(pp.read_text(encoding="utf-8")))
        except Exception:
            pass
    return {"remediations": n_remediations, "by_action": by_action,
            "open_proposals": open_props}


# ── Proposal generator ──────────────────────────────────────────────────────

def generate_proposals(report: dict) -> list[dict]:
    """Synthesize observed-vs-expected gaps into actionable proposals.
    Each proposal is risk-tiered:
      LOW  — tweak a constant in risk/config.py, change a threshold
      MED  — change a code path (e.g., adjust filter logic, add a guard)
      HIGH — change a strategy or trade mechanism
    """
    proposals: list[dict] = []
    today = report["today"]

    # 1. Daily P&L vs cap — has the daily-loss cap been approached?
    pl = report.get("realized_pl", 0) + report.get("unrealized_pl", 0)
    # Don't propose halt on small days — only flag if approaching the actual cap
    if pl < -300:
        proposals.append({
            "id": f"daily_loss_warn_{today}",
            "risk": "MED",
            "title": "Daily loss approaching cap",
            "rationale": f"Combined realized + unrealized = ${pl:+.0f}",
            "suggested_action": "Consider /halt for tomorrow morning until the "
                                "loss source is identified, OR tighten SL further.",
        })

    # 2. Filter effectiveness — was the universe filter doing real work?
    fe = report.get("filter_effectiveness", {})
    skips = fe.get("total_skips", 0)
    if skips > 0:
        top = list(fe.get("by_symbol", {}).items())[:3]
        sym_summary = ", ".join(f"{s}({n})" for s, n in top)
        proposals.append({
            "id": f"filter_skips_summary_{today}",
            "risk": "LOW",
            "title": f"Filters blocked {skips} trades today",
            "rationale": f"Top-blocked: {sym_summary}",
            "suggested_action": "If any of these names recently outperformed "
                                "after a >5d delay, consider extending the "
                                "trend lookback. Otherwise no change.",
        })

    # 3. Pipeline P&L comparison
    pipe = report.get("pipelines", {})
    div_pl = pipe.get("divergence", {}).get("realized_pl", 0)
    sc_pl = pipe.get("scorer", {}).get("realized_pl", 0)
    div_n = pipe.get("divergence", {}).get("entries", 0)
    sc_n = pipe.get("scorer", {}).get("entries", 0)
    if div_n + sc_n > 0:
        comparison = (
            f"divergence: {div_n} entries, ${div_pl:+.0f} realized | "
            f"scorer: {sc_n} entries, ${sc_pl:+.0f} realized"
        )
        proposals.append({
            "id": f"pipeline_pl_{today}",
            "risk": "LOW",
            "title": "Per-pipeline P&L breakdown",
            "rationale": comparison,
            "suggested_action": (
                "If divergence pipeline outperforms scorer by >2x cumulatively "
                "over 5 trading days, propose disabling the scorer-driven "
                "MorningAutoRun and IntradayScan tasks (HIGH-risk change)."
                if div_pl > 0 and div_pl > 2 * sc_pl
                else "Wait for more data."
            ),
        })

    # 4. Backtest trends
    bt = report.get("backtests", {})
    sc = bt.get("scorer", {})
    if sc.get("spearman_score_vs_return") is not None:
        rho = sc["spearman_score_vs_return"]
        if rho < -0.2:
            proposals.append({
                "id": f"scorer_rho_negative_{today}",
                "risk": "HIGH",
                "title": f"Scorer correlation still negative (rho={rho:+.2f})",
                "rationale": f"n={sc.get('n_trades')} trades. "
                             f"Score is anti-correlated with outcome.",
                "suggested_action": (
                    "Disable scorer-driven entries (set --min-score 200 in "
                    "morning_auto_run.bat to effectively pause). Continue with "
                    "divergence-only path until scorer is rebuilt."
                ),
            })
        elif rho > 0.2:
            proposals.append({
                "id": f"scorer_rho_positive_{today}",
                "risk": "MED",
                "title": f"Scorer correlation turned POSITIVE (rho={rho:+.2f})",
                "rationale": f"n={sc.get('n_trades')} trades. Was negative; "
                             f"now positive. Could be regime change or larger sample.",
                "suggested_action": "Consider raising min_score_to_trade from "
                                    "65 toward 75 to filter out weak signal end.",
            })
    se = bt.get("sentinel_events", {})
    div_stats = se.get("divergence", {})
    if div_stats.get("n", 0) >= 10:
        wr = div_stats.get("win_rate", 0)
        if wr >= 0.7:
            proposals.append({
                "id": f"divergence_win_rate_{today}",
                "risk": "MED",
                "title": f"Divergence win rate at {wr:.0%} (n={div_stats['n']})",
                "rationale": f"Sample now sufficient (n>=10). Mean dir return "
                             f"{div_stats.get('mean', 0):+.2f}%.",
                "suggested_action": "Consider raising divergence_picker --qty "
                                    "from 1 to 2, or increasing --max-picks.",
            })
        elif wr < 0.5:
            proposals.append({
                "id": f"divergence_win_rate_drop_{today}",
                "risk": "HIGH",
                "title": f"Divergence win rate dropped to {wr:.0%}",
                "rationale": f"n={div_stats['n']} — early 100% rate may have "
                             f"been small-sample bias.",
                "suggested_action": "Disable OptionsEdge-DivergencePicker until "
                                    "the regime / signal is re-examined.",
            })

    # 5. Health monitor noise
    hm = report.get("health", {})
    if hm.get("by_action", {}).get("rerun_failed_tasks", 0) >= 50:
        proposals.append({
            "id": f"override_server_noise_{today}",
            "risk": "LOW",
            "title": "OptionsEdge-OverrideServer keeps re-failing",
            "rationale": f"rerun_failed_tasks fired "
                         f"{hm['by_action']['rerun_failed_tasks']} times today.",
            "suggested_action": "Either fix the OverrideServer crash root "
                                "cause or remove it from health_check's "
                                "scheduled_tasks watch list.",
        })

    # 6. Universe staleness — are we capturing universe data yet?
    snap_today = list((REPO_ROOT / "snapshots").glob(f"{today}*.json"))
    if snap_today:
        try:
            last_snap = json.loads(snap_today[-1].read_text(encoding="utf-8"))
            uni_n = len(last_snap.get("universe", []))
            tr_n = len(last_snap.get("trades", []))
            if uni_n == 0 and tr_n > 0:
                proposals.append({
                    "id": f"universe_capture_missing_{today}",
                    "risk": "LOW",
                    "title": "Today's snapshot has no `universe` field",
                    "rationale": "snapshot.py was patched 2026-04-30 but today's "
                                 "snapshot was generated before the deploy.",
                    "suggested_action": "Verify the deployed snapshot.py and confirm "
                                        "tomorrow's snapshot includes universe.",
                })
        except Exception:
            pass

    # 7. Strategy A/B winners (built 2026-05-16). If a candidate variant
    # beats the production baseline by significance threshold, propose it.
    ab = report.get("strategy_ab", {})
    for c in ab.get("candidates_for_proposal", []):
        proposals.append({
            "id": f"strategy_ab_winner_{c['name']}_{today}",
            "risk": "MED",
            "title": f"Strategy A/B: `{c['name']}` beats baseline by "
                     f"${c['delta_vs_baseline']:+.0f}",
            "rationale": (
                f"n={c['n']} trades. "
                f"win_rate={c['win_rate']*100:.0f}% (vs baseline "
                f"{ab['baseline']['win_rate']*100:.0f}%), "
                f"avg={c['avg_return']*100:+.1f}% (vs "
                f"{ab['baseline']['avg_return']*100:+.1f}%), "
                f"sharpe={c['sharpe']:+.2f}. "
                f"Sample size meets significance gate (>=12)."
            ),
            "suggested_action": (
                f"Review tools/strategy_backtest.py STRATEGIES for "
                f"`{c['name']}` definition. If acceptable, update "
                f"_strategy_v1_gate in tools/paper_trade.py to match, "
                f"bump strat_version, update BACKTEST_BASELINE in "
                f"strategy_tracker.py to new expectations."
            ),
        })

    # 8. Correlation miner — emerging combos beating baseline by >=15pts
    for hz_key in ("correlations_d1", "correlations_d5"):
        cm = report.get(hz_key, {})
        for combo in cm.get("top_combos", [])[:3]:
            lift = combo.get("lift", 0) * 100
            if abs(lift) < 15:
                continue
            proposals.append({
                "id": f"correlation_{hz_key}_{abs(hash(combo['rule']))%10000}_{today}",
                "risk": "LOW",
                "title": f"Correlation finding ({hz_key.split('_')[-1]}): "
                         f"`{combo['rule']}`",
                "rationale": (
                    f"n={combo['n']}, win rate {combo['win_rate']*100:.0f}% "
                    f"(lift {lift:+.0f}pts vs baseline). "
                    f"avg return {combo['avg_return']*100:+.1f}%."
                ),
                "suggested_action": (
                    "Add this combo as a candidate strategy variant in "
                    "tools/strategy_backtest.py STRATEGIES and A/B test "
                    "against production baseline. If it beats baseline by "
                    "$200+ at n>=12, MED-risk proposal will follow."
                ),
            })

    return proposals


# ── Markdown rendering ──────────────────────────────────────────────────────

def render_markdown(report: dict, proposals: list[dict]) -> str:
    today = report["today"]
    lines = [f"# Options Edge — EOD analysis {today}", ""]
    eq = report.get("equity")
    rl = report.get("realized_pl", 0)
    ur = report.get("unrealized_pl", 0)
    lines.append(f"## P&L")
    lines.append(f"- Equity at close: **${eq:,.2f}**" if eq else "- Equity: n/a")
    lines.append(f"- Realized today: **${rl:+,.2f}**")
    lines.append(f"- Unrealized open: **${ur:+,.2f}**")
    lines.append(f"- Combined: **${rl + ur:+,.2f}**")
    lines.append("")

    # Pipelines
    pp = report.get("pipelines", {})
    if pp:
        lines.append("## Per-pipeline")
        lines.append("| Pipeline | Entries | Exits | Realized P&L |")
        lines.append("|---|---|---|---|")
        for k in ("divergence", "scorer"):
            d = pp.get(k, {})
            lines.append(f"| {k} | {d.get('entries', 0)} | {d.get('exits', 0)} | "
                         f"${d.get('realized_pl', 0):+.0f} |")
        lines.append("")

    # Today's entries / exits
    if report.get("entries"):
        lines.append("## Entries today")
        lines.append("| Time | OCC | Score | Sig | Qty | Cost | Tier |")
        lines.append("|---|---|---|---|---|---|---|")
        for e in report["entries"]:
            lines.append(f"| {e['ts']} | `{e['occ']}` | {e.get('score','?')} | "
                         f"{e.get('signal','-')} | {e.get('qty','?')} | "
                         f"${e.get('cost', 0):.0f} | {e.get('tag', '-')} |")
        lines.append("")

    if report.get("exits_filled"):
        lines.append("## Exits filled today")
        lines.append("| OCC | Entered | Entry | Exit | P&L | Reason |")
        lines.append("|---|---|---|---|---|---|")
        for x in report["exits_filled"]:
            ep = x.get("entry"); xp = x.get("exit"); pl = x.get("pl")
            ep_s = f"${ep:.2f}" if _isnum(ep) else "-"
            xp_s = f"${xp:.2f}" if _isnum(xp) else "-"
            pl_s = f"${pl:+.0f}" if _isnum(pl) else "-"
            reason = (x.get('reason') or '')[:50]
            lines.append(f"| `{x['occ']}` | {x['entry_date']} | {ep_s} | "
                         f"{xp_s} | {pl_s} | {reason} |")
        lines.append("")

    if report.get("queued_exits"):
        lines.append("## Queued exits (will fire next session open)")
        for q in report["queued_exits"]:
            lines.append(f"- `{q['occ']}` — entry {q['entry_date']}, "
                         f"reason: {q.get('reason','-')}")
        lines.append("")

    if report.get("still_open"):
        lines.append("## Still-open positions")
        lines.append("| OCC | Entered | Entry | Score | Qty | Status |")
        lines.append("|---|---|---|---|---|---|")
        for o in report["still_open"]:
            lines.append(f"| `{o['occ']}` | {o['entry_date']} | "
                         f"${o.get('entry', 0):.2f} | {o.get('score','?')} | "
                         f"{o.get('qty','?')} | {o.get('status')} |")
        lines.append("")

    # Filter activity
    fe = report.get("filter_effectiveness", {})
    if fe.get("total_skips", 0):
        lines.append("## Filter activity")
        lines.append(f"- Total skips: **{fe['total_skips']}**")
        for r, n in fe.get("by_reason", {}).items():
            if n > 0:
                lines.append(f"  - {r}: {n}")
        if fe.get("by_symbol"):
            top = list(fe["by_symbol"].items())[:5]
            lines.append(f"- Most-blocked: " +
                         ", ".join(f"`{s}` ({n})" for s, n in top))
        lines.append("")

    # Backtests
    bt = report.get("backtests", {})
    if bt.get("scorer") or bt.get("sentinel_events"):
        lines.append("## Cumulative backtests")
        sc = bt.get("scorer", {})
        if sc.get("n_trades") is not None:
            lines.append(f"- Scorer (5d hold): n={sc['n_trades']} "
                         f"win={sc.get('win_rate',0):.1%} "
                         f"mean={sc.get('mean_dir_return_pct',0):+.2f}% "
                         f"rho={sc.get('spearman_score_vs_return') or '?'}")
        se = bt.get("sentinel_events", {})
        for k in ("divergence", "news", "social"):
            d = se.get(k, {})
            if d.get("n", 0):
                lines.append(f"- {k}: n={d['n']} win={d.get('win_rate',0):.1%} "
                             f"mean={d.get('mean',0):+.2f}%")
        lines.append("")

    # Health
    hm = report.get("health", {})
    if hm:
        lines.append("## Health monitor activity")
        lines.append(f"- Remediations fired: {hm.get('remediations', 0)}")
        for action, n in (hm.get("by_action") or {}).items():
            lines.append(f"  - {action}: {n}")
        if hm.get("open_proposals"):
            lines.append(f"- Open anomaly_classifier proposals: "
                         f"{hm['open_proposals']}")
        lines.append("")

    # Shadow ledger — strategy_v1.2 trades we WOULD HAVE taken
    sl = report.get("shadow_ledger") or {}
    if sl and "error" not in sl:
        lines.append("## Shadow ledger (strategy_v1.2 unconstrained)")
        lines.append(f"- Planted today: {sl.get('planted_today', 0)}")
        lines.append(f"- Total open: {sl.get('open_after', 0)}")
        lines.append(f"- Total closed: {sl.get('closed_after', 0)}")
        lines.append(f"- Realized so far: ${sl.get('realized_so_far', 0):+,.2f}")
        if sl.get("closed_today"):
            lines.append(f"- **Closed TODAY: {sl['closed_today']} positions, "
                         f"${sl.get('realized_today_dollar', 0):+,.2f} realized**")
        lines.append(f"- Full per-position detail: "
                     f"`logs/shadow_ledger.md`")
        lines.append("")
    elif sl.get("error"):
        lines.append("## Shadow ledger")
        lines.append(f"_error: {sl['error']}_")
        lines.append("")

    # PROPOSALS — the part the user actually wants when they get home
    lines.append("## Proposals for review")
    if not proposals:
        lines.append("_No proposals today._")
    else:
        for p in proposals:
            lines.append(f"### [{p['risk']}] {p['title']}")
            lines.append(f"- Rationale: {p['rationale']}")
            lines.append(f"- Suggested action: {p['suggested_action']}")
            lines.append(f"- Proposal id: `{p['id']}`")
            lines.append("")

    return "\n".join(lines)


def render_telegram_summary(report: dict, proposals: list[dict]) -> str:
    rl = report.get("realized_pl", 0)
    ur = report.get("unrealized_pl", 0)
    eq = report.get("equity")
    eq_str = f"${eq:,.0f}" if eq else "n/a"
    head = (
        f"EOD {report['today']}\n"
        f"Equity {eq_str}  realized ${rl:+.0f}  unrealized ${ur:+.0f}\n"
        f"Entries: {len(report.get('entries', []))}  "
        f"Exits filled: {len(report.get('exits_filled', []))}\n"
        f"Queued exits: {len(report.get('queued_exits', []))}\n"
    )
    # Shadow ledger one-liner — what would v1.2 have done if unconstrained
    sl = report.get("shadow_ledger") or {}
    if sl and "error" not in sl:
        head += (f"Shadow: +{sl.get('planted_today', 0)} planted today, "
                 f"{sl.get('open_after', 0)} open / "
                 f"{sl.get('closed_after', 0)} closed total, "
                 f"realized ${sl.get('realized_so_far', 0):+.0f}\n")
        if sl.get("closed_today"):
            head += (f"  Shadow closes today: {sl['closed_today']} "
                     f"(${sl.get('realized_today_dollar', 0):+.0f})\n")
    if not proposals:
        return head + "\nNo proposals — quiet day."
    n_high = sum(1 for p in proposals if p["risk"] == "HIGH")
    n_med = sum(1 for p in proposals if p["risk"] == "MED")
    n_low = sum(1 for p in proposals if p["risk"] == "LOW")
    head += f"\nProposals: {len(proposals)} ({n_high} HIGH, {n_med} MED, {n_low} LOW)\n"
    head += "Top:\n"
    for p in proposals[:3]:
        head += f"- [{p['risk']}] {p['title']}\n"
    head += "\nReply /report for full markdown."
    return head[:3900]


# ── Main ────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> dict:
    today = target_date or date.today()
    print(f"EOD analysis for {today}")

    report = collect_today_results(today)
    report["pipelines"] = split_by_pipeline(report.get("entries", []),
                                             report.get("exits_filled", []))
    report["filter_effectiveness"] = filter_effectiveness(today)
    print("  refreshing backtests (~30s)...")
    report["backtests"] = refresh_backtests()
    print("  running strategy A/B vs production baseline...")
    report["strategy_ab"] = refresh_strategy_ab()
    print("  mining correlations on accumulated data...")
    report["correlations_d1"] = refresh_correlation_findings(horizon=1)
    report["correlations_d5"] = refresh_correlation_findings(horizon=5)
    print("  advancing shadow ledger (plant today + walk forward)...")
    report["shadow_ledger"] = refresh_shadow_simulate()
    report["health"] = health_activity(today)

    proposals = generate_proposals(report)

    md = render_markdown(report, proposals)
    md_path = LOG_DIR / f"eod-analysis-{today.isoformat()}.md"
    md_path.write_text(md, encoding="utf-8")

    prop_path = LOG_DIR / f"eod-proposals-{today.isoformat()}.json"
    prop_path.write_text(json.dumps(proposals, indent=2, default=str),
                         encoding="utf-8")

    print(f"  wrote {md_path}")
    print(f"  wrote {prop_path}")
    print(f"  {len(proposals)} proposals generated")

    # Telegram summary
    try:
        from tools.notify import send
        summary = render_telegram_summary(report, proposals)
        send("INFO", f"EOD analysis {today.isoformat()}", summary)
    except Exception as e:
        print(f"  notify failed: {e}")

    return {"report": report, "proposals": proposals, "md_path": str(md_path)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (default today)")
    args = ap.parse_args()
    d = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
    run(d)
    return 0


if __name__ == "__main__":
    sys.exit(main())
