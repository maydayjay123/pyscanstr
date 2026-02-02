"""Trade simulation - track coins and calculate best trades with CSV export."""

import asyncio
import aiohttp
import json
import csv
import os
import glob
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional
from config import SIM_UPDATE_MINS, TRADE_LOG_KEEP

TRADES_FILE = "trades.json"
CSV_FILE = "trade_log.csv"
LOG_ARCHIVE_DIR = "trade_logs"

def select_best_pair(pairs: list[dict]) -> Optional[dict]:
    """Pick the most reliable pair (highest liquidity, then volume)."""
    if not pairs:
        return None

    def score(p: dict) -> tuple[float, float]:
        liq = float(p.get("liquidity", {}).get("usd", 0) or 0)
        vol = float(p.get("volume", {}).get("h1", 0) or 0)
        return (liq, vol)

    return max(pairs, key=score)


def load_trades() -> list[dict]:
    """Load tracked trades from file."""
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return []


def save_trades(trades: list[dict]):
    """Save trades to file."""
    with open(TRADES_FILE, "w") as f:
        json.dump(trades, f, indent=2)
    # Also export to CSV
    export_csv(trades)


def export_csv(trades: list[dict]):
    """Export trades to CSV file with readable formatting."""
    if not trades:
        return

    # Clear, readable headers - most important info first
    headers = [
        "STATUS", "SYMBOL", "RATING",
        "CURRENT_PNL", "MAX_PNL", "MC_MULTIPLE",
        "ENTRY_MC", "CURRENT_MC", "HIGH_MC",
        "T1_HIT", "T2_HIT", "T3_HIT", "T4_HIT",
        "ENTRY_TIME", "AGE_HOURS",
        "LP_LOCKED", "HAS_TWITTER", "SAFETY",
        "BUY_PRESSURE", "RECOVERING", "TREND",
        "ENTRY_REASON", "ENTRY_FLAGS",
        "BUYS_5M", "SELLS_5M", "VOL_DIR",
        "POS_PCT", "REALIZED_PNL",
        "EXIT_REASON",
        "TARGET_1", "TARGET_2", "TARGET_3", "TARGET_4",
        "ADDRESS", "CHART_URL"
    ]

    tmp_file = f"{CSV_FILE}.tmp"
    with open(tmp_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for t in trades:
            # Calculate values
            entry_mc = t.get("entry_mc", 0) or 0
            current_mc = t.get("current_mc", 0) or 0
            high_mc = t.get("high_mc", 0) or 0
            pnl = t.get("pnl_pct", 0) or 0
            max_pnl = t.get("max_pnl_pct", 0) or 0
            mc_mult = high_mc / max(1, entry_mc)

            # Format MC as readable (e.g., "50K", "1.2M")
            def fmt_mc(mc):
                if mc >= 1_000_000:
                    return f"${mc/1_000_000:.1f}M"
                elif mc >= 1000:
                    return f"${mc/1000:.0f}K"
                return f"${mc:.0f}"

            # Calculate age
            age_hours = 0
            try:
                entry_time = datetime.fromisoformat(t.get("entry_time", ""))
                age_hours = (datetime.now() - entry_time).total_seconds() / 3600
            except:
                pass

            # Format status clearly
            status = t.get("status", "open").upper()
            if status == "OPEN":
                if pnl > 0:
                    status = "OPEN (+)"
                elif pnl < -20:
                    status = "OPEN (!!)"
                else:
                    status = "OPEN"

            row = [
                status,
                t.get("symbol", "?"),
                t.get("rating", "?"),
                f"{pnl:+.0f}%",
                f"{max_pnl:+.0f}%",
                f"{mc_mult:.1f}x",
                fmt_mc(entry_mc),
                fmt_mc(current_mc),
                fmt_mc(high_mc),
                "YES" if t.get("hit_target_1") else "no",
                "YES" if t.get("hit_target_2") else "no",
                "YES" if t.get("hit_target_3") else "no",
                "YES" if t.get("hit_target_4") else "no",
                t.get("entry_time", "")[:16].replace("T", " "),
                f"{age_hours:.1f}h",
                "YES" if t.get("lp_locked") else "no",
                "YES" if t.get("has_twitter") else "no",
                t.get("safety_score", 0),
                f"{t.get('buy_pressure', 0):.1f}x",
                "YES" if t.get("is_recovering") else "no",
                t.get("trend_match", "") or "-",
                t.get("entry_reason", "") or "-",
                t.get("entry_flags", "") or "-",
                t.get("last_buys_5m", 0),
                t.get("last_sells_5m", 0),
                "UP" if t.get("last_buys_5m", 0) > t.get("last_sells_5m", 0) else "DOWN",
                f"{t.get('position_pct', 100):.0f}%",
                f"{t.get('realized_pnl_pct', 0):+.0f}%",
                t.get("exit_reason", "") or "-",
                fmt_mc(t.get("target_1", 0)),
                fmt_mc(t.get("target_2", 0)),
                fmt_mc(t.get("target_3", 0)),
                fmt_mc(t.get("target_4", 0)),
                t.get("address", ""),
                t.get("dexscreener_url", "")
            ]
            writer.writerow(row)

    try:
        os.replace(tmp_file, CSV_FILE)
    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback = CSV_FILE.replace(".csv", f".{ts}.csv")
        try:
            os.replace(tmp_file, fallback)
            print(f"  [SIM] CSV locked. Wrote {fallback} instead.")
        except Exception:
            pass
    finally:
        cleanup_trade_logs()


def cleanup_trade_logs(keep_latest: int = TRADE_LOG_KEEP):
    """Archive rotated logs and keep only the most recent ones."""
    try:
        os.makedirs(LOG_ARCHIVE_DIR, exist_ok=True)

        # Move rotated logs into archive
        for path in glob.glob("trade_log.*.csv"):
            if os.path.basename(path) == CSV_FILE:
                continue
            dst = os.path.join(LOG_ARCHIVE_DIR, os.path.basename(path))
            if os.path.abspath(path) != os.path.abspath(dst):
                try:
                    os.replace(path, dst)
                except Exception:
                    pass

        # Remove temp/lock files if present
        for path in glob.glob("*.tmp") + glob.glob(".~lock.trade_log.csv#"):
            try:
                os.remove(path)
            except Exception:
                pass

        # Prune old archived logs
        archived = glob.glob(os.path.join(LOG_ARCHIVE_DIR, "trade_log.*.csv"))
        archived.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for old_path in archived[keep_latest:]:
            try:
                os.remove(old_path)
            except Exception:
                pass
    except Exception:
        pass


def reset_trade_data(delete_archived: bool = True):
    """Delete all sim trade data (JSON + CSV logs)."""
    for path in [TRADES_FILE, CSV_FILE]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    if delete_archived and os.path.isdir(LOG_ARCHIVE_DIR):
        try:
            for path in glob.glob(os.path.join(LOG_ARCHIVE_DIR, "trade_log.*.csv")):
                try:
                    os.remove(path)
                except Exception:
                    pass
        except Exception:
            pass


def add_trade(coin) -> dict:
    """Add a new trade entry from a FreshCoin with full details."""
    trades = load_trades()

    # Check if already tracking
    for t in trades:
        if t["address"] == coin.address:
            return t  # Already tracking

    # Calculate targets based on entry MC
    entry_mc = coin.market_cap or 0
    if entry_mc <= 0:
        print(f"  [SIM] Skipped {coin.symbol} (missing MC)")
        return None
    targets = calculate_targets(entry_mc)

    trade = {
        # Basic info
        "symbol": coin.symbol,
        "name": coin.name,
        "address": coin.address,
        "dexscreener_url": coin.dexscreener_url,

        # Entry data
        "entry_time": datetime.now().isoformat(),
        "entry_price": coin.price_usd,
        "entry_mc": entry_mc,
        "entry_liq": coin.liquidity,

        # Current data
        "current_price": coin.price_usd,
        "current_mc": entry_mc,

        # Tracking
        "high_price": coin.price_usd,
        "high_mc": entry_mc,
        "low_price": coin.price_usd,
        "pnl_pct": 0.0,
        "max_pnl_pct": 0.0,

        # Targets (step selling)
        "target_1": targets[0] if len(targets) > 0 else 0,
        "target_2": targets[1] if len(targets) > 1 else 0,
        "target_3": targets[2] if len(targets) > 2 else 0,
        "target_4": targets[3] if len(targets) > 3 else 0,
        "hit_target_1": False,
        "hit_target_2": False,
        "hit_target_3": False,
        "hit_target_4": False,

        # Status
        "status": "open",
        "exit_reason": "",
        "exit_time": "",

        # Entry criteria (for analysis)
        "rating": get_rating(coin.total_score),
        "lp_locked": coin.lp_locked or False,
        "has_twitter": coin.has_twitter,
        "has_website": coin.has_website,
        "holder_count": coin.holder_count,
        "top_holder_pct": coin.top_holder_pct,
        "safety_score": coin.safety_score,
        "buy_pressure": coin.buys_5m / max(1, coin.sells_5m),
        "is_recovering": coin.is_recovering,
        "trend_match": coin.matched_trend if coin.has_tiktok_match else "",
        "entry_reason": coin.entry_reason or "",
        "entry_flags": " | ".join(
            f for f in [
                "EARLY" if coin.is_early else "",
                "RECOVERING" if coin.is_recovering else "",
                "PUMPED" if coin.is_pumped else "",
            ] if f
        ),
        "last_buys_5m": 0,
        "last_sells_5m": 0,
        "position_pct": 100,
        "realized_pnl_pct": 0.0,
        "hit_step_1": False,
        "hit_step_2": False,
        "hit_step_3": False,
        "hit_step_4": False,
    }

    trades.append(trade)
    save_trades(trades)
    print(f"  [SIM] Added {coin.symbol} @ ${entry_mc/1000:.0f}K")
    return trade


def calculate_targets(entry_mc: float) -> list[float]:
    """Calculate step selling targets based on entry MC."""
    if entry_mc < 20000:
        # Very early: 50K, 100K, 200K, 500K
        return [50000, 100000, 200000, 500000]
    elif entry_mc < 50000:
        # Early: 100K, 150K, 250K, 500K
        return [100000, 150000, 250000, 500000]
    elif entry_mc < 100000:
        # Mid: 150K, 200K, 300K, 500K
        return [150000, 200000, 300000, 500000]
    else:
        # Higher entry: 150K, 200K, 300K
        return [150000, 200000, 300000, 500000]


def get_rating(score: int) -> str:
    if score >= 80:
        return "A"
    elif score >= 60:
        return "B"
    elif score >= 40:
        return "C"
    elif score >= 20:
        return "D"
    return "F"


async def fetch_current_price(address: str) -> tuple[float, float, float, int, int]:
    """Fetch current price, MC, liquidity, and 5m buy/sell counts for a token."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and "pairs" in data and data["pairs"]:
                        pair = select_best_pair(data["pairs"])
                        if not pair:
                            return 0.0, 0.0, 0.0, 0, 0
                        price = float(pair.get("priceUsd", 0) or 0)
                        mc = float(pair.get("marketCap", 0) or 0)
                        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
                        txns = pair.get("txns", {}).get("m5", {})
                        buys = int(txns.get("buys", 0) or 0)
                        sells = int(txns.get("sells", 0) or 0)
                        return price, mc, liq, buys, sells
    except:
        pass
    return 0.0, 0.0, 0.0, 0, 0


async def update_all_trades() -> list[dict]:
    """Update all tracked trades with current prices and check targets."""
    trades = load_trades()
    updated = []

    # Aggressive profit steps when volume is bullish
    profit_steps = [
        (5, 25, "step_1"),
        (10, 25, "step_2"),
        (20, 25, "step_3"),
        (30, 25, "step_4"),
    ]

    for trade in trades:
        if trade["status"] != "open":
            updated.append(trade)
            continue

        price, mc, liq, buys, sells = await fetch_current_price(trade["address"])

        if price > 0 and mc > 0:
            trade["current_price"] = price
            trade["current_mc"] = mc
            trade["last_buys_5m"] = buys
            trade["last_sells_5m"] = sells

            # Update high/low
            if mc > trade.get("high_mc", 0):
                trade["high_price"] = price
                trade["high_mc"] = mc
            if price < trade.get("low_price", price) or trade.get("low_price", 0) == 0:
                trade["low_price"] = price

            # Calculate PnL
            entry_mc = trade.get("entry_mc", 1)
            if entry_mc > 0:
                trade["pnl_pct"] = ((mc - entry_mc) / entry_mc) * 100
                trade["max_pnl_pct"] = ((trade.get("high_mc", mc) - entry_mc) / entry_mc) * 100

            # Check targets hit
            high_mc = trade.get("high_mc", 0)
            if high_mc >= trade.get("target_1", float("inf")):
                trade["hit_target_1"] = True
            if high_mc >= trade.get("target_2", float("inf")):
                trade["hit_target_2"] = True
            if high_mc >= trade.get("target_3", float("inf")):
                trade["hit_target_3"] = True
            if high_mc >= trade.get("target_4", float("inf")):
                trade["hit_target_4"] = True

            # Check exit conditions
            entry_time = datetime.fromisoformat(trade["entry_time"])
            age_hours = (datetime.now() - entry_time).total_seconds() / 3600

            # Auto-close after 24h
            if age_hours > 24:
                if trade["pnl_pct"] > 0:
                    trade["status"] = "win"
                    trade["exit_reason"] = "24h timeout - profit"
                else:
                    trade["status"] = "loss"
                    trade["exit_reason"] = "24h timeout - loss"
                trade["exit_time"] = datetime.now().isoformat()

            # Mark as loss if rugged (down >90% or liquidity pulled)
            if trade["pnl_pct"] < -90 or liq < 500:
                trade["status"] = "loss"
                trade["exit_reason"] = "rugged" if liq < 500 else "down >90%"
                trade["exit_time"] = datetime.now().isoformat()
            else:
                # Aggressive profit taking: use volume direction to decide
                pnl = trade.get("pnl_pct", 0)
                vol_down = sells >= buys and (buys + sells) > 0
                vol_up = buys > sells and (buys + sells) > 0

                # If volume turns against us and we're up at least 5%, exit fully
                if pnl >= 5 and vol_down:
                    trade["status"] = "win"
                    trade["exit_reason"] = "take profit (5%+, sell pressure)"
                    trade["exit_time"] = datetime.now().isoformat()
                    trade["position_pct"] = 0
                elif pnl >= 5 and vol_up and trade.get("position_pct", 100) > 0:
                    # Step out in chunks on strength
                    for step_pnl, step_pct, tag in profit_steps:
                        hit_key = f"hit_{tag}"
                        if pnl >= step_pnl and not trade.get(hit_key, False):
                            trade[hit_key] = True
                            trade["position_pct"] = max(0, trade.get("position_pct", 100) - step_pct)
                            trade["realized_pnl_pct"] = trade.get("realized_pnl_pct", 0) + (pnl * (step_pct / 100))
                            trade["exit_reason"] = f"partial TP {step_pnl}%"

                    if trade.get("position_pct", 0) == 0:
                        trade["status"] = "win"
                        trade["exit_reason"] = "fully exited (step TP)"
                        trade["exit_time"] = datetime.now().isoformat()

        updated.append(trade)
        await asyncio.sleep(0.1)  # Rate limit

    save_trades(updated)
    return updated


def get_trade_stats() -> dict:
    """Get statistics on tracked trades."""
    trades = load_trades()

    stats = {
        "total": len(trades),
        "open": 0,
        "wins": 0,
        "losses": 0,
        "total_pnl": 0.0,
        "best_trade": None,
        "worst_trade": None,
        "avg_max_gain": 0.0,
        "by_rating": {"A": [], "B": [], "C": [], "D": [], "F": []},
        "targets_hit": [0, 0, 0, 0]
    }

    max_gains = []

    for t in trades:
        if t["status"] == "open":
            stats["open"] += 1
        elif t["status"] == "win":
            stats["wins"] += 1
        else:
            stats["losses"] += 1

        stats["total_pnl"] += t.get("pnl_pct", 0)
        max_gains.append(t.get("max_pnl_pct", 0))

        # Track by rating
        rating = t.get("rating", "F")
        if rating in stats["by_rating"]:
            stats["by_rating"][rating].append(t)

        # Track targets hit
        if t.get("hit_target_1"):
            stats["targets_hit"][0] += 1
        if t.get("hit_target_2"):
            stats["targets_hit"][1] += 1
        if t.get("hit_target_3"):
            stats["targets_hit"][2] += 1
        if t.get("hit_target_4"):
            stats["targets_hit"][3] += 1

        # Best/worst
        if stats["best_trade"] is None or t.get("max_pnl_pct", 0) > stats["best_trade"].get("max_pnl_pct", 0):
            stats["best_trade"] = t
        if stats["worst_trade"] is None or t.get("pnl_pct", 0) < stats["worst_trade"].get("pnl_pct", 0):
            stats["worst_trade"] = t

    if max_gains:
        stats["avg_max_gain"] = sum(max_gains) / len(max_gains)

    return stats


def get_best_trade_criteria() -> dict:
    """Analyze trades to find what criteria lead to best trades."""
    trades = load_trades()

    criteria = {
        "lp_locked": {"total": 0, "wins": 0, "avg_gain": 0},
        "has_twitter": {"total": 0, "wins": 0, "avg_gain": 0},
        "high_safety": {"total": 0, "wins": 0, "avg_gain": 0},
        "recovering": {"total": 0, "wins": 0, "avg_gain": 0},
        "trend_match": {"total": 0, "wins": 0, "avg_gain": 0},
        "low_holders": {"total": 0, "wins": 0, "avg_gain": 0}
    }

    for t in trades:
        max_gain = t.get("max_pnl_pct", 0)
        is_win = max_gain >= 50  # 50% gain = win

        # LP locked
        if t.get("lp_locked"):
            criteria["lp_locked"]["total"] += 1
            criteria["lp_locked"]["avg_gain"] += max_gain
            if is_win:
                criteria["lp_locked"]["wins"] += 1

        # Twitter
        if t.get("has_twitter"):
            criteria["has_twitter"]["total"] += 1
            criteria["has_twitter"]["avg_gain"] += max_gain
            if is_win:
                criteria["has_twitter"]["wins"] += 1

        # High safety
        if t.get("safety_score", 0) >= 70:
            criteria["high_safety"]["total"] += 1
            criteria["high_safety"]["avg_gain"] += max_gain
            if is_win:
                criteria["high_safety"]["wins"] += 1

        # Recovering
        if t.get("is_recovering"):
            criteria["recovering"]["total"] += 1
            criteria["recovering"]["avg_gain"] += max_gain
            if is_win:
                criteria["recovering"]["wins"] += 1

        # Trend match
        if t.get("trend_match"):
            criteria["trend_match"]["total"] += 1
            criteria["trend_match"]["avg_gain"] += max_gain
            if is_win:
                criteria["trend_match"]["wins"] += 1

        # Low top holder (good distribution)
        if t.get("top_holder_pct", 100) < 30:
            criteria["low_holders"]["total"] += 1
            criteria["low_holders"]["avg_gain"] += max_gain
            if is_win:
                criteria["low_holders"]["wins"] += 1

    # Calculate averages
    for key in criteria:
        if criteria[key]["total"] > 0:
            criteria[key]["avg_gain"] /= criteria[key]["total"]

    return criteria


def format_stats_message() -> str:
    """Format trade stats for Telegram."""
    stats = get_trade_stats()

    if stats["total"] == 0:
        return "No trades tracked yet."

    win_rate = (stats["wins"] / max(1, stats["wins"] + stats["losses"])) * 100

    lines = [
        "*Trade Simulation Stats*",
        f"Total: {stats['total']} | Open: {stats['open']}",
        f"W/L: {stats['wins']}/{stats['losses']} ({win_rate:.0f}% win)",
        f"Avg Max Gain: +{stats['avg_max_gain']:.0f}%",
        ""
    ]

    # Best trade
    if stats["best_trade"]:
        t = stats["best_trade"]
        lines.append(f"Best: *${t['symbol']}* +{t.get('max_pnl_pct', 0):.0f}%")

    # Targets hit
    if stats["total"] > 0:
        lines.append("\n*Targets Hit:*")
        labels = ["T1 (2x)", "T2 (3x)", "T3 (5x)", "T4 (10x)"]
        for i, (count, label) in enumerate(zip(stats["targets_hit"], labels)):
            pct = (count / stats["total"]) * 100
            lines.append(f"  {label}: {count}/{stats['total']} ({pct:.0f}%)")

    # By rating performance
    lines.append("\n*Performance by Rating:*")
    for rating in ["A", "B", "C", "D"]:
        trades = stats["by_rating"][rating]
        if trades:
            avg = sum(t.get("max_pnl_pct", 0) for t in trades) / len(trades)
            lines.append(f"  [{rating}] {len(trades)} trades, avg +{avg:.0f}%")

    # Criteria analysis
    criteria = get_best_trade_criteria()
    lines.append("\n*Criteria Analysis:*")

    for name, data in criteria.items():
        if data["total"] > 0:
            rate = (data["wins"] / data["total"]) * 100
            label = name.replace("_", " ").title()
            lines.append(f"  {label}: {rate:.0f}% hit 50%+ (avg +{data['avg_gain']:.0f}%)")

    lines.append(f"\nCSV: {CSV_FILE}")

    return "\n".join(lines)


async def simulate_entry(coin) -> str:
    """Simulate entering a trade and return summary."""
    trade = add_trade(coin)
    if not trade:
        return f"Skipped *${coin.symbol}* (missing MC)"

    entry_mc = trade["entry_mc"]
    mc_str = f"${entry_mc/1000:.0f}K" if entry_mc < 1_000_000 else f"${entry_mc/1_000_000:.1f}M"

    return f"Tracking *${coin.symbol}* @ {mc_str}"


async def run_sim_loop(interval_mins: int = SIM_UPDATE_MINS):
    """Run continuous sim updates in a loop."""
    if interval_mins <= 0:
        return
    while True:
        try:
            await update_all_trades()
        except Exception:
            pass
        await asyncio.sleep(interval_mins * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", action="store_true", help="Run continuous sim loop")
    parser.add_argument("--reset", action="store_true", help="Delete all sim trade data")
    args = parser.parse_args()

    if args.reset:
        reset_trade_data(delete_archived=True)
        print("Deleted sim trade data (trades.json + trade_log.csv + archived logs).")
    elif args.loop:
        print(f"Running sim loop every {SIM_UPDATE_MINS} min")
        asyncio.run(run_sim_loop(SIM_UPDATE_MINS))
    else:
        async def test():
            print("Updating trades...")
            trades = await update_all_trades()
            print(f"Updated {len(trades)} trades")
            print(f"CSV exported to: {CSV_FILE}")

            print("\n" + format_stats_message())

        asyncio.run(test())
