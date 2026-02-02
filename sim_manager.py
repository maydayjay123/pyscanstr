"""
Sim Manager - Track positions and manage trades.

Runs 24/7, picks up signals from scanner, tracks positions.
Simple clean CSV output + Telegram updates.
"""

import asyncio
import aiohttp
import json
import csv
import os
from datetime import datetime
from typing import Optional, List, Dict

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


# Files
SIGNALS_FILE = "signals.json"
POSITIONS_FILE = "positions.json"
TRADES_CSV = "trades.csv"


class Position:
    """A tracked position."""

    def __init__(self, data: dict):
        self.address = data["address"]
        self.symbol = data["symbol"]
        self.entry_time = data.get("entry_time", datetime.now().isoformat())
        self.entry_mc = data.get("entry_mc", data.get("mc", 0))
        self.entry_price = data.get("entry_price", data.get("price", 0))
        self.entry_reason = data.get("entry_reason", data.get("reason", ""))

        # Trade type and target
        self.trade_type = data.get("trade_type", "QUICK")  # QUICK, MOMENTUM, GEM
        self.target = data.get("target", 8)  # Target profit %

        # Current state
        self.current_mc = data.get("current_mc", self.entry_mc)
        self.current_price = data.get("current_price", self.entry_price)
        self.high_mc = data.get("high_mc", self.entry_mc)

        # Volume tracking
        self.last_buys = data.get("last_buys", 0)
        self.last_sells = data.get("last_sells", 0)

        # Status
        self.status = data.get("status", "OPEN")  # OPEN, WIN, LOSS
        self.exit_reason = data.get("exit_reason", "")
        self.exit_time = data.get("exit_time", "")

        # Chart
        self.chart = data.get("chart", f"https://dexscreener.com/solana/{self.address}")

    @property
    def pnl(self) -> float:
        """Current PnL %."""
        if self.entry_mc <= 0:
            return 0
        return ((self.current_mc - self.entry_mc) / self.entry_mc) * 100

    @property
    def max_pnl(self) -> float:
        """Max PnL reached."""
        if self.entry_mc <= 0:
            return 0
        return ((self.high_mc - self.entry_mc) / self.entry_mc) * 100

    @property
    def vol_direction(self) -> str:
        if self.last_buys > self.last_sells:
            return "UP"
        elif self.last_sells > self.last_buys:
            return "DOWN"
        return "FLAT"

    @property
    def age_hours(self) -> float:
        try:
            entry = datetime.fromisoformat(self.entry_time)
            return (datetime.now() - entry).total_seconds() / 3600
        except:
            return 0

    def mc_str(self, mc: float) -> str:
        if mc >= 1_000_000:
            return f"${mc/1_000_000:.1f}M"
        return f"${mc/1000:.0f}K"

    @property
    def type_icon(self) -> str:
        if self.trade_type == "QUICK":
            return "⚡"
        elif self.trade_type == "MOMENTUM":
            return "📈"
        elif self.trade_type == "GEM":
            return "💎"
        return ""

    def to_dict(self) -> dict:
        return {
            "address": self.address,
            "symbol": self.symbol,
            "entry_time": self.entry_time,
            "entry_mc": self.entry_mc,
            "entry_price": self.entry_price,
            "entry_reason": self.entry_reason,
            "trade_type": self.trade_type,
            "target": self.target,
            "current_mc": self.current_mc,
            "current_price": self.current_price,
            "high_mc": self.high_mc,
            "last_buys": self.last_buys,
            "last_sells": self.last_sells,
            "status": self.status,
            "exit_reason": self.exit_reason,
            "exit_time": self.exit_time,
            "chart": self.chart
        }


def load_positions() -> List[Position]:
    """Load positions from file."""
    if os.path.exists(POSITIONS_FILE):
        try:
            with open(POSITIONS_FILE, "r") as f:
                data = json.load(f)
                return [Position(p) for p in data]
        except:
            pass
    return []


def save_positions(positions: List[Position]):
    """Save positions to file and CSV."""
    with open(POSITIONS_FILE, "w") as f:
        json.dump([p.to_dict() for p in positions], f, indent=2)

    export_csv(positions)


def export_csv(positions: List[Position]):
    """Export simple clean CSV."""
    headers = [
        "STATUS",
        "TYPE",
        "SYMBOL",
        "PNL",
        "TARGET",
        "MAX_PNL",
        "ENTRY_MC",
        "NOW_MC",
        "VOL_DIR",
        "BUYS",
        "SELLS",
        "AGE_H",
        "REASON",
        "EXIT",
        "CHART"
    ]

    with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for p in positions:
            # Check if target hit
            target_status = "HIT" if p.max_pnl >= p.target else f"{p.target}%"

            row = [
                p.status,
                p.trade_type,
                p.symbol,
                f"{p.pnl:+.0f}%",
                target_status,
                f"{p.max_pnl:+.0f}%",
                p.mc_str(p.entry_mc),
                p.mc_str(p.current_mc),
                p.vol_direction,
                p.last_buys,
                p.last_sells,
                f"{p.age_hours:.1f}",
                p.entry_reason[:30],
                p.exit_reason[:20] if p.exit_reason else "-",
                p.chart
            ]
            writer.writerow(row)


def load_signals() -> List[dict]:
    """Load signals from scanner."""
    if os.path.exists(SIGNALS_FILE):
        try:
            with open(SIGNALS_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return []


async def fetch_current(address: str) -> tuple:
    """Fetch current price, MC, buys, sells."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and "pairs" in data and data["pairs"]:
                        pair = max(data["pairs"], key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                        price = float(pair.get("priceUsd", 0) or 0)
                        mc = float(pair.get("marketCap", 0) or 0)
                        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
                        txns = pair.get("txns", {}).get("m5", {})
                        buys = int(txns.get("buys", 0) or 0)
                        sells = int(txns.get("sells", 0) or 0)
                        return price, mc, liq, buys, sells
    except:
        pass
    return 0, 0, 0, 0, 0


def process_signals(signals: List[dict], positions: List[Position]) -> tuple:
    """Process new signals and add positions. Returns (positions, new_entries)."""
    existing = {p.address for p in positions}
    new_entries = []

    for sig in signals:
        if sig.get("signal") != "BUY":
            continue
        if sig["address"] in existing:
            continue

        trade_type = sig.get("trade_type", "QUICK")
        target = sig.get("target", 8)

        # Add new position
        pos = Position({
            "address": sig["address"],
            "symbol": sig["symbol"],
            "entry_time": datetime.now().isoformat(),
            "entry_mc": sig["mc"],
            "entry_price": sig["price"],
            "entry_reason": sig.get("reason", ""),
            "trade_type": trade_type,
            "target": target,
            "chart": sig.get("chart", "")
        })

        positions.append(pos)
        entry_str = f"{pos.type_icon} `{pos.symbol}` @ {pos.mc_str(pos.entry_mc)} [chart]({pos.chart})"
        new_entries.append(entry_str)
        print(f"  + {pos.type_icon} {pos.symbol} @ {pos.mc_str(pos.entry_mc)} | {trade_type} {target}%")
        print(f"    {pos.chart}")

    return positions, new_entries


async def update_positions(positions: List[Position]) -> tuple:
    """Update all positions with current data. Returns (positions, exits)."""
    exits = []

    for pos in positions:
        if pos.status != "OPEN":
            continue

        old_status = pos.status
        price, mc, liq, buys, sells = await fetch_current(pos.address)

        if mc > 0:
            pos.current_price = price
            pos.current_mc = mc
            pos.last_buys = buys
            pos.last_sells = sells

            if mc > pos.high_mc:
                pos.high_mc = mc

            # Check exit conditions
            check_exit(pos, liq)

            # Track exits
            if pos.status != "OPEN" and old_status == "OPEN":
                exit_str = f"`{pos.symbol}` {pos.status} {pos.pnl:+.0f}%"
                exits.append(exit_str)

        await asyncio.sleep(0.1)

    return positions, exits


def check_exit(pos: Position, liquidity: float):
    """Check if position should exit based on trade type."""
    pnl = pos.pnl
    vol_down = pos.last_sells > pos.last_buys
    target = pos.target

    # === TYPE-SPECIFIC EXIT RULES ===

    if pos.trade_type == "QUICK":
        # QUICK: Fast exit, target 8%+
        # Win: hit target and vol down, or hit 2x target
        if pnl >= target and vol_down:
            pos.status = "WIN"
            pos.exit_reason = f"TP +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        if pnl >= target * 2:  # 16%+ take profit
            pos.status = "WIN"
            pos.exit_reason = f"TP 2x +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Quick stop loss: -10%
        if pnl <= -10:
            pos.status = "LOSS"
            pos.exit_reason = f"SL {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Timeout: 2h for quick trades
        if pos.age_hours >= 2:
            if pnl > 0:
                pos.status = "WIN"
                pos.exit_reason = f"2h timeout +{pnl:.0f}%"
            else:
                pos.status = "LOSS"
                pos.exit_reason = f"2h timeout {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return

    elif pos.trade_type == "MOMENTUM":
        # MOMENTUM: Target 25-30%
        if pnl >= target and vol_down:
            pos.status = "WIN"
            pos.exit_reason = f"TP +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        if pnl >= 40:  # Strong win
            pos.status = "WIN"
            pos.exit_reason = f"TP +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Stop loss: -20%
        if pnl <= -20:
            pos.status = "LOSS"
            pos.exit_reason = f"SL {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Timeout: 6h
        if pos.age_hours >= 6:
            if pnl > 0:
                pos.status = "WIN"
                pos.exit_reason = f"6h timeout +{pnl:.0f}%"
            else:
                pos.status = "LOSS"
                pos.exit_reason = f"6h timeout {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return

    elif pos.trade_type == "GEM":
        # GEM: Let it run, target 100%+
        # Only exit on huge win or clear loss
        if pnl >= 100 and vol_down:
            pos.status = "WIN"
            pos.exit_reason = f"TP +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        if pnl >= 200:  # 3x take some profit
            pos.status = "WIN"
            pos.exit_reason = f"TP 3x +{pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Stop loss: -40% (give it room)
        if pnl <= -40:
            pos.status = "LOSS"
            pos.exit_reason = f"SL {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return
        # Timeout: 24h
        if pos.age_hours >= 24:
            if pnl > 0:
                pos.status = "WIN"
                pos.exit_reason = f"24h timeout +{pnl:.0f}%"
            else:
                pos.status = "LOSS"
                pos.exit_reason = f"24h timeout {pnl:.0f}%"
            pos.exit_time = datetime.now().isoformat()
            return

    # Common: liquidity pulled = rug
    if liquidity < 1000:
        pos.status = "LOSS"
        pos.exit_reason = "rug/liq pulled"
        pos.exit_time = datetime.now().isoformat()


def print_stats(positions: List[Position]):
    """Print detailed stats to console."""
    open_pos = [p for p in positions if p.status == "OPEN"]
    wins = [p for p in positions if p.status == "WIN"]
    losses = [p for p in positions if p.status == "LOSS"]

    # Count by type
    quick = [p for p in open_pos if p.trade_type == "QUICK"]
    momentum = [p for p in open_pos if p.trade_type == "MOMENTUM"]
    gems = [p for p in open_pos if p.trade_type == "GEM"]

    print(f"  [{len(open_pos)} open: ⚡{len(quick)} 📈{len(momentum)} 💎{len(gems)}] | {len(wins)}W {len(losses)}L")

    if open_pos:
        total_pnl = sum(p.pnl for p in open_pos)
        print(f"  Total PnL: {total_pnl:+.0f}%")

        # Sort by PnL
        open_pos.sort(key=lambda x: x.pnl, reverse=True)
        for p in open_pos:
            vol = "↑" if p.vol_direction == "UP" else "↓" if p.vol_direction == "DOWN" else "→"
            target_hit = "✓" if p.max_pnl >= p.target else ""
            print(f"    {p.type_icon} {p.symbol.ljust(8)} {p.pnl:+5.0f}% (t:{p.target}%{target_hit}) | {p.mc_str(p.entry_mc)}→{p.mc_str(p.current_mc)} | {vol} {p.last_buys}b/{p.last_sells}s")
            print(f"      {p.chart}")

    if wins or losses:
        total_closed = wins + losses
        win_rate = len(wins) / len(total_closed) * 100 if total_closed else 0
        print(f"  Win rate: {win_rate:.0f}%")


async def send_tg(text: str):
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
    except:
        pass


def format_positions_msg(positions: List[Position], new_entries: List[str] = None, exits: List[str] = None) -> str:
    """Format positions for Telegram - detailed view."""
    open_pos = [p for p in positions if p.status == "OPEN"]
    wins = [p for p in positions if p.status == "WIN"]
    losses = [p for p in positions if p.status == "LOSS"]

    # Count by type
    quick = [p for p in open_pos if p.trade_type == "QUICK"]
    momentum = [p for p in open_pos if p.trade_type == "MOMENTUM"]
    gems = [p for p in open_pos if p.trade_type == "GEM"]

    ts = datetime.now().strftime("%H:%M:%S")
    lines = [f"*SIM* {ts}"]
    lines.append(f"⚡{len(quick)} 📈{len(momentum)} 💎{len(gems)} | W:{len(wins)} L:{len(losses)}")

    # New entries
    if new_entries:
        lines.append("")
        lines.append("*NEW*")
        for e in new_entries:
            lines.append(f"+ {e}")

    # Exits
    if exits:
        lines.append("")
        lines.append("*EXIT*")
        for e in exits:
            lines.append(f"x {e}")

    # Open positions by type
    if open_pos:
        total_pnl = sum(p.pnl for p in open_pos)
        lines.append("")
        lines.append(f"*OPEN* {total_pnl:+.0f}%")

        # Sort by PnL
        open_pos.sort(key=lambda x: x.pnl, reverse=True)

        for p in open_pos[:10]:
            vol = "↑" if p.vol_direction == "UP" else "↓" if p.vol_direction == "DOWN" else "→"
            target_hit = "✓" if p.max_pnl >= p.target else ""

            # Color based on PnL
            if p.pnl >= p.target:
                pnl_icon = "🟢"
            elif p.pnl >= 0:
                pnl_icon = "🔵"
            elif p.pnl >= -10:
                pnl_icon = "🟡"
            else:
                pnl_icon = "🔴"

            lines.append(f"{p.type_icon}{pnl_icon} `{p.symbol}` {p.pnl:+.0f}% t:{p.target}%{target_hit}")
            lines.append(f"   {p.mc_str(p.entry_mc)}→{p.mc_str(p.current_mc)} {vol} {p.last_buys}b/{p.last_sells}s")
            lines.append(f"   [chart]({p.chart})")

    # Stats by type
    if wins or losses:
        lines.append("")
        total_closed = wins + losses
        win_rate = len(wins) / len(total_closed) * 100 if total_closed else 0

        # Stats by type
        for t in ["QUICK", "MOMENTUM", "GEM"]:
            t_wins = [p for p in wins if p.trade_type == t]
            t_losses = [p for p in losses if p.trade_type == t]
            if t_wins or t_losses:
                t_total = len(t_wins) + len(t_losses)
                t_wr = len(t_wins) / t_total * 100 if t_total else 0
                icon = "⚡" if t == "QUICK" else "📈" if t == "MOMENTUM" else "💎"
                lines.append(f"{icon} {t_wr:.0f}% ({len(t_wins)}W/{len(t_losses)}L)")

        lines.append(f"Total: {win_rate:.0f}%")

    return "\n".join(lines)


async def run_manager(interval_secs: int = 15, send_to_tg: bool = True):
    """Run position manager loop."""
    print(f"Sim Manager started - interval {interval_secs}s")
    print(f"Watching: {SIGNALS_FILE}")
    print(f"Output: {TRADES_CSV}")
    print(f"Telegram: {'ON' if send_to_tg else 'OFF'}\n")

    last_tg_update = None
    tg_interval = 60  # Send TG update every 60s when positions exist

    while True:
        try:
            # Load
            positions = load_positions()
            signals = load_signals()

            # Process new signals
            positions, new_entries = process_signals(signals, positions)

            # Update existing
            positions, exits = await update_positions(positions)

            # Save
            save_positions(positions)

            # Print status
            ts = datetime.now().strftime('%H:%M:%S')
            open_count = len([p for p in positions if p.status == "OPEN"])
            print(f"[{ts}] Updated {len(positions)} positions ({open_count} open)")

            print_stats(positions)

            # Send to Telegram on new entries, exits, or periodic update
            now = datetime.now()
            should_send = False

            if new_entries or exits:
                should_send = True
            elif last_tg_update is None or (now - last_tg_update).total_seconds() >= tg_interval:
                should_send = True

            if send_to_tg and should_send and positions:
                msg = format_positions_msg(positions, new_entries, exits)
                await send_tg(msg)
                last_tg_update = now

        except Exception as e:
            print(f"  Error: {e}")

        await asyncio.sleep(interval_secs)


def reset():
    """Reset all position data."""
    for f in [POSITIONS_FILE, TRADES_CSV, SIGNALS_FILE]:
        if os.path.exists(f):
            os.remove(f)
            print(f"Deleted {f}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset all data")
    parser.add_argument("--once", action="store_true", help="Single update")
    parser.add_argument("--interval", type=int, default=30, help="Update interval in seconds")
    args = parser.parse_args()

    if args.reset:
        reset()
        print("Data reset complete")
    elif args.once:
        async def once():
            positions = load_positions()
            signals = load_signals()
            positions = process_signals(signals, positions)
            positions = await update_positions(positions)
            save_positions(positions)
            print(f"Updated {len(positions)} positions")
            print_stats(positions)
        asyncio.run(once())
    else:
        asyncio.run(run_manager(args.interval))
