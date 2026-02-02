"""
Run scanner with sim or live trading.

Usage:
    python run.py          # Scanner + Sim (default)
    python run.py --live   # Scanner + LIVE TRADING
    python run.py --scan   # Scanner only
    python run.py --sim    # Sim manager only
    python run.py --reset  # Reset all data
"""

import asyncio
import argparse
from datetime import datetime
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    from sim_manager import load_positions, format_positions_msg

    positions = load_positions()
    if positions:
        msg = format_positions_msg(positions)
    else:
        msg = "*SIM STATUS*\nNo positions yet"

    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)


async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /live command - show live positions."""
    from live_trader import format_live_status

    msg = format_live_status()
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /scan command - trigger immediate scan."""
    from scanner import scan, format_signal_msg

    await update.message.reply_text("Scanning...")

    signals = await scan()
    if signals:
        msg = format_signal_msg(signals)
        if msg:
            await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)
        else:
            await update.message.reply_text("No signals found")
    else:
        await update.message.reply_text("No tokens found")


async def run_bot():
    """Run Telegram bot for commands."""
    if not TELEGRAM_BOT_TOKEN:
        print("No TG token, bot disabled")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("live", cmd_live))
    app.add_handler(CommandHandler("s", cmd_status))  # Short alias
    app.add_handler(CommandHandler("l", cmd_live))    # Short alias

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    print("TG Bot: /status /scan /live")

    # Keep running
    while True:
        await asyncio.sleep(60)


async def run_all(no_tg: bool = False):
    """Run scanner and sim manager together."""
    from scanner import run_scanner
    from sim_manager import run_manager

    send_tg = not no_tg

    print("=" * 50)
    print("MEME SCANNER + SIM MANAGER")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Scanner:  30s interval - volume direction")
    print("Manager:  15s interval - position tracking")
    print("Telegram: 60s updates")
    print("Output:   trades.csv")
    print(f"TG: {'ON' if send_tg else 'OFF'}")
    print("Commands: /status /scan /live")
    print()
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

    # Run all concurrently
    tasks = [
        run_scanner(interval_secs=30, send_to_tg=send_tg),
        run_manager(interval_secs=15, send_to_tg=send_tg),
    ]

    if send_tg:
        tasks.append(run_bot())

    await asyncio.gather(*tasks)


async def run_scanner_only():
    """Run just the scanner."""
    from scanner import run_scanner

    print("Running Scanner only...")
    await run_scanner(interval_secs=60)


async def run_sim_only():
    """Run just the sim manager."""
    from sim_manager import run_manager

    print("Running Sim Manager only...")
    await run_manager(interval_secs=30)


def reset_all():
    """Reset all data."""
    import os

    files = [
        "signals.json",
        "positions.json",
        "trades.csv",
        "trades.json",
        "trade_log.csv"
    ]

    for f in files:
        if os.path.exists(f):
            os.remove(f)
            print(f"Deleted: {f}")

    print("\nData reset complete")


async def run_live():
    """Run scanner with LIVE trading."""
    from scanner import run_scanner
    from live_trader import run_live_manager

    print("=" * 50)
    print("!!! LIVE TRADING MODE !!!")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Scanner:  30s interval")
    print("Trader:   30s position check")
    print()
    print("THIS USES REAL MONEY!")
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

    await asyncio.gather(
        run_scanner(interval_secs=30, send_to_tg=True, live_mode=True),
        run_live_manager(interval_secs=30),
        run_bot(),
    )


def main():
    parser = argparse.ArgumentParser(description="Meme Scanner + Sim/Live Manager")
    parser.add_argument("--scan", action="store_true", help="Run scanner only")
    parser.add_argument("--sim", action="store_true", help="Run sim manager only")
    parser.add_argument("--live", action="store_true", help="Run with LIVE trading")
    parser.add_argument("--reset", action="store_true", help="Reset all data")
    parser.add_argument("--no-tg", action="store_true", help="Disable Telegram output")
    args = parser.parse_args()

    if args.reset:
        reset_all()
    elif args.scan:
        asyncio.run(run_scanner_only())
    elif args.sim:
        asyncio.run(run_sim_only())
    elif args.live:
        try:
            asyncio.run(run_live())
        except KeyboardInterrupt:
            print("\nLive trading stopped")
    else:
        try:
            asyncio.run(run_all(no_tg=args.no_tg))
        except KeyboardInterrupt:
            print("\nStopped")


if __name__ == "__main__":
    main()
