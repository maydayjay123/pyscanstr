"""
Run scanner with sim or live trading.

Usage:
    python run.py          # Scanner + Sim (default)
    python run.py --live   # Scanner + LIVE TRADING
    python run.py --pair   # Pair trader (manual CA via TG)
    python run.py --scan   # Scanner only (data collection)
    python run.py --sim    # Sim manager only
    python run.py --reset  # Reset all data
    python run.py --reset-live  # Reset live data only
"""

import asyncio
import argparse
from datetime import datetime
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


# ============== MENU BUTTONS ==============

def get_main_menu():
    keyboard = [
        [
            InlineKeyboardButton("📊 Slots", callback_data="pair_positions"),
            InlineKeyboardButton("💰 Wallet", callback_data="pair_wallet"),
        ],
        [
            InlineKeyboardButton("📈 Stats", callback_data="pair_stats"),
            InlineKeyboardButton("📜 History", callback_data="pair_history"),
        ],
        [
            InlineKeyboardButton("📥 Export CSV", callback_data="pair_export"),
            InlineKeyboardButton("ℹ️ Help", callback_data="pair_help"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_position_menu():
    keyboard = [
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="pair_positions"),
            InlineKeyboardButton("💰 Wallet", callback_data="pair_wallet"),
            InlineKeyboardButton("🏠 Menu", callback_data="menu"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_back_menu():
    keyboard = [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
    return InlineKeyboardMarkup(keyboard)


# ============== COMMAND HANDLERS ==============

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command - show main menu."""
    msg = (
        "*PAIR TRADER — 8 SLOTS*\n\n"
        "You pick the CA, bot handles entry, DCA & exit.\n"
        "_Trail TP by MC tier. Auto-buys after 4h watch. No-loss guard._\n\n"
        "*/trade <CA>* — watch a token\n"
        "*/cancel <sym|slot>* — cancel watching\n"
        "*/close <sym|slot>* — force sell now\n"
        "*/closeall* — close every position\n"
        "*/pos* — all slot status (live PnL)\n"
        "*/stats* — budgets & profit per slot\n"
        "*/export* — get trade history CSV\n"
    )
    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=get_main_menu()
    )


async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /menu command."""
    msg = "*PAIR TRADER*\n\nSelect an option:"
    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=get_main_menu()
    )


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    from sim_manager import load_positions, format_positions_msg

    positions = load_positions()
    if positions:
        msg = format_positions_msg(positions)
    else:
        msg = "*SIM STATUS*\nNo positions yet"

    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        disable_web_page_preview=True,
        reply_markup=get_back_menu()
    )


async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /live command - show live positions with momentum data."""
    from live_trader import format_live_status_detailed

    await update.message.reply_text("⏳ Fetching live data...")
    msg = await format_live_status_detailed()
    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        disable_web_page_preview=True,
        reply_markup=get_position_menu()
    )


async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /scan command - trigger immediate scan."""
    from scanner import scan, format_signal_msg

    await update.message.reply_text("🔍 Scanning...")

    signals = await scan()
    if signals:
        msg = format_signal_msg(signals)
        if msg:
            await update.message.reply_text(
                msg,
                parse_mode="Markdown",
                disable_web_page_preview=True,
                reply_markup=get_back_menu()
            )
        else:
            await update.message.reply_text("No signals found", reply_markup=get_back_menu())
    else:
        await update.message.reply_text("No tokens found", reply_markup=get_back_menu())


# ============== PAIR TRADER COMMANDS ==============

async def cmd_trade_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /trade <CA> — queue a token for pair trader."""
    if not ctx.args:
        await update.message.reply_text("Usage: /trade <contract_address>")
        return
    from pair_trader import cmd_trade
    msg = await cmd_trade(ctx.args[0])
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)


async def cmd_cancel_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /cancel <symbol|slot> — cancel a watching slot."""
    if not ctx.args:
        await update.message.reply_text("Usage: /cancel <symbol> or /cancel <slot_number>")
        return
    from pair_trader import cmd_cancel
    msg = await cmd_cancel(ctx.args[0])
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_close_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /close <symbol|slot> — manually close an open position."""
    if not ctx.args:
        await update.message.reply_text("Usage: /close <symbol> or /close <slot_number>")
        return
    from pair_trader import cmd_close
    msg = await cmd_close(ctx.args[0])
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_positions_pair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /pos — show pair trader slot status."""
    from pair_trader import cmd_positions
    msg = await cmd_positions()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_stats_pair(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /stats — show slot budgets and profit summary."""
    from pair_trader import cmd_stats
    msg = await cmd_stats()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_resetbudget_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /resetbudget — re-init slot budgets from current wallet balance."""
    from pair_trader import cmd_resetbudget
    msg = await cmd_resetbudget()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_closeall_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /closeall — force sell every open position."""
    from pair_trader import cmd_closeall
    await update.message.reply_text("Closing all positions...")
    msg = await cmd_closeall()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_export_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /export — send pair_trades.csv as a file."""
    import os
    from pair_trader import TRADES_FILE
    if not os.path.exists(TRADES_FILE):
        await update.message.reply_text("No trade data yet — pair_trades.csv doesn't exist")
        return
    with open(TRADES_FILE, "rb") as f:
        await update.message.reply_document(document=f, filename="pair_trades.csv",
                                            caption="Trade history CSV")


async def cmd_exportprices_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /exportprices — send price_data.csv as a file."""
    import os
    from pair_trader import PRICE_DATA_FILE
    if not os.path.exists(PRICE_DATA_FILE):
        await update.message.reply_text("No price data yet — start trading to collect data")
        return
    with open(PRICE_DATA_FILE, "rb") as f:
        await update.message.reply_document(document=f, filename="price_data.csv",
                                            caption="Price tick data CSV")


# ============== BUTTON CALLBACK HANDLERS ==============

async def button_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle button presses."""
    query = update.callback_query
    await query.answer()

    data = query.data

    try:
        await _handle_button(query, data)
    except Exception as e:
        if "not modified" in str(e).lower():
            pass  # Content unchanged, ignore
        else:
            print(f"Button error: {e}")


async def _handle_button(query, data):
    """Process button callback data."""

    if data == "menu":
        msg = "*PAIR TRADER*\n\nSelect an option:"
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_main_menu())

    elif data == "pair_positions":
        from pair_trader import cmd_positions
        msg = await cmd_positions()
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_position_menu())

    elif data == "pair_stats":
        from pair_trader import cmd_stats
        msg = await cmd_stats()
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())

    elif data == "pair_wallet":
        from pair_trader import get_wallet_pubkey, get_sol_balance, load_budgets
        wallet = get_wallet_pubkey()
        if wallet:
            balance = await get_sol_balance(wallet)
            budgets = load_budgets() or []
            invested = sum(b.budget_sol for b in budgets)
            msg = (
                f"*WALLET*\n\n"
                f"`{wallet[:8]}...{wallet[-4:]}`\n\n"
                f"Balance: *{balance:.4f} SOL*\n"
                f"Slot budgets: {invested:.4f} SOL\n"
                f"Free: {max(0, balance - invested):.4f} SOL"
            )
        else:
            msg = "*WALLET*\n\n⚠️ No wallet configured"
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())

    elif data == "pair_history":
        import os, csv as _csv
        from pair_trader import TRADES_FILE
        msg = "*TRADE HISTORY*\n\n"
        if not os.path.exists(TRADES_FILE):
            msg += "No trades yet"
        else:
            rows = []
            with open(TRADES_FILE) as f:
                reader = _csv.DictReader(f)
                rows = list(reader)
            if not rows:
                msg += "No trades yet"
            else:
                rows = rows[-15:][::-1]  # last 15, newest first
                for r in rows:
                    pnl = float(r.get("pnl_pct", 0))
                    emoji = "🟢" if pnl >= 0 else "🔴"
                    sym = r.get("symbol", "?")
                    held = r.get("held_mins", "?")
                    reason = r.get("exit_reason", "")[:20]
                    msg += f"{emoji} `{sym}` *{pnl:+.1f}%* | {held}m | {reason}\n"
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())

    elif data == "pair_export":
        import os, csv as _csv
        from pair_trader import TRADES_FILE, PRICE_DATA_FILE
        lines = ["*EXPORT*\n"]
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE) as f:
                trades = list(_csv.DictReader(f))
            lines.append(f"📄 *pair\_trades.csv* — {len(trades)} trades\n/export to receive")
        else:
            lines.append("📄 *pair\_trades.csv* — no trades yet")
        if os.path.exists(PRICE_DATA_FILE):
            with open(PRICE_DATA_FILE) as f:
                ticks = sum(1 for _ in f) - 1
            lines.append(f"\n📈 *price\_data.csv* — {ticks} price ticks\n/exportprices to receive")
        else:
            lines.append("\n📈 *price\_data.csv* — no data yet")
        msg = "\n".join(lines)
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())

    elif data == "pair_help":
        msg = (
            "*COMMANDS*\n\n"
            "*/trade <CA>* — start watching a token\n"
            "*/cancel <sym|slot>* — cancel before entry\n"
            "*/close <sym|slot>* — force sell now\n"
            "*/closeall* — close all open positions\n"
            "*/pos* — all slots, live PnL\n"
            "*/stats* — budget & profit per slot\n"
            "*/resetbudget* — reinit budgets from wallet\n"
            "*/export* — download trade CSV\n\n"
            "*How it works:*\n"
            "1. Send /trade <CA> — bot watches price\n"
            "2. Waits for MC-based dip, then buys Step 1 (15%)\n"
            "3. If no dip after 4h → auto-buys at market\n"
            "4. Step 2 (25%) at −8% | Step 3 (60%) at −14%\n"
            "5. Trail TP activates +8–15% (by MC), trails 3–6% below peak\n"
            "6. Close → auto re-watches same token"
        )
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())


# ============== BOT RUNNER ==============

async def run_bot():
    """Run Telegram bot for commands."""
    if not TELEGRAM_BOT_TOKEN:
        print("No TG token, bot disabled")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("live", cmd_live))
    app.add_handler(CommandHandler("s", cmd_status))
    app.add_handler(CommandHandler("l", cmd_live))
    app.add_handler(CommandHandler("m", cmd_menu))

    # Pair trader commands
    app.add_handler(CommandHandler("trade", cmd_trade_handler))
    app.add_handler(CommandHandler("cancel", cmd_cancel_handler))
    app.add_handler(CommandHandler("close", cmd_close_handler))
    app.add_handler(CommandHandler("pos", cmd_positions_pair))
    app.add_handler(CommandHandler("p", cmd_positions_pair))
    app.add_handler(CommandHandler("stats", cmd_stats_pair))
    app.add_handler(CommandHandler("resetbudget", cmd_resetbudget_handler))
    app.add_handler(CommandHandler("rb", cmd_resetbudget_handler))
    app.add_handler(CommandHandler("closeall", cmd_closeall_handler))
    app.add_handler(CommandHandler("export", cmd_export_handler))
    app.add_handler(CommandHandler("exportprices", cmd_exportprices_handler))

    # Button callback handler
    app.add_handler(CallbackQueryHandler(button_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    print("TG Bot: /start /menu /status /scan /live")
    print("Pair:   /trade <CA>  /cancel  /close  /pos  /stats")

    while True:
        await asyncio.sleep(60)


# ============== MODE RUNNERS ==============

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
    print("Commands: /start /menu /status /scan /live")
    print()
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

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


def reset_live():
    """Reset live trading data."""
    import os
    import shutil

    files = [
        "live_positions.json",
        "live_trades.csv",
        "trading_stats.json",
        "signals.json",
    ]

    for f in files:
        if os.path.exists(f):
            os.remove(f)
            print(f"Deleted: {f}")

    # Archive old logs instead of deleting
    if os.path.exists("logs"):
        archive_name = f"logs_archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.move("logs", archive_name)
        print(f"Archived logs -> {archive_name}/")

    print("\nLive data reset complete - fresh start!")


async def run_live():
    """Run scanner with LIVE trading."""
    from scanner import run_scanner
    from live_trader import (
        run_live_manager, run_tg_position_updates, WALLET_UTILIZATION,
        MAX_OPEN_TRADES, MAX_SLIPPAGE_BPS, MAX_TRADES_PER_TOKEN,
        MC_STALL_MINS, VOL_DECAY_THRESHOLD, MIN_FEE_RESERVE,
        CONFIRMATION_COUNT, CONFIRMATION_WINDOW_SECS, MIN_BUY_RATIO, MIN_SIGNAL_LIQUIDITY,
        SIGNAL_MIN_AGE_MINS, SIGNAL_MAX_AGE_MINS, DIP_FROM_PEAK_PCT, MIN_DIP_PCT
    )

    print("=" * 50)
    print("!!! LIVE TRADING MODE !!!")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Scanner:  30s interval")
    print("Trader:   15s position check (FAST)")
    print("TG:       2min position updates")
    print()
    print("=== ENTRY FILTERS (DIP BUYING) ===")
    print(f"Signal aging:      {SIGNAL_MIN_AGE_MINS}m min (max {SIGNAL_MAX_AGE_MINS}m)")
    print(f"Dip required:      {DIP_FROM_PEAK_PCT}% from peak (min {MIN_DIP_PCT}%)")
    print(f"Min buy ratio:     {MIN_BUY_RATIO}x")
    print(f"Min liquidity:     ${MIN_SIGNAL_LIQUIDITY:,.0f}")
    print()
    print("=== POSITION MANAGEMENT ===")
    print(f"Sizing:            dynamic ({WALLET_UTILIZATION*100:.0f}% wallet / {MAX_OPEN_TRADES} trades)")
    print(f"Fee reserve:       {MIN_FEE_RESERVE} SOL")
    print(f"Max open trades:   {MAX_OPEN_TRADES}")
    print(f"Slippage:          {MAX_SLIPPAGE_BPS/100}%")
    print()
    print("=== EXIT CONDITIONS (AGGRESSIVE) ===")
    print(f"Max trades/token:  {MAX_TRADES_PER_TOKEN}")
    print(f"Trailing stops:    8%→4% (<3m) | 6%→3% (3-8m) | 5%→2% (>8m)")
    print(f"Profit lock:       Exit at +3% if max was +6%")
    print(f"Emergency save:    Exit at 0% if max was +10%")
    print(f"Vol decay:         {VOL_DECAY_THRESHOLD*100:.0f}% of entry vol AND -5% pnl")
    print(f"Dump exit:         sells > 2x buys AND -10% pnl")
    print(f"MC stall:          {MC_STALL_MINS}m if MC -15% from peak")
    print()
    print("THIS USES REAL MONEY!")
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

    await asyncio.gather(
        run_scanner(interval_secs=30, send_to_tg=True, live_mode=True),
        run_live_manager(interval_secs=15),
        run_tg_position_updates(interval_secs=120),
        run_bot(),
    )


async def run_pair():
    """Run pair trader (manual CA trading) + scanner data collection."""
    from scanner import run_scanner
    from pair_trader import run_pair_trader

    print("=" * 50)
    print("PAIR TRADER MODE")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Slots:    8 manual CA slots (85% wallet)")
    print("Trader:   30s check interval")
    print("Scanner:  data collection only (no auto-buys)")
    print()
    print("Commands: /trade <CA>  /cancel  /close  /pos  /stats")
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

    await asyncio.gather(
        run_pair_trader(),
        run_scanner(interval_secs=60, send_to_tg=False, live_mode=False),
        run_bot(),
    )


# ============== NANO BOT COMMANDS ==============

async def nano_cmd_pos(update, ctx):
    from nano_trader import cmd_nano_pos
    msg = await cmd_nano_pos()
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)


async def nano_cmd_stats(update, ctx):
    from nano_trader import cmd_nano_stats
    msg = await cmd_nano_stats()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def nano_cmd_close(update, ctx):
    if not ctx.args:
        await update.message.reply_text("Usage: /close <symbol> or /close <slot_number>")
        return
    from nano_trader import cmd_nano_close
    msg = await cmd_nano_close(ctx.args[0])
    await update.message.reply_text(msg, parse_mode="Markdown")


async def nano_cmd_watch(update, ctx):
    from nano_trader import cmd_nano_watch
    msg = await cmd_nano_watch()
    await update.message.reply_text(msg, parse_mode="Markdown", disable_web_page_preview=True)


async def nano_cmd_cancel(update, ctx):
    if not ctx.args:
        await update.message.reply_text("Usage: /cancel <symbol>")
        return
    from nano_trader import cmd_nano_cancel
    msg = await cmd_nano_cancel(ctx.args[0])
    await update.message.reply_text(msg, parse_mode="Markdown")


async def nano_cmd_resetbudget(update, ctx):
    from nano_trader import cmd_nano_resetbudget
    msg = await cmd_nano_resetbudget()
    await update.message.reply_text(msg, parse_mode="Markdown")


async def nano_cmd_export(update, ctx):
    import os
    from nano_trader import NANO_TRADES_FILE
    if not os.path.exists(NANO_TRADES_FILE):
        await update.message.reply_text("No trades yet — nano_trades.csv doesn't exist")
        return
    with open(NANO_TRADES_FILE, "rb") as f:
        await update.message.reply_document(document=f, filename="nano_trades.csv",
                                            caption="Nano trade history CSV")


async def nano_cmd_start(update, ctx):
    msg = (
        "*NANO TRADER*\n\n"
        "Auto-buys new pump.fun pairs at $10K MC.\n"
        "_3-step DCA | Fixed +88% TP | Rug-speed filter_\n\n"
        "*/pos* — open slots + live PnL\n"
        "*/watch* — full watchlist (tokens being tracked)\n"
        "*/stats* — budget & profit\n"
        "*/close <sym>* — manual sell\n"
        "*/cancel <sym>* — remove from watchlist\n"
        "*/resetbudget* — reinit budget\n"
        "*/export* — get trades CSV\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def run_nano_bot():
    """Run nano trader Telegram bot (separate key)."""
    from config import NANO_BOT_TOKEN
    from telegram.ext import Application, CommandHandler as CH

    if not NANO_BOT_TOKEN:
        print("[nano] No NANO_BOT_TOKEN set — TG bot disabled")
        return

    app = Application.builder().token(NANO_BOT_TOKEN).build()
    app.add_handler(CH("start",       nano_cmd_start))
    app.add_handler(CH("pos",         nano_cmd_pos))
    app.add_handler(CH("p",           nano_cmd_pos))
    app.add_handler(CH("watch",       nano_cmd_watch))
    app.add_handler(CH("w",           nano_cmd_watch))
    app.add_handler(CH("stats",       nano_cmd_stats))
    app.add_handler(CH("close",       nano_cmd_close))
    app.add_handler(CH("cancel",      nano_cmd_cancel))
    app.add_handler(CH("resetbudget", nano_cmd_resetbudget))
    app.add_handler(CH("rb",          nano_cmd_resetbudget))
    app.add_handler(CH("export",      nano_cmd_export))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    print("[nano] TG bot running")
    while True:
        await asyncio.sleep(60)


async def run_nano():
    """Run nano trader — automated new-pair DCA bot."""
    from nano_trader import run_nano_trader, run_nano_scanner

    print("=" * 50)
    print("NANO TRADER MODE")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("Slots:    2 auto slots (35% wallet)")
    print("Entry:    $10K MC | TP: +88% fixed")
    print("DCA:      -50% / -80% from entry")
    print("Filter:   ≥20 min fall (rug-speed check)")
    print()
    print("Commands: /pos  /stats  /close  /cancel  /export")
    print("Ctrl+C to stop")
    print("=" * 50)
    print()

    await asyncio.gather(
        run_nano_trader(interval_secs=30),
        run_nano_scanner(interval_secs=60),
        run_nano_bot(),
    )


def main():
    parser = argparse.ArgumentParser(description="Meme Scanner + Sim/Live Manager")
    parser.add_argument("--scan", action="store_true", help="Run scanner only")
    parser.add_argument("--sim", action="store_true", help="Run sim manager only")
    parser.add_argument("--live", action="store_true", help="Run with LIVE trading")
    parser.add_argument("--pair", action="store_true", help="Run pair trader (manual CA trading)")
    parser.add_argument("--nano", action="store_true", help="Run nano trader (auto new-pair DCA)")
    parser.add_argument("--reset", action="store_true", help="Reset all data")
    parser.add_argument("--reset-live", action="store_true", help="Reset live data only")
    parser.add_argument("--no-tg", action="store_true", help="Disable Telegram output")
    args = parser.parse_args()

    if args.reset:
        reset_all()
    elif args.reset_live:
        reset_live()
    elif args.scan:
        asyncio.run(run_scanner_only())
    elif args.sim:
        asyncio.run(run_sim_only())
    elif args.live:
        try:
            asyncio.run(run_live())
        except KeyboardInterrupt:
            print("\nLive trading stopped")
    elif args.pair:
        try:
            asyncio.run(run_pair())
        except KeyboardInterrupt:
            print("\nPair trader stopped")
    elif args.nano:
        try:
            asyncio.run(run_nano())
        except KeyboardInterrupt:
            print("\nNano trader stopped")
    else:
        try:
            asyncio.run(run_all(no_tg=args.no_tg))
        except KeyboardInterrupt:
            print("\nStopped")


if __name__ == "__main__":
    main()
