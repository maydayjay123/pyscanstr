"""
Run scanner with sim or live trading.

Usage:
    python run.py          # Scanner + Sim (default)
    python run.py --live   # Scanner + LIVE TRADING
    python run.py --scan   # Scanner only
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
    """Main menu with buttons."""
    keyboard = [
        [
            InlineKeyboardButton("📊 Positions", callback_data="positions"),
            InlineKeyboardButton("📡 Tracking", callback_data="tracking"),
        ],
        [
            InlineKeyboardButton("📈 Stats", callback_data="stats"),
            InlineKeyboardButton("📜 History", callback_data="history"),
        ],
        [
            InlineKeyboardButton("💰 Wallet", callback_data="wallet"),
            InlineKeyboardButton("🔄 Sync", callback_data="sync"),
        ],
        [
            InlineKeyboardButton("🔍 Scan", callback_data="scan"),
            InlineKeyboardButton("🚨 SELL ALL", callback_data="sell_all_confirm"),
        ],
        [
            InlineKeyboardButton("💀 SELL WALLET", callback_data="sell_wallet_confirm"),
            InlineKeyboardButton("📋 Export CSV", callback_data="export_csv"),
        ],
        [
            InlineKeyboardButton("📉 Price Action", callback_data="export_price_action"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_position_menu():
    """Position view buttons."""
    keyboard = [
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="positions"),
            InlineKeyboardButton("📊 Detailed", callback_data="positions_detail"),
        ],
        [
            InlineKeyboardButton("🏠 Menu", callback_data="menu"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_back_menu():
    """Simple back to menu button."""
    keyboard = [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
    return InlineKeyboardMarkup(keyboard)


# ============== COMMAND HANDLERS ==============

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command - show main menu."""
    msg = "*🚀 MEME TRADER BOT*\n\n"
    msg += "Select an option below:"

    await update.message.reply_text(
        msg,
        parse_mode="Markdown",
        reply_markup=get_main_menu()
    )


async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /menu command."""
    msg = "*🚀 MEME TRADER*\n\n"
    msg += "Select an option:"

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
        msg = "*🚀 MEME TRADER*\n\n"
        msg += "Select an option:"
        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_main_menu()
        )

    elif data == "positions":
        from live_trader import load_positions, get_token_metrics, get_token_balance_full, compute_pnl_sol, get_wallet_pubkey

        positions = load_positions()
        open_pos = [p for p in positions if p.status == "OPEN"]

        if not open_pos:
            msg = "*📊 POSITIONS*\n\n"
            msg += "No open positions"
        else:
            msg = f"*📊 POSITIONS* ({len(open_pos)} open)\n\n"
            wallet = get_wallet_pubkey()

            total_pnl = 0.0
            for p in open_pos:
                metrics = await get_token_metrics(p.token_address)
                entry = datetime.fromisoformat(p.entry_time)
                held_mins = (datetime.now() - entry).total_seconds() / 60

                if metrics and metrics.price > 0:
                    # SOL-based PnL (same as detailed view + manage_positions)
                    total_sol = p.dca_total_sol if p.dca_total_sol and p.dca_total_sol > 0 else p.sol_amount
                    if metrics.price_sol > 0:
                        _, ui_bal = await get_token_balance_full(wallet, p.token_address)
                        pnl = compute_pnl_sol(total_sol, ui_bal, metrics.price_sol)
                    else:
                        pnl = p.pnl_percent if p.pnl_percent != 0 else 0.0

                    total_pnl += pnl

                    # Status emoji
                    if pnl >= 10:
                        emoji = "🟢"
                    elif pnl >= 0:
                        emoji = "🟡"
                    else:
                        emoji = "🔴"

                    mc_str = f"{metrics.mc/1000:.0f}K" if metrics.mc < 1_000_000 else f"{metrics.mc/1_000_000:.1f}M"
                    msg += f"{emoji} `{p.symbol}` *{pnl:+.1f}%*\n"
                    msg += f"   MC: {mc_str} | {metrics.buy_ratio:.1f}x | {held_mins:.0f}m\n\n"
                else:
                    msg += f"⚪ `{p.symbol}` (no data)\n\n"

            msg += f"━━━━━━━━━━━━━━\n"
            msg += f"*Total: {total_pnl:+.1f}%*"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_position_menu()
        )

    elif data == "positions_detail":
        from live_trader import format_live_status_detailed

        msg = await format_live_status_detailed()
        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_markup=get_position_menu()
        )

    elif data == "scan":
        from scanner import scan, format_signal_msg

        await query.edit_message_text("🔍 *Scanning...*", parse_mode="Markdown")

        signals = await scan()
        if signals:
            msg = format_signal_msg(signals)
            if not msg:
                msg = "No signals found"
        else:
            msg = "No tokens found"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_markup=get_back_menu()
        )

    elif data == "stats":
        from live_trader import (
            get_session_stats, format_session_summary,
            format_alltime_stats, SESSION_ID
        )

        stats = get_session_stats()

        # Session stats
        msg = "*📊 CURRENT SESSION*\n"
        msg += f"ID: `{SESSION_ID[:15]}...`\n\n"

        # Balance tracking
        if stats.starting_balance > 0:
            change_sol = stats.wallet_change_sol
            change_pct = stats.wallet_change_pct
            bal_emoji = "🟢" if change_sol >= 0 else "🔴"
            msg += f"*Start:* `{stats.starting_balance:.4f}` SOL\n"
            msg += f"*Now:*   `{stats.current_balance:.4f}` SOL\n"
            msg += f"{bal_emoji} *Change:* `{change_sol:+.4f}` SOL ({change_pct:+.1f}%)\n\n"

        msg += f"Buys: {stats.buys} | Sells: {stats.sells}\n"
        msg += f"W/L: {stats.wins}/{stats.losses}"
        if stats.wins + stats.losses > 0:
            msg += f" ({stats.win_rate:.0f}%)"
        msg += "\n\n"
        msg += f"SOL In:  `{stats.sol_in:.6f}`\n"
        msg += f"SOL Out: `{stats.sol_out:.6f}`\n"
        msg += f"*Net: {stats.net_pnl_sol:+.6f} SOL*"
        if stats.sol_in > 0:
            msg += f" ({stats.net_pnl_pct:+.1f}%)"
        msg += "\n\n"

        if stats.best_trade_symbol:
            msg += f"Best: {stats.best_trade_symbol} +{stats.best_trade_pnl:.1f}%\n"
        if stats.worst_trade_symbol:
            msg += f"Worst: {stats.worst_trade_symbol} {stats.worst_trade_pnl:.1f}%\n"

        # All-time stats
        msg += "\n━━━━━━━━━━━━━━\n"
        msg += format_alltime_stats()

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "wallet":
        from live_trader import get_wallet_pubkey, get_sol_balance, load_positions

        wallet = get_wallet_pubkey()
        if wallet:
            balance = await get_sol_balance(wallet)
            positions = load_positions()
            open_pos = [p for p in positions if p.status == "OPEN"]
            used = sum(p.sol_amount for p in open_pos)

            msg = "*💰 WALLET*\n\n"
            msg += f"Address: `{wallet[:8]}...{wallet[-4:]}`\n\n"
            msg += f"Balance: *{balance:.4f} SOL*\n"
            msg += f"In Trades: {used:.4f} SOL\n"
            msg += f"Available: {balance - used:.4f} SOL\n"
        else:
            msg = "*💰 WALLET*\n\n"
            msg += "⚠️ No wallet configured"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "sync":
        from live_trader import (
            load_positions, get_wallet_pubkey, get_token_metrics,
            get_all_token_accounts, sync_positions
        )

        await query.edit_message_text("???? *Syncing with blockchain...*", parse_mode="Markdown")

        wallet = get_wallet_pubkey()
        msg = "*???? SYNC RESULTS*\n\n"

        # Use canonical sync (fixes OPEN/CLOSED mismatches)
        sync_result = await sync_positions()
        closed_syms = sync_result.get("closed_syms", [])
        reopened_syms = sync_result.get("reopened_syms", [])

        if closed_syms or reopened_syms:
            msg += "*Tracked Positions:*\n"
            if closed_syms:
                msg += f"??? Closed: {', '.join([f'`{s}`' for s in closed_syms])}\n"
            if reopened_syms:
                msg += f"??? Re-opened: {', '.join([f'`{s}`' for s in reopened_syms])}\n"
            msg += "\n"

        # Find UNTRACKED tokens in wallet
        msg += "*Wallet Scan:*\n"
        positions = load_positions()
        open_pos = [p for p in positions if p.status == "OPEN"]
        tracked_addresses = {p.token_address for p in open_pos}

        all_tokens = await get_all_token_accounts(wallet)
        untracked = []

        for token in all_tokens:
            mint = token["mint"]
            if mint not in tracked_addresses and mint != "So11111111111111111111111111111111111111112":
                metrics = await get_token_metrics(mint)
                if metrics and metrics.mc > 0:
                    untracked.append({
                        "mint": mint,
                        "amount": token["amount"],
                        "symbol": "???",
                        "mc": metrics.mc,
                        "price": metrics.price
                    })

        if untracked:
            msg += f"?????? *{len(untracked)} UNTRACKED tokens!*\n"
            for t in untracked[:5]:  # Show max 5
                mc_str = f"{t['mc']/1000:.0f}K" if t['mc'] < 1_000_000 else f"{t['mc']/1_000_000:.1f}M"
                msg += f"  ??? MC:{mc_str} `{t['mint'][:8]}...`\n"
            if len(untracked) > 5:
                msg += f"  _...and {len(untracked)-5} more_\n"
            msg += "\n_These tokens are in wallet but not tracked!_"
        else:
            msg += "??? No untracked tokens"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )
    elif data == "tracking":
        from live_trader import get_tracked_signals_status, SIGNAL_MIN_AGE_MINS, DIP_FROM_PEAK_PCT

        msg = get_tracked_signals_status()
        msg += f"\n\n_Buy after {SIGNAL_MIN_AGE_MINS}m + {DIP_FROM_PEAK_PCT}% dip_"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "history":
        from live_trader import load_positions

        positions = load_positions()
        closed = [p for p in positions if p.status == "CLOSED"]

        msg = "*📜 TRADE HISTORY*\n\n"

        if closed:
            # Sort by exit time, most recent first
            closed.sort(key=lambda p: p.exit_time, reverse=True)

            for p in closed[:15]:  # Show last 15 trades
                emoji = "🟢" if p.pnl_percent > 0 else "🔴"
                exit_time = datetime.fromisoformat(p.exit_time).strftime("%m/%d %H:%M")
                entry_time = datetime.fromisoformat(p.entry_time)
                exit_dt = datetime.fromisoformat(p.exit_time)
                held_mins = (exit_dt - entry_time).total_seconds() / 60

                mc_str = f"{p.entry_mc/1000:.0f}K" if p.entry_mc < 1_000_000 else f"{p.entry_mc/1_000_000:.1f}M"

                msg += f"{emoji} `{p.symbol}` *{p.pnl_percent:+.1f}%*\n"
                msg += f"   {exit_time} | {held_mins:.0f}m | MC:{mc_str}\n"

                # Show max vs exit for analysis
                if p.max_pnl_percent > p.pnl_percent + 5:
                    msg += f"   ⚠️ Max was +{p.max_pnl_percent:.0f}%\n"

            if len(closed) > 15:
                msg += f"\n_...and {len(closed)-15} more trades_"
        else:
            msg += "No trade history yet"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "export_csv":
        from live_trader import load_positions
        import io

        positions = load_positions()
        closed = [p for p in positions if p.status == "CLOSED"]
        open_pos = [p for p in positions if p.status == "OPEN"]

        if not closed and not open_pos:
            await query.edit_message_text("No trade data to export", reply_markup=get_back_menu())
        else:
            # Build CSV - comprehensive analytical export
            headers = [
                "status", "symbol", "trade_type",
                "entry_time", "exit_time", "held_mins",
                "entry_mc", "max_mc", "exit_mc",
                "entry_price", "exit_price",
                "sol_invested", "sol_step1",
                "pnl_pct", "max_pnl_pct",
                "exit_reason", "dca_steps",
                "entry_vol_5m", "entry_buys_5m", "entry_sells_5m",
                "entry_buy_ratio", "entry_liquidity",
                "mc_growth_pct", "token_address", "tx_hash"
            ]
            csv_lines = [",".join(headers)]
            all_trades = closed + open_pos
            all_trades.sort(key=lambda p: p.entry_time, reverse=True)

            for p in all_trades:
                entry_dt = datetime.fromisoformat(p.entry_time)
                if p.exit_time:
                    exit_dt = datetime.fromisoformat(p.exit_time)
                    held = (exit_dt - entry_dt).total_seconds() / 60
                    exit_str = exit_dt.strftime("%Y-%m-%d %H:%M")
                else:
                    held = (datetime.now() - entry_dt).total_seconds() / 60
                    exit_str = ""

                sol_in = p.dca_total_sol if p.dca_total_sol and p.dca_total_sol > 0 else p.sol_amount
                steps = p.dca_step if p.dca_step else 1
                reason = (getattr(p, 'exit_reason', '') or "").replace(",", ";")
                entry_vol = getattr(p, 'entry_vol_5m', 0) or 0
                entry_buys = getattr(p, 'entry_buys_5m', 0) or 0
                entry_sells = getattr(p, 'entry_sells_5m', 0) or 0
                entry_ratio = getattr(p, 'entry_buy_ratio', 0) or 0
                entry_liq = getattr(p, 'entry_liquidity', 0) or 0
                mc_growth = ((p.max_mc - p.entry_mc) / p.entry_mc * 100) if p.entry_mc > 0 else 0
                exit_mc = getattr(p, 'last_mc', 0) or 0

                csv_lines.append(
                    f"{p.status},{p.symbol},{p.trade_type},"
                    f"{entry_dt.strftime('%Y-%m-%d %H:%M')},{exit_str},"
                    f"{held:.0f},{p.entry_mc:.0f},{p.max_mc:.0f},{exit_mc:.0f},"
                    f"{p.entry_price:.10f},{p.exit_price:.10f},"
                    f"{sol_in:.6f},{p.sol_amount:.6f},"
                    f"{p.pnl_percent:.2f},{p.max_pnl_percent:.2f},"
                    f"{reason},{steps},"
                    f"{entry_vol:.0f},{entry_buys},{entry_sells},"
                    f"{entry_ratio:.2f},{entry_liq:.0f},"
                    f"{mc_growth:.1f},{p.token_address},{p.tx_hash or ''}"
                )

            csv_content = "\n".join(csv_lines)

            # Send as document via TG API
            try:
                import aiohttp as aio
                csv_bytes = csv_content.encode("utf-8")
                form = aio.FormData()
                form.add_field("chat_id", str(TELEGRAM_CHAT_ID))
                form.add_field("document", csv_bytes, filename=f"trades_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", content_type="text/csv")
                form.add_field("caption", f"Trade export: {len(closed)} closed, {len(open_pos)} open")

                async with aio.ClientSession() as session:
                    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
                    async with session.post(url, data=form, timeout=15) as resp:
                        if resp.status == 200:
                            await query.edit_message_text(f"Exported {len(all_trades)} trades as CSV", reply_markup=get_back_menu())
                        else:
                            await query.edit_message_text("Export failed - TG API error", reply_markup=get_back_menu())
            except Exception as e:
                await query.edit_message_text(f"Export error: {e}", reply_markup=get_back_menu())

    elif data == "export_price_action":
        import os
        pa_file = "price_action.csv"
        if not os.path.exists(pa_file):
            await query.edit_message_text("No price action data yet - bot needs to run first", reply_markup=get_back_menu())
        else:
            try:
                import aiohttp as aio
                with open(pa_file, "rb") as f:
                    csv_bytes = f.read()

                # Count rows for caption
                line_count = csv_bytes.count(b'\n')
                size_kb = len(csv_bytes) / 1024

                form = aio.FormData()
                form.add_field("chat_id", str(TELEGRAM_CHAT_ID))
                form.add_field("document", csv_bytes, filename=f"price_action_{datetime.now().strftime('%Y%m%d_%H%M')}.csv", content_type="text/csv")
                form.add_field("caption", f"Price action: {line_count} snapshots ({size_kb:.0f}KB)")

                async with aio.ClientSession() as session:
                    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
                    async with session.post(url, data=form, timeout=30) as resp:
                        if resp.status == 200:
                            await query.edit_message_text(f"Exported {line_count} price snapshots", reply_markup=get_back_menu())
                        else:
                            await query.edit_message_text("Export failed - TG API error", reply_markup=get_back_menu())
            except Exception as e:
                await query.edit_message_text(f"Export error: {e}", reply_markup=get_back_menu())

    elif data == "sell_all_confirm":
        from live_trader import load_positions

        positions = load_positions()
        open_pos = [p for p in positions if p.status == "OPEN"]

        if not open_pos:
            msg = "*🚨 SELL ALL*\n\nNo open positions to sell"
            await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())
        else:
            msg = "*🚨 SELL ALL POSITIONS?*\n\n"
            msg += f"This will sell *{len(open_pos)}* positions:\n\n"
            for p in open_pos:
                msg += f"• `{p.symbol}` ({p.sol_amount:.4f} SOL)\n"
            msg += "\n⚠️ *This cannot be undone!*"

            keyboard = [
                [
                    InlineKeyboardButton("✅ YES, SELL ALL", callback_data="sell_all_execute"),
                    InlineKeyboardButton("❌ Cancel", callback_data="menu"),
                ],
            ]
            await query.edit_message_text(
                msg,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    elif data == "sell_all_execute":
        from live_trader import sell_all_positions

        await query.edit_message_text("🚨 *SELLING ALL POSITIONS...*", parse_mode="Markdown")

        result = await sell_all_positions("PANIC_SELL")

        msg = "*🚨 SELL ALL COMPLETE*\n\n"
        msg += f"✅ Sold: {result['sold']}\n"
        if result['failed'] > 0:
            msg += f"❌ Failed: {result['failed']}\n"
        msg += f"\n*Total PnL: {result['total_pnl']:+.1f}%*"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "sell_wallet_confirm":
        from live_trader import get_all_token_accounts, get_wallet_pubkey, load_positions

        wallet = get_wallet_pubkey()
        if not wallet:
            await query.edit_message_text("⚠️ No wallet configured", parse_mode="Markdown", reply_markup=get_back_menu())
            return

        # Count all tokens in wallet
        all_tokens = await get_all_token_accounts(wallet)
        positions = load_positions()
        tracked_addresses = {p.token_address for p in positions if p.status == "OPEN"}

        # Filter out native SOL
        sol_mint = "So11111111111111111111111111111111111111112"
        token_count = len([t for t in all_tokens if t["mint"] != sol_mint and t["amount"] > 0])
        untracked_count = len([t for t in all_tokens if t["mint"] != sol_mint and t["amount"] > 0 and t["mint"] not in tracked_addresses])

        if token_count == 0:
            msg = "*💀 SELL WALLET*\n\nNo tokens in wallet"
            await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=get_back_menu())
        else:
            msg = "*💀 SELL ALL WALLET TOKENS?*\n\n"
            msg += f"This will sell *{token_count}* tokens:\n"
            msg += f"• {len(tracked_addresses)} tracked positions\n"
            msg += f"• {untracked_count} UNTRACKED tokens\n\n"
            msg += "⚠️ *Sells EVERYTHING including untracked!*"

            keyboard = [
                [
                    InlineKeyboardButton("💀 YES, SELL WALLET", callback_data="sell_wallet_execute"),
                    InlineKeyboardButton("❌ Cancel", callback_data="menu"),
                ],
            ]
            await query.edit_message_text(
                msg,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    elif data == "sell_wallet_execute":
        from live_trader import sell_all_wallet_tokens

        await query.edit_message_text("💀 *SELLING ALL WALLET TOKENS...*", parse_mode="Markdown")

        result = await sell_all_wallet_tokens()

        msg = "*💀 WALLET CLEARED*\n\n"
        msg += f"✅ Sold: {result['sold']}\n"
        if result['failed'] > 0:
            msg += f"❌ Failed: {result['failed']}\n"
        msg += f"\n*SOL recovered: {result.get('total_sol', 0):.6f}*"

        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_back_menu()
        )

    elif data == "refresh":
        msg = "*🚀 MEME TRADER*\n\n"
        msg += f"_Updated: {datetime.now().strftime('%H:%M:%S')}_\n\n"
        msg += "Select an option:"
        await query.edit_message_text(
            msg,
            parse_mode="Markdown",
            reply_markup=get_main_menu()
        )


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

    # Button callback handler
    app.add_handler(CallbackQueryHandler(button_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    print("TG Bot: /start /menu /status /scan /live")

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


def main():
    parser = argparse.ArgumentParser(description="Meme Scanner + Sim/Live Manager")
    parser.add_argument("--scan", action="store_true", help="Run scanner only")
    parser.add_argument("--sim", action="store_true", help="Run sim manager only")
    parser.add_argument("--live", action="store_true", help="Run with LIVE trading")
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
    else:
        try:
            asyncio.run(run_all(no_tg=args.no_tg))
        except KeyboardInterrupt:
            print("\nStopped")


if __name__ == "__main__":
    main()
