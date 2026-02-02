"""Meme Radar - Fresh Solana coins with TikTok correlation."""

import asyncio
import argparse
import logging
from datetime import datetime

# Suppress noisy logs
logging.getLogger("TikTokApi").setLevel(logging.CRITICAL)
logging.getLogger("TikTokApi.tiktok").setLevel(logging.CRITICAL)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("playwright").setLevel(logging.WARNING)

import database as db
from config import SCAN_INTERVAL_MINS, DIGEST_HOUR, SIM_UPDATE_MINS


async def scan_fresh_coins():
    """Scan for fresh coins and alert on good ones."""
    from new_pairs import get_fresh_coins
    from telegram_bot import send_alert
    from trade_sim import add_trade, update_all_trades

    print(f"[{datetime.now().strftime('%H:%M')}] Scanning fresh coins...")

    try:
        coins = await get_fresh_coins(30)  # Last 30 min

        if coins:
            a_rated = [c for c in coins if c.total_score >= 80]
            b_rated = [c for c in coins if 60 <= c.total_score < 80]
            tt_match = [c for c in coins if c.has_tiktok_match]
            good_entries = [c for c in coins if c.good_entry]

            print(f"  Found: {len(coins)} total, {len(a_rated)} A, {len(b_rated)} B, {len(tt_match)} TT, {len(good_entries)} ENTRY")

            # TIGHTER CRITERIA: Only add to simulation if meets strict entry criteria
            # Must be: good_entry=True (recovering, in MC range, buy pressure)
            # OR: A-rated with good safety
            sim_coins = []
            for c in coins:
                mc = c.market_cap or 0

                # Strict criteria for simulation
                if c.good_entry and not c.is_dumping:
                    sim_coins.append(c)
                elif c.total_score >= 80 and c.safety_score >= 60 and 10000 < mc < 150000:
                    sim_coins.append(c)
                elif c.has_tiktok_match and c.is_recovering and not c.has_bundles:
                    sim_coins.append(c)

            for c in sim_coins[:3]:  # Limit to 3 per scan
                add_trade(c)

            if sim_coins:
                print(f"  Sim added: {len(sim_coins[:3])} coins")
                # Update sim immediately after new entries
                await update_all_trades()

            # Alert for A-rated or TikTok matches
            alert_coins = a_rated + [c for c in tt_match if c not in a_rated]
            if alert_coins:
                await send_alert(alert_coins[:5])
                print(f"  Alert sent: {len(alert_coins[:5])} coins")
        else:
            print("  No fresh coins")

    except Exception as e:
        print(f"  Error: {e}")


async def morning_digest():
    """Send morning digest with menu."""
    from telegram_bot import send_startup

    print(f"[{datetime.now().strftime('%H:%M')}] MORNING DIGEST")

    try:
        await send_startup()
        print("  Menu sent")
    except Exception as e:
        print(f"  Error: {e}")


async def run_once():
    """Single scan test."""
    from new_pairs import get_fresh_coins, format_fresh_coin
    from telegram_bot import send_startup

    print("=== MEME RADAR ===\n")
    db.init_db()

    print("Scanning fresh coins...")
    coins = await get_fresh_coins(60)

    if coins:
        print(f"\nFound {len(coins)} fresh coins:\n")
        for i, c in enumerate(coins[:8], 1):
            print(format_fresh_coin(c, i))
            print()
    else:
        print("No fresh coins found")

    await send_startup()
    print("\nMenu sent to Telegram!")


async def run_full():
    """Full bot with background scanning."""
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler
    from telegram import Update
    from telegram_bot import cmd_start, handle_callback, send_startup
    from config import TELEGRAM_BOT_TOKEN

    db.init_db()

    print("=== MEME RADAR ===")
    print(f"Scan interval: {SCAN_INTERVAL_MINS} min")
    print(f"Daily digest: {DIGEST_HOUR}:00")
    print("Interactive buttons - no spam")
    print("Ctrl+C to stop\n")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

    # Send startup menu
    await send_startup()
    print("Startup menu sent!")

    last_digest_date = None
    last_sim_update = None

    # Start sim loop as a separate task
    from trade_sim import run_sim_loop
    sim_task = asyncio.create_task(run_sim_loop())

    try:
        while True:
            now = datetime.now()

            if now.hour == DIGEST_HOUR and last_digest_date != now.date():
                last_digest_date = now.date()
                await morning_digest()
            else:
                await scan_fresh_coins()

            if SIM_UPDATE_MINS > 0:
                if last_sim_update is None or (now - last_sim_update).total_seconds() >= SIM_UPDATE_MINS * 60:
                    from trade_sim import update_all_trades
                    await update_all_trades()
                    last_sim_update = datetime.now()

            await asyncio.sleep(SCAN_INTERVAL_MINS * 60)

    except (KeyboardInterrupt, SystemExit):
        print("\nStopping...")
    finally:
        sim_task.cancel()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Single scan")
    parser.add_argument("--bot", action="store_true", help="Bot only")
    parser.add_argument("--digest", action="store_true", help="Send digest now")
    args = parser.parse_args()

    if args.test:
        asyncio.run(run_once())
    elif args.digest:
        db.init_db()
        asyncio.run(morning_digest())
    elif args.bot:
        from telegram_bot import run_bot
        run_bot()
    else:
        asyncio.run(run_full())


if __name__ == "__main__":
    main()
