"""Telegram bot - Single interactive menu with buttons. No spam."""

import asyncio
import logging
from datetime import datetime
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Suppress noisy logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)


def fmt_num(n: float) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.0f}K"
    return f"{n:.0f}"


# ============ KEYBOARDS ============

def get_main_keyboard() -> InlineKeyboardMarkup:
    """Main menu buttons."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Fresh Coins", callback_data="fresh"),
            InlineKeyboardButton("Hot Trends", callback_data="trends"),
        ],
        [
            InlineKeyboardButton("Quick Scan", callback_data="scan"),
            InlineKeyboardButton("Sim Stats", callback_data="simstats"),
        ],
        [
            InlineKeyboardButton("Status", callback_data="status"),
            InlineKeyboardButton("Refresh", callback_data="menu"),
        ]
    ])


def get_back_keyboard() -> InlineKeyboardMarkup:
    """Back to menu button."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("< Menu", callback_data="menu")]
    ])


# ============ FORMATTERS ============

async def format_main_menu() -> str:
    """Format the main menu message."""
    now = datetime.now().strftime("%H:%M")
    return f"""*MEME RADAR* {now}
================

Solana meme coins + TikTok
Fresh pairs with ratings

Tap a button below"""


async def format_fresh_coins() -> str:
    """Format fresh coins with ratings."""
    from new_pairs import get_fresh_coins, format_fresh_coin, format_rating

    now = datetime.now().strftime("%H:%M")
    msg = f"*FRESH COINS* {now}\n================\n\n"

    try:
        coins = await get_fresh_coins(60)

        if not coins:
            return msg + "No fresh coins right now.\n\nTry again soon."

        # Summary
        a_rated = sum(1 for c in coins if c.total_score >= 80)
        b_rated = sum(1 for c in coins if 60 <= c.total_score < 80)
        tt_match = sum(1 for c in coins if c.has_tiktok_match)

        if a_rated:
            msg += f"[A] {a_rated} top rated\n"
        if b_rated:
            msg += f"[B] {b_rated} good\n"
        if tt_match:
            msg += f"TT {tt_match} TikTok match\n"
        msg += "\n"

        # List coins
        for i, c in enumerate(coins[:8], 1):
            msg += format_fresh_coin(c, i) + "\n\n"

        msg += "================\n"
        msg += "[A]=80+ [B]=60+ | TT=TikTok"

    except Exception as e:
        msg += f"Error: {e}"

    return msg


async def format_trends() -> str:
    """Format cross-platform trends."""
    now = datetime.now().strftime("%H:%M")
    msg = f"*HOT TRENDS* {now}\n================\n\n"

    try:
        from trends import get_google_trends, get_dexscreener_gainers
        import database as db

        # Google Trends
        google = get_google_trends()
        if google:
            msg += "*Google*\n"
            for i, t in enumerate(google[:6], 1):
                msg += f"{i}. {t.term}\n"
            msg += "\n"

        # DexScreener gainers
        gainers = await get_dexscreener_gainers()
        if gainers:
            msg += "*Gainers 1h*\n"
            for i, t in enumerate(gainers[:5], 1):
                msg += f"{i}. {t.term} +{t.score:.0f}%\n"
            msg += "\n"

        # TikTok from DB
        db.init_db()
        tags = db.get_trending_hashtags(hours=6, limit=6)
        if tags:
            msg += "*TikTok*\n"
            for i, t in enumerate(tags, 1):
                msg += f"{i}. #{t['tag']}\n"
            msg += "\n"

        msg += "================\nCoins matched to trends"

    except Exception as e:
        msg += f"Error: {e}"

    return msg


async def format_status() -> str:
    """Format status info."""
    import database as db
    from config import DIGEST_HOUR, SCAN_INTERVAL_MINS

    now = datetime.now().strftime("%H:%M")
    msg = f"*STATUS* {now}\n================\n\n"

    try:
        db.init_db()
        conn = db.get_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM sounds")
        sounds = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM videos")
        videos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM hashtags")
        hashtags = cur.fetchone()[0]

        conn.close()

        msg += f"Sounds: {sounds}\n"
        msg += f"Videos: {videos}\n"
        msg += f"Hashtags: {hashtags}\n"
        msg += f"\nDigest: {DIGEST_HOUR}:00\n"
        msg += f"Scan: every {SCAN_INTERVAL_MINS} min\n"

        msg += "\n================\nBot running"

    except Exception as e:
        msg += f"Error: {e}"

    return msg


async def format_sim_stats() -> str:
    """Format trade simulation stats."""
    from trade_sim import format_stats_message, update_all_trades

    now = datetime.now().strftime("%H:%M")
    msg = f"*SIM STATS* {now}\n================\n\n"

    try:
        # Update trades first
        await update_all_trades()
        msg += format_stats_message()

    except Exception as e:
        msg += f"Error: {e}"

    return msg


async def format_scan_result() -> str:
    """Run quick scan and format results."""
    from new_pairs import get_fresh_coins, format_rating, md_safe

    now = datetime.now().strftime("%H:%M")
    msg = f"*SCAN* {now}\n================\n\n"

    try:
        coins = await get_fresh_coins(30)

        if not coins:
            msg += "No new coins in last 30 min.\n"
        else:
            msg += f"Found {len(coins)} coins:\n\n"
            for i, c in enumerate(coins[:6], 1):
                rating = format_rating(c.total_score)
                age = f"{c.age_minutes}m"
                trend = f" TT" if c.has_tiktok_match else ""
                mc = ""
                if c.market_cap and c.market_cap > 0:
                    if c.market_cap >= 1_000_000:
                        mc = f" ${c.market_cap/1_000_000:.1f}M"
                    else:
                        mc = f" ${c.market_cap/1000:.0f}K"
                msg += f"[{rating}] *${md_safe(c.symbol)}* {age}{mc} +{c.price_change_5m:.0f}%{trend}\n"

        msg += "\n================\n'Fresh Coins' for details"

    except Exception as e:
        msg += f"Error: {e}"

    return msg


# ============ HANDLERS ============

async def send_or_edit(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                       text: str, keyboard: InlineKeyboardMarkup):
    """Send new message or edit existing one."""
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        else:
            await update.message.reply_text(
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
    except BadRequest:
        # Fallback to plain text if markdown entities are malformed.
        fallback_text = text.replace("`", "")
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=fallback_text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
        else:
            await update.message.reply_text(
                text=fallback_text,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
    except Exception:
        if update.effective_message:
            await update.effective_message.reply_text(
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show main menu."""
    text = await format_main_menu()
    await send_or_edit(update, ctx, text, get_main_keyboard())


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle button presses."""
    query = update.callback_query
    await query.answer()

    action = query.data

    if action == "menu":
        text = await format_main_menu()
        keyboard = get_main_keyboard()

    elif action == "fresh":
        await query.edit_message_text("Loading...", parse_mode=ParseMode.MARKDOWN)
        text = await format_fresh_coins()
        keyboard = get_back_keyboard()

    elif action == "trends":
        await query.edit_message_text("Loading...", parse_mode=ParseMode.MARKDOWN)
        text = await format_trends()
        keyboard = get_back_keyboard()

    elif action == "scan":
        await query.edit_message_text("Scanning...", parse_mode=ParseMode.MARKDOWN)
        text = await format_scan_result()
        keyboard = get_back_keyboard()

    elif action == "status":
        text = await format_status()
        keyboard = get_back_keyboard()

    elif action == "simstats":
        await query.edit_message_text("Loading sim stats...", parse_mode=ParseMode.MARKDOWN)
        text = await format_sim_stats()
        keyboard = get_back_keyboard()

    else:
        text = await format_main_menu()
        keyboard = get_main_keyboard()

    await send_or_edit(update, ctx, text, keyboard)


# ============ ALERTS ============

async def send_alert(coins: list) -> bool:
    """Send alert for high-rated fresh coins."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    if not coins:
        return False

    good_coins = [c for c in coins if c.total_score >= 60]
    if not good_coins:
        return False

    try:
        from new_pairs import format_fresh_coin

        now = datetime.now().strftime("%H:%M")
        msg = f"*NEW SIGNAL* {now}\n================\n\n"

        for i, c in enumerate(good_coins[:5], 1):
            msg += format_fresh_coin(c, i) + "\n\n"

        msg += "================"

        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_main_keyboard(),
            disable_web_page_preview=True
        )
        return True

    except Exception as e:
        print(f"Alert error: {e}")
        return False


async def send_startup() -> bool:
    """Send startup message with menu."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    try:
        text = await format_main_menu()
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=get_main_keyboard(),
            disable_web_page_preview=True
        )
        return True
    except:
        return False


# Legacy functions for compatibility
async def send_digest(memes: list) -> bool:
    """Legacy: send digest."""
    return await send_startup()


async def send_coins_alert(coins) -> bool:
    """Legacy: send coins."""
    return await send_startup()


def run_bot():
    """Run the bot."""
    if not TELEGRAM_BOT_TOKEN:
        print("No token")
        return

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))

    print("Bot running with buttons...")
    app.run_polling()


if __name__ == "__main__":
    run_bot()
