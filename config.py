"""Configuration for TikTok Meme Radar."""

import os
from dotenv import load_dotenv

load_dotenv()

# Telegram settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
DIGEST_HOUR = int(os.getenv("DIGEST_HOUR", "7"))  # 7 AM daily digest

# Scraping settings
SCAN_INTERVAL_MINS = int(os.getenv("SCAN_INTERVAL_MINS", "5"))  # Every 5 mins
VIDEOS_PER_SEARCH = 50  # More videos = more sounds to analyze
SIM_UPDATE_MINS = int(os.getenv("SIM_UPDATE_MINS", "10"))  # Update sim stats

# Trade data cleanup
TRADE_LOG_KEEP = int(os.getenv("TRADE_LOG_KEEP", "20"))  # Keep latest rotated logs
# Database
DB_PATH = "meme_radar.db"

# Detection settings
MAX_COIN_AGE_HOURS = 48  # Only show coins younger than this (early gems)
MIN_LIQUIDITY = 5000  # Minimum $5k liquidity
TOP_N = 10  # Top memes to show

# Trading / scoring thresholds
# Ideal entry: 15-20K MC, volume direction decides
ENTRY_MIN_MC = int(os.getenv("ENTRY_MIN_MC", "15000"))
ENTRY_MAX_MC = int(os.getenv("ENTRY_MAX_MC", "25000"))
MAX_ENTRY_MC = int(os.getenv("MAX_ENTRY_MC", "100000"))  # Absolute max
