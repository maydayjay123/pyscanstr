"""
Live trader - Execute real trades on Solana via Jupiter.

Mirrors sim_manager logic but with real wallet transactions.
"""

import os
import json
import asyncio
import aiohttp
import base58
import base64
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Load keys
load_dotenv("keys.env")


async def send_tg(text: str, reply_markup: dict = None) -> int:
    """Send message to Telegram with optional buttons. Returns message_id."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return 0
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                data = await resp.json()
                return data.get("result", {}).get("message_id", 0)
    except:
        return 0


async def edit_tg(message_id: int, text: str, reply_markup: dict = None) -> bool:
    """Edit an existing TG message. Returns True if successful."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not message_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": message_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                data = await resp.json()
                return data.get("ok", False)
    except:
        return False


def get_quick_buttons():
    """Quick action buttons for TG messages."""
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Positions", "callback_data": "positions"},
                {"text": "🔍 Scan", "callback_data": "scan"},
            ],
            [
                {"text": "📈 Stats", "callback_data": "stats"},
                {"text": "🏠 Menu", "callback_data": "menu"},
            ],
        ]
    }


def get_trade_buttons():
    """Buttons for trade notifications."""
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Positions", "callback_data": "positions"},
                {"text": "📈 Stats", "callback_data": "stats"},
            ],
        ]
    }

SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
MAX_POSITION_SOL = float(os.getenv("MAX_POSITION_SOL", "0.1"))
MAX_SLIPPAGE_BPS = int(float(os.getenv("MAX_SLIPPAGE_PERCENT", "5")) * 100)  # Convert to basis points
WALLET_UTILIZATION = float(os.getenv("WALLET_UTILIZATION", "0.85"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "4"))
MIN_FEE_RESERVE = float(os.getenv("MIN_FEE_RESERVE", "0.005"))  # Always keep this much SOL for fees
CONFIRMATION_COUNT = int(os.getenv("CONFIRMATION_COUNT", "4"))  # Increased from 3
CONFIRMATION_WINDOW_SECS = int(os.getenv("CONFIRMATION_WINDOW_SECS", "90"))  # Tighter window
MIN_BUY_RATIO = float(os.getenv("MIN_BUY_RATIO", "1.3"))  # Lowered to match scanner
MIN_SIGNAL_LIQUIDITY = float(os.getenv("MIN_SIGNAL_LIQUIDITY", "12000"))  # Lowered to match scanner

# TG update interval for positions
TG_POSITION_UPDATE_SECS = int(os.getenv("TG_POSITION_UPDATE_SECS", "120"))  # 2 mins

# Signal aging - wait before buying
SIGNAL_MIN_AGE_MINS = int(os.getenv("SIGNAL_MIN_AGE_MINS", "10"))  # Wait 10 mins after first signal
SIGNAL_MAX_AGE_MINS = int(os.getenv("SIGNAL_MAX_AGE_MINS", "60"))  # Forget signals after 60 mins
DIP_FROM_PEAK_PCT = float(os.getenv("DIP_FROM_PEAK_PCT", "10"))  # Buy when dipped 10% from peak
MIN_DIP_PCT = float(os.getenv("MIN_DIP_PCT", "5"))  # Minimum dip to consider

# SOL mint address
SOL_MINT = "So11111111111111111111111111111111111111112"
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Jupiter API
JUPITER_BASE_URL = os.getenv("JUPITER_BASE_URL", "https://lite-api.jup.ag/swap/v1")
JUPITER_QUOTE_API = f"{JUPITER_BASE_URL}/quote"
JUPITER_SWAP_API = f"{JUPITER_BASE_URL}/swap"

# Position file
POSITIONS_FILE = "live_positions.json"
TRADES_FILE = "live_trades.csv"
STATS_FILE = "trading_stats.json"
LOGS_DIR = "logs"

# ===== SESSION TRACKING =====
import uuid
SESSION_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
SESSION_START = datetime.now()

@dataclass
class SessionStats:
    """Track stats for current session."""
    session_id: str
    start_time: str
    starting_balance: float = 0.0  # SOL balance when bot started
    current_balance: float = 0.0   # Latest known SOL balance
    sol_in: float = 0.0            # Total SOL spent on buys
    sol_out: float = 0.0           # Total SOL received from sells
    buys: int = 0
    sells: int = 0
    wins: int = 0
    losses: int = 0
    best_trade_pnl: float = 0.0
    best_trade_symbol: str = ""
    worst_trade_pnl: float = 0.0
    worst_trade_symbol: str = ""

    @property
    def net_pnl_sol(self) -> float:
        return self.sol_out - self.sol_in

    @property
    def net_pnl_pct(self) -> float:
        if self.sol_in <= 0:
            return 0.0
        return ((self.sol_out - self.sol_in) / self.sol_in) * 100

    @property
    def wallet_change_sol(self) -> float:
        """Actual wallet change since start (includes open positions)."""
        if self.starting_balance <= 0:
            return 0.0
        return self.current_balance - self.starting_balance

    @property
    def wallet_change_pct(self) -> float:
        if self.starting_balance <= 0:
            return 0.0
        return ((self.current_balance - self.starting_balance) / self.starting_balance) * 100

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        return (self.wins / total * 100) if total > 0 else 0.0

# Global session stats
_SESSION_STATS = SessionStats(
    session_id=SESSION_ID,
    start_time=SESSION_START.isoformat()
)

def get_session_stats() -> SessionStats:
    return _SESSION_STATS

def save_session_stats():
    """Save session stats to file."""
    stats = get_session_stats()
    stats_data = {
        "current_session": asdict(stats),
        "last_updated": datetime.now().isoformat()
    }

    # Load existing stats to preserve history
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE) as f:
                existing = json.load(f)
            if "sessions" not in existing:
                existing["sessions"] = []
        except:
            existing = {"sessions": []}
    else:
        existing = {"sessions": []}

    existing["current_session"] = asdict(stats)
    existing["last_updated"] = datetime.now().isoformat()

    with open(STATS_FILE, "w") as f:
        json.dump(existing, f, indent=2)

def log_session(message: str):
    """Log message to session log file."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"session_{SESSION_ID}.log")
    timestamp = datetime.now().strftime("%H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

def update_session_buy(sol_amount: float):
    """Update session stats after a buy."""
    stats = get_session_stats()
    stats.sol_in += sol_amount
    stats.buys += 1
    save_session_stats()
    log_session(f"BUY: {sol_amount:.6f} SOL | Total in: {stats.sol_in:.6f}")

def update_session_sell(sol_received: float, pnl_pct: float, symbol: str):
    """Update session stats after a sell."""
    stats = get_session_stats()
    stats.sol_out += sol_received
    stats.sells += 1

    if pnl_pct >= 0:
        stats.wins += 1
        if pnl_pct > stats.best_trade_pnl:
            stats.best_trade_pnl = pnl_pct
            stats.best_trade_symbol = symbol
    else:
        stats.losses += 1
        if pnl_pct < stats.worst_trade_pnl:
            stats.worst_trade_pnl = pnl_pct
            stats.worst_trade_symbol = symbol

    save_session_stats()
    log_session(f"SELL: {sol_received:.6f} SOL ({pnl_pct:+.1f}%) | Net: {stats.net_pnl_sol:+.6f} SOL")


def get_alltime_stats() -> dict:
    """Calculate all-time stats from trades CSV."""
    import csv

    stats = {
        "total_buys": 0,
        "total_sells": 0,
        "wins": 0,
        "losses": 0,
        "total_pnl_pct": 0.0,
        "best_trade": {"symbol": "", "pnl": 0.0},
        "worst_trade": {"symbol": "", "pnl": 0.0},
        "by_type": {}
    }

    if not os.path.exists(TRADES_FILE):
        return stats

    try:
        with open(TRADES_FILE) as f:
            reader = csv.DictReader(f)
            for row in reader:
                action = row.get("action", "")
                symbol = row.get("symbol", "???")
                trade_type = row.get("type", "QUICK")
                pnl_str = row.get("pnl_pct", "").replace("%", "")

                if action == "BUY":
                    stats["total_buys"] += 1
                elif action == "SELL":
                    stats["total_sells"] += 1
                    try:
                        pnl = float(pnl_str) if pnl_str else 0
                    except:
                        pnl = 0

                    stats["total_pnl_pct"] += pnl

                    if pnl >= 0:
                        stats["wins"] += 1
                    else:
                        stats["losses"] += 1

                    if pnl > stats["best_trade"]["pnl"]:
                        stats["best_trade"] = {"symbol": symbol, "pnl": pnl}
                    if pnl < stats["worst_trade"]["pnl"]:
                        stats["worst_trade"] = {"symbol": symbol, "pnl": pnl}

                    # Track by trade type
                    if trade_type not in stats["by_type"]:
                        stats["by_type"][trade_type] = {"wins": 0, "losses": 0, "pnl": 0.0}
                    stats["by_type"][trade_type]["pnl"] += pnl
                    if pnl >= 0:
                        stats["by_type"][trade_type]["wins"] += 1
                    else:
                        stats["by_type"][trade_type]["losses"] += 1
    except Exception as e:
        print(f"Error reading trades: {e}")

    return stats


def format_alltime_stats() -> str:
    """Format all-time stats for TG display."""
    stats = get_alltime_stats()
    total = stats["wins"] + stats["losses"]
    win_rate = (stats["wins"] / total * 100) if total > 0 else 0

    lines = [
        "*📈 ALL-TIME STATS*",
        f"Trades: {total} ({stats['wins']}W / {stats['losses']}L)",
        f"Win Rate: {win_rate:.0f}%",
        f"Total PnL: `{stats['total_pnl_pct']:+.1f}%`",
        "",
    ]

    if stats["best_trade"]["symbol"]:
        lines.append(f"Best:  {stats['best_trade']['symbol']} +{stats['best_trade']['pnl']:.1f}%")
    if stats["worst_trade"]["symbol"]:
        lines.append(f"Worst: {stats['worst_trade']['symbol']} {stats['worst_trade']['pnl']:.1f}%")

    # By type breakdown
    if stats["by_type"]:
        lines.append("")
        lines.append("*By Type:*")
        for t, d in stats["by_type"].items():
            total_t = d["wins"] + d["losses"]
            wr = (d["wins"] / total_t * 100) if total_t > 0 else 0
            lines.append(f"  {t}: {total_t} trades | {wr:.0f}% WR | {d['pnl']:+.1f}%")

    return "\n".join(lines)


# Trade type configs (same as sim)
TRADE_CONFIGS = {
    "QUICK": {"target": 20, "stop": -85, "timeout_hours": 2},
    "MOMENTUM": {"target": 37, "stop": -85, "timeout_hours": 6},
    "GEM": {"target": 112, "stop": -85, "timeout_hours": 24},
    "RANGE": {"target": 25, "stop": -30, "timeout_hours": 48},  # Higher TP, tighter stop
}

# DCA step buying for ALL trades
DCA_STEPS = [0.15, 0.25, 0.60]  # 15% / 25% / 60% position allocation
DCA_STEP_TRIGGERS = [0, 12, 28]  # Entry, 12% dip, 28% dip from avg

# Cached keypair
_KEYPAIR_CACHE = None
_SIGNAL_HISTORY = {}

# Signal tracker - tracks tokens over time for dip buying
@dataclass
class TrackedSignal:
    """Track a signal over time for dip detection."""
    address: str
    symbol: str
    trade_type: str
    first_seen: float  # timestamp
    last_seen: float   # timestamp
    signal_count: int
    peak_price: float
    peak_mc: float
    current_price: float
    current_mc: float
    liquidity: float
    buy_ratio: float
    prices: list  # [(timestamp, price), ...]

    @property
    def age_mins(self) -> float:
        return (datetime.now().timestamp() - self.first_seen) / 60

    @property
    def dip_from_peak_pct(self) -> float:
        if self.peak_price <= 0:
            return 0
        return ((self.peak_price - self.current_price) / self.peak_price) * 100

    @property
    def is_aged(self) -> bool:
        return self.age_mins >= SIGNAL_MIN_AGE_MINS

    @property
    def is_dipping(self) -> bool:
        return self.dip_from_peak_pct >= MIN_DIP_PCT

    @property
    def is_good_dip(self) -> bool:
        return self.dip_from_peak_pct >= DIP_FROM_PEAK_PCT

    @property
    def is_expired(self) -> bool:
        return self.age_mins >= SIGNAL_MAX_AGE_MINS


# Global signal tracker
_TRACKED_SIGNALS: dict[str, TrackedSignal] = {}


def load_keypair():
    """Load keypair from keys.env (handles multi-line JSON array)."""
    global _KEYPAIR_CACHE
    if _KEYPAIR_CACHE is not None:
        return _KEYPAIR_CACHE

    key_bytes = None

    # Read directly from file (handles multi-line JSON)
    try:
        with open("keys.env", "r") as f:
            content = f.read()

        start = content.find("[")
        end = content.find("]") + 1
        if start != -1 and end > start:
            json_str = content[start:end]
            key_bytes = bytes(json.loads(json_str))
    except:
        pass

    # Fallback to env var (base58)
    if key_bytes is None:
        raw_key = os.getenv("SOLANA_PRIVATE_KEY", "")
        if raw_key and raw_key != "your_private_key_here":
            try:
                if raw_key.strip().startswith("["):
                    key_bytes = bytes(json.loads(raw_key))
                else:
                    key_bytes = base58.b58decode(raw_key)
            except:
                pass

    if key_bytes is None:
        _KEYPAIR_CACHE = (None, None)
        return _KEYPAIR_CACHE

    try:
        if len(key_bytes) == 64:
            pubkey_bytes = key_bytes[32:]
        else:
            pubkey_bytes = key_bytes[:32]

        pubkey = base58.b58encode(pubkey_bytes).decode()
        _KEYPAIR_CACHE = (key_bytes, pubkey)
        return _KEYPAIR_CACHE
    except:
        _KEYPAIR_CACHE = (None, None)
        return _KEYPAIR_CACHE


@dataclass
class LivePosition:
    token_address: str
    symbol: str
    trade_type: str
    entry_price: float
    entry_time: str
    sol_amount: float
    token_amount: float
    entry_mc: float
    tx_hash: str
    status: str = "OPEN"
    exit_price: float = 0.0
    exit_time: str = ""
    exit_tx: str = ""
    pnl_percent: float = 0.0
    max_pnl_percent: float = 0.0
    # Enhanced tracking
    entry_vol_5m: float = 0.0       # Volume at entry for decay detection
    entry_buys_5m: int = 0          # Buy pressure at entry
    entry_sells_5m: int = 0         # Sell pressure at entry
    last_mc: float = 0.0            # Last recorded MC
    last_mc_time: str = ""          # When MC was last checked
    max_mc: float = 0.0             # Highest MC seen
    # DCA tracking for RANGE trades
    dca_step: int = 0               # 0=none, 1=step1 (15%), 2=step2 (25%), 3=step3 (60%)
    dca_total_sol: float = 0.0      # Total SOL invested across all steps
    dca_avg_price: float = 0.0      # Average entry price across steps
    dca_buys: list = None           # List of DCA buy details [(sol, price, time), ...]


def get_wallet_pubkey() -> str:
    """Get public key from private key."""
    key_bytes, pubkey = load_keypair()
    return pubkey or ""


def load_positions() -> list[LivePosition]:
    """Load live positions from file."""
    if not os.path.exists(POSITIONS_FILE):
        return []
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
        return [LivePosition(**p) for p in data]
    except:
        return []


def save_positions(positions: list[LivePosition]):
    """Save positions to file."""
    with open(POSITIONS_FILE, "w") as f:
        json.dump([asdict(p) for p in positions], f, indent=2)


def log_trade(pos: LivePosition, action: str):
    """Log trade to CSV."""
    import csv

    file_exists = os.path.exists(TRADES_FILE)
    with open(TRADES_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "time", "action", "symbol", "type", "price", "sol_amount",
                "token_amount", "mc", "pnl_pct", "tx_hash"
            ])

        if action == "BUY":
            writer.writerow([
                pos.entry_time, "BUY", pos.symbol, pos.trade_type,
                pos.entry_price, pos.sol_amount, pos.token_amount,
                pos.entry_mc, "", pos.tx_hash
            ])
        else:
            writer.writerow([
                pos.exit_time, "SELL", pos.symbol, pos.trade_type,
                pos.exit_price, pos.sol_amount, pos.token_amount,
                pos.entry_mc, f"{pos.pnl_percent:.1f}%", pos.exit_tx
            ])


async def get_sol_balance(pubkey: str) -> float:
    """Get SOL balance."""
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [pubkey],
            }
            async with session.post(SOLANA_RPC_URL, json=payload) as resp:
                data = await resp.json()
                lamports = data.get("result", {}).get("value", 0)
                return lamports / 1_000_000_000
    except Exception:
        return 0.0


async def get_token_balance_raw(pubkey: str, mint: str) -> int:
    """Get raw token balance for a mint (supports SPL + Token-2022)."""
    try:
        async with aiohttp.ClientSession() as session:
            for program_id in (SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        pubkey,
                        {"programId": program_id},
                        {"encoding": "jsonParsed"},
                    ],
                }
                async with session.post(SOLANA_RPC_URL, json=payload) as resp:
                    data = await resp.json()
                    accounts = data.get("result", {}).get("value", [])
                    for acc in accounts:
                        info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                        if info.get("mint") != mint:
                            continue
                        amt = info.get("tokenAmount", {}).get("amount", "0")
                        try:
                            return int(amt)
                        except Exception:
                            return 0
    except Exception:
        return 0
    return 0


@dataclass
class TokenMetrics:
    """Live token metrics from DexScreener."""
    price: float = 0.0
    mc: float = 0.0
    vol_5m: float = 0.0
    vol_1h: float = 0.0
    buys_5m: int = 0
    sells_5m: int = 0
    buys_1h: int = 0
    sells_1h: int = 0
    change_5m: float = 0.0
    change_1h: float = 0.0
    liquidity: float = 0.0

    @property
    def buy_ratio(self) -> float:
        return self.buys_5m / max(1, self.sells_5m)

    @property
    def is_dumping(self) -> bool:
        return self.sells_5m > self.buys_5m * 1.5

    @property
    def vol_dying(self) -> bool:
        # Vol 5m extrapolated to 1h should be at least 30% of actual 1h vol
        return self.vol_1h > 0 and (self.vol_5m * 12) < (self.vol_1h * 0.3)


async def get_token_metrics(token_address: str, retries: int = 3) -> Optional[TokenMetrics]:
    """Get comprehensive token metrics from DexScreener with retry."""
    for attempt in range(retries):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("pairs"):
                            pair = data["pairs"][0]
                            txns = pair.get("txns", {})
                            volume = pair.get("volume", {})
                            price_change = pair.get("priceChange", {})
                            metrics = TokenMetrics(
                                price=float(pair.get("priceUsd", 0) or 0),
                                mc=float(pair.get("marketCap", 0) or 0),
                                vol_5m=float(volume.get("m5", 0) or 0),
                                vol_1h=float(volume.get("h1", 0) or 0),
                                buys_5m=int(txns.get("m5", {}).get("buys", 0) or 0),
                                sells_5m=int(txns.get("m5", {}).get("sells", 0) or 0),
                                buys_1h=int(txns.get("h1", {}).get("buys", 0) or 0),
                                sells_1h=int(txns.get("h1", {}).get("sells", 0) or 0),
                                change_5m=float(price_change.get("m5", 0) or 0),
                                change_1h=float(price_change.get("h1", 0) or 0),
                                liquidity=float(pair.get("liquidity", {}).get("usd", 0) or 0),
                            )
                            # Validate we got real data
                            if metrics.price > 0 or metrics.mc > 0:
                                return metrics
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(0.5)  # Brief pause before retry
            continue
    return None


async def get_token_price(token_address: str) -> Optional[float]:
    """Get token price from DexScreener."""
    metrics = await get_token_metrics(token_address)
    return metrics.price if metrics else None


async def get_token_value_sol(token_address: str, raw_amount: int) -> Optional[float]:
    """Estimate SOL value for a raw token amount using Jupiter quote."""
    if raw_amount <= 0:
        return 0.0
    quote = await get_jupiter_quote(token_address, SOL_MINT, int(raw_amount))
    if not quote:
        return None
    out_amount = int(quote.get("outAmount", 0))
    return out_amount / 1_000_000_000


def count_trades_for_token(token_address: str) -> int:
    """Count how many times we've traded this token in the session."""
    positions = load_positions()
    return len([p for p in positions if p.token_address == token_address])


# Config for position management
MAX_TRADES_PER_TOKEN = int(os.getenv("MAX_TRADES_PER_TOKEN", "2"))  # Max trades per token
MC_STALL_MINS = int(os.getenv("MC_STALL_MINS", "15"))               # Cut if MC stalls for X mins
VOL_DECAY_THRESHOLD = float(os.getenv("VOL_DECAY_THRESHOLD", "0.2")) # Cut if vol drops to 20% of entry


async def get_jupiter_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = MAX_SLIPPAGE_BPS
) -> Optional[dict]:
    """Get swap quote from Jupiter."""
    try:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": slippage_bps,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(JUPITER_QUOTE_API, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    print(f"Quote error: {resp.status}")
    except Exception as e:
        print(f"Quote error: {e}")
    return None


async def execute_swap(quote: dict, wallet_pubkey: str) -> Optional[str]:
    """Execute swap via Jupiter."""
    key_bytes, _ = load_keypair()
    if not key_bytes:
        print("ERROR: No valid private key configured!")
        return None

    try:
        # Get swap transaction
        payload = {
            "quoteResponse": quote,
            "userPublicKey": wallet_pubkey,
            "wrapAndUnwrapSol": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(JUPITER_SWAP_API, json=payload) as resp:
                if resp.status != 200:
                    print(f"Swap API error: {resp.status}")
                    return None

                # Jupiter returns swapTransaction as base64.
                text = await resp.text()
                try:
                    swap_data = json.loads(text)
                except Exception:
                    print(f"Swap API response (non-json): {text[:200]}")
                    return False
                swap_tx = swap_data.get("swapTransaction")

                if not swap_tx:
                    print("No swap transaction returned")
                    return None

        # Sign and send transaction
        # This requires solders/solana-py for signing
        try:
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
            from solders.pubkey import Pubkey
            from solders.signature import Signature
            from solana.rpc.async_api import AsyncClient

            # Use already loaded key_bytes
            keypair = Keypair.from_bytes(key_bytes)

            # Decode and sign transaction
            tx_bytes = base64.b64decode(swap_tx)
            tx = VersionedTransaction.from_bytes(tx_bytes)
            # Sign versioned message bytes (includes version prefix)
            try:
                from solders.message import to_bytes_versioned
                msg_bytes = to_bytes_versioned(tx.message)
            except Exception:
                msg_bytes = bytes(tx.message)
            sig = keypair.sign_message(msg_bytes)

            signer_count = tx.message.header.num_required_signatures
            signer_keys = list(tx.message.account_keys)[:signer_count]
            our_pubkey = Pubkey.from_string(wallet_pubkey)
            try:
                signer_index = signer_keys.index(our_pubkey)
            except ValueError:
                print("Signer mismatch: wallet pubkey not in required signers")
                return None

            sigs = list(tx.signatures)
            if len(sigs) < signer_count:
                sigs += [Signature.default()] * (signer_count - len(sigs))
            sigs[signer_index] = sig
            tx = VersionedTransaction.populate(tx.message, sigs)

            # Send transaction
            async with AsyncClient(SOLANA_RPC_URL) as client:
                result = await client.send_raw_transaction(bytes(tx))
                if result.value:
                    return str(result.value)

        except ImportError:
            print("ERROR: Install solders and solana-py:")
            print("  pip install solders solana")
            return None

    except Exception as e:
        print(f"Swap error: {e}")

    return None


async def close_token_account(token_address: str) -> bool:
    """Close empty token account to reclaim rent (~0.002 SOL)."""
    key_bytes, wallet_pubkey = load_keypair()
    if not key_bytes or not wallet_pubkey:
        return False

    try:
        from solders.keypair import Keypair
        from solders.pubkey import Pubkey
        from solders.transaction import Transaction
        from solders.system_program import ID as SYS_PROGRAM_ID
        from solders.instruction import Instruction, AccountMeta
        from solana.rpc.async_api import AsyncClient

        keypair = Keypair.from_bytes(key_bytes)
        owner = Pubkey.from_string(wallet_pubkey)
        token_mint = Pubkey.from_string(token_address)

        # Find ATA
        from solders.pubkey import Pubkey
        ATA_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
        TOKEN_PROGRAM = Pubkey.from_string(SPL_TOKEN_PROGRAM)

        # Derive ATA address
        seeds = [bytes(owner), bytes(TOKEN_PROGRAM), bytes(token_mint)]
        ata, _ = Pubkey.find_program_address(seeds, ATA_PROGRAM)

        # Check if account exists and is empty
        async with AsyncClient(SOLANA_RPC_URL) as client:
            resp = await client.get_token_account_balance(ata)
            if resp.value is None:
                return False  # Account doesn't exist

            balance = int(resp.value.amount)
            if balance > 0:
                print(f"  Account not empty ({balance}), skipping close")
                return False

            # Create closeAccount instruction
            # CloseAccount = index 9 in SPL Token program
            close_ix = Instruction(
                program_id=TOKEN_PROGRAM,
                accounts=[
                    AccountMeta(pubkey=ata, is_signer=False, is_writable=True),      # account to close
                    AccountMeta(pubkey=owner, is_signer=False, is_writable=True),    # destination for rent
                    AccountMeta(pubkey=owner, is_signer=True, is_writable=False),    # authority
                ],
                data=bytes([9])  # CloseAccount instruction
            )

            # Get recent blockhash
            blockhash_resp = await client.get_latest_blockhash()
            recent_blockhash = blockhash_resp.value.blockhash

            # Build and sign transaction
            tx = Transaction.new_signed_with_payer(
                [close_ix],
                owner,
                [keypair],
                recent_blockhash
            )

            # Send
            result = await client.send_raw_transaction(bytes(tx))
            if result.value:
                print(f"  Closed token account, reclaimed rent")
                return True

    except Exception as e:
        # Non-critical - just log and continue
        print(f"  Close account skipped: {e}")

    return False


async def buy_token(
    token_address: str,
    symbol: str,
    trade_type: str,
    current_price: float,
    market_cap: float,
    sol_amount: float = MAX_POSITION_SOL,
    entry_metrics: Optional[TokenMetrics] = None
) -> Optional[LivePosition]:
    """Execute buy order with entry metrics capture."""
    wallet = get_wallet_pubkey()
    if not wallet:
        print("ERROR: No wallet configured!")
        return None

    print(f"\n{'='*40}")
    print(f"LIVE BUY: ${symbol}")
    print(f"Type: {trade_type}")
    print(f"Amount: {sol_amount} SOL")
    print(f"MC: ${market_cap:,.0f}")
    if entry_metrics:
        print(f"Vol5m: ${entry_metrics.vol_5m:.0f} | Buys: {entry_metrics.buys_5m} | Sells: {entry_metrics.sells_5m}")
    print(f"{'='*40}")

    # Convert SOL to lamports (1 SOL = 1e9 lamports)
    lamports = int(sol_amount * 1_000_000_000)

    # Track balance before swap (for accurate received amount)
    raw_before = await get_token_balance_raw(wallet, token_address)

    # Get quote
    quote = await get_jupiter_quote(SOL_MINT, token_address, lamports)
    if not quote:
        print("Failed to get quote")
        return None

    out_amount = int(quote.get("outAmount", 0))
    if out_amount == 0:
        print("Quote returned 0 tokens")
        return None

    print(f"Quote: {out_amount} tokens")

    # Execute swap
    tx_hash = await execute_swap(quote, wallet)
    if not tx_hash:
        print("Swap failed!")
        return None

    print(f"TX: {tx_hash}")
    print(f"https://solscan.io/tx/{tx_hash}")

    # Re-check balance to CONFIRM tokens were received
    # Solana can take 20-30+ seconds to finalize sometimes
    raw_after = raw_before
    print("Confirming transaction (up to 30s)...")

    # Phase 1: Quick checks every 1s for first 10s
    for attempt in range(10):
        await asyncio.sleep(1.0)
        raw_after = await get_token_balance_raw(wallet, token_address)
        if raw_after > raw_before:
            print(f"  Confirmed after {attempt+1}s")
            break

    # Phase 2: If not confirmed, slower checks for another 20s
    if raw_after <= raw_before:
        print("  Still waiting...")
        for attempt in range(10):
            await asyncio.sleep(2.0)
            raw_after = await get_token_balance_raw(wallet, token_address)
            if raw_after > raw_before:
                print(f"  Confirmed after {10 + (attempt+1)*2}s")
                break

    actual_received = max(0, raw_after - raw_before)

    # CRITICAL: Don't save position if no tokens received!
    if actual_received == 0:
        print(f"WARNING: No tokens detected after 30s")
        print(f"TX: https://solscan.io/tx/{tx_hash}")
        print("Will check once more in 10s...")

        # Final check after extra delay
        await asyncio.sleep(10.0)
        raw_after = await get_token_balance_raw(wallet, token_address)
        actual_received = max(0, raw_after - raw_before)

        if actual_received == 0:
            print(f"ERROR: No tokens received after 40s! TX may have failed.")
            await send_tg(f"⚠️ *FAILED BUY* `{symbol}`\nNo tokens after 40s!\n[Check TX](https://solscan.io/tx/{tx_hash})")
            return None
        else:
            print(f"  Finally confirmed after 40s!")

    print(f"Received: {actual_received} tokens")

    # Create position with entry metrics
    # ALL trades use DCA step buying now
    pos = LivePosition(
        token_address=token_address,
        symbol=symbol,
        trade_type=trade_type,
        entry_price=current_price,
        entry_time=datetime.now().isoformat(),
        sol_amount=sol_amount,
        token_amount=actual_received,
        entry_mc=market_cap,
        tx_hash=tx_hash,
        # Enhanced tracking
        entry_vol_5m=entry_metrics.vol_5m if entry_metrics else 0.0,
        entry_buys_5m=entry_metrics.buys_5m if entry_metrics else 0,
        entry_sells_5m=entry_metrics.sells_5m if entry_metrics else 0,
        max_mc=market_cap,
        last_mc=market_cap,
        last_mc_time=datetime.now().isoformat(),
        # DCA tracking for ALL trades
        dca_step=1,  # Step 1 = initial entry
        dca_total_sol=sol_amount,
        dca_avg_price=current_price,
        dca_buys=[{"sol": sol_amount, "price": current_price, "time": datetime.now().isoformat(), "tx": tx_hash}],
    )

    # Save
    positions = load_positions()
    positions.append(pos)
    save_positions(positions)
    log_trade(pos, "BUY")

    # Update session stats
    update_session_buy(sol_amount)

    # Send TG notification with buttons
    stats = get_session_stats()
    mc_str = f"{market_cap/1000:.0f}K" if market_cap < 1_000_000 else f"{market_cap/1_000_000:.1f}M"
    tg_msg = f"🟢 *STEP 1* `{symbol}` [{trade_type}]\n"
    tg_msg += f"MC: {mc_str} | {sol_amount:.4f} SOL (15%)\n"
    tg_msg += f"Session: {stats.buys}B/{stats.sells}S | Net: {stats.net_pnl_sol:+.4f} SOL\n"
    if entry_metrics:
        tg_msg += f"Vol: ${entry_metrics.vol_5m:.0f} | {entry_metrics.buy_ratio:.1f}x buy\n"
    tg_msg += f"[TX](https://solscan.io/tx/{tx_hash})"
    await send_tg(tg_msg, reply_markup=get_trade_buttons())

    return pos


async def sell_token(pos: LivePosition, reason: str) -> bool:
    """Execute sell order."""
    wallet = get_wallet_pubkey()
    if not wallet:
        return False

    # Estimate current value in SOL for PnL
    current_price = await get_token_price(pos.token_address)

    print(f"\n{'='*40}")
    print(f"LIVE SELL: ${pos.symbol}")
    print(f"Reason: {reason}")
    print(f"{'='*40}")

    # Refresh actual token balance to avoid over-selling
    wallet = get_wallet_pubkey()
    raw_amount = await get_token_balance_raw(wallet, pos.token_address)
    if raw_amount <= 0:
        print(f"No token balance for {pos.symbol}, skipping sell")
        return False

    # Get quote (sell tokens for SOL)
    quote = await get_jupiter_quote(
        pos.token_address,
        SOL_MINT,
        int(raw_amount)
    )

    if not quote:
        print("Failed to get sell quote")
        return False

    sol_value = int(quote.get("outAmount", 0)) / 1_000_000_000
    pnl = ((sol_value - pos.sol_amount) / pos.sol_amount) * 100
    print(f"PnL: {pnl:+.1f}%")

    # Execute swap
    tx_hash = await execute_swap(quote, wallet)
    if not tx_hash:
        print("Sell failed!")
        return False

    print(f"TX: {tx_hash}")
    print(f"https://solscan.io/tx/{tx_hash}")

    # VERIFY tokens were actually sold before marking CLOSED
    print("Verifying sell...")
    await asyncio.sleep(3)
    remaining = await get_token_balance_raw(wallet, pos.token_address)

    # If still have >10% of tokens, sell failed
    if remaining > raw_amount * 0.1:
        print(f"WARNING: Sell may have failed! Still have {remaining} tokens (had {raw_amount})")
        print("Position NOT marked as closed - will retry")
        return False

    print(f"Confirmed: {remaining} tokens remaining (sold {raw_amount - remaining})")

    # Recalculate PnL based on actual SOL received if possible
    # For now use quote estimate
    sol_received = sol_value

    # Update position
    pos.status = "CLOSED"
    pos.exit_price = current_price
    pos.exit_time = datetime.now().isoformat()
    pos.exit_tx = tx_hash
    pos.pnl_percent = pnl

    # Save
    positions = load_positions()
    for i, p in enumerate(positions):
        if p.token_address == pos.token_address and p.status == "OPEN":
            positions[i] = pos
            break
    save_positions(positions)
    log_trade(pos, "SELL")

    # Update session stats with actual SOL received
    update_session_sell(sol_received, pnl, pos.symbol)

    # Try to close empty token account to reclaim rent (~0.002 SOL)
    await close_token_account(pos.token_address)

    # Send TG notification with buttons
    stats = get_session_stats()
    emoji = "🟢" if pnl >= 0 else "🔴"
    held_mins = (datetime.now() - datetime.fromisoformat(pos.entry_time)).total_seconds() / 60
    tg_msg = f"{emoji} *SELL* `{pos.symbol}` [{pos.trade_type}]\n"
    tg_msg += f"PnL: *{pnl:+.1f}%* | SOL: {pos.sol_amount:.4f} → {sol_received:.4f}\n"
    tg_msg += f"Held: {held_mins:.0f}m | {reason}\n"
    tg_msg += f"Session: {stats.wins}W/{stats.losses}L | Net: *{stats.net_pnl_sol:+.4f}* SOL\n"
    tg_msg += f"[TX](https://solscan.io/tx/{tx_hash})"
    await send_tg(tg_msg, reply_markup=get_trade_buttons())

    return True


def check_exit_conditions(pos: LivePosition, pnl: float, metrics: Optional[TokenMetrics] = None) -> Optional[str]:
    """Check if position should be closed with enhanced logic.

    IMPORTANT: Loss-based exits only apply AFTER step 3 (fully invested).
    Before step 3, dips are DCA opportunities, not exit signals.
    """
    config = TRADE_CONFIGS.get(pos.trade_type, TRADE_CONFIGS["QUICK"])

    entry = datetime.fromisoformat(pos.entry_time)
    held_mins = (datetime.now() - entry).total_seconds() / 60

    # DCA step check - if not fully invested yet, only allow profit exits
    dca_step = pos.dca_step if pos.dca_step else 1
    fully_invested = dca_step >= 3

    # ===== PROFIT EXITS (always allowed) =====
    # NOTE: Account for ~3% round-trip fees (buy + sell slippage/fees)
    # Real profit = displayed PnL - 3%

    # Target hit - always exit on target regardless of DCA step
    if pnl >= config["target"]:
        return f"TARGET {pnl:.1f}%"

    # Trailing profit - if we hit big gains and falling back
    # Was 8%/4%, now 15%/6% (exit at ~9% = ~6% real after fees)
    if pos.max_pnl_percent >= 15 and pnl <= (pos.max_pnl_percent - 6):
        return f"TRAIL {pnl:.1f}% (max {pos.max_pnl_percent:.1f}%)"

    # Quick profit lock - if we hit good profit and dropping
    # Was 6%/3%, now 12%/8% (exit at ~8% = ~5% real after fees)
    if pos.max_pnl_percent >= 12 and pnl <= 8:
        return f"PROFIT_LOCK {pnl:.1f}% (max {pos.max_pnl_percent:.1f}%)"

    # Emergency save - if max was high and now low, save something
    # Was 10%/0%, now 18%/6% (exit at ~6% = ~3% real after fees)
    if pos.max_pnl_percent >= 18 and pnl <= 6:
        return f"EMERGENCY_SAVE {pnl:.1f}% (max {pos.max_pnl_percent:.1f}%)"

    # ===== LOSS EXITS (only after step 3 complete) =====
    if not fully_invested:
        # Before step 3: Don't exit on losses - we're still DCA'ing in
        # Only allow timeout after very long time (24h+) even without step 3
        if held_mins > 1440:  # 24 hours
            return f"DCA_TIMEOUT {pnl:.1f}% (step {dca_step}/3, {held_mins/60:.0f}h)"
        return None

    # ===== STEP 3 COMPLETE - NOW APPLY LOSS-BASED EXITS =====

    # For RANGE trades, use calmer exit logic
    if pos.trade_type == "RANGE":
        # Hard stop at -30% (tighter than other types)
        if pnl <= config["stop"]:
            return f"RANGE STOP {pnl:.1f}%"

        # Timeout (48h for RANGE)
        timeout = timedelta(hours=config["timeout_hours"])
        if datetime.now() - entry > timeout:
            return f"RANGE TIMEOUT {pnl:.1f}%"

        # RANGE trades don't use aggressive momentum exits
        return None

    # Non-RANGE trades: apply standard loss exits
    # Stop loss
    if pnl <= config["stop"]:
        return f"STOP {pnl:.1f}%"

    # ===== MOMENTUM/VOLUME-BASED EXITS (only if losing) =====
    # Only kick in after 5 mins - let position settle first
    if metrics and held_mins >= 5:
        # Volume dying - vol dropped to <20% of entry AND we're losing
        if pos.entry_vol_5m > 0 and metrics.vol_5m < pos.entry_vol_5m * VOL_DECAY_THRESHOLD:
            if pnl < -5:  # Only exit if losing >5%
                return f"VOL_DECAY {pnl:.1f}% (vol {metrics.vol_5m:.0f} vs entry {pos.entry_vol_5m:.0f})"

        # Dump detection - heavy selling pressure (sells > 2x buys) AND significant loss
        if metrics.sells_5m > metrics.buys_5m * 2 and pnl < -10:
            return f"DUMP {pnl:.1f}% (sells {metrics.sells_5m} > 2x buys {metrics.buys_5m})"

        # MC stall detection - MC dropped 15% from peak after 15 mins
        current_mc = metrics.mc
        if pos.max_mc > 0 and current_mc < pos.max_mc * 0.85:
            if held_mins > 15 and pnl < 5:
                return f"MC_STALL {pnl:.1f}% (mc {current_mc/1000:.0f}K vs peak {pos.max_mc/1000:.0f}K)"

        # Quick trade specific: if down >15% and losing momentum after 15m, cut
        if pos.trade_type == "QUICK" and held_mins > 15:
            if pnl < -15 and metrics.buy_ratio < 0.8:
                return f"QUICK_CUT {pnl:.1f}% ({held_mins:.0f}m, ratio {metrics.buy_ratio:.1f})"

    # Timeout
    timeout = timedelta(hours=config["timeout_hours"])
    if datetime.now() - entry > timeout:
        return f"TIMEOUT {pnl:.1f}%"

    return None


async def manage_positions():
    """Check and manage all open positions with enhanced metrics."""
    positions = load_positions()
    open_positions = [p for p in positions if p.status == "OPEN"]

    if not open_positions:
        return

    print(f"\nChecking {len(open_positions)} tracked positions...")

    wallet = get_wallet_pubkey()
    active_count = 0
    closed_zero = 0
    for pos in open_positions:
        metrics = await get_token_metrics(pos.token_address)
        if not metrics:
            continue

        raw_amount = await get_token_balance_raw(wallet, pos.token_address)
        if raw_amount <= 0:
            # Auto-close phantom positions (no on-chain balance)
            current_price = metrics.price if metrics else 0
            pnl = ((current_price - pos.entry_price) / pos.entry_price) * 100 if current_price > 0 else -100

            pos.status = "CLOSED"
            pos.exit_price = current_price
            pos.exit_time = datetime.now().isoformat()
            pos.exit_tx = "BALANCE_ZERO"
            pos.pnl_percent = pnl

            for i, p in enumerate(positions):
                if p.token_address == pos.token_address and p.entry_time == pos.entry_time:
                    positions[i] = pos
                    break
            save_positions(positions)

            closed_zero += 1
            print(f"  ${pos.symbol}: OPEN but 0 balance -> CLOSED (PnL {pnl:+.1f}%)")
            continue

        sol_value = await get_token_value_sol(pos.token_address, raw_amount)
        if sol_value is None or pos.sol_amount <= 0:
            continue
        active_count += 1

        pnl_now = ((sol_value - pos.sol_amount) / pos.sol_amount) * 100
        entry = datetime.fromisoformat(pos.entry_time)
        held_mins = (datetime.now() - entry).total_seconds() / 60

        # Update tracking fields
        updated = False
        if pnl_now > pos.max_pnl_percent:
            old_max = pos.max_pnl_percent
            pos.max_pnl_percent = pnl_now
            updated = True
            # Log when crossing profit thresholds
            if old_max < 12 <= pnl_now:
                log_session(f"PROFIT_LOCK ARMED: {pos.symbol} hit {pnl_now:.1f}% (will sell at 8%)")
            if old_max < 15 <= pnl_now:
                log_session(f"TRAIL ARMED: {pos.symbol} hit {pnl_now:.1f}% (will trail at -{6}%)")

        if metrics.mc > pos.max_mc:
            pos.max_mc = metrics.mc
            updated = True

        # Track MC changes
        pos.last_mc = metrics.mc
        pos.last_mc_time = datetime.now().isoformat()

        if updated:
            # Persist updated tracking data
            positions = load_positions()
            for i, p in enumerate(positions):
                if p.token_address == pos.token_address and p.status == "OPEN":
                    positions[i] = pos
                    break
            save_positions(positions)

        reason = check_exit_conditions(pos, pnl_now, metrics)
        if reason:
            log_session(f"EXIT TRIGGER: {pos.symbol} - {reason} (PnL: {pnl_now:+.1f}%, max: {pos.max_pnl_percent:+.1f}%)")
            sell_success = await sell_token(pos, reason)
            if not sell_success:
                # Sell failed - check if tokens are actually gone (swap executed but we missed it)
                await asyncio.sleep(2)
                remaining = await get_token_balance_raw(wallet, pos.token_address)
                if remaining <= 0:
                    # Tokens gone but sell wasn't confirmed - mark as sold anyway
                    log_session(f"SELL RECOVERY: {pos.symbol} - tokens gone, marking closed")
                    pos.status = "CLOSED"
                    pos.exit_price = metrics.price if metrics else 0
                    pos.exit_time = datetime.now().isoformat()
                    pos.exit_tx = "SELL_UNCONFIRMED"
                    pos.pnl_percent = pnl_now
                    for i, p in enumerate(positions):
                        if p.token_address == pos.token_address and p.status == "OPEN":
                            positions[i] = pos
                            break
                    save_positions(positions)
                    update_session_sell(0, pnl_now, pos.symbol)  # Can't know exact SOL received
                    await send_tg(f"⚠️ *SELL (unconfirmed)* `{pos.symbol}`\nPnL: {pnl_now:+.1f}% | {reason}")
                else:
                    log_session(f"SELL FAILED: {pos.symbol} - still have {remaining} tokens, will retry")
        else:
            # Check for DCA opportunity on ALL trades (step buying)
            # Handle old positions that may have dca_step=0 or None
            current_step = pos.dca_step if pos.dca_step and pos.dca_step > 0 else 1
            if current_step < 3:
                dca_success = await process_dca_step(pos, metrics.price, metrics.mc, metrics)
                if dca_success:
                    # Reload position after DCA
                    positions = load_positions()
                    for p in positions:
                        if p.token_address == pos.token_address and p.status == "OPEN":
                            pos = p
                            break

            # Enhanced status display
            mc_str = f"{metrics.mc/1000:.0f}K" if metrics.mc < 1_000_000 else f"{metrics.mc/1_000_000:.1f}M"
            ratio_str = f"{metrics.buy_ratio:.1f}x" if metrics.buys_5m > 0 else "0x"
            vol_str = f"${metrics.vol_5m:.0f}"
            trend = "↑" if metrics.change_5m > 0 else "↓" if metrics.change_5m < 0 else "→"
            dca_step = pos.dca_step if pos.dca_step and pos.dca_step > 0 else 1
            # Show dip from original entry for next step info
            dip_from_entry = ((pos.entry_price - metrics.price) / pos.entry_price) * 100 if pos.entry_price > 0 else 0
            dca_str = f" [{dca_step}/3 dip:{dip_from_entry:.0f}%]"
            print(f"  ${pos.symbol}: {pnl_now:+.1f}% | MC:{mc_str} | {ratio_str} {trend} | vol:{vol_str} | {held_mins:.0f}m{dca_str}")

    if closed_zero > 0:
        print(f"Auto-closed {closed_zero} phantom positions (zero balance)")

    # Update current balance for session tracking
    try:
        bal = await get_sol_balance(wallet)
        if bal > 0:
            _SESSION_STATS.current_balance = bal
            save_session_stats()
    except:
        pass


async def process_dca_step(pos: LivePosition, current_price: float, current_mc: float, entry_metrics: Optional[TokenMetrics] = None) -> bool:
    """Process DCA step for ANY position - buy next step on dip.

    DCA triggers are based on dip from ORIGINAL entry price:
    - Step 2: 12% dip from entry
    - Step 3: 28% dip from entry
    """
    # Handle old positions without DCA tracking
    dca_step = pos.dca_step if pos.dca_step and pos.dca_step > 0 else 1

    if dca_step >= 3:
        return False  # Already fully invested

    # IMPORTANT: Calculate dip from ORIGINAL entry price, not average
    # This ensures step 2 triggers at 12% below entry, step 3 at 28% below entry
    original_entry = pos.entry_price
    if original_entry <= 0:
        return False

    dip_percent = ((original_entry - current_price) / original_entry) * 100

    # Check if dipped enough for next step
    next_step = dca_step + 1
    required_dip = DCA_STEP_TRIGGERS[next_step - 1] if next_step <= len(DCA_STEP_TRIGGERS) else 999

    if dip_percent < required_dip:
        return False  # Not enough dip for this step

    step_percent = DCA_STEPS[next_step - 1] if next_step <= len(DCA_STEPS) else 0

    if step_percent <= 0:
        return False

    # Calculate budget for this step
    wallet = get_wallet_pubkey()
    sol_balance = await get_sol_balance(wallet)
    usable_balance = max(0.0, sol_balance - MIN_FEE_RESERVE)

    # Use same max position but scaled by step percentage
    step_sol = min(MAX_POSITION_SOL * step_percent, usable_balance * 0.5)

    if step_sol < 0.001:
        print(f"DCA: Not enough balance for step {next_step}")
        return False

    print(f"\n{'='*40}")
    print(f"STEP {next_step} BUY: ${pos.symbol} [{pos.trade_type}]")
    print(f"Dip: {dip_percent:.1f}% (trigger: {required_dip}%)")
    print(f"Step size: {step_percent*100:.0f}% = {step_sol:.4f} SOL")
    print(f"{'='*40}")

    # Execute buy
    lamports = int(step_sol * 1_000_000_000)

    # Track balance before
    raw_before = await get_token_balance_raw(wallet, pos.token_address)

    # Get quote
    quote = await get_jupiter_quote(SOL_MINT, pos.token_address, lamports)
    if not quote:
        print("DCA: Failed to get quote")
        return False

    out_amount = int(quote.get("outAmount", 0))
    if out_amount == 0:
        print("DCA: Quote returned 0 tokens")
        return False

    # Execute swap
    tx_hash = await execute_swap(quote, wallet)
    if not tx_hash:
        print("DCA: Swap failed!")
        return False

    print(f"DCA TX: {tx_hash}")

    # Confirm tokens received
    await asyncio.sleep(5)
    raw_after = await get_token_balance_raw(wallet, pos.token_address)
    actual_received = max(0, raw_after - raw_before)

    if actual_received == 0:
        # Wait a bit more
        await asyncio.sleep(10)
        raw_after = await get_token_balance_raw(wallet, pos.token_address)
        actual_received = max(0, raw_after - raw_before)

    if actual_received == 0:
        print(f"DCA: Warning - no tokens confirmed after buy")
        # Still update position, tokens may arrive later
        actual_received = out_amount

    # Update position with DCA info
    old_total_sol = pos.dca_total_sol if pos.dca_total_sol and pos.dca_total_sol > 0 else pos.sol_amount
    new_total_sol = old_total_sol + step_sol
    old_total_tokens = pos.token_amount
    new_total_tokens = old_total_tokens + actual_received

    # Calculate new average price using proper weighted average
    # avg_price = weighted average of entry prices by SOL spent
    old_avg = pos.dca_avg_price if pos.dca_avg_price and pos.dca_avg_price > 0 else pos.entry_price
    # Formula: new_avg = (old_sol * old_price + new_sol * new_price) / total_sol
    new_avg_price = ((old_total_sol * old_avg) + (step_sol * current_price)) / new_total_sol if new_total_sol > 0 else current_price

    # Update DCA buys list
    dca_buys = pos.dca_buys or []
    dca_buys.append({"sol": step_sol, "price": current_price, "time": datetime.now().isoformat(), "tx": tx_hash})

    # Update position
    pos.dca_step = next_step
    pos.dca_total_sol = new_total_sol
    pos.dca_avg_price = new_avg_price
    pos.dca_buys = dca_buys
    pos.sol_amount = new_total_sol  # Total investment
    pos.token_amount = new_total_tokens

    # Save
    positions = load_positions()
    for i, p in enumerate(positions):
        if p.token_address == pos.token_address and p.status == "OPEN":
            positions[i] = pos
            break
    save_positions(positions)

    # Update session stats for DCA buy
    update_session_buy(step_sol)

    # Send TG notification
    stats = get_session_stats()
    mc_str = f"{current_mc/1000:.0f}K" if current_mc < 1_000_000 else f"{current_mc/1_000_000:.1f}M"
    tg_msg = f"📊 *STEP {next_step}* `{pos.symbol}` [{pos.trade_type}]\n"
    tg_msg += f"Dip: {dip_percent:.1f}% | +{step_sol:.4f} SOL\n"
    tg_msg += f"Total position: {new_total_sol:.4f} SOL\n"
    tg_msg += f"Session: {stats.buys}B | Net: {stats.net_pnl_sol:+.4f} SOL\n"
    tg_msg += f"[TX](https://solscan.io/tx/{tx_hash})"
    await send_tg(tg_msg, reply_markup=get_trade_buttons())

    print(f"Step {next_step} complete: total {new_total_sol:.4f} SOL")
    return True


async def process_signal(signal: dict) -> bool:
    """Process a BUY signal - track first, buy on dips after aging."""
    if signal.get("signal") != "BUY":
        return False

    token_address = signal.get("address")
    symbol = signal.get("symbol", "???")
    trade_type = signal.get("trade_type", "QUICK")
    price = float(signal.get("price", 0) or 0)
    mc = float(signal.get("market_cap", 0) or 0)
    buy_ratio = float(signal.get("buy_ratio", 0) or 0)
    liquidity = float(signal.get("liquidity", 0) or 0)

    if not token_address or price <= 0:
        return False

    now_ts = datetime.now().timestamp()

    # ===== CLEAN UP EXPIRED SIGNALS =====
    expired = [addr for addr, sig in _TRACKED_SIGNALS.items() if sig.is_expired]
    for addr in expired:
        del _TRACKED_SIGNALS[addr]

    # ===== UPDATE OR CREATE SIGNAL TRACKER =====
    if token_address in _TRACKED_SIGNALS:
        # Update existing tracker
        tracked = _TRACKED_SIGNALS[token_address]
        tracked.last_seen = now_ts
        tracked.signal_count += 1
        tracked.current_price = price
        tracked.current_mc = mc
        tracked.buy_ratio = buy_ratio
        tracked.liquidity = liquidity
        tracked.prices.append((now_ts, price))

        # Update peak
        if price > tracked.peak_price:
            tracked.peak_price = price
            tracked.peak_mc = mc
    else:
        # New signal - start tracking (DON'T BUY YET)
        tracked = TrackedSignal(
            address=token_address,
            symbol=symbol,
            trade_type=trade_type,
            first_seen=now_ts,
            last_seen=now_ts,
            signal_count=1,
            peak_price=price,
            peak_mc=mc,
            current_price=price,
            current_mc=mc,
            liquidity=liquidity,
            buy_ratio=buy_ratio,
            prices=[(now_ts, price)]
        )
        _TRACKED_SIGNALS[token_address] = tracked
        print(f"📡 Tracking ${symbol} - waiting {SIGNAL_MIN_AGE_MINS}m + dip...")
        return False

    # ===== CHECK IF READY TO BUY (DATA-DRIVEN) =====
    # Instead of strict time wait, use data signals to decide entry

    # Calculate entry score based on data
    entry_score = 0
    entry_reasons = []

    # Factor 1: Age gives some confidence (but not required)
    if tracked.age_mins >= SIGNAL_MIN_AGE_MINS:
        entry_score += 30
        entry_reasons.append(f"aged {tracked.age_mins:.0f}m")
    elif tracked.age_mins >= 5:
        entry_score += 15
        entry_reasons.append(f"aging {tracked.age_mins:.0f}m")

    # Factor 2: Dipping from peak (key signal!)
    if tracked.is_good_dip:  # >= 10% dip
        entry_score += 40
        entry_reasons.append(f"dip {tracked.dip_from_peak_pct:.0f}%")
    elif tracked.is_dipping:  # >= 5% dip
        entry_score += 25
        entry_reasons.append(f"small dip {tracked.dip_from_peak_pct:.0f}%")

    # Factor 3: Buy pressure returning
    if buy_ratio >= 2.0:
        entry_score += 25
        entry_reasons.append(f"strong {buy_ratio:.1f}x")
    elif buy_ratio >= 1.5:
        entry_score += 15
        entry_reasons.append(f"buy {buy_ratio:.1f}x")

    # Factor 4: Multiple signal confirmations
    if tracked.signal_count >= 3:
        entry_score += 15
        entry_reasons.append(f"{tracked.signal_count} signals")

    # Need minimum entry score to buy
    MIN_ENTRY_SCORE = 50  # Flexible threshold

    if entry_score < MIN_ENTRY_SCORE:
        print(f"⏳ ${symbol}: score {entry_score}/{MIN_ENTRY_SCORE} | {' | '.join(entry_reasons) if entry_reasons else 'waiting'}")
        return False

    print(f"✅ ${symbol}: ENTRY score {entry_score} | {' | '.join(entry_reasons)}")

    # 3. Basic filters
    if buy_ratio < MIN_BUY_RATIO:
        print(f"Skip ${symbol}: buy ratio {buy_ratio:.2f} < {MIN_BUY_RATIO}")
        return False
    if liquidity < MIN_SIGNAL_LIQUIDITY:
        print(f"Skip ${symbol}: liq ${liquidity:,.0f} < ${MIN_SIGNAL_LIQUIDITY:,.0f}")
        return False

    # 4. Check if already in position
    positions = load_positions()
    open_positions = [p for p in positions if p.status == "OPEN"]
    for p in open_positions:
        if p.token_address == token_address:
            return False

    # 5. Trade count limit
    trade_count = count_trades_for_token(token_address)
    if trade_count >= MAX_TRADES_PER_TOKEN:
        print(f"Max trades reached for ${symbol} ({trade_count}/{MAX_TRADES_PER_TOKEN})")
        return False

    # 6. Max open trades
    if len(open_positions) >= MAX_OPEN_TRADES:
        print(f"Max open trades reached ({MAX_OPEN_TRADES})")
        return False

    # ===== READY TO BUY =====

    # Calculate budget
    wallet = get_wallet_pubkey()
    sol_balance = await get_sol_balance(wallet)
    usable_balance = max(0.0, sol_balance - MIN_FEE_RESERVE)
    budget = usable_balance * WALLET_UTILIZATION
    used = sum(p.sol_amount for p in open_positions)
    available = max(0.0, budget - used)
    remaining_slots = max(1, MAX_OPEN_TRADES - len(open_positions))
    sol_amount = min(MAX_POSITION_SOL, available / remaining_slots)

    # ALL trades use step buying - first buy is only 15%
    sol_amount = sol_amount * DCA_STEPS[0]  # 15% of normal position
    print(f"DCA Step 1: {DCA_STEPS[0]*100:.0f}% = {sol_amount:.4f} SOL")

    if sol_amount <= 0:
        print(f"No budget")
        return False

    # Get entry metrics
    entry_metrics = await get_token_metrics(token_address, retries=3)
    if entry_metrics:
        print(f"Entry: vol=${entry_metrics.vol_5m:.0f} buys={entry_metrics.buys_5m} sells={entry_metrics.sells_5m}")

    # Remove from tracker (we're buying now)
    del _TRACKED_SIGNALS[token_address]

    # Execute buy
    pos = await buy_token(token_address, symbol, trade_type, price, mc, sol_amount=sol_amount, entry_metrics=entry_metrics)
    return pos is not None


def get_tracked_signals_status() -> str:
    """Get status of currently tracked signals."""
    if not _TRACKED_SIGNALS:
        return "No signals being tracked"

    lines = [f"*📡 TRACKING* ({len(_TRACKED_SIGNALS)} signals)\n"]

    for addr, sig in sorted(_TRACKED_SIGNALS.items(), key=lambda x: x[1].age_mins, reverse=True):
        status = "⏳" if not sig.is_aged else ("📉" if sig.is_dipping else "📈")
        dip_str = f"-{sig.dip_from_peak_pct:.0f}%" if sig.dip_from_peak_pct > 0 else "PEAK"
        mc_str = f"{sig.current_mc/1000:.0f}K" if sig.current_mc < 1_000_000 else f"{sig.current_mc/1_000_000:.1f}M"

        lines.append(f"{status} `{sig.symbol}` {sig.age_mins:.0f}m | {dip_str} | {mc_str}")

    return "\n".join(lines)


def format_session_summary() -> str:
    """Format session stats for display."""
    stats = get_session_stats()
    runtime = datetime.now() - SESSION_START
    hours = runtime.total_seconds() / 3600

    lines = [
        "*📊 SESSION SUMMARY*",
        f"ID: `{stats.session_id}`",
        f"Runtime: {hours:.1f}h",
        "",
    ]

    # Balance tracking
    if stats.starting_balance > 0:
        lines.append(f"*Start:* `{stats.starting_balance:.4f}` SOL")
        lines.append(f"*Now:*   `{stats.current_balance:.4f}` SOL")
        change = stats.wallet_change_sol
        pct = stats.wallet_change_pct
        emoji = "🟢" if change >= 0 else "🔴"
        lines.append(f"{emoji} *Change:* `{change:+.4f}` SOL ({pct:+.1f}%)")
        lines.append("")

    lines.append(f"*Trades:* {stats.buys} buys / {stats.sells} sells")
    lines.append(f"*Win Rate:* {stats.win_rate:.0f}% ({stats.wins}W / {stats.losses}L)")
    lines.append("")
    lines.append(f"*SOL In:*  {stats.sol_in:.6f}")
    lines.append(f"*SOL Out:* {stats.sol_out:.6f}")
    lines.append(f"*Net PnL:* `{stats.net_pnl_sol:+.6f}` SOL ({stats.net_pnl_pct:+.1f}%)")

    if stats.best_trade_symbol:
        lines.append(f"Best:  {stats.best_trade_symbol} +{stats.best_trade_pnl:.1f}%")
    if stats.worst_trade_symbol:
        lines.append(f"Worst: {stats.worst_trade_symbol} {stats.worst_trade_pnl:.1f}%")

    return "\n".join(lines)


async def run_live_manager(interval_secs: int = 30):
    """Run live position manager loop with enhanced exit logic."""
    print("\n" + "=" * 50)
    print("LIVE TRADER ACTIVE")
    print(f"Session: {SESSION_ID}")
    print("=" * 50)

    # Log session start
    log_session(f"=== SESSION START ===")
    log_session(f"Session ID: {SESSION_ID}")

    wallet = get_wallet_pubkey()
    if wallet:
        balance = await get_sol_balance(wallet)
        # Record starting balance for session tracking
        _SESSION_STATS.starting_balance = balance
        _SESSION_STATS.current_balance = balance
        save_session_stats()
        print(f"Wallet: {wallet[:8]}...{wallet[-4:]}")
        print(f"Balance: {balance:.4f} SOL")
        log_session(f"Wallet: {wallet[:8]}...{wallet[-4:]} | Balance: {balance:.4f} SOL")
        log_session(f"Starting balance: {balance:.6f} SOL")
    else:
        print("WARNING: No wallet configured!")
        print("Add your private key to keys.env")

    print(f"\nSession Log: logs/session_{SESSION_ID}.log")

    print(f"\nConfig:")
    print(f"  Max position: {MAX_POSITION_SOL} SOL | Max open: {MAX_OPEN_TRADES}")
    print(f"  DCA Steps: 15%/25%/60% at Entry/12%/28% dip")
    print(f"  Loss exits only after Step 3 complete")
    print(f"  Targets: Q=20% M=37% G=112% R=25%")
    print("=" * 50 + "\n")

    while True:
        try:
            await manage_positions()
        except Exception as e:
            import traceback
            print(f"Manager error: {e}")
            traceback.print_exc()

        await asyncio.sleep(interval_secs)


def format_live_status() -> str:
    """Format live positions for basic display."""
    positions = load_positions()
    open_pos = [p for p in positions if p.status == "OPEN"]
    closed_pos = [p for p in positions if p.status == "CLOSED"]

    msg = "*LIVE POSITIONS*\n"
    msg += "=" * 20 + "\n\n"

    if not open_pos:
        msg += "No open positions\n"
    else:
        for p in open_pos:
            entry = datetime.fromisoformat(p.entry_time)
            held_mins = (datetime.now() - entry).total_seconds() / 60
            mc_str = f"{p.entry_mc/1000:.0f}K" if p.entry_mc < 1_000_000 else f"{p.entry_mc/1_000_000:.1f}M"
            msg += f"${p.symbol} [{p.trade_type}]\n"
            msg += f"  Entry: ${p.entry_price:.8f} | MC: {mc_str}\n"
            msg += f"  Amount: {p.sol_amount:.4f} SOL | {held_mins:.0f}m\n"
            if p.max_pnl_percent > 0:
                msg += f"  Max PnL: {p.max_pnl_percent:+.1f}%\n"
            msg += "\n"

    if closed_pos:
        wins = len([p for p in closed_pos if p.pnl_percent > 0])
        total_pnl = sum(p.pnl_percent for p in closed_pos)
        msg += f"\nClosed: {len(closed_pos)} ({wins}W/{len(closed_pos)-wins}L)\n"
        msg += f"Total PnL: {total_pnl:+.1f}%\n"

    return msg


async def format_live_status_detailed() -> str:
    """Format live positions with LIVE momentum/volume data."""
    positions = load_positions()
    open_pos = [p for p in positions if p.status == "OPEN"]
    closed_pos = [p for p in positions if p.status == "CLOSED"]

    msg = "*LIVE POSITIONS (DETAILED)*\n"
    msg += "=" * 25 + "\n\n"

    if not open_pos:
        msg += "No open positions\n"
    else:
        for p in open_pos:
            entry = datetime.fromisoformat(p.entry_time)
            held_mins = (datetime.now() - entry).total_seconds() / 60

            # Fetch live metrics
            metrics = await get_token_metrics(p.token_address)

            msg += f"*${p.symbol}* [{p.trade_type}]\n"

            if metrics and metrics.price:
                # For RANGE trades with DCA, use avg price for PnL
                entry_price = p.dca_avg_price if p.dca_avg_price > 0 else p.entry_price
                pnl = ((metrics.price - entry_price) / entry_price) * 100
                mc_str = f"{metrics.mc/1000:.0f}K" if metrics.mc < 1_000_000 else f"{metrics.mc/1_000_000:.1f}M"
                trend = "[+]" if metrics.change_5m > 0 else "[-]" if metrics.change_5m < -5 else "[=]"

                msg += f"  PnL: `{pnl:+.1f}%` | Max: {p.max_pnl_percent:+.1f}%\n"
                msg += f"  MC: {mc_str} | {held_mins:.0f}m held\n"
                msg += f"  {trend} Vol: ${metrics.vol_5m:.0f} | {metrics.buy_ratio:.1f}x buy\n"

                # DCA info for ALL trades
                dca_step = p.dca_step if p.dca_step and p.dca_step > 0 else 1
                total_sol = p.dca_total_sol if p.dca_total_sol and p.dca_total_sol > 0 else p.sol_amount
                msg += f"  📊 Step: {dca_step}/3 | Total: {total_sol:.4f} SOL\n"
                if dca_step < 3:
                    # Calculate dip from ORIGINAL entry price (not average)
                    dip_now = ((p.entry_price - metrics.price) / p.entry_price) * 100 if p.entry_price > 0 else 0
                    next_trigger = DCA_STEP_TRIGGERS[dca_step] if dca_step < len(DCA_STEP_TRIGGERS) else 999
                    msg += f"  Next step: {dip_now:.1f}% / {next_trigger}% from entry\n"

                # Volume decay warning
                if p.entry_vol_5m > 0:
                    vol_ratio = metrics.vol_5m / p.entry_vol_5m
                    if vol_ratio < 0.5:
                        msg += f"  [!] Vol decay: {vol_ratio*100:.0f}% of entry\n"

                # Dump warning
                if metrics.is_dumping:
                    msg += f"  [!] Dump: {metrics.sells_5m}s > {metrics.buys_5m}b\n"
            else:
                msg += f"  [!] Could not fetch live data\n"
                msg += f"  Entry MC: ${p.entry_mc:,.0f}\n"

            msg += f"  Amount: {p.sol_amount:.4f} SOL\n\n"

    # Trade count per token
    if open_pos:
        msg += "*Trade Counts:*\n"
        token_counts = {}
        for p in positions:
            addr = p.token_address
            token_counts[p.symbol] = token_counts.get(p.symbol, 0) + 1
        for symbol, count in sorted(token_counts.items(), key=lambda x: -x[1])[:5]:
            status = "[X]" if count >= MAX_TRADES_PER_TOKEN else "[OK]"
            msg += f"  {status} ${symbol}: {count}/{MAX_TRADES_PER_TOKEN}\n"
        msg += "\n"

    if closed_pos:
        wins = len([p for p in closed_pos if p.pnl_percent > 0])
        total_pnl = sum(p.pnl_percent for p in closed_pos)
        avg_win = sum(p.pnl_percent for p in closed_pos if p.pnl_percent > 0) / max(1, wins)
        avg_loss = sum(p.pnl_percent for p in closed_pos if p.pnl_percent <= 0) / max(1, len(closed_pos) - wins)
        msg += f"*Stats:* {len(closed_pos)} trades ({wins}W/{len(closed_pos)-wins}L)\n"
        msg += f"Total: `{total_pnl:+.1f}%` | Avg W: {avg_win:+.1f}% | Avg L: {avg_loss:+.1f}%\n"

    return msg


async def format_tg_position_update() -> str:
    """Format a compact TG position update message."""
    positions = load_positions()
    open_pos = [p for p in positions if p.status == "OPEN"]

    if not open_pos:
        return ""

    wallet = get_wallet_pubkey()
    lines = [f"*📊 POSITIONS* ({len(open_pos)} open)"]

    total_pnl = 0.0
    for p in open_pos:
        metrics = await get_token_metrics(p.token_address)
        entry = datetime.fromisoformat(p.entry_time)
        held_mins = (datetime.now() - entry).total_seconds() / 60

        if metrics and metrics.price > 0:
            # Get actual SOL value
            raw_amount = await get_token_balance_raw(wallet, p.token_address)
            sol_value = await get_token_value_sol(p.token_address, raw_amount)
            if sol_value and p.sol_amount > 0:
                pnl = ((sol_value - p.sol_amount) / p.sol_amount) * 100
            else:
                pnl = ((metrics.price - p.entry_price) / p.entry_price) * 100

            total_pnl += pnl

            # Momentum indicator
            if metrics.buy_ratio >= 2.0:
                momentum = "🟢"
            elif metrics.buy_ratio >= 1.5:
                momentum = "🟡"
            elif metrics.buy_ratio < 1.0:
                momentum = "🔴"
            else:
                momentum = "⚪"

            mc_str = f"{metrics.mc/1000:.0f}K" if metrics.mc < 1_000_000 else f"{metrics.mc/1_000_000:.1f}M"

            lines.append(f"{momentum} `{p.symbol}` {pnl:+.1f}% | {mc_str} | {metrics.buy_ratio:.1f}x | {held_mins:.0f}m")

            # Warnings
            if p.entry_vol_5m > 0 and metrics.vol_5m < p.entry_vol_5m * 0.3:
                lines.append(f"  ⚠️ Vol decay")
            if metrics.sells_5m > metrics.buys_5m * 1.5:
                lines.append(f"  ⚠️ Dump pressure")
        else:
            lines.append(f"⚪ `{p.symbol}` ? | {held_mins:.0f}m (no data)")

    lines.append(f"\n*Total: {total_pnl:+.1f}%*")

    # Show balance if tracked
    stats = get_session_stats()
    if stats.starting_balance > 0:
        change = stats.wallet_change_sol
        pct = stats.wallet_change_pct
        bal_emoji = "🟢" if change >= 0 else "🔴"
        lines.append(f"{bal_emoji} Bal: `{stats.current_balance:.4f}` SOL ({change:+.4f} / {pct:+.1f}%)")

    return "\n".join(lines)


async def run_tg_position_updates(interval_secs: int = None):
    """Send periodic position updates to TG - edits same message in place."""
    if interval_secs is None:
        interval_secs = TG_POSITION_UPDATE_SECS

    print(f"TG position updates: every {interval_secs}s (single message)")

    live_msg_id = 0  # Track the message to edit

    while True:
        try:
            msg = await format_tg_position_update()
            if msg:
                # Add timestamp so user knows it's fresh
                msg += f"\n_Updated: {datetime.now().strftime('%H:%M:%S')}_"

                if live_msg_id:
                    # Try to edit existing message
                    success = await edit_tg(live_msg_id, msg, reply_markup=get_quick_buttons())
                    if not success:
                        # Message was deleted or too old, send new one
                        live_msg_id = await send_tg(msg, reply_markup=get_quick_buttons())
                else:
                    # First update - send new message
                    live_msg_id = await send_tg(msg, reply_markup=get_quick_buttons())
        except Exception as e:
            print(f"TG update error: {e}")

        await asyncio.sleep(interval_secs)


async def get_all_token_accounts(pubkey: str) -> list:
    """Get all SPL token accounts for a wallet."""
    accounts = []
    try:
        async with aiohttp.ClientSession() as session:
            for program_id in (SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        pubkey,
                        {"programId": program_id},
                        {"encoding": "jsonParsed"},
                    ],
                }
                async with session.post(SOLANA_RPC_URL, json=payload) as resp:
                    data = await resp.json()
                    for acc in data.get("result", {}).get("value", []):
                        info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                        mint = info.get("mint", "")
                        amount = int(info.get("tokenAmount", {}).get("amount", "0") or 0)
                        if mint and amount > 0:
                            accounts.append({"mint": mint, "amount": amount})
    except Exception as e:
        print(f"Error getting token accounts: {e}")
    return accounts


async def sync_positions() -> dict:
    """Sync positions with on-chain balances - fix OPEN/CLOSED mismatches."""
    wallet = get_wallet_pubkey()
    if not wallet:
        print("No wallet configured")
        return {"closed": 0, "reopened": 0, "closed_syms": [], "reopened_syms": []}

    positions = load_positions()
    open_positions = [p for p in positions if p.status == "OPEN"]
    closed_positions = [p for p in positions if p.status == "CLOSED"]

    print(f"\nSyncing positions with on-chain balances...")
    print(f"Open: {len(open_positions)} | Closed: {len(closed_positions)}\n")

    closed_count = 0
    reopened_count = 0
    closed_syms = []
    reopened_syms = []

    # Check OPEN positions - close any with 0 balance
    for pos in open_positions:
        raw_balance = await get_token_balance_raw(wallet, pos.token_address)

        if raw_balance == 0:
            metrics = await get_token_metrics(pos.token_address)
            current_price = metrics.price if metrics else 0
            pnl = ((current_price - pos.entry_price) / pos.entry_price) * 100 if current_price > 0 else -100

            print(f"  ${pos.symbol}: OPEN but 0 balance -> marking CLOSED (PnL: {pnl:+.1f}%)")

            pos.status = "CLOSED"
            pos.exit_price = current_price
            pos.exit_time = datetime.now().isoformat()
            pos.exit_tx = "SYNC_SOLD"
            pos.pnl_percent = pnl

            for i, p in enumerate(positions):
                if p.token_address == pos.token_address and p.entry_time == pos.entry_time:
                    positions[i] = pos
                    break
            closed_count += 1
            closed_syms.append(pos.symbol)
        else:
            print(f"  ${pos.symbol}: OK ({raw_balance} tokens)")

    # Check recently CLOSED positions - reopen any that still have tokens!
    # Only check positions closed in last 24h
    cutoff = datetime.now() - timedelta(hours=24)
    recent_closed = [p for p in closed_positions if p.exit_time and datetime.fromisoformat(p.exit_time) > cutoff]

    for pos in recent_closed:
        raw_balance = await get_token_balance_raw(wallet, pos.token_address)

        if raw_balance > 0:
            print(f"  ${pos.symbol}: CLOSED but has {raw_balance} tokens -> RE-OPENING!")

            pos.status = "OPEN"
            pos.exit_price = 0.0
            pos.exit_time = ""
            pos.exit_tx = ""
            pos.pnl_percent = 0.0
            pos.token_amount = raw_balance

            for i, p in enumerate(positions):
                if p.token_address == pos.token_address and p.entry_time == pos.entry_time:
                    positions[i] = pos
                    break
            reopened_count += 1
            reopened_syms.append(pos.symbol)

    if closed_count > 0 or reopened_count > 0:
        save_positions(positions)
        print(f"\nSync complete: {closed_count} closed, {reopened_count} re-opened")
    else:
        print("\nAll positions in sync")
    return {
        "closed": closed_count,
        "reopened": reopened_count,
        "closed_syms": closed_syms,
        "reopened_syms": reopened_syms,
    }


async def sell_all_positions(reason: str = "SELL_ALL") -> dict:
    """Sell ALL open positions immediately."""
    positions = load_positions()
    open_pos = [p for p in positions if p.status == "OPEN"]

    if not open_pos:
        return {"sold": 0, "failed": 0, "total_pnl": 0}

    sold = 0
    failed = 0
    total_pnl = 0.0

    print(f"\n{'='*50}")
    print(f"SELLING ALL {len(open_pos)} POSITIONS")
    print(f"{'='*50}")

    for pos in open_pos:
        try:
            success = await sell_token(pos, reason)
            if success:
                sold += 1
                # Reload to get updated pnl
                updated_positions = load_positions()
                for p in updated_positions:
                    if p.token_address == pos.token_address and p.entry_time == pos.entry_time:
                        total_pnl += p.pnl_percent
                        break
            else:
                failed += 1
        except Exception as e:
            print(f"Error selling {pos.symbol}: {e}")
            failed += 1

        await asyncio.sleep(0.5)  # Brief pause between sells

    print(f"\nSold: {sold} | Failed: {failed} | Total PnL: {total_pnl:+.1f}%")

    return {"sold": sold, "failed": failed, "total_pnl": total_pnl}


async def sell_all_wallet_tokens() -> dict:
    """Sell ALL tokens in wallet - tracked AND untracked."""
    wallet = get_wallet_pubkey()
    if not wallet:
        return {"sold": 0, "failed": 0, "tokens": []}

    print(f"\n{'='*50}")
    print("SELLING ALL WALLET TOKENS")
    print(f"{'='*50}")

    # Get all token accounts
    all_tokens = await get_all_token_accounts(wallet)
    positions = load_positions()
    tracked_addresses = {p.token_address for p in positions if p.status == "OPEN"}

    sold = 0
    failed = 0
    sold_tokens = []

    for token in all_tokens:
        mint = token["mint"]
        raw_amount = token["amount"]

        # Skip native SOL
        if mint == SOL_MINT:
            continue

        # Skip if no balance
        if raw_amount <= 0:
            continue

        try:
            # Get token info
            metrics = await get_token_metrics(mint)
            symbol = "???"
            if metrics:
                # Try to get symbol from pair info
                pass

            print(f"\nSelling {mint[:8]}... ({raw_amount} tokens)")

            # Get quote
            quote = await get_jupiter_quote(mint, SOL_MINT, int(raw_amount))
            if not quote:
                print(f"  No quote available")
                failed += 1
                continue

            sol_out = int(quote.get("outAmount", 0)) / 1_000_000_000
            print(f"  Est: {sol_out:.6f} SOL")

            # Execute swap
            tx_hash = await execute_swap(quote, wallet)
            if tx_hash:
                print(f"  TX: {tx_hash}")
                sold += 1
                sold_tokens.append({
                    "mint": mint,
                    "sol": sol_out,
                    "tracked": mint in tracked_addresses
                })

                # If it was tracked, mark as closed
                if mint in tracked_addresses:
                    for i, pos in enumerate(positions):
                        if pos.token_address == mint and pos.status == "OPEN":
                            pos.status = "CLOSED"
                            pos.exit_time = datetime.now().isoformat()
                            pos.exit_tx = tx_hash
                            if pos.sol_amount > 0:
                                pos.pnl_percent = ((sol_out - pos.sol_amount) / pos.sol_amount) * 100
                            positions[i] = pos
                    save_positions(positions)

                # Try to close token account
                await close_token_account(mint)
            else:
                print(f"  Swap failed")
                failed += 1

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"  Error: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Sold: {sold} | Failed: {failed}")
    total_sol = sum(t["sol"] for t in sold_tokens)
    print(f"Total SOL recovered: {total_sol:.6f}")
    print(f"{'='*50}")

    return {"sold": sold, "failed": failed, "tokens": sold_tokens, "total_sol": total_sol}


async def reopen_position(symbol: str) -> bool:
    """Re-open a position that was incorrectly marked as CLOSED (still has tokens)."""
    wallet = get_wallet_pubkey()
    if not wallet:
        print("No wallet configured")
        return False

    positions = load_positions()

    # Find the most recent closed position for this symbol
    closed_pos = [p for p in positions if p.symbol.lower() == symbol.lower() and p.status == "CLOSED"]
    if not closed_pos:
        print(f"No closed position found for ${symbol}")
        return False

    # Sort by exit_time descending to get most recent
    closed_pos.sort(key=lambda p: p.exit_time, reverse=True)
    pos = closed_pos[0]

    # Check if we still have tokens
    raw_balance = await get_token_balance_raw(wallet, pos.token_address)
    if raw_balance <= 0:
        print(f"No tokens found for ${symbol} - position correctly closed")
        return False

    print(f"Found {raw_balance} tokens for ${symbol} - re-opening position!")

    # Re-open the position
    pos.status = "OPEN"
    pos.exit_price = 0.0
    pos.exit_time = ""
    pos.exit_tx = ""
    pos.pnl_percent = 0.0
    pos.token_amount = raw_balance  # Update to actual balance

    # Update in positions list
    for i, p in enumerate(positions):
        if p.token_address == pos.token_address and p.entry_time == pos.entry_time:
            positions[i] = pos
            break
    save_positions(positions)

    print(f"Position ${symbol} re-opened!")
    return True


async def force_close_position(symbol: str, reason: str = "MANUAL"):
    """Force close a position by symbol (for stuck trades)."""
    positions = load_positions()

    for i, pos in enumerate(positions):
        if pos.symbol.lower() == symbol.lower() and pos.status == "OPEN":
            metrics = await get_token_metrics(pos.token_address)
            current_price = metrics.price if metrics else 0

            if current_price > 0:
                pnl = ((current_price - pos.entry_price) / pos.entry_price) * 100
            else:
                pnl = -100

            pos.status = "CLOSED"
            pos.exit_price = current_price
            pos.exit_time = datetime.now().isoformat()
            pos.exit_tx = reason
            pos.pnl_percent = pnl

            positions[i] = pos
            save_positions(positions)

            print(f"Force closed ${symbol} - PnL: {pnl:+.1f}%")
            return True

    print(f"No open position found for ${symbol}")
    return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "sync":
            # Sync positions with on-chain balances
            asyncio.run(sync_positions())
        elif cmd == "close" and len(sys.argv) > 2:
            # Force close a position: python live_trader.py close SYMBOL
            symbol = sys.argv[2]
            asyncio.run(force_close_position(symbol))
        elif cmd == "status":
            # Show detailed status
            async def show_status():
                msg = await format_live_status_detailed()
                print(msg)
            asyncio.run(show_status())
        else:
            print("Usage:")
            print("  python live_trader.py sync    - Sync positions with on-chain balances")
            print("  python live_trader.py close SYMBOL - Force close a position")
            print("  python live_trader.py status  - Show detailed position status")
    else:
        # Test wallet
        wallet = get_wallet_pubkey()
        if wallet:
            print(f"Wallet loaded: {wallet}")
        else:
            print("No wallet - add key to keys.env")

        # Run manager
        asyncio.run(run_live_manager())
