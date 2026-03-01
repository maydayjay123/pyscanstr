"""
Pair Trader — Manual CA-based trading with 2 slots.

Flow:
  1. User sends /trade <CA> via Telegram
  2. Bot watches token, waits for MC-based entry dip from current price
  3. Step 1 fills (15%) → Step 2/3 levels pre-set from entry price
  4. Steps 2 (25%) and 3 (60%) auto-buy on deeper dips
  5. Trail TP activates at +12%, trails 4% below peak
  6. On close → auto re-watch same token for next cycle

Wallet: 85% split across 2 slots. Each slot compounds its own profit.
Persistent state saved to slot_budgets.json and pair_slots.json.
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from dotenv import load_dotenv
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

load_dotenv("keys.env")

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
SOLANA_RPC_URL      = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
MAX_SLIPPAGE_BPS    = int(float(os.getenv("MAX_SLIPPAGE_PERCENT", "15")) * 100)
MIN_FEE_RESERVE     = float(os.getenv("MIN_FEE_RESERVE", "0.005"))
WALLET_UTILIZATION  = 0.85
NUM_SLOTS           = 4
DCA_SPLITS          = [0.15, 0.25, 0.60]   # step1 / step2 / step3

SOL_MINT            = "So11111111111111111111111111111111111111112"
SPL_TOKEN_PROGRAM   = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM  = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

SLOTS_FILE          = "pair_slots.json"
BUDGETS_FILE        = "slot_budgets.json"
PAIR_HISTORY_FILE   = "pair_history.json"
TRADES_FILE         = "pair_trades.csv"


# ─────────────────────────────────────────
# MC-based parameter tables
# ─────────────────────────────────────────

def get_entry_dip(mc: float) -> float:
    """How far to wait for price to dip before Step 1 entry (% from current price)."""
    if mc < 500_000:   return 10.0
    if mc < 2_000_000: return 7.0
    if mc < 10_000_000:return 5.0
    return 3.0


def get_dca_drops(mc: float) -> tuple:
    """Step 2 and Step 3 drop % from Step 1 entry price."""
    if mc < 500_000:    return (15.0, 35.0)
    if mc < 2_000_000:  return (10.0, 25.0)
    if mc < 10_000_000: return (7.0,  18.0)
    return (5.0, 12.0)


def get_trail_params(mc: float) -> tuple:
    """Returns (activate_pct, trail_pct). Trail activates at activate_pct, sits trail_pct below peak."""
    # Trail always activates at 12%, trails 4% below peak regardless of MC
    return (12.0, 4.0)


# ─────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────

@dataclass
class SlotBudget:
    slot_id: int
    budget_sol: float       # current budget (grows with profit)
    start_budget_sol: float # original starting budget
    total_profit_sol: float = 0.0
    trade_count: int        = 0


@dataclass
class PairSlot:
    slot_id: int
    status: str             # "empty" | "watching" | "open"
    token_address: str      = ""
    symbol: str             = ""
    entry_mc: float         = 0.0

    # Entry watching
    watch_price: float      = 0.0   # price when /trade was sent
    watch_time: str         = ""
    entry_dip_pct: float    = 0.0   # how much dip we're waiting for

    # DCA levels (set on Step 1 fill)
    dca_step: int           = 0     # 0=waiting, 1=step1 filled, 2=step2 filled, 3=fully invested
    entry_price: float      = 0.0   # Step 1 fill price
    step2_price: float      = 0.0   # price trigger for step 2
    step3_price: float      = 0.0   # price trigger for step 3
    dca_avg_price: float    = 0.0   # running weighted average

    # Position tracking
    total_sol_invested: float = 0.0
    step1_sol: float          = 0.0
    step2_sol: float          = 0.0
    step3_sol: float          = 0.0
    token_amount: float       = 0.0  # total tokens held

    # Profit tracking
    max_pnl_pct: float        = 0.0
    trail_active: bool        = False
    peak_price: float         = 0.0  # highest price seen while open

    # Timestamps
    entry_time: str           = ""
    exit_time: str            = ""

    # Exit
    exit_price: float         = 0.0
    exit_pnl_pct: float       = 0.0
    exit_reason: str          = ""
    exit_tx: str              = ""


# ─────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────

def load_slots() -> list:
    if not os.path.exists(SLOTS_FILE):
        return [PairSlot(slot_id=i, status="empty") for i in range(1, NUM_SLOTS + 1)]
    try:
        with open(SLOTS_FILE) as f:
            raw = json.load(f)
        return [PairSlot(**s) for s in raw]
    except:
        return [PairSlot(slot_id=i, status="empty") for i in range(1, NUM_SLOTS + 1)]


def save_slots(slots: list):
    with open(SLOTS_FILE, "w") as f:
        json.dump([asdict(s) for s in slots], f, indent=2)


def load_budgets() -> list:
    if not os.path.exists(BUDGETS_FILE):
        return None  # will be initialised on first run
    try:
        with open(BUDGETS_FILE) as f:
            raw = json.load(f)
        return [SlotBudget(**b) for b in raw]
    except:
        return None


def save_budgets(budgets: list):
    with open(BUDGETS_FILE, "w") as f:
        json.dump([asdict(b) for b in budgets], f, indent=2)


def load_pair_history() -> dict:
    if not os.path.exists(PAIR_HISTORY_FILE):
        return {}
    try:
        with open(PAIR_HISTORY_FILE) as f:
            return json.load(f)
    except:
        return {}


def save_pair_history(history: dict):
    with open(PAIR_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


MIN_TRADE_SOL = 0.002   # minimum SOL to attempt a buy

async def init_budgets() -> list:
    """Initialise slot budgets from current wallet balance.
    Re-initialises if saved budgets are effectively zero (wallet was empty at startup)."""
    existing = load_budgets()
    if existing:
        total = sum(b.budget_sol for b in existing)
        if total >= MIN_TRADE_SOL:
            return existing
        # Budgets saved but all near-zero — wallet was empty; re-init now
        print("Budgets near-zero, re-initialising from wallet...")

    wallet = get_wallet_pubkey()
    sol = await get_sol_balance(wallet) if wallet else 0.0
    usable = max(0.0, sol - MIN_FEE_RESERVE)
    per_slot = (usable * WALLET_UTILIZATION) / NUM_SLOTS
    budgets = [
        SlotBudget(slot_id=i, budget_sol=per_slot, start_budget_sol=per_slot)
        for i in range(1, NUM_SLOTS + 1)
    ]
    save_budgets(budgets)
    print(f"Budgets initialised: {per_slot:.4f} SOL per slot (wallet: {sol:.4f} SOL)")
    return budgets


# ─────────────────────────────────────────
# Wallet / token helpers (reused from live_trader)
# ─────────────────────────────────────────

_KEYPAIR_CACHE = None

def _load_keypair():
    """Load keypair from keys.env — handles JSON array or base58 format."""
    global _KEYPAIR_CACHE
    if _KEYPAIR_CACHE is not None:
        return _KEYPAIR_CACHE

    import base58 as _b58
    key_bytes = None

    # Try reading JSON array directly from file
    try:
        with open("keys.env", "r") as f:
            content = f.read()
        start = content.find("[")
        end = content.find("]") + 1
        if start != -1 and end > start:
            key_bytes = bytes(json.loads(content[start:end]))
    except:
        pass

    # Fallback: env var (base58 or JSON string)
    if key_bytes is None:
        raw = os.getenv("SOLANA_PRIVATE_KEY", "")
        if raw and raw != "your_private_key_here":
            try:
                key_bytes = bytes(json.loads(raw)) if raw.strip().startswith("[") else _b58.b58decode(raw)
            except:
                pass

    if key_bytes is None:
        _KEYPAIR_CACHE = (None, None)
        return _KEYPAIR_CACHE

    try:
        import base58 as _b58
        pubkey_bytes = key_bytes[32:] if len(key_bytes) == 64 else key_bytes[:32]
        pubkey = _b58.b58encode(pubkey_bytes).decode()
        _KEYPAIR_CACHE = (key_bytes, pubkey)
    except:
        _KEYPAIR_CACHE = (None, None)
    return _KEYPAIR_CACHE


def get_wallet_pubkey() -> Optional[str]:
    _, pubkey = _load_keypair()
    return pubkey or None


async def get_sol_balance(pubkey: str) -> float:
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance", "params": [pubkey]}
            async with session.post(SOLANA_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                return data.get("result", {}).get("value", 0) / 1_000_000_000
    except:
        return 0.0


async def get_token_price_and_mc(token_address: str) -> tuple:
    """Returns (price_sol, price_usd, mc_usd) from DexScreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if not pairs:
                    return (0.0, 0.0, 0.0)
                # Use highest liquidity Solana pair
                sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                if not sol_pairs:
                    sol_pairs = pairs
                best = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                price_usd = float(best.get("priceUsd") or 0)
                price_native = float(best.get("priceNative") or 0)
                mc = float(best.get("marketCap") or 0)
                return (price_native, price_usd, mc)
    except:
        return (0.0, 0.0, 0.0)


async def get_token_symbol(token_address: str) -> str:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if pairs:
                    return pairs[0].get("baseToken", {}).get("symbol", "???")
    except:
        pass
    return "???"


async def get_token_balance_raw(pubkey: str, mint: str) -> int:
    try:
        async with aiohttp.ClientSession() as session:
            for program_id in (SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
                payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [pubkey, {"programId": program_id}, {"encoding": "jsonParsed"}],
                }
                async with session.post(SOLANA_RPC_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    for acc in data.get("result", {}).get("value", []):
                        info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                        if info.get("mint") == mint:
                            return int(info.get("tokenAmount", {}).get("amount", "0"))
    except:
        pass
    return 0


# ─────────────────────────────────────────
# Jupiter swap helpers
# ─────────────────────────────────────────

async def get_jupiter_quote(input_mint: str, output_mint: str, amount: int, slippage_bps: int) -> Optional[dict]:
    try:
        url = "https://api.jup.ag/swap/v1/quote"
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": slippage_bps,
            "restrictIntermediateTokens": "true",
        }
        headers = {"x-api-key": os.getenv("JUPITER_API_KEY", "")}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except:
        pass
    return None


async def execute_swap(quote: dict, wallet_pubkey: str) -> Optional[str]:
    from solders.keypair import Keypair
    import base64
    try:
        key_bytes, _ = _load_keypair()
        if not key_bytes:
            return None
        kp = Keypair.from_bytes(key_bytes)

        swap_url = "https://api.jup.ag/swap/v1/swap"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": os.getenv("JUPITER_API_KEY", ""),
        }
        payload = {
            "quoteResponse": quote,
            "userPublicKey": wallet_pubkey,
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": "auto",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(swap_url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                data = await resp.json()
                tx_b64 = data.get("swapTransaction")
                if not tx_b64:
                    return None

            # Sign and send
            from solders.transaction import VersionedTransaction
            tx_bytes = base64.b64decode(tx_b64)
            tx = VersionedTransaction.from_bytes(tx_bytes)
            signed = VersionedTransaction(tx.message, [kp])
            signed_b64 = base64.b64encode(bytes(signed)).decode()

            send_payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "sendTransaction",
                "params": [signed_b64, {"encoding": "base64", "maxRetries": 3,
                                        "preflightCommitment": "confirmed"}],
            }
            async with session.post(SOLANA_RPC_URL, json=send_payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                result = await resp.json()
                tx_hash = result.get("result")
                if tx_hash:
                    return tx_hash
    except Exception as e:
        print(f"Swap error: {e}")
    return None


async def buy_tokens(sol_amount: float, token_address: str, wallet: str) -> tuple:
    """Buy tokens with SOL. Returns (tx_hash, tokens_received)."""
    if sol_amount < MIN_TRADE_SOL:
        return (None, 0)   # silently skip — not enough SOL
    lamports = int(sol_amount * 1_000_000_000)
    for attempt in range(2):
        slippage = MAX_SLIPPAGE_BPS if attempt == 0 else int(MAX_SLIPPAGE_BPS * 1.3)
        quote = await get_jupiter_quote(SOL_MINT, token_address, lamports, slippage)
        if not quote:
            continue
        tokens_out = int(quote.get("outAmount", 0))
        tx = await execute_swap(quote, wallet)
        if tx:
            return (tx, tokens_out)
        await asyncio.sleep(2)
    return (None, 0)


async def sell_tokens(token_address: str, wallet: str) -> tuple:
    """Sell all tokens. Returns (tx_hash, sol_received)."""
    raw = await get_token_balance_raw(wallet, token_address)
    if raw <= 0:
        return (None, 0.0)
    sol_before = await get_sol_balance(wallet)
    for attempt in range(2):
        slippage = MAX_SLIPPAGE_BPS if attempt == 0 else int(MAX_SLIPPAGE_BPS * 1.3)
        quote = await get_jupiter_quote(token_address, SOL_MINT, raw, slippage)
        if not quote:
            continue
        tx = await execute_swap(quote, wallet)
        if tx:
            await asyncio.sleep(6)
            sol_after = await get_sol_balance(wallet)
            sol_received = max(0.0, sol_after - sol_before)
            return (tx, sol_received)
        await asyncio.sleep(2)
    return (None, 0.0)


# ─────────────────────────────────────────
# Telegram helper
# ─────────────────────────────────────────

async def notify(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
                   "parse_mode": "Markdown", "disable_web_page_preview": True}
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10))
    except:
        pass


# ─────────────────────────────────────────
# CSV logging
# ─────────────────────────────────────────

def log_trade_csv(slot: PairSlot):
    import csv
    file_exists = os.path.exists(TRADES_FILE)
    with open(TRADES_FILE, "a", newline="") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["slot_id", "symbol", "token_address", "entry_time", "exit_time",
                        "held_mins", "entry_mc", "entry_price", "exit_price",
                        "sol_invested", "pnl_pct", "max_pnl_pct", "exit_reason",
                        "dca_steps", "exit_tx"])
        if slot.entry_time and slot.exit_time:
            held = (datetime.fromisoformat(slot.exit_time) -
                    datetime.fromisoformat(slot.entry_time)).total_seconds() / 60
        else:
            held = 0
        w.writerow([slot.slot_id, slot.symbol, slot.token_address,
                    slot.entry_time, slot.exit_time, f"{held:.0f}",
                    f"{slot.entry_mc:.0f}", f"{slot.entry_price:.10f}",
                    f"{slot.exit_price:.10f}", f"{slot.total_sol_invested:.6f}",
                    f"{slot.exit_pnl_pct:.2f}", f"{slot.max_pnl_pct:.2f}",
                    slot.exit_reason, slot.dca_step, slot.exit_tx])


# ─────────────────────────────────────────
# Pair history — learns entry dip per coin
# ─────────────────────────────────────────

def record_trade_history(slot: PairSlot):
    """Save completed trade data for this token to improve next entry."""
    history = load_pair_history()
    addr = slot.token_address
    if addr not in history:
        history[addr] = {"symbol": slot.symbol, "trades": []}

    history[addr]["trades"].append({
        "entry_price":    slot.entry_price,
        "exit_price":     slot.exit_price,
        "entry_mc":       slot.entry_mc,
        "pnl_pct":        slot.exit_pnl_pct,
        "max_pnl_pct":    slot.max_pnl_pct,
        "dca_steps":      slot.dca_step,
        "entry_dip_used": slot.entry_dip_pct,
        "time":           slot.exit_time,
    })
    # Keep last 20 trades per pair
    history[addr]["trades"] = history[addr]["trades"][-20:]
    save_pair_history(history)


def get_adjusted_entry_dip(token_address: str, mc: float) -> float:
    """
    Base entry dip from MC table, adjusted by past trade history for this token.
    If past trades show the token tends to dip deeper before recovering,
    we wait a bit longer to get a better entry.
    """
    base = get_entry_dip(mc)
    history = load_pair_history()
    trades = history.get(token_address, {}).get("trades", [])
    if len(trades) < 2:
        return base

    # Look at what dip % we entered at and whether the trade was profitable
    # If most profitable trades came from deeper entries, nudge deeper
    profitable = [t for t in trades if t.get("pnl_pct", 0) > 0]
    losing     = [t for t in trades if t.get("pnl_pct", 0) <= 0]

    if not profitable:
        # All losses — try entering slightly deeper
        return min(base * 1.3, base + 5.0)

    avg_profitable_dip = sum(t.get("entry_dip_used", base) for t in profitable) / len(profitable)
    avg_losing_dip     = sum(t.get("entry_dip_used", base) for t in losing) / len(losing) if losing else base

    # If profitable trades entered deeper than losing ones, nudge toward profitable avg
    if avg_profitable_dip > avg_losing_dip:
        adjusted = (base + avg_profitable_dip) / 2
    else:
        adjusted = base

    # Cap adjustment to ±50% of base
    return round(max(base * 0.5, min(base * 1.5, adjusted)), 1)


# ─────────────────────────────────────────
# Core: process a single slot each cycle
# ─────────────────────────────────────────

async def process_slot(slot: PairSlot, budget: SlotBudget, wallet: str) -> PairSlot:
    """
    Called every cycle for each slot.
    Returns the (possibly updated) slot.
    """
    if slot.status == "empty":
        return slot

    price_sol, price_usd, mc = await get_token_price_and_mc(slot.token_address)
    if price_sol <= 0:
        return slot

    # ── WATCHING: waiting for entry dip ──────────────────────────────────
    if slot.status == "watching":
        # Trail watch_price up with price — always measure dip from recent high
        if price_sol > slot.watch_price:
            slot.watch_price = price_sol

        dip_from_watch = ((slot.watch_price - price_sol) / slot.watch_price) * 100
        if dip_from_watch >= slot.entry_dip_pct:
            # Entry condition met — buy Step 1
            step1_sol = budget.budget_sol * DCA_SPLITS[0]
            tx, tokens = await buy_tokens(step1_sol, slot.token_address, wallet)
            if not tx:
                print(f"[{slot.symbol}] Step 1 buy failed (sol: {step1_sol:.4f}) — still watching")
                return slot

            # Pre-calculate Step 2 and 3 trigger prices
            step2_drop, step3_drop = get_dca_drops(mc)
            slot.entry_price    = price_sol
            slot.step2_price    = price_sol * (1 - step2_drop / 100)
            slot.step3_price    = price_sol * (1 - step3_drop / 100)
            slot.dca_avg_price  = price_sol
            slot.dca_step       = 1
            slot.step1_sol      = step1_sol
            slot.total_sol_invested = step1_sol
            slot.token_amount   = tokens
            slot.entry_mc       = mc
            slot.entry_time     = datetime.now().isoformat()
            slot.peak_price     = price_sol
            slot.status         = "open"

            await notify(
                f"✅ *ENTERED* `{slot.symbol}` [Slot {slot.slot_id}]\n"
                f"Step 1 @ ${price_usd:.6f} ({step1_sol:.4f} SOL)\n"
                f"Step 2 @ -{step2_drop:.0f}%  |  Step 3 @ -{step3_drop:.0f}%\n"
                f"Trail activates at +12%, trails 4% below peak"
            )
        return slot

    # ── OPEN: manage the position ─────────────────────────────────────────
    if slot.status == "open":
        pnl_pct = ((price_sol - slot.dca_avg_price) / slot.dca_avg_price) * 100

        # Update max PnL and peak price
        if pnl_pct > slot.max_pnl_pct:
            slot.max_pnl_pct = pnl_pct
        if price_sol > slot.peak_price:
            slot.peak_price = price_sol

        # ── DCA Step 2 ──
        if slot.dca_step == 1 and price_sol <= slot.step2_price:
            step2_sol = budget.budget_sol * DCA_SPLITS[1]
            tx, tokens = await buy_tokens(step2_sol, slot.token_address, wallet)
            if tx:
                slot.step2_sol = step2_sol
                slot.total_sol_invested += step2_sol
                slot.token_amount += tokens
                slot.dca_step = 2
                # Recalculate average price
                slot.dca_avg_price = slot.entry_price * (slot.step1_sol / slot.total_sol_invested) + \
                                     price_sol * (step2_sol / slot.total_sol_invested)
                await notify(
                    f"📉 *DCA Step 2* `{slot.symbol}` [Slot {slot.slot_id}]\n"
                    f"Bought @ ${price_usd:.6f} ({step2_sol:.4f} SOL)\n"
                    f"New avg: ${slot.dca_avg_price:.8f} | PnL: {pnl_pct:+.1f}%"
                )

        # ── DCA Step 3 ──
        elif slot.dca_step == 2 and price_sol <= slot.step3_price:
            step3_sol = budget.budget_sol * DCA_SPLITS[2]
            tx, tokens = await buy_tokens(step3_sol, slot.token_address, wallet)
            if tx:
                slot.step3_sol = step3_sol
                slot.total_sol_invested += step3_sol
                slot.token_amount += tokens
                slot.dca_step = 3
                # Recalculate weighted average
                prev_weight = slot.total_sol_invested - step3_sol
                slot.dca_avg_price = (slot.dca_avg_price * prev_weight + price_sol * step3_sol) / slot.total_sol_invested
                await notify(
                    f"📉 *DCA Step 3 (MAX)* `{slot.symbol}` [Slot {slot.slot_id}]\n"
                    f"Bought @ ${price_usd:.6f} ({step3_sol:.4f} SOL)\n"
                    f"New avg: ${slot.dca_avg_price:.8f} | PnL: {pnl_pct:+.1f}%"
                )

        # ── Trailing TP ──
        activate_pct, trail_pct = get_trail_params(mc)

        if pnl_pct >= activate_pct:
            slot.trail_active = True

        if slot.trail_active:
            # Trail sits trail_pct% below peak price
            trail_price = slot.peak_price * (1 - trail_pct / 100)
            if price_sol <= trail_price:
                # Sell
                reason = f"TRAIL +{slot.max_pnl_pct:.1f}% (trail {trail_pct:.0f}% below peak)"
                await _close_slot(slot, budget, wallet, price_sol, price_usd, reason)
                return slot

    return slot


async def _close_slot(slot: PairSlot, budget: SlotBudget, wallet: str,
                      price_sol: float, price_usd: float, reason: str):
    """Execute sell and update slot + budget state."""
    tx, sol_received = await sell_tokens(slot.token_address, wallet)
    if not tx:
        await notify(f"⚠️ `{slot.symbol}` sell failed — retrying next cycle")
        return

    pnl_pct = ((sol_received - slot.total_sol_invested) / slot.total_sol_invested) * 100 \
               if slot.total_sol_invested > 0 else 0.0
    profit_sol = sol_received - slot.total_sol_invested

    slot.exit_price   = price_sol
    slot.exit_time    = datetime.now().isoformat()
    slot.exit_pnl_pct = pnl_pct
    slot.exit_reason  = reason
    slot.exit_tx      = tx

    # Update budget — compound profit back into this slot
    budget.budget_sol      += profit_sol
    budget.total_profit_sol += profit_sol
    budget.trade_count      += 1
    save_budgets_global(slot.slot_id, budget)

    # Log
    log_trade_csv(slot)
    record_trade_history(slot)

    emoji = "🟢" if pnl_pct >= 0 else "🔴"
    await notify(
        f"{emoji} *CLOSED* `{slot.symbol}` [Slot {slot.slot_id}]\n"
        f"Reason: {reason}\n"
        f"PnL: *{pnl_pct:+.1f}%* | {slot.total_sol_invested:.4f} → {sol_received:.4f} SOL\n"
        f"Slot budget: {budget.budget_sol:.4f} SOL (+{budget.total_profit_sol:+.4f} all time)\n"
        f"[TX](https://solscan.io/tx/{tx})"
    )

    # Reset slot — keep token_address and symbol for auto re-watch
    prev_addr   = slot.token_address
    prev_symbol = slot.symbol
    prev_mc     = slot.entry_mc

    _reset_slot(slot)

    # Auto re-watch same token
    slot.token_address = prev_addr
    slot.symbol        = prev_symbol
    slot.status        = "watching"
    price_sol_new, _, mc_new = await get_token_price_and_mc(prev_addr)
    mc_for_dip = mc_new if mc_new > 0 else prev_mc
    slot.watch_price   = price_sol_new if price_sol_new > 0 else price_sol
    slot.watch_time    = datetime.now().isoformat()
    slot.entry_dip_pct = get_adjusted_entry_dip(prev_addr, mc_for_dip)

    await notify(
        f"👀 *Re-watching* `{prev_symbol}` [Slot {slot.slot_id}]\n"
        f"Waiting for -{slot.entry_dip_pct:.1f}% dip from current price\n"
        f"(Adjusted from trade history)"
    )


def _reset_slot(slot: PairSlot):
    """Clear all trade state from a slot (keeps slot_id)."""
    slot.status = "empty"
    slot.token_address = ""
    slot.symbol = ""
    slot.entry_mc = 0.0
    slot.watch_price = 0.0
    slot.watch_time = ""
    slot.entry_dip_pct = 0.0
    slot.dca_step = 0
    slot.entry_price = 0.0
    slot.step2_price = 0.0
    slot.step3_price = 0.0
    slot.dca_avg_price = 0.0
    slot.total_sol_invested = 0.0
    slot.step1_sol = 0.0
    slot.step2_sol = 0.0
    slot.step3_sol = 0.0
    slot.token_amount = 0.0
    slot.max_pnl_pct = 0.0
    slot.trail_active = False
    slot.peak_price = 0.0
    slot.entry_time = ""
    slot.exit_time = ""
    slot.exit_price = 0.0
    slot.exit_pnl_pct = 0.0
    slot.exit_reason = ""
    slot.exit_tx = ""


# Global budget cache so _close_slot can save without needing the full list
_BUDGETS_CACHE: list = []

def save_budgets_global(slot_id: int, updated_budget: SlotBudget):
    for i, b in enumerate(_BUDGETS_CACHE):
        if b.slot_id == slot_id:
            _BUDGETS_CACHE[i] = updated_budget
            break
    save_budgets(_BUDGETS_CACHE)


# ─────────────────────────────────────────
# Public API: TG command handlers
# ─────────────────────────────────────────

async def cmd_trade(token_address: str) -> str:
    """
    Called by /trade <CA>. Assigns token to a free slot and starts watching.
    Returns a status message string.
    """
    slots = load_slots()

    # Check if already tracking
    for s in slots:
        if s.token_address == token_address and s.status in ("watching", "open"):
            return f"Already tracking `{s.symbol}` in Slot {s.slot_id}"

    # Find free slot
    free = next((s for s in slots if s.status == "empty"), None)
    if not free:
        active = [f"Slot {s.slot_id}: `{s.symbol}` ({s.status})" for s in slots]
        return f"No free slots. Active:\n" + "\n".join(active)

    # Look up token
    price_sol, price_usd, mc = await get_token_price_and_mc(token_address)
    if price_sol <= 0:
        return f"Could not fetch price for `{token_address[:8]}...` — check the CA"

    symbol = await get_token_symbol(token_address)
    entry_dip = get_adjusted_entry_dip(token_address, mc)

    free.token_address = token_address
    free.symbol        = symbol
    free.status        = "watching"
    free.watch_price   = price_sol
    free.watch_time    = datetime.now().isoformat()
    free.entry_dip_pct = entry_dip
    free.entry_mc      = mc

    save_slots(slots)

    mc_fmt = f"${mc/1_000_000:.2f}M" if mc >= 1_000_000 else f"${mc/1_000:.0f}K"
    return (
        f"👀 *Watching* `{symbol}` in Slot {free.slot_id}\n"
        f"Current price: ${price_usd:.6f} | MC: {mc_fmt}\n"
        f"Waiting for -{entry_dip:.1f}% dip to enter\n"
        f"Step 2/3 drops pre-set on entry"
    )


async def cmd_cancel(symbol_or_slot: str) -> str:
    """Cancel watching a token before entry. /cancel <symbol> or /cancel <slot_id>"""
    slots = load_slots()
    target = None
    for s in slots:
        if s.status == "watching":
            if symbol_or_slot.upper() == s.symbol.upper() or symbol_or_slot == str(s.slot_id):
                target = s
                break
    if not target:
        return f"No watching slot found for `{symbol_or_slot}`"

    sym = target.symbol
    _reset_slot(target)
    save_slots(slots)
    return f"Cancelled watching `{sym}` — Slot {target.slot_id} is now free"


async def cmd_close(symbol_or_slot: str) -> str:
    """Manually close an open position. /close <symbol> or /close <slot_id>"""
    slots  = load_slots()
    budgets = load_budgets() or await init_budgets()
    wallet  = get_wallet_pubkey()
    if not wallet:
        return "Wallet not configured"

    target = None
    for s in slots:
        if s.status == "open":
            if symbol_or_slot.upper() == s.symbol.upper() or symbol_or_slot == str(s.slot_id):
                target = s
                break
    if not target:
        return f"No open position found for `{symbol_or_slot}`"

    budget = next((b for b in budgets if b.slot_id == target.slot_id), None)
    if not budget:
        return "Budget data missing"

    price_sol, price_usd, _ = await get_token_price_and_mc(target.token_address)
    await _close_slot(target, budget, wallet, price_sol, price_usd, "MANUAL_CLOSE")
    save_slots(slots)
    return f"Closing `{target.symbol}`..."


async def cmd_positions() -> str:
    """Return formatted positions string."""
    slots = load_slots()
    budgets = load_budgets() or []
    budget_map = {b.slot_id: b for b in budgets}

    active = [s for s in slots if s.status in ("watching", "open")]
    if not active:
        return "*No active slots*\nUse /trade <CA> to start"

    msg = "*SLOTS*\n\n"
    for s in active:
        b = budget_map.get(s.slot_id)
        budget_str = f"{b.budget_sol:.4f} SOL" if b else "?"

        if s.status == "watching":
            msg += f"*Slot {s.slot_id} — WATCHING* `{s.symbol}`\n"
            msg += f"  Waiting for -{s.entry_dip_pct:.1f}% dip\n"
            msg += f"  Budget: {budget_str}\n\n"
        elif s.status == "open":
            price_sol, price_usd, _ = await get_token_price_and_mc(s.token_address)
            if price_sol > 0 and s.dca_avg_price > 0:
                pnl = ((price_sol - s.dca_avg_price) / s.dca_avg_price) * 100
            else:
                pnl = 0.0
            trail_str = "ACTIVE" if s.trail_active else f"triggers at +12%"
            msg += f"*Slot {s.slot_id} — OPEN* `{s.symbol}` (step {s.dca_step}/3)\n"
            msg += f"  PnL: *{pnl:+.1f}%* | Max: {s.max_pnl_pct:+.1f}%\n"
            msg += f"  Invested: {s.total_sol_invested:.4f} SOL | Trail: {trail_str}\n"
            msg += f"  Budget: {budget_str}\n\n"

    return msg


async def cmd_closeall() -> str:
    """Force-sell all open positions immediately."""
    slots   = load_slots()
    budgets = load_budgets() or await init_budgets()
    wallet  = get_wallet_pubkey()
    if not wallet:
        return "Wallet not configured"

    open_slots = [s for s in slots if s.status == "open"]
    if not open_slots:
        return "No open positions to close"

    results = []
    for s in open_slots:
        budget = next((b for b in budgets if b.slot_id == s.slot_id), None)
        if not budget:
            results.append(f"Slot {s.slot_id} `{s.symbol}` — budget missing, skipped")
            continue
        price_sol, price_usd, _ = await get_token_price_and_mc(s.token_address)
        await _close_slot(s, budget, wallet, price_sol, price_usd, "CLOSEALL")
        results.append(f"Slot {s.slot_id} `{s.symbol}` — closed")

    save_slots(slots)
    return "Force close all:\n" + "\n".join(results)


async def cmd_resetbudget() -> str:
    """Force re-initialise slot budgets from current wallet balance."""
    import os as _os
    if _os.path.exists(BUDGETS_FILE):
        _os.remove(BUDGETS_FILE)
    budgets = await init_budgets()
    if not budgets:
        return "Reset failed — wallet not configured"
    global _BUDGETS_CACHE
    _BUDGETS_CACHE = budgets
    lines = [f"Slot {b.slot_id}: {b.budget_sol:.4f} SOL" for b in budgets]
    return "Budget reset from wallet:\n" + "\n".join(lines)


async def cmd_stats() -> str:
    """Slot budget summary."""
    budgets = load_budgets()
    if not budgets:
        budgets = await init_budgets()
    wallet  = get_wallet_pubkey()
    sol_bal = await get_sol_balance(wallet) if wallet else 0.0

    if not budgets:
        return f"Budgets not initialised — check wallet config\nWallet: {sol_bal:.4f} SOL"

    msg = f"*STATS*\nWallet: `{sol_bal:.4f}` SOL\n\n"
    for b in budgets:
        pnl_emoji = "🟢" if b.total_profit_sol >= 0 else "🔴"
        msg += f"*Slot {b.slot_id}*\n"
        msg += f"  Budget: {b.budget_sol:.4f} SOL (start: {b.start_budget_sol:.4f})\n"
        msg += f"  {pnl_emoji} Profit: {b.total_profit_sol:+.4f} SOL | Trades: {b.trade_count}\n\n"

    total_profit = sum(b.total_profit_sol for b in budgets)
    msg += f"*Total profit: {total_profit:+.4f} SOL*"
    return msg


# ─────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────

async def run_pair_trader():
    """Main loop — checks all slots every 30 seconds."""
    global _BUDGETS_CACHE

    print("Pair trader starting...")
    wallet = get_wallet_pubkey()
    if not wallet:
        print("ERROR: No wallet configured")
        return

    budgets = await init_budgets()
    _BUDGETS_CACHE = budgets
    slots = load_slots()

    sol = await get_sol_balance(wallet)
    print(f"Wallet: {sol:.4f} SOL")
    for b in budgets:
        print(f"  Slot {b.slot_id}: {b.budget_sol:.4f} SOL budget")

    await notify(
        f"*Pair Trader Online*\n"
        f"Wallet: `{sol:.4f}` SOL\n"
        f"Slots: {NUM_SLOTS} | Use /trade <CA> to start"
    )

    while True:
        try:
            slots = load_slots()
            budgets = _BUDGETS_CACHE

            for i, slot in enumerate(slots):
                budget = next((b for b in budgets if b.slot_id == slot.slot_id), None)
                if not budget:
                    continue
                slots[i] = await process_slot(slot, budget, wallet)

            save_slots(slots)
        except Exception as e:
            print(f"Pair trader error: {e}")

        await asyncio.sleep(30)