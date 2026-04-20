"""
Nano Trader — Automated new-pair DCA bot.

Strategy:
  1. Scanner watches pump.fun for new pairs ($15K–$200K MC)
  2. Each token tracked in watchlist with first-seen MC + timestamp
  3. When MC drops to ≤$10K → time-factor check (must take ≥20 min to fall)
  4. Step 1 (15%) entry → Step 2 (25%) at -50% price → Step 3 (60%) at -80% price
  5. Fixed TP: sell when price ≥ avg × 1.88 (+88%)
  6. Slot freed on close

DCA math (all 3 steps filled, price at $2K MC):
  avg = (0.15×$10K) + (0.25×$5K) + (0.60×$2K) = ~$3.95K MC avg

Wallet: 35% of total wallet split across 2 slots.
"""

import os
import json
import asyncio
import aiohttp
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

from config import NANO_BOT_TOKEN, NANO_CHAT_ID

# Import trading primitives from pair_trader (same wallet, same RPC)
from pair_trader import (
    get_wallet_pubkey,
    get_sol_balance,
    buy_tokens,
    sell_tokens,
    MIN_TRADE_SOL,
    MIN_FEE_RESERVE,
    SOLANA_RPC_URL,
)

# ─────────────────────────────────────────
# Constants
# ─────────────────────────────────────────
NANO_UTILIZATION    = 0.35
NUM_NANO_SLOTS      = 2
DCA_SPLITS          = [0.15, 0.25, 0.60]

ENTRY_MC            = 10_000    # Enter when MC drops to this ($10K)
WATCH_MC_MIN        = 15_000    # Start tracking tokens above this MC
WATCH_MC_MAX        = 200_000   # Don't add tokens above this MC
MIN_FALL_MINUTES    = 20        # Time-factor: must take ≥20 min to fall to entry
MIN_LIQUIDITY_ENTRY = 5_000     # Minimum $5K liquidity at entry point

STEP2_DROP_PCT      = 50.0      # Step 2 at -50% from entry price (~$5K MC)
STEP3_DROP_PCT      = 80.0      # Step 3 at -80% from entry price (~$2K MC)
TARGET_TP_PCT       = 88.0      # Fixed TP: sell at +88% from avg

MAX_WATCH_HOURS     = 8         # Expire watchlist entry after 8h with no entry
MAX_WATCHLIST       = 30        # Maximum tokens tracked at once

NANO_SLOTS_FILE     = "nano_slots.json"
NANO_BUDGET_FILE    = "nano_budget.json"
NANO_WATCH_FILE     = "nano_watchlist.json"
NANO_TRADES_FILE    = "nano_trades.csv"


# ─────────────────────────────────────────
# Telegram notify (nano bot — separate key)
# ─────────────────────────────────────────
async def notify(msg: str):
    if not NANO_BOT_TOKEN or not NANO_CHAT_ID:
        print(f"[nano-tg] {msg}")
        return
    try:
        url = f"https://api.telegram.org/bot{NANO_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={
                "chat_id": NANO_CHAT_ID,
                "text": msg,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        print(f"[nano-tg] notify error: {e}")


# ─────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────

@dataclass
class NanoBudget:
    slot_id: int
    budget_sol: float
    start_budget_sol: float
    total_profit_sol: float = 0.0
    trade_count: int = 0


@dataclass
class NanoWatch:
    address: str
    symbol: str
    first_seen_mc: float
    first_seen_time: str
    current_mc: float = 0.0


@dataclass
class NanoSlot:
    slot_id: int
    status: str             # "empty" | "open"

    token_address: str      = ""
    symbol: str             = ""
    entry_mc: float         = 0.0

    dca_step: int           = 0
    entry_price: float      = 0.0
    step2_price: float      = 0.0
    step3_price: float      = 0.0
    dca_avg_price: float    = 0.0

    total_sol_invested: float = 0.0
    step1_sol: float          = 0.0
    step2_sol: float          = 0.0
    step3_sol: float          = 0.0
    token_amount: float       = 0.0

    tp_price: float           = 0.0
    max_pnl_pct: float        = 0.0

    entry_time: str           = ""
    exit_time: str            = ""
    exit_price: float         = 0.0
    exit_pnl_pct: float       = 0.0
    exit_reason: str          = ""


# ─────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────

def load_nano_slots() -> list:
    if not os.path.exists(NANO_SLOTS_FILE):
        return [NanoSlot(slot_id=i, status="empty") for i in range(1, NUM_NANO_SLOTS + 1)]
    try:
        with open(NANO_SLOTS_FILE) as f:
            raw = json.load(f)
        slots = [NanoSlot(**s) for s in raw]
        existing_ids = {s.slot_id for s in slots}
        for i in range(1, NUM_NANO_SLOTS + 1):
            if i not in existing_ids:
                slots.append(NanoSlot(slot_id=i, status="empty"))
        slots.sort(key=lambda s: s.slot_id)
        return slots
    except:
        return [NanoSlot(slot_id=i, status="empty") for i in range(1, NUM_NANO_SLOTS + 1)]


def save_nano_slots(slots: list):
    with open(NANO_SLOTS_FILE, "w") as f:
        json.dump([asdict(s) for s in slots], f, indent=2)


def load_nano_budget() -> list:
    if not os.path.exists(NANO_BUDGET_FILE):
        return None
    try:
        with open(NANO_BUDGET_FILE) as f:
            raw = json.load(f)
        return [NanoBudget(**b) for b in raw]
    except:
        return None


def save_nano_budget(budgets: list):
    with open(NANO_BUDGET_FILE, "w") as f:
        json.dump([asdict(b) for b in budgets], f, indent=2)


def load_watchlist() -> list:
    if not os.path.exists(NANO_WATCH_FILE):
        return []
    try:
        with open(NANO_WATCH_FILE) as f:
            raw = json.load(f)
        return [NanoWatch(**w) for w in raw]
    except:
        return []


def save_watchlist(watchlist: list):
    with open(NANO_WATCH_FILE, "w") as f:
        json.dump([asdict(w) for w in watchlist], f, indent=2)


async def init_nano_budget() -> list:
    existing = load_nano_budget()
    if existing:
        total = sum(b.budget_sol for b in existing)
        if total >= MIN_TRADE_SOL:
            return existing
        print("[nano] Budgets near-zero, re-initialising...")

    wallet = get_wallet_pubkey()
    sol = await get_sol_balance(wallet) if wallet else 0.0
    usable = max(0.0, sol - MIN_FEE_RESERVE)
    per_slot = (usable * NANO_UTILIZATION) / NUM_NANO_SLOTS
    budgets = [
        NanoBudget(slot_id=i, budget_sol=per_slot, start_budget_sol=per_slot)
        for i in range(1, NUM_NANO_SLOTS + 1)
    ]
    save_nano_budget(budgets)
    print(f"[nano] Budgets initialised: {per_slot:.4f} SOL/slot (wallet: {sol:.4f} SOL)")
    return budgets


def _reset_nano_slot(slot: NanoSlot):
    slot.status             = "empty"
    slot.token_address      = ""
    slot.symbol             = ""
    slot.entry_mc           = 0.0
    slot.dca_step           = 0
    slot.entry_price        = 0.0
    slot.step2_price        = 0.0
    slot.step3_price        = 0.0
    slot.dca_avg_price      = 0.0
    slot.total_sol_invested = 0.0
    slot.step1_sol          = 0.0
    slot.step2_sol          = 0.0
    slot.step3_sol          = 0.0
    slot.token_amount       = 0.0
    slot.tp_price           = 0.0
    slot.max_pnl_pct        = 0.0
    slot.entry_time         = ""
    slot.exit_time          = ""
    slot.exit_price         = 0.0
    slot.exit_pnl_pct       = 0.0
    slot.exit_reason        = ""


def _log_nano_trade(slot: NanoSlot, profit_sol: float):
    header = not os.path.exists(NANO_TRADES_FILE)
    held_mins = 0
    if slot.entry_time and slot.exit_time:
        try:
            held_mins = int((datetime.fromisoformat(slot.exit_time) -
                             datetime.fromisoformat(slot.entry_time)).total_seconds() / 60)
        except:
            pass
    with open(NANO_TRADES_FILE, "a") as f:
        if header:
            f.write("time,symbol,address,pnl_pct,exit_reason,sol_invested,profit_sol,dca_step,entry_mc,held_mins\n")
        f.write(
            f"{slot.exit_time},{slot.symbol},{slot.token_address},"
            f"{slot.exit_pnl_pct:.2f},{slot.exit_reason},"
            f"{slot.total_sol_invested:.4f},{profit_sol:.4f},"
            f"{slot.dca_step},{slot.entry_mc:.0f},{held_mins}\n"
        )


# ─────────────────────────────────────────
# Price + liquidity fetch (cached SOL price)
# ─────────────────────────────────────────

_sol_usd_cache: float = 0.0
_sol_usd_last_fetch: float = 0.0
_SOL_CACHE_TTL = 120  # refresh SOL/USD price every 2 minutes


async def _get_sol_usd() -> float:
    global _sol_usd_cache, _sol_usd_last_fetch
    import time
    if time.time() - _sol_usd_last_fetch < _SOL_CACHE_TTL and _sol_usd_cache > 0:
        return _sol_usd_cache
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                data = await r.json()
                _sol_usd_cache = float(data.get("solana", {}).get("usd", 0) or 0)
                _sol_usd_last_fetch = time.time()
    except:
        pass
    return _sol_usd_cache


async def get_nano_token_data(address: str) -> tuple:
    """Returns (price_sol, price_usd, mc_usd, liquidity_usd) from DexScreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                pairs = data.get("pairs") or []
                if not pairs:
                    return 0.0, 0.0, 0.0, 0.0
                sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                if not sol_pairs:
                    return 0.0, 0.0, 0.0, 0.0
                pair = max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))

                price_usd = float(pair.get("priceUsd", 0) or 0)
                mc = float(pair.get("marketCap", 0) or 0)
                liquidity = float(pair.get("liquidity", {}).get("usd", 0) or 0)
                sol_usd = await _get_sol_usd()
                price_sol = price_usd / sol_usd if sol_usd > 0 else 0.0
                return price_sol, price_usd, mc, liquidity
    except:
        return 0.0, 0.0, 0.0, 0.0


# ─────────────────────────────────────────
# Close slot
# ─────────────────────────────────────────

async def _close_nano_slot(slot: NanoSlot, budget: NanoBudget, wallet: str,
                            price_sol: float, price_usd: float, reason: str):
    if slot.token_amount <= 0:
        _reset_nano_slot(slot)
        return

    sol_received = await sell_tokens(slot.token_address, slot.token_amount, wallet, price_sol)

    pnl_pct = ((sol_received - slot.total_sol_invested) / slot.total_sol_invested * 100
               if slot.total_sol_invested > 0 else 0.0)
    profit_sol = sol_received - slot.total_sol_invested

    budget.budget_sol       += profit_sol
    budget.total_profit_sol += profit_sol
    budget.trade_count      += 1

    slot.exit_time    = datetime.now().isoformat()
    slot.exit_price   = price_sol
    slot.exit_pnl_pct = pnl_pct
    slot.exit_reason  = reason

    _log_nano_trade(slot, profit_sol)

    emoji = "✅" if pnl_pct >= 0 else "❌"
    await notify(
        f"{emoji} *NANO CLOSE* `{slot.symbol}` [Slot {slot.slot_id}]\n"
        f"PnL: *{pnl_pct:+.1f}%* | Profit: {profit_sol:+.4f} SOL\n"
        f"Reason: {reason} | Steps: {slot.dca_step}/3\n"
        f"Budget now: {budget.budget_sol:.4f} SOL"
    )

    _reset_nano_slot(slot)


# ─────────────────────────────────────────
# Process open slot (TP + DCA)
# ─────────────────────────────────────────

async def process_nano_slot(slot: NanoSlot, budget: NanoBudget, wallet: str):
    if slot.status != "open":
        return

    price_sol, price_usd, mc, _ = await get_nano_token_data(slot.token_address)
    if price_sol <= 0:
        return

    pnl_pct = ((price_sol - slot.dca_avg_price) / slot.dca_avg_price * 100
               if slot.dca_avg_price > 0 else 0.0)
    if pnl_pct > slot.max_pnl_pct:
        slot.max_pnl_pct = pnl_pct

    # ── Fixed TP ──
    if price_sol >= slot.tp_price:
        reason = f"TP +{pnl_pct:.1f}% (target +{TARGET_TP_PCT:.0f}%)"
        await _close_nano_slot(slot, budget, wallet, price_sol, price_usd, reason)
        return

    # ── DCA Step 2 ──
    if slot.dca_step == 1 and price_sol <= slot.step2_price:
        step2_sol = budget.budget_sol * DCA_SPLITS[1]
        if step2_sol >= MIN_TRADE_SOL:
            tokens = await buy_tokens(slot.token_address, step2_sol, wallet)
            if tokens > 0:
                slot.step2_sol          = step2_sol
                slot.total_sol_invested += step2_sol
                slot.token_amount       += tokens
                slot.dca_step           = 2
                slot.dca_avg_price      = slot.total_sol_invested / slot.token_amount
                slot.tp_price           = slot.dca_avg_price * (1 + TARGET_TP_PCT / 100)
                budget.budget_sol       -= step2_sol
                await notify(
                    f"🔵 *NANO DCA Step 2* `{slot.symbol}` [Slot {slot.slot_id}]\n"
                    f"Spent: {step2_sol:.4f} SOL\n"
                    f"New avg: {slot.dca_avg_price:.8f} | TP: +{TARGET_TP_PCT:.0f}%\n"
                    f"Step 3 triggers at: {slot.step3_price:.8f}"
                )

    # ── DCA Step 3 ──
    elif slot.dca_step == 2 and price_sol <= slot.step3_price:
        step3_sol = budget.budget_sol * DCA_SPLITS[2]
        if step3_sol >= MIN_TRADE_SOL:
            tokens = await buy_tokens(slot.token_address, step3_sol, wallet)
            if tokens > 0:
                slot.step3_sol          = step3_sol
                slot.total_sol_invested += step3_sol
                slot.token_amount       += tokens
                slot.dca_step           = 3
                slot.dca_avg_price      = slot.total_sol_invested / slot.token_amount
                slot.tp_price           = slot.dca_avg_price * (1 + TARGET_TP_PCT / 100)
                budget.budget_sol       -= step3_sol
                await notify(
                    f"🟣 *NANO DCA Step 3* `{slot.symbol}` [Slot {slot.slot_id}]\n"
                    f"Spent: {step3_sol:.4f} SOL — fully invested\n"
                    f"New avg: {slot.dca_avg_price:.8f} | TP: +{TARGET_TP_PCT:.0f}%\n"
                    f"Avg MC equiv: ~${slot.entry_mc * (slot.dca_avg_price / slot.entry_price) / 1000:.1f}K"
                )


# ─────────────────────────────────────────
# Watchlist management
# ─────────────────────────────────────────

def _time_factor_ok(watch: NanoWatch) -> tuple:
    """Returns (ok: bool, fall_mins: float). Token must take ≥MIN_FALL_MINUTES to drop to entry."""
    fall_mins = (datetime.now() - datetime.fromisoformat(watch.first_seen_time)).total_seconds() / 60
    return fall_mins >= MIN_FALL_MINUTES, fall_mins


async def scan_new_pairs(watchlist: list) -> list:
    """Find recently bonded Solana tokens via DexScreener token profiles.
    These are post-bond tokens that have actual DEX pairs — the ones we want to watch."""
    watched = {w.address for w in watchlist}
    new_items = []

    try:
        async with aiohttp.ClientSession() as session:
            # DexScreener token profiles = recently added/bonded tokens
            async with session.get(
                "https://api.dexscreener.com/token-profiles/latest/v1",
                timeout=aiohttp.ClientTimeout(total=12)
            ) as resp:
                if resp.status != 200:
                    print(f"[nano] DexScreener profiles returned {resp.status}")
                    return []
                profiles = await resp.json()

            if not isinstance(profiles, list):
                return []

            sol_profiles = [p for p in profiles if p.get("chainId") == "solana"]
            print(f"[nano] Scanning {len(sol_profiles)} new Solana token profiles...")

            for profile in sol_profiles[:40]:
                addr = profile.get("tokenAddress", "")
                if not addr or addr in watched:
                    continue

                _, _, mc, liquidity = await get_nano_token_data(addr)
                if mc <= 0:
                    await asyncio.sleep(0.1)
                    continue

                symbol = profile.get("header", addr[:8])[:10]

                if WATCH_MC_MIN <= mc <= WATCH_MC_MAX and liquidity >= MIN_LIQUIDITY_ENTRY:
                    item = NanoWatch(
                        address=addr,
                        symbol=symbol,
                        first_seen_mc=mc,
                        first_seen_time=datetime.now().isoformat(),
                        current_mc=mc,
                    )
                    new_items.append(item)
                    watched.add(addr)
                    print(f"[nano] Watching {symbol} @ ${mc/1000:.0f}K MC (liq: ${liquidity/1000:.0f}K)")
                else:
                    print(f"[nano] Skip {symbol}: MC=${mc/1000:.0f}K liq=${liquidity/1000:.0f}K")

                await asyncio.sleep(0.15)

    except Exception as e:
        print(f"[nano] scan_new_pairs error: {e}")

    return new_items


async def process_watchlist(watchlist: list, slots: list, budgets: list, wallet: str):
    """Check all watched tokens. Fire entry when MC ≤ ENTRY_MC and time factor passes."""
    now = datetime.now()
    to_remove = []

    for watch in watchlist:
        # Expire old entries
        age_hours = (now - datetime.fromisoformat(watch.first_seen_time)).total_seconds() / 3600
        if age_hours >= MAX_WATCH_HOURS:
            print(f"[nano] {watch.symbol} expired ({age_hours:.1f}h)")
            to_remove.append(watch.address)
            continue

        price_sol, price_usd, mc, liquidity = await get_nano_token_data(watch.address)
        if mc <= 0:
            await asyncio.sleep(0.1)
            continue

        watch.current_mc = mc

        if mc <= ENTRY_MC:
            # Find free slot
            free_slot = next((s for s in slots if s.status == "empty"), None)
            if not free_slot:
                continue

            budget = next((b for b in budgets if b.slot_id == free_slot.slot_id), None)
            if not budget:
                continue

            # Time-factor check
            ok, fall_mins = _time_factor_ok(watch)
            if not ok:
                print(f"[nano] {watch.symbol} hit ${mc/1000:.1f}K in {fall_mins:.0f}m — rug risk, skipping")
                await notify(
                    f"⚠️ *NANO SKIP* `{watch.symbol}`\n"
                    f"Hit ${mc/1000:.1f}K MC too fast ({fall_mins:.0f}m < {MIN_FALL_MINUTES}m)\n"
                    f"First seen: ${watch.first_seen_mc/1000:.0f}K — possible rug"
                )
                to_remove.append(watch.address)
                continue

            # Liquidity check at entry
            if liquidity < MIN_LIQUIDITY_ENTRY:
                print(f"[nano] {watch.symbol} low liquidity ${liquidity:.0f} at entry — skipping")
                to_remove.append(watch.address)
                continue

            step1_sol = budget.budget_sol * DCA_SPLITS[0]
            if step1_sol < MIN_TRADE_SOL:
                continue

            tokens = await buy_tokens(watch.address, step1_sol, wallet)
            if tokens > 0:
                free_slot.status            = "open"
                free_slot.token_address     = watch.address
                free_slot.symbol            = watch.symbol
                free_slot.entry_mc          = mc
                free_slot.dca_step          = 1
                free_slot.entry_price       = price_sol
                free_slot.step2_price       = price_sol * (1 - STEP2_DROP_PCT / 100)
                free_slot.step3_price       = price_sol * (1 - STEP3_DROP_PCT / 100)
                free_slot.dca_avg_price     = price_sol
                free_slot.step1_sol         = step1_sol
                free_slot.total_sol_invested = step1_sol
                free_slot.token_amount      = tokens
                free_slot.tp_price          = price_sol * (1 + TARGET_TP_PCT / 100)
                free_slot.entry_time        = now.isoformat()
                budget.budget_sol           -= step1_sol

                await notify(
                    f"🟢 *NANO ENTRY* `{watch.symbol}` [Slot {free_slot.slot_id}]\n"
                    f"Step 1: {step1_sol:.4f} SOL @ ${mc/1000:.1f}K MC\n"
                    f"Fell {fall_mins:.0f}m from ${watch.first_seen_mc/1000:.0f}K MC\n"
                    f"TP: +{TARGET_TP_PCT:.0f}% | DCA2 @ −{STEP2_DROP_PCT:.0f}% | DCA3 @ −{STEP3_DROP_PCT:.0f}%\n"
                    f"[chart](https://dexscreener.com/solana/{watch.address})"
                )

                to_remove.append(watch.address)

        await asyncio.sleep(0.1)

    watchlist[:] = [w for w in watchlist if w.address not in to_remove]


# ─────────────────────────────────────────
# TG command handlers
# ─────────────────────────────────────────

async def cmd_nano_pos() -> str:
    slots = load_nano_slots()
    watchlist = load_watchlist()
    lines = ["*NANO TRADER — SLOTS*\n"]

    for slot in slots:
        if slot.status == "open":
            price_sol, _, mc, _ = await get_nano_token_data(slot.token_address)
            pnl = ((price_sol - slot.dca_avg_price) / slot.dca_avg_price * 100
                   if slot.dca_avg_price > 0 and price_sol > 0 else 0.0)
            to_tp = TARGET_TP_PCT - pnl
            lines.append(
                f"🔵 *Slot {slot.slot_id}* `{slot.symbol}`\n"
                f"  PnL: {pnl:+.1f}% | Max: {slot.max_pnl_pct:+.1f}%\n"
                f"  Steps: {slot.dca_step}/3 | Invested: {slot.total_sol_invested:.4f} SOL\n"
                f"  TP at +{TARGET_TP_PCT:.0f}% ({to_tp:.1f}% to go)\n"
                f"  MC: ${mc/1000:.1f}K | Entry MC: ${slot.entry_mc/1000:.1f}K"
            )
        else:
            lines.append(f"⚪ *Slot {slot.slot_id}* — Empty")

    if watchlist:
        lines.append(f"\n*Watchlist* ({len(watchlist)} tokens):")
        for w in watchlist[:10]:
            age_m = int((datetime.now() - datetime.fromisoformat(w.first_seen_time)).total_seconds() / 60)
            lines.append(
                f"  👁 `{w.symbol}` ${w.current_mc/1000:.0f}K MC "
                f"(seen {age_m}m ago @ ${w.first_seen_mc/1000:.0f}K)"
            )
        if len(watchlist) > 10:
            lines.append(f"  _...and {len(watchlist)-10} more_")
    else:
        lines.append("\n_Watchlist empty_")

    return "\n".join(lines)


async def cmd_nano_stats() -> str:
    budgets = load_nano_budget() or []
    wallet = get_wallet_pubkey()
    sol = await get_sol_balance(wallet) if wallet else 0.0

    lines = ["*NANO TRADER — STATS*\n"]
    total_profit = 0.0
    total_trades = 0
    for b in budgets:
        total_profit += b.total_profit_sol
        total_trades += b.trade_count
        lines.append(
            f"Slot {b.slot_id}: {b.budget_sol:.4f} SOL "
            f"(start: {b.start_budget_sol:.4f} | profit: {b.total_profit_sol:+.4f})"
        )
    lines.append(f"\nTrades: {total_trades} | Total profit: {total_profit:+.4f} SOL")
    lines.append(f"Wallet balance: {sol:.4f} SOL")
    return "\n".join(lines)


async def cmd_nano_close(target: str) -> str:
    slots = load_nano_slots()
    budgets = load_nano_budget() or []
    wallet = get_wallet_pubkey()

    slot = next(
        (s for s in slots if s.status == "open" and
         (s.symbol.upper() == target.upper() or str(s.slot_id) == target)),
        None
    )
    if not slot:
        return f"No open position matching '{target}'"

    budget = next((b for b in budgets if b.slot_id == slot.slot_id), None)
    if not budget:
        return "Budget not found"

    price_sol, price_usd, _, _ = await get_nano_token_data(slot.token_address)
    if price_sol <= 0:
        return "Could not fetch price — try again"

    await _close_nano_slot(slot, budget, wallet, price_sol, price_usd, "MANUAL")
    save_nano_slots(slots)
    save_nano_budget(budgets)
    return f"Closed `{slot.symbol}` — check above for result"


async def cmd_nano_watch() -> str:
    watchlist = load_watchlist()
    if not watchlist:
        return "*NANO WATCHLIST*\n\n_Nothing being watched yet — scanner runs every 60s_"
    lines = [f"*NANO WATCHLIST* ({len(watchlist)} tokens)\n"]
    for w in watchlist:
        age_m = int((datetime.now() - datetime.fromisoformat(w.first_seen_time)).total_seconds() / 60)
        drop_needed = ((w.current_mc - ENTRY_MC) / w.current_mc * 100) if w.current_mc > ENTRY_MC else 0
        time_ok = "✅" if age_m >= MIN_FALL_MINUTES else f"⏳{MIN_FALL_MINUTES - age_m}m"
        lines.append(
            f"• `{w.symbol}` — ${w.current_mc/1000:.0f}K MC "
            f"(seen {age_m}m @ ${w.first_seen_mc/1000:.0f}K)\n"
            f"  Needs −{drop_needed:.0f}% to entry | Time filter: {time_ok}\n"
            f"  [chart](https://dexscreener.com/solana/{w.address})"
        )
    return "\n".join(lines)


async def cmd_nano_cancel(symbol: str) -> str:
    watchlist = load_watchlist()
    before = len(watchlist)
    watchlist = [w for w in watchlist
                 if w.symbol.upper() != symbol.upper() and w.address != symbol]
    save_watchlist(watchlist)
    removed = before - len(watchlist)
    return f"Removed {removed} token(s) matching '{symbol}' from watchlist"


async def cmd_nano_resetbudget() -> str:
    if os.path.exists(NANO_BUDGET_FILE):
        os.remove(NANO_BUDGET_FILE)
    budgets = await init_nano_budget()
    save_nano_budget(budgets)
    per = budgets[0].budget_sol if budgets else 0
    return f"Budget reset: {len(budgets)} slots × {per:.4f} SOL each"


# ─────────────────────────────────────────
# Main loops
# ─────────────────────────────────────────

_shared_watchlist: list = []
_shared_slots: list = []
_shared_budgets: list = []


async def run_nano_scanner(interval_secs: int = 60):
    """Periodic loop: scan pump.fun for new pairs and add to watchlist."""
    global _shared_watchlist, _shared_slots, _shared_budgets
    while True:
        try:
            if len(_shared_watchlist) < MAX_WATCHLIST:
                new_items = await scan_new_pairs(_shared_watchlist)
                added = []
                for item in new_items:
                    if len(_shared_watchlist) < MAX_WATCHLIST:
                        _shared_watchlist.append(item)
                        added.append(item)
                if added:
                    save_watchlist(_shared_watchlist)
                    print(f"[nano] Watchlist: {len(_shared_watchlist)} tokens")
                    # Notify TG for each new pair added
                    for item in added:
                        await notify(
                            f"👁 *New Pair Watching* `{item.symbol}`\n"
                            f"MC: ${item.first_seen_mc/1000:.0f}K | Waiting for drop to ${ENTRY_MC/1000:.0f}K\n"
                            f"[chart](https://dexscreener.com/solana/{item.address})"
                        )
        except Exception as e:
            print(f"[nano] Scanner loop error: {e}")
        await asyncio.sleep(interval_secs)


async def run_nano_trader(interval_secs: int = 30):
    """Main trading loop: watchlist entries + open slot management."""
    global _shared_watchlist, _shared_slots, _shared_budgets

    print("[nano] Initialising...")
    wallet = get_wallet_pubkey()
    if not wallet:
        print("[nano] ERROR: No wallet key found")
        return

    _shared_slots    = load_nano_slots()
    _shared_budgets  = await init_nano_budget()
    _shared_watchlist = load_watchlist()
    save_nano_budget(_shared_budgets)

    await notify(
        f"🤖 *Nano Trader Started*\n"
        f"Slots: {NUM_NANO_SLOTS} | Entry: ≤${ENTRY_MC/1000:.0f}K MC\n"
        f"DCA: −{STEP2_DROP_PCT:.0f}% / −{STEP3_DROP_PCT:.0f}% | TP: +{TARGET_TP_PCT:.0f}%\n"
        f"Time filter: ≥{MIN_FALL_MINUTES}m fall | Budget: {_shared_budgets[0].budget_sol:.4f} SOL/slot"
    )

    while True:
        try:
            await process_watchlist(_shared_watchlist, _shared_slots, _shared_budgets, wallet)

            for slot in _shared_slots:
                if slot.status == "open":
                    budget = next((b for b in _shared_budgets if b.slot_id == slot.slot_id), None)
                    if budget:
                        await process_nano_slot(slot, budget, wallet)

            save_nano_slots(_shared_slots)
            save_nano_budget(_shared_budgets)
            save_watchlist(_shared_watchlist)

        except Exception as e:
            print(f"[nano] Trader loop error: {e}")
            import traceback; traceback.print_exc()

        await asyncio.sleep(interval_secs)
