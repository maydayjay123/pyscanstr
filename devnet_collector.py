"""
Devnet SOL Collector — runs alongside the trading bot.
2 airdrops per cycle, every 8 hours. Notifies via Telegram.

Usage: python devnet_collector.py
"""

import asyncio
import aiohttp
import json
from datetime import datetime

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

WALLET       = "3taTiQLc2NQQPQAjt2MurGNGekReHs3KgXaTkjCqZGJh"
DEVNET_RPC   = "https://api.devnet.solana.com"
DROPS_PER_CYCLE = 2
SOL_PER_DROP    = 1          # SOL requested per airdrop (2 was rejected, trying 1)
LAMPORTS        = SOL_PER_DROP * 1_000_000_000
CYCLE_HOURS     = 8          # wait between cycles
DROP_GAP_SECS   = 30         # gap between the 2 drops in same cycle


async def notify(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(msg)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": msg,
                "parse_mode": "Markdown",
            }, timeout=aiohttp.ClientTimeout(total=10))
    except:
        pass


async def get_balance() -> float:
    try:
        async with aiohttp.ClientSession() as s:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getBalance",
                "params": [WALLET]
            }
            async with s.post(DEVNET_RPC, json=payload,
                              timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
                return data.get("result", {}).get("value", 0) / 1_000_000_000
    except:
        return 0.0


async def request_airdrop() -> tuple:
    """Returns (success: bool, message: str)"""
    try:
        async with aiohttp.ClientSession() as s:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "requestAirdrop",
                "params": [WALLET, LAMPORTS]
            }
            async with s.post(DEVNET_RPC, json=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()

                if "result" in data:
                    sig = data["result"]
                    return True, f"tx: `{sig[:20]}...`"

                err = data.get("error", {})
                code = err.get("code", "?")
                msg  = err.get("message", "unknown error")

                # Rate limited — try alternative faucets
                if code == 429 or "limit" in str(msg).lower():
                    return await _try_alt_faucet()

                return False, f"Error {code}: {msg}"
    except Exception as e:
        return False, f"Request failed: {e}"


async def _try_alt_faucet() -> tuple:
    """Fallback: retry main RPC with smaller amounts."""
    for lamports in [1_000_000_000, 500_000_000]:  # 1 SOL, 0.5 SOL
        try:
            async with aiohttp.ClientSession() as s:
                payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "requestAirdrop",
                    "params": [WALLET, lamports]
                }
                async with s.post(DEVNET_RPC, json=payload,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                    data = await r.json()
                    if "result" in data:
                        sol = lamports / 1_000_000_000
                        return True, f"{sol} SOL via fallback"
        except:
            continue
    return False, "All attempts failed — faucet may be down"


async def run_cycle(cycle_num: int):
    ts = datetime.now().strftime("%H:%M")
    print(f"\n[{ts}] === Cycle {cycle_num} ===")

    balance_before = await get_balance()
    print(f"Balance before: {balance_before:.2f} SOL")

    results = []
    for drop_num in range(1, DROPS_PER_CYCLE + 1):
        print(f"  Drop {drop_num}/{DROPS_PER_CYCLE}...")
        ok, msg = await request_airdrop()
        status = "✅" if ok else "❌"
        results.append(f"{status} Drop {drop_num}: {msg}")
        print(f"  {status} {msg}")

        if drop_num < DROPS_PER_CYCLE:
            await asyncio.sleep(DROP_GAP_SECS)

    await asyncio.sleep(5)  # let chain settle
    balance_after = await get_balance()
    gained = balance_after - balance_before

    print(f"Balance after:  {balance_after:.2f} SOL (+{gained:.2f})")

    # TG notification
    result_lines = "\n".join(results)
    await notify(
        f"💧 *Devnet Collector — Cycle {cycle_num}*\n"
        f"{result_lines}\n\n"
        f"Balance: *{balance_after:.2f} SOL* (+{gained:.2f} this cycle)\n"
        f"Next drop in {CYCLE_HOURS}h"
    )


async def cmd_balance():
    """Quick balance check — called by /devbal command if wired up."""
    bal = await get_balance()
    return f"Devnet wallet: *{bal:.4f} SOL*\n`{WALLET[:8]}...{WALLET[-6:]}`"


async def main():
    print("=" * 45)
    print("DEVNET SOL COLLECTOR")
    print("=" * 45)
    print(f"Wallet:  {WALLET[:8]}...{WALLET[-6:]}")
    print(f"Target:  {SOL_PER_DROP} SOL × {DROPS_PER_CYCLE} drops every {CYCLE_HOURS}h")
    print(f"Max/day: {SOL_PER_DROP * DROPS_PER_CYCLE * (24 // CYCLE_HOURS)} SOL")
    print("Ctrl+C to stop")
    print("=" * 45)

    bal = await get_balance()
    print(f"Starting balance: {bal:.4f} SOL")

    await notify(
        f"💧 *Devnet Collector Started*\n"
        f"`{WALLET[:8]}...{WALLET[-6:]}`\n"
        f"Balance: {bal:.4f} SOL\n"
        f"{SOL_PER_DROP} SOL × {DROPS_PER_CYCLE} drops every {CYCLE_HOURS}h"
    )

    cycle = 1
    while True:
        await run_cycle(cycle)
        cycle += 1
        print(f"\nSleeping {CYCLE_HOURS}h until next cycle...")
        await asyncio.sleep(CYCLE_HOURS * 3600)


if __name__ == "__main__":
    asyncio.run(main())
