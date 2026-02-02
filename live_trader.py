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

# Load keys
load_dotenv("keys.env")

SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
MAX_POSITION_SOL = float(os.getenv("MAX_POSITION_SOL", "0.1"))
MAX_SLIPPAGE_BPS = int(float(os.getenv("MAX_SLIPPAGE_PERCENT", "5")) * 100)  # Convert to basis points
WALLET_UTILIZATION = float(os.getenv("WALLET_UTILIZATION", "0.85"))
MAX_OPEN_TRADES = int(os.getenv("MAX_OPEN_TRADES", "4"))

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

# Trade type configs (same as sim)
TRADE_CONFIGS = {
    "QUICK": {"target": 20, "stop": -85, "timeout_hours": 2},
    "MOMENTUM": {"target": 37, "stop": -85, "timeout_hours": 6},
    "GEM": {"target": 112, "stop": -85, "timeout_hours": 24},
}

# Cached keypair
_KEYPAIR_CACHE = None


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


async def get_token_price(token_address: str) -> Optional[float]:
    """Get token price from DexScreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("pairs"):
                        return float(data["pairs"][0].get("priceUsd", 0))
    except:
        pass
    return None


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


async def buy_token(
    token_address: str,
    symbol: str,
    trade_type: str,
    current_price: float,
    market_cap: float,
    sol_amount: float = MAX_POSITION_SOL
) -> Optional[LivePosition]:
    """Execute buy order."""
    wallet = get_wallet_pubkey()
    if not wallet:
        print("ERROR: No wallet configured!")
        return None

    print(f"\n{'='*40}")
    print(f"LIVE BUY: ${symbol}")
    print(f"Type: {trade_type}")
    print(f"Amount: {sol_amount} SOL")
    print(f"MC: ${market_cap:,.0f}")
    print(f"{'='*40}")

    # Convert SOL to lamports (1 SOL = 1e9 lamports)
    lamports = int(sol_amount * 1_000_000_000)

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

    # Create position
    pos = LivePosition(
        token_address=token_address,
        symbol=symbol,
        trade_type=trade_type,
        entry_price=current_price,
        entry_time=datetime.now().isoformat(),
        sol_amount=sol_amount,
        token_amount=out_amount,
        entry_mc=market_cap,
        tx_hash=tx_hash,
    )

    # Save
    positions = load_positions()
    positions.append(pos)
    save_positions(positions)
    log_trade(pos, "BUY")

    return pos


async def sell_token(pos: LivePosition, reason: str) -> bool:
    """Execute sell order."""
    wallet = get_wallet_pubkey()
    if not wallet:
        return False

    current_price = await get_token_price(pos.token_address)
    if not current_price:
        print(f"Cannot get price for {pos.symbol}")
        return False

    pnl = ((current_price - pos.entry_price) / pos.entry_price) * 100

    print(f"\n{'='*40}")
    print(f"LIVE SELL: ${pos.symbol}")
    print(f"Reason: {reason}")
    print(f"PnL: {pnl:+.1f}%")
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

    # Execute swap
    tx_hash = await execute_swap(quote, wallet)
    if not tx_hash:
        print("Sell failed!")
        return False

    print(f"TX: {tx_hash}")
    print(f"https://solscan.io/tx/{tx_hash}")

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

    return True


def check_exit_conditions(pos: LivePosition, current_price: float) -> Optional[str]:
    """Check if position should be closed."""
    config = TRADE_CONFIGS.get(pos.trade_type, TRADE_CONFIGS["QUICK"])

    pnl = ((current_price - pos.entry_price) / pos.entry_price) * 100

    # Target hit
    if pnl >= config["target"]:
        return f"TARGET {pnl:.1f}%"

    # Trailing stop: trigger after 20% max, trail back 5%
    if pos.max_pnl_percent >= 20 and pnl <= (pos.max_pnl_percent - 5):
        return f"TRAIL {pnl:.1f}% (max {pos.max_pnl_percent:.1f}%)"

    # Timeout
    entry = datetime.fromisoformat(pos.entry_time)
    timeout = timedelta(hours=config["timeout_hours"])
    if datetime.now() - entry > timeout:
        return f"TIMEOUT {pnl:.1f}%"

    return None


async def manage_positions():
    """Check and manage all open positions."""
    positions = load_positions()
    open_positions = [p for p in positions if p.status == "OPEN"]

    if not open_positions:
        return

    print(f"\nChecking {len(open_positions)} live positions...")

    for pos in open_positions:
        price = await get_token_price(pos.token_address)
        if not price:
            continue

        pnl_now = ((price - pos.entry_price) / pos.entry_price) * 100
        if pnl_now > pos.max_pnl_percent:
            pos.max_pnl_percent = pnl_now
            # Persist updated max while position is open
            positions = load_positions()
            for i, p in enumerate(positions):
                if p.token_address == pos.token_address and p.status == "OPEN":
                    positions[i] = pos
                    break
            save_positions(positions)

        reason = check_exit_conditions(pos, price)
        if reason:
            await sell_token(pos, reason)
        else:
            print(f"  ${pos.symbol}: {pnl_now:+.1f}% (max {pos.max_pnl_percent:+.1f}%)")


async def process_signal(signal: dict) -> bool:
    """Process a BUY signal from scanner."""
    if signal.get("signal") != "BUY":
        return False

    token_address = signal.get("address")
    symbol = signal.get("symbol", "???")
    trade_type = signal.get("trade_type", "QUICK")
    price = signal.get("price", 0)
    mc = signal.get("market_cap", 0)

    if not token_address:
        return False

    # Check if already in position
    positions = load_positions()
    open_positions = [p for p in positions if p.status == "OPEN"]
    for p in open_positions:
        if p.token_address == token_address and p.status == "OPEN":
            print(f"Already in ${symbol}")
            return False

    # Max open trades guard
    if len(open_positions) >= MAX_OPEN_TRADES:
        print(f"Max open trades reached ({MAX_OPEN_TRADES})")
        return False

    # Calculate available budget from wallet balance
    wallet = get_wallet_pubkey()
    sol_balance = await get_sol_balance(wallet)
    budget = sol_balance * WALLET_UTILIZATION
    used = sum(p.sol_amount for p in open_positions)
    available = max(0.0, budget - used)
    remaining_slots = max(1, MAX_OPEN_TRADES - len(open_positions))
    sol_amount = min(MAX_POSITION_SOL, available / remaining_slots)

    if sol_amount <= 0:
        print(f"No available budget (balance {sol_balance:.4f} SOL, budget {budget:.4f} SOL, used {used:.4f} SOL)")
        return False

    # Execute buy
    pos = await buy_token(token_address, symbol, trade_type, price, mc, sol_amount=sol_amount)
    return pos is not None


async def run_live_manager(interval_secs: int = 30):
    """Run live position manager loop."""
    print("\n" + "=" * 50)
    print("LIVE TRADER ACTIVE")
    print("=" * 50)

    wallet = get_wallet_pubkey()
    if wallet:
        print(f"Wallet: {wallet[:8]}...{wallet[-4:]}")
    else:
        print("WARNING: No wallet configured!")
        print("Add your private key to keys.env")

    print(f"Max position: {MAX_POSITION_SOL} SOL")
    print(f"Wallet utilization: {WALLET_UTILIZATION*100:.0f}%")
    print(f"Max open trades: {MAX_OPEN_TRADES}")
    print(f"Slippage: {MAX_SLIPPAGE_BPS/100}%")
    print("=" * 50 + "\n")

    while True:
        try:
            await manage_positions()
        except Exception as e:
            print(f"Manager error: {e}")

        await asyncio.sleep(interval_secs)


def format_live_status() -> str:
    """Format live positions for display."""
    positions = load_positions()
    open_pos = [p for p in positions if p.status == "OPEN"]
    closed_pos = [p for p in positions if p.status == "CLOSED"]

    msg = "*LIVE POSITIONS*\n"
    msg += "=" * 20 + "\n\n"

    if not open_pos:
        msg += "No open positions\n"
    else:
        for p in open_pos:
            msg += f"${p.symbol} [{p.trade_type}]\n"
            msg += f"  Entry: ${p.entry_price:.8f}\n"
            msg += f"  Amount: {p.sol_amount} SOL\n"
            msg += f"  MC: ${p.entry_mc:,.0f}\n\n"

    if closed_pos:
        wins = len([p for p in closed_pos if p.pnl_percent > 0])
        total_pnl = sum(p.pnl_percent for p in closed_pos)
        msg += f"\nClosed: {len(closed_pos)} ({wins}W)\n"
        msg += f"Total PnL: {total_pnl:+.1f}%\n"

    return msg


if __name__ == "__main__":
    # Test wallet
    wallet = get_wallet_pubkey()
    if wallet:
        print(f"Wallet loaded: {wallet}")
    else:
        print("No wallet - add key to keys.env")

    # Run manager
    asyncio.run(run_live_manager())
