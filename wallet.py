"""
Wallet Manager - Check balances, sell tokens.

Usage:
    python wallet.py           # Show balances
    python wallet.py --sell    # Sell menu
"""

import os
import json
import asyncio
import aiohttp
import base58
import base64
from dotenv import load_dotenv

load_dotenv("keys.env")

# Read RPC URL directly from file (dotenv may fail with multi-line key above)
def get_rpc_url():
    try:
        with open("keys.env", "r") as f:
            for line in f:
                if line.startswith("SOLANA_RPC_URL="):
                    return line.split("=", 1)[1].strip()
    except:
        pass
    return os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

SOLANA_RPC_URL = get_rpc_url()
MAX_SLIPPAGE_BPS = int(float(os.getenv("MAX_SLIPPAGE_PERCENT", "5")) * 100)

SOL_MINT = "So11111111111111111111111111111111111111112"
# Jupiter migrated off quote-api.jup.ag; use Lite Swap API by default.
JUPITER_BASE_URL = os.getenv("JUPITER_BASE_URL", "https://lite-api.jup.ag/swap/v1")
JUPITER_QUOTE_API = f"{JUPITER_BASE_URL}/quote"
JUPITER_SWAP_API = f"{JUPITER_BASE_URL}/swap"


def load_keypair():
    """Load keypair from keys.env (supports both base58 and JSON array format)."""
    key_bytes = None

    # First try reading directly from file (handles multi-line JSON)
    try:
        with open("keys.env", "r") as f:
            content = f.read()

        # Find JSON array in file
        start = content.find("[")
        end = content.find("]") + 1
        if start != -1 and end > start:
            json_str = content[start:end]
            key_bytes = bytes(json.loads(json_str))
    except:
        pass

    # Fallback to env var (for base58 format)
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
        return None, None

    try:
        # Prefer solders for accurate pubkey derivation.
        try:
            from solders.keypair import Keypair
            kp = Keypair.from_bytes(key_bytes)
            pubkey = str(kp.pubkey())
            return key_bytes, pubkey
        except Exception:
            pass

        # Fallback: derive from key bytes.
        if len(key_bytes) == 64:
            pubkey_bytes = key_bytes[32:]
        else:
            pubkey_bytes = key_bytes[:32]

        pubkey = base58.b58encode(pubkey_bytes).decode()
        return key_bytes, pubkey

    except Exception as e:
        print(f"Error loading key: {e}")
        return None, None


async def get_sol_balance(pubkey: str) -> float:
    """Get SOL balance."""
    try:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [pubkey]
            }
            async with session.post(SOLANA_RPC_URL, json=payload) as resp:
                data = await resp.json()
                lamports = data.get("result", {}).get("value", 0)
                return lamports / 1_000_000_000  # Convert to SOL
    except Exception as e:
        print(f"Error getting SOL balance: {e}")
        return 0


async def get_token_accounts(pubkey: str) -> list:
    """Get all token accounts with balances (SPL + Token-2022/pump.fun)."""
    tokens = []

    # Both SPL Token and Token-2022 (pump.fun) program IDs
    program_ids = [
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022 (pump.fun)
    ]

    try:
        async with aiohttp.ClientSession() as session:
            for program_id in program_ids:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        pubkey,
                        {"programId": program_id},
                        {"encoding": "jsonParsed"}
                    ]
                }
                async with session.post(SOLANA_RPC_URL, json=payload) as resp:
                    data = await resp.json()

                    if "error" in data:
                        print(f"RPC Error ({program_id[:8]}...): {data['error']}")
                        continue

                    accounts = data.get("result", {}).get("value", [])
                    prog_name = "SPL" if "keg" in program_id else "Token2022"
                    print(f"Found {len(accounts)} {prog_name} accounts")

                    for acc in accounts:
                        info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
                        mint = info.get("mint", "")
                        token_amount = info.get("tokenAmount", {})
                        amount = float(token_amount.get("uiAmount", 0) or 0)
                        decimals = token_amount.get("decimals", 0)

                        if amount > 0:
                            tokens.append({
                                "mint": mint,
                                "amount": amount,
                                "decimals": decimals,
                                "raw_amount": int(token_amount.get("amount", 0))
                            })
                        else:
                            print(f"  (zero: {mint[:16]}...)")

    except Exception as e:
        print(f"Error getting tokens: {e}")
        import traceback
        traceback.print_exc()

    return tokens


async def get_token_info(mint: str) -> dict:
    """Get token info from DexScreener."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("pairs"):
                        pair = data["pairs"][0]
                        return {
                            "symbol": pair.get("baseToken", {}).get("symbol", "???"),
                            "name": pair.get("baseToken", {}).get("name", "Unknown"),
                            "price": float(pair.get("priceUsd", 0) or 0),
                            "mc": float(pair.get("marketCap", 0) or 0),
                            "liq": float(pair.get("liquidity", {}).get("usd", 0) or 0),
                        }
    except:
        pass
    return {"symbol": "???", "name": "Unknown", "price": 0, "mc": 0, "liq": 0}


async def get_jupiter_quote(input_mint: str, output_mint: str, amount: int) -> dict:
    """Get swap quote from Jupiter."""
    try:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": MAX_SLIPPAGE_BPS,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(JUPITER_QUOTE_API, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        print(f"Quote error: {e}")
    return None


async def execute_sell(mint: str, raw_amount: int, pubkey: str, key_bytes: bytes) -> bool:
    """Execute sell via Jupiter."""
    print(f"\nGetting quote...")

    quote = await get_jupiter_quote(mint, SOL_MINT, raw_amount)
    if not quote:
        print("Failed to get quote")
        return False

    out_amount = int(quote.get("outAmount", 0))
    sol_out = out_amount / 1_000_000_000
    print(f"Quote: {sol_out:.6f} SOL")

    confirm = input("Execute sell? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled")
        return False

    try:
        # Get swap transaction
        payload = {
            "quoteResponse": quote,
            "userPublicKey": pubkey,
            "wrapAndUnwrapSol": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(JUPITER_SWAP_API, json=payload) as resp:
                if resp.status != 200:
                    print(f"Swap API error: {resp.status}")
                    return False

                # Jupiter returns swapTransaction as base64.
                text = await resp.text()
                try:
                    swap_data = json.loads(text)
                except Exception:
                    print(f"Swap API response (non-json): {text[:200]}")
                    return False
                swap_tx = swap_data.get("swapTransaction")

                if not swap_tx:
                    print("No swap transaction")
                    return False

        # Sign and send
        try:
            from solders.keypair import Keypair
            from solders.transaction import VersionedTransaction
            from solders.signature import Signature
            from solana.rpc.async_api import AsyncClient

            keypair = Keypair.from_bytes(key_bytes)
            tx_bytes = base64.b64decode(swap_tx)
            tx = VersionedTransaction.from_bytes(tx_bytes)
            # For v0 transactions, sign the versioned message bytes.
            try:
                from solders.message import to_bytes_versioned
                msg_bytes = to_bytes_versioned(tx.message)
            except Exception:
                msg_bytes = bytes(tx.message)
            sig = keypair.sign_message(msg_bytes)

            signer_count = tx.message.header.num_required_signatures
            signer_keys = list(tx.message.account_keys)[:signer_count]
            our_pubkey = keypair.pubkey()
            if os.getenv("DEBUG_WALLET", "0") == "1":
                print(f"Signer count: {signer_count}")
                print("Required signers:")
                for k in signer_keys:
                    print(f"  {k}")
                print(f"Wallet signer: {our_pubkey}")
            try:
                signer_index = signer_keys.index(our_pubkey)
            except ValueError:
                print("Signer mismatch: wallet pubkey not in required signers")
                print(f"Wallet: {pubkey}")
                print("Required signers:")
                for k in signer_keys:
                    print(f"  {k}")
                return False

            sigs = list(tx.signatures)
            if len(sigs) < signer_count:
                sigs += [Signature.default()] * (signer_count - len(sigs))
            sigs[signer_index] = sig
            tx = VersionedTransaction.populate(tx.message, sigs)

            async with AsyncClient(SOLANA_RPC_URL) as client:
                result = await client.send_raw_transaction(bytes(tx))
                if result.value:
                    sig_str = str(result.value)
                    print(f"\nSold! TX: {sig_str}")
                    print(f"https://solscan.io/tx/{sig_str}")

                    # Confirm and re-check balance
                    try:
                        for _ in range(20):
                            status = await client.get_signature_statuses([result.value])
                            st = status.value[0]
                            if st and st.confirmation_status:
                                print(f"Confirmation: {st.confirmation_status}")
                                break
                            await asyncio.sleep(1.0)
                    except Exception:
                        print("Confirmation check failed")

                    try:
                        tokens = await get_token_accounts(pubkey)
                        new_amt = 0
                        for t in tokens:
                            if t["mint"] == mint:
                                new_amt = t["amount"]
                                break
                        print(f"Post-sell token balance: {new_amt:,.6f}")
                    except Exception:
                        print("Post-sell balance check failed")

                    return True

        except ImportError:
            print("Install: pip install solders solana")
            return False

    except Exception as e:
        print(f"Sell error: {e}")

    return False


async def show_balances():
    """Show all balances."""
    key_bytes, pubkey = load_keypair()

    if not pubkey:
        print("No wallet configured in keys.env")
        return []

    print(f"\nWallet: {pubkey}")
    print(f"RPC: {SOLANA_RPC_URL[:50]}...")
    print("=" * 50)

    # SOL balance
    sol = await get_sol_balance(pubkey)
    print(f"\nSOL: {sol:.4f}")

    # Token balances
    tokens = await get_token_accounts(pubkey)

    if not tokens:
        print("\nNo tokens found")
        return []

    print(f"\nTokens ({len(tokens)}):")
    print("-" * 50)

    enriched = []
    for t in tokens:
        info = await get_token_info(t["mint"])
        t.update(info)
        enriched.append(t)

        value = t["amount"] * t["price"]
        mc_str = f"${t['mc']/1000:.0f}K" if t["mc"] < 1_000_000 else f"${t['mc']/1_000_000:.1f}M"

        print(f"  {t['symbol'][:10].ljust(10)} | {t['amount']:,.2f} | ${value:.2f} | MC: {mc_str}")
        print(f"    {t['mint'][:20]}...")

    print("-" * 50)
    total_value = sum(t["amount"] * t["price"] for t in enriched)
    print(f"Total token value: ${total_value:.2f}")
    print(f"Total (incl SOL): ${total_value + sol * 150:.2f}")  # Rough SOL price

    return enriched


async def sell_menu():
    """Interactive sell menu."""
    key_bytes, pubkey = load_keypair()

    if not pubkey:
        print("No wallet configured")
        return

    tokens = await show_balances()

    if not tokens:
        return

    print("\n" + "=" * 50)
    print("SELL MENU")
    print("=" * 50)

    # Number the tokens
    for i, t in enumerate(tokens, 1):
        value = t["amount"] * t["price"]
        print(f"  {i}. {t['symbol'][:10].ljust(10)} | ${value:.2f}")

    print(f"  0. Cancel")
    print()

    try:
        choice = input("Select token to sell (number): ").strip()
        if not choice or choice == "0":
            print("Cancelled")
            return

        idx = int(choice) - 1
        if idx < 0 or idx >= len(tokens):
            print("Invalid selection")
            return

        token = tokens[idx]
        print(f"\nSelected: {token['symbol']}")
        print(f"Balance: {token['amount']:,.2f}")
        print(f"Value: ${token['amount'] * token['price']:.2f}")

        # Amount to sell
        pct = input("Sell % (100 for all, or enter amount): ").strip()

        if pct.endswith('%') or pct == "100":
            pct_val = float(pct.replace('%', ''))
            sell_amount = int(token["raw_amount"] * pct_val / 100)
        else:
            try:
                sell_amount = int(float(pct) * (10 ** token["decimals"]))
            except:
                sell_amount = token["raw_amount"]  # Sell all

        if sell_amount > token["raw_amount"]:
            sell_amount = token["raw_amount"]

        print(f"\nSelling: {sell_amount / (10 ** token['decimals']):,.2f} {token['symbol']}")

        await execute_sell(token["mint"], sell_amount, pubkey, key_bytes)

    except (ValueError, KeyboardInterrupt):
        print("\nCancelled")


async def main():
    import sys

    if "--sell" in sys.argv:
        await sell_menu()
    else:
        await show_balances()


if __name__ == "__main__":
    asyncio.run(main())
