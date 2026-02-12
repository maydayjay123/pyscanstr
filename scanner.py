"""
Token Scanner - 4 Trade Types

TYPE 1: QUICK - Low MC fast trades
  - MC: 20K-120K
  - Target: 8%+
  - Fast in/out

TYPE 2: MOMENTUM - High MC momentum plays
  - MC: 100K-500K
  - Target: 25-30%
  - Starting momentum, high volume

TYPE 3: GEM - Runner finder
  - MC: <800K
  - Key signals of a runner
  - Big potential

TYPE 4: RANGE - Mature token range plays (NEW)
  - Age: 24h+
  - MC: 50K-2M
  - Buy near 24h low (support)
  - 3-step DCA: 15/25/60%
  - Target: 15-25%
  - Calmer, more predictable
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import json

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Pool price engine - import from live_trader for accurate on-chain pricing
try:
    from live_trader import get_pool_price, get_sol_usd_price
    _HAS_POOL_PRICE = True
except ImportError:
    _HAS_POOL_PRICE = False


@dataclass
class TokenSignal:
    """A potential trade signal."""
    address: str
    symbol: str
    name: str
    price: float
    mc: float
    liquidity: float

    # Volume
    buys_5m: int
    sells_5m: int
    buys_1h: int
    sells_1h: int
    vol_5m: float
    vol_1h: float

    # Price action
    change_5m: float
    change_1h: float
    change_6h: float
    change_24h: float

    # Age
    age_mins: int

    # Signal
    trade_type: str  # QUICK, MOMENTUM, GEM
    signal: str      # BUY, WATCH, SKIP
    reason: str
    score: int
    target: int      # Target profit %

    chart: str

    @property
    def vol_direction(self) -> str:
        if self.buys_5m > self.sells_5m:
            return "UP"
        elif self.sells_5m > self.buys_5m:
            return "DOWN"
        return "FLAT"

    @property
    def buy_ratio(self) -> float:
        return self.buys_5m / max(1, self.sells_5m)

    @property
    def buy_ratio_1h(self) -> float:
        return self.buys_1h / max(1, self.sells_1h)

    @property
    def mc_str(self) -> str:
        if self.mc >= 1_000_000:
            return f"${self.mc/1_000_000:.1f}M"
        return f"${self.mc/1000:.0f}K"


# Trade type configs - EXPANDED RANGES
QUICK_MC_MIN = 20_000      # Lower floor to catch earlier
QUICK_MC_MAX = 120_000     # Expanded from 80K
QUICK_TARGET = 8

MOMENTUM_MC_MIN = 100_000  # Overlap slightly with QUICK
MOMENTUM_MC_MAX = 500_000  # Much higher - bigger plays
MOMENTUM_TARGET = 25

GEM_MC_MAX = 800_000       # Can find gems at higher MC too
GEM_TARGET = 100

# RANGE trade - mature tokens (24h+)
RANGE_MC_MIN = 50_000      # Established tokens
RANGE_MC_MAX = 2_000_000   # Up to 2M MC
RANGE_TARGET = 20          # More conservative target
RANGE_MIN_AGE_HOURS = 24   # Must be 24h+ old

MIN_LIQUIDITY = 12_000     # Slightly lower for more options


async def fetch_json(url: str, session: aiohttp.ClientSession, timeout: int = 12) -> Optional[dict]:
    try:
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.json()
    except:
        pass
    return None


async def get_dexscreener_boosts(session: aiohttp.ClientSession) -> List[dict]:
    """Get boosted tokens from DexScreener."""
    pairs = []
    boosts = await fetch_json("https://api.dexscreener.com/token-boosts/latest/v1", session)
    if boosts and isinstance(boosts, list):
        for token in boosts[:50]:
            if token.get("chainId") == "solana":
                addr = token.get("tokenAddress", "")
                if addr:
                    data = await fetch_json(f"https://api.dexscreener.com/latest/dex/tokens/{addr}", session)
                    if data and "pairs" in data and data["pairs"]:
                        best = max(data["pairs"], key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                        pairs.append(best)
                    await asyncio.sleep(0.03)
    return pairs


async def get_pump_fun_new(session: aiohttp.ClientSession) -> List[dict]:
    """Get newest pump.fun coins."""
    url = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
    data = await fetch_json(url, session, timeout=15)
    return data if isinstance(data, list) else []


async def get_dexscreener_trending(session: aiohttp.ClientSession) -> List[dict]:
    """Get trending/hot pairs from DexScreener."""
    pairs = []

    # Try multiple endpoints
    urls = [
        "https://api.dexscreener.com/latest/dex/pairs/solana",
        "https://api.dexscreener.com/token-profiles/latest/v1",
    ]

    for url in urls:
        try:
            data = await fetch_json(url, session)
            if data:
                if isinstance(data, list):
                    # Token profiles - get pair data for each
                    for item in data[:20]:
                        if item.get("chainId") == "solana":
                            addr = item.get("tokenAddress", "")
                            if addr:
                                pair_data = await fetch_json(f"https://api.dexscreener.com/latest/dex/tokens/{addr}", session)
                                if pair_data and "pairs" in pair_data and pair_data["pairs"]:
                                    best = max(pair_data["pairs"], key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                                    pairs.append(best)
                                await asyncio.sleep(0.03)
                elif "pairs" in data and data["pairs"]:
                    pairs.extend(data["pairs"][:30])
        except:
            continue

    return pairs


def extract_pair_data(pair: dict) -> Optional[dict]:
    """Extract all data from a DexScreener pair."""
    try:
        if pair.get("chainId") != "solana":
            return None

        base = pair.get("baseToken", {})
        address = base.get("address", "")
        if not address:
            return None

        txns = pair.get("txns", {})
        volume = pair.get("volume", {})
        price_change = pair.get("priceChange", {})

        # Get 24h high/low for range detection
        price = float(pair.get("priceUsd", 0) or 0)
        change_24h = float(price_change.get("h24", 0) or 0)

        # Estimate 24h high/low from current price and 24h change
        # If +50% in 24h, price was ~66% of current 24h ago
        # If -50% in 24h, price was 200% of current 24h ago
        if change_24h != 0 and price > 0:
            price_24h_ago = price / (1 + change_24h / 100)
            # Rough estimate: high = max(current, 24h_ago * 1.1), low = min(current, 24h_ago * 0.9)
            high_24h = max(price, price_24h_ago) * 1.05  # Add 5% buffer
            low_24h = min(price, price_24h_ago) * 0.95   # Subtract 5% buffer
        else:
            high_24h = price
            low_24h = price

        return {
            "address": address,
            "symbol": base.get("symbol", "?")[:12],
            "name": base.get("name", "?")[:25],
            "price": price,
            "mc": float(pair.get("marketCap", 0) or 0),
            "liquidity": float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "buys_5m": int(txns.get("m5", {}).get("buys", 0) or 0),
            "sells_5m": int(txns.get("m5", {}).get("sells", 0) or 0),
            "buys_1h": int(txns.get("h1", {}).get("buys", 0) or 0),
            "sells_1h": int(txns.get("h1", {}).get("sells", 0) or 0),
            "vol_5m": float(volume.get("m5", 0) or 0),
            "vol_1h": float(volume.get("h1", 0) or 0),
            "vol_24h": float(volume.get("h24", 0) or 0),
            "change_5m": float(price_change.get("m5", 0) or 0),
            "change_1h": float(price_change.get("h1", 0) or 0),
            "change_6h": float(price_change.get("h6", 0) or 0),
            "change_24h": change_24h,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "created": pair.get("pairCreatedAt", 0),
            "chart": f"https://dexscreener.com/solana/{address}"
        }
    except:
        return None


def evaluate_quick(data: dict) -> Optional[TokenSignal]:
    """TYPE 1: QUICK - Low MC fast trades. Target 8%+"""
    mc = data["mc"]

    # MC must be in range
    if not (QUICK_MC_MIN <= mc <= QUICK_MC_MAX):
        return None

    if data["liquidity"] < MIN_LIQUIDITY:
        return None

    score = 0
    reasons = []

    # Minimum volume activity - skip dead/ultra-low vol
    if data["vol_5m"] < 2000:
        return None  # Not enough volume to confirm interest

    # Volume direction - must be UP
    if data["buys_5m"] <= data["sells_5m"]:
        return None  # Skip if selling

    buy_ratio = data["buys_5m"] / max(1, data["sells_5m"])
    if buy_ratio < 1.3:
        return None  # Too weak buy pressure for QUICK

    # Strong buy pressure
    if buy_ratio >= 2.0:
        score += 35
        reasons.append(f"strong {buy_ratio:.1f}x")
    elif buy_ratio >= 1.5:
        score += 25
        reasons.append(f"buy {buy_ratio:.1f}x")
    else:
        score += 15

    # 5m momentum
    if data["change_5m"] > 10:
        score += 25
        reasons.append(f"+{data['change_5m']:.0f}% 5m")
    elif data["change_5m"] > 5:
        score += 15
        reasons.append(f"+{data['change_5m']:.0f}% 5m")
    elif data["change_5m"] > 0:
        score += 5

    # Volume activity
    if data["vol_5m"] > 5000:
        score += 15
        reasons.append(f"${data['vol_5m']/1000:.0f}K vol")
    elif data["vol_5m"] > 2000:
        score += 10

    # Fresh coins bonus
    age_mins = 9999
    if data["created"]:
        age_mins = int((datetime.now().timestamp() * 1000 - data["created"]) / (1000 * 60))
    if age_mins < 30:
        score += 10
        reasons.append(f"{age_mins}m old")

    signal = "BUY" if score >= 40 else "WATCH" if score >= 25 else "SKIP"

    return TokenSignal(
        address=data["address"],
        symbol=data["symbol"],
        name=data["name"],
        price=data["price"],
        mc=mc,
        liquidity=data["liquidity"],
        buys_5m=data["buys_5m"],
        sells_5m=data["sells_5m"],
        buys_1h=data["buys_1h"],
        sells_1h=data["sells_1h"],
        vol_5m=data["vol_5m"],
        vol_1h=data["vol_1h"],
        change_5m=data["change_5m"],
        change_1h=data["change_1h"],
        change_6h=data["change_6h"],
        change_24h=data["change_24h"],
        age_mins=age_mins,
        trade_type="QUICK",
        signal=signal,
        reason=" | ".join(reasons) if reasons else "low MC",
        score=min(100, score),
        target=QUICK_TARGET,
        chart=data["chart"]
    )


def evaluate_momentum(data: dict) -> Optional[TokenSignal]:
    """TYPE 2: MOMENTUM - High MC starting momentum. Target 25-30%"""
    mc = data["mc"]

    # MC must be in range
    if not (MOMENTUM_MC_MIN <= mc <= MOMENTUM_MC_MAX):
        return None

    if data["liquidity"] < MIN_LIQUIDITY * 1.5:  # Relaxed liq requirement
        return None

    score = 0
    reasons = []

    # Need decent volume - raised from 5K (MOMENTUM was worst performer at 30% win rate)
    if data["vol_1h"] < 10000:
        return None  # Skip low volume

    reasons.append(f"${data['vol_1h']/1000:.0f}K vol")
    score += 20

    # Volume direction - must be UP
    buy_ratio_5m = data["buys_5m"] / max(1, data["sells_5m"])
    buy_ratio_1h = data["buys_1h"] / max(1, data["sells_1h"])

    if buy_ratio_5m < 1.5:
        return None  # Skip weak buying (raised from 1.2 - too many losers)

    # Starting momentum - 5m stronger than 1h
    if buy_ratio_5m > buy_ratio_1h:
        score += 25
        reasons.append("momentum starting")

    # Strong current buying
    if buy_ratio_5m >= 2.0:
        score += 20
        reasons.append(f"buy {buy_ratio_5m:.1f}x")
    elif buy_ratio_5m >= 1.7:
        score += 10

    # Price recovering or pushing
    if data["change_5m"] > 5:
        score += 15
        reasons.append(f"+{data['change_5m']:.0f}% 5m")

    # Dip recovery pattern - 1h down but 5m up
    if data["change_1h"] < 0 and data["change_5m"] > 0:
        score += 15
        reasons.append("dip recovery")

    # Not at ATH (24h change reasonable)
    if data["change_24h"] > 200:
        score -= 20  # Penalize already pumped

    signal = "BUY" if score >= 45 else "WATCH" if score >= 30 else "SKIP"

    age_mins = 9999
    if data["created"]:
        age_mins = int((datetime.now().timestamp() * 1000 - data["created"]) / (1000 * 60))

    return TokenSignal(
        address=data["address"],
        symbol=data["symbol"],
        name=data["name"],
        price=data["price"],
        mc=mc,
        liquidity=data["liquidity"],
        buys_5m=data["buys_5m"],
        sells_5m=data["sells_5m"],
        buys_1h=data["buys_1h"],
        sells_1h=data["sells_1h"],
        vol_5m=data["vol_5m"],
        vol_1h=data["vol_1h"],
        change_5m=data["change_5m"],
        change_1h=data["change_1h"],
        change_6h=data["change_6h"],
        change_24h=data["change_24h"],
        age_mins=age_mins,
        trade_type="MOMENTUM",
        signal=signal,
        reason=" | ".join(reasons) if reasons else "high MC momentum",
        score=min(100, score),
        target=MOMENTUM_TARGET,
        chart=data["chart"]
    )


def evaluate_gem(data: dict) -> Optional[TokenSignal]:
    """TYPE 3: GEM - Runner finder. Key signals of big potential."""
    mc = data["mc"]

    # Gems found early
    if mc > GEM_MC_MAX:
        return None

    if data["liquidity"] < MIN_LIQUIDITY:
        return None

    score = 0
    reasons = []
    gem_signals = 0

    age_mins = 9999
    if data["created"]:
        age_mins = int((datetime.now().timestamp() * 1000 - data["created"]) / (1000 * 60))

    # GEM SIGNAL 1: Very fresh with strong buying
    if age_mins < 15 and data["buys_5m"] > data["sells_5m"] * 2:
        gem_signals += 1
        score += 25
        reasons.append(f"fresh {age_mins}m + strong buy")

    # GEM SIGNAL 2: Consistent buying across timeframes
    buy_ratio_5m = data["buys_5m"] / max(1, data["sells_5m"])
    buy_ratio_1h = data["buys_1h"] / max(1, data["sells_1h"])

    if buy_ratio_5m >= 1.7 and buy_ratio_1h >= 1.3:
        gem_signals += 1
        score += 20
        reasons.append("consistent buying")

    # GEM SIGNAL 3: Volume increasing (5m vol high relative to 1h)
    if data["vol_1h"] > 0:
        vol_ratio = (data["vol_5m"] * 12) / data["vol_1h"]  # Normalize to 1h
        if vol_ratio > 1.5:
            gem_signals += 1
            score += 20
            reasons.append("vol increasing")

    # GEM SIGNAL 4: Strong uptrend
    if data["change_5m"] > 15 and data["change_1h"] > 20:
        gem_signals += 1
        score += 20
        reasons.append(f"+{data['change_1h']:.0f}% 1h")

    # GEM SIGNAL 5: Healthy liquidity ratio
    if data["liquidity"] > 0 and mc > 0:
        liq_ratio = data["liquidity"] / mc
        if liq_ratio > 0.1:  # >10% liquidity
            gem_signals += 1
            score += 15
            reasons.append(f"{liq_ratio*100:.0f}% liq")

    # Need multiple gem signals
    if gem_signals < 3:
        return None

    reasons.insert(0, f"{gem_signals} signals")

    signal = "BUY" if gem_signals >= 3 and score >= 40 else "WATCH" if gem_signals >= 3 else "SKIP"

    return TokenSignal(
        address=data["address"],
        symbol=data["symbol"],
        name=data["name"],
        price=data["price"],
        mc=mc,
        liquidity=data["liquidity"],
        buys_5m=data["buys_5m"],
        sells_5m=data["sells_5m"],
        buys_1h=data["buys_1h"],
        sells_1h=data["sells_1h"],
        vol_5m=data["vol_5m"],
        vol_1h=data["vol_1h"],
        change_5m=data["change_5m"],
        change_1h=data["change_1h"],
        change_6h=data["change_6h"],
        change_24h=data["change_24h"],
        age_mins=age_mins,
        trade_type="GEM",
        signal=signal,
        reason=" | ".join(reasons),
        score=min(100, score),
        target=GEM_TARGET,
        chart=data["chart"]
    )


def evaluate_range(data: dict) -> Optional[TokenSignal]:
    """TYPE 4: RANGE - Mature token range plays. Buy near support."""
    mc = data["mc"]

    # MC must be in range
    if not (RANGE_MC_MIN <= mc <= RANGE_MC_MAX):
        return None

    # Higher liquidity requirement for range trades
    if data["liquidity"] < MIN_LIQUIDITY * 2:
        return None

    # Must be 24h+ old (mature token)
    age_mins = 9999
    if data["created"]:
        age_mins = int((datetime.now().timestamp() * 1000 - data["created"]) / (1000 * 60))

    age_hours = age_mins / 60
    if age_hours < RANGE_MIN_AGE_HOURS:
        return None  # Too young for range trading

    score = 0
    reasons = []

    # ===== RANGE DETECTION =====
    price = data["price"]
    high_24h = data.get("high_24h", price)
    low_24h = data.get("low_24h", price)

    if high_24h <= low_24h or price <= 0:
        return None

    # Calculate position in range (0 = at low, 1 = at high)
    range_size = high_24h - low_24h
    if range_size > 0:
        position_in_range = (price - low_24h) / range_size
    else:
        position_in_range = 0.5

    # RANGE SIGNAL 1: Near support (lower 30% of range)
    if position_in_range <= 0.30:
        score += 35
        reasons.append(f"near support ({position_in_range*100:.0f}%)")
    elif position_in_range <= 0.45:
        score += 20
        reasons.append(f"mid-low range ({position_in_range*100:.0f}%)")
    else:
        return None  # Don't buy at resistance or mid/high range

    # RANGE SIGNAL 2: Bouncing (5m positive after dip)
    if data["change_5m"] > 0 and data["change_1h"] < 0:
        score += 25
        reasons.append("bounce forming")
    elif data["change_5m"] > 2:
        score += 15
        reasons.append(f"+{data['change_5m']:.0f}% 5m")

    # RANGE SIGNAL 3: Buy pressure returning (need real buying, not just 1.2x)
    buy_ratio_5m = data["buys_5m"] / max(1, data["sells_5m"])
    if buy_ratio_5m < 1.3:
        return None  # No buy pressure = no entry
    if buy_ratio_5m >= 1.5:
        score += 20
        reasons.append(f"{buy_ratio_5m:.1f}x buy")
    elif buy_ratio_5m >= 1.3:
        score += 10

    # RANGE SIGNAL 4: Decent volume (not dead)
    if data["vol_24h"] > 50000:
        score += 15
        reasons.append(f"${data['vol_24h']/1000:.0f}K 24h vol")
    elif data["vol_1h"] > 2000:
        score += 10

    # RANGE SIGNAL 5: Not crashed (24h change reasonable)
    if -30 <= data["change_24h"] <= 30:
        score += 10
        reasons.append("stable")
    elif data["change_24h"] < -50:
        score -= 20  # Penalize crashed tokens

    # Liquidity health
    if mc > 0 and data["liquidity"] / mc > 0.15:
        score += 10
        reasons.append("good liq")

    signal = "BUY" if score >= 50 else "WATCH" if score >= 35 else "SKIP"

    return TokenSignal(
        address=data["address"],
        symbol=data["symbol"],
        name=data["name"],
        price=data["price"],
        mc=mc,
        liquidity=data["liquidity"],
        buys_5m=data["buys_5m"],
        sells_5m=data["sells_5m"],
        buys_1h=data["buys_1h"],
        sells_1h=data["sells_1h"],
        vol_5m=data["vol_5m"],
        vol_1h=data["vol_1h"],
        change_5m=data["change_5m"],
        change_1h=data["change_1h"],
        change_6h=data["change_6h"],
        change_24h=data["change_24h"],
        age_mins=age_mins,
        trade_type="RANGE",
        signal=signal,
        reason=" | ".join(reasons) if reasons else "range play",
        score=min(100, score),
        target=RANGE_TARGET,
        chart=data["chart"]
    )


async def _verify_pool_price(signal: TokenSignal) -> TokenSignal:
    """Verify BUY signal price/MC with on-chain pool data.
    If pool price differs significantly, update the signal.
    Returns updated signal (may downgrade BUY to WATCH if MC out of range)."""
    if not _HAS_POOL_PRICE:
        return signal
    try:
        pool = await get_pool_price(signal.address)
        if not pool or pool["price_usd"] <= 0:
            return signal

        pool_price = pool["price_usd"]
        dex_price = signal.price

        # Calculate how far off DexScreener is
        if dex_price > 0:
            drift_pct = abs(pool_price - dex_price) / dex_price * 100
        else:
            drift_pct = 100

        # Update signal with pool price
        if dex_price > 0 and signal.mc > 0:
            pool_mc = signal.mc * (pool_price / dex_price)
        else:
            pool_mc = signal.mc

        signal.price = pool_price
        signal.mc = pool_mc

        if drift_pct > 5:
            print(f"  Pool price {signal.symbol}: ${pool_price:.10f} (DexScreener was {drift_pct:.0f}% off)")

        # Check if corrected MC still fits the trade type range
        if signal.signal == "BUY":
            mc = pool_mc
            out_of_range = False
            if signal.trade_type == "QUICK" and not (QUICK_MC_MIN <= mc <= QUICK_MC_MAX):
                out_of_range = True
            elif signal.trade_type == "MOMENTUM" and not (MOMENTUM_MC_MIN <= mc <= MOMENTUM_MC_MAX):
                out_of_range = True
            elif signal.trade_type == "GEM" and mc > GEM_MC_MAX:
                out_of_range = True
            elif signal.trade_type == "RANGE" and not (RANGE_MC_MIN <= mc <= RANGE_MC_MAX):
                out_of_range = True

            if out_of_range:
                print(f"  {signal.symbol} BUY->WATCH: pool MC ${mc:,.0f} out of {signal.trade_type} range")
                signal.signal = "WATCH"
                signal.reason += " | MC drift"

    except Exception as e:
        pass  # Fall back to DexScreener price
    return signal


async def scan() -> List[TokenSignal]:
    """Scan for all 4 trade types."""
    signals = []
    seen = set()

    async with aiohttp.ClientSession() as session:
        # Get pairs from multiple sources
        all_pairs = []

        try:
            # DexScreener boosts
            boosts = await get_dexscreener_boosts(session)
            if boosts:
                all_pairs.extend(boosts)
        except Exception as e:
            print(f"  Boosts error: {e}")

        try:
            # Pump.fun new
            pump_coins = await get_pump_fun_new(session)
            for coin in pump_coins[:30]:
                addr = coin.get("mint", "")
                if addr:
                    data = await fetch_json(f"https://api.dexscreener.com/latest/dex/tokens/{addr}", session)
                    if data and "pairs" in data and data["pairs"]:
                        best = max(data["pairs"], key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                        all_pairs.append(best)
                    await asyncio.sleep(0.03)
        except Exception as e:
            print(f"  Pump.fun error: {e}")

        try:
            # Trending
            trending = await get_dexscreener_trending(session)
            if trending:
                all_pairs.extend(trending)
        except Exception as e:
            print(f"  Trending error: {e}")

        # Evaluate each pair for all 3 types
        for pair in all_pairs:
            data = extract_pair_data(pair)
            if not data or data["address"] in seen:
                continue

            seen.add(data["address"])

            # Try each type
            quick = evaluate_quick(data)
            if quick and quick.signal != "SKIP":
                signals.append(quick)
                continue

            momentum = evaluate_momentum(data)
            if momentum and momentum.signal != "SKIP":
                signals.append(momentum)
                continue

            gem = evaluate_gem(data)
            if gem and gem.signal != "SKIP":
                signals.append(gem)
                continue

            # RANGE - mature tokens (24h+)
            range_sig = evaluate_range(data)
            if range_sig and range_sig.signal != "SKIP":
                signals.append(range_sig)

    # Verify BUY signals with on-chain pool price (only BUYs to limit RPC calls)
    if _HAS_POOL_PRICE:
        buy_signals = [s for s in signals if s.signal == "BUY"]
        for i, sig in enumerate(buy_signals):
            try:
                verified = await _verify_pool_price(sig)
                # Signal was modified in-place, no need to replace
            except:
                pass

    # Sort: BUY first, then by score
    signals.sort(key=lambda s: (s.signal != "BUY", -s.score))

    return signals


def save_signals(signals: List[TokenSignal], filepath: str = "signals.json"):
    """Save signals to JSON."""
    data = []
    for s in signals:
        if s.signal in ("BUY", "WATCH"):
            data.append({
                "address": s.address,
                "symbol": s.symbol,
                "name": s.name,
                "price": s.price,
                "mc": s.mc,
                "liquidity": s.liquidity,
                "buys_5m": s.buys_5m,
                "sells_5m": s.sells_5m,
                "vol_5m": s.vol_5m,
                "vol_1h": s.vol_1h,
                "change_5m": s.change_5m,
                "change_1h": s.change_1h,
                "trade_type": s.trade_type,
                "signal": s.signal,
                "reason": s.reason,
                "score": s.score,
                "target": s.target,
                "chart": s.chart,
                "timestamp": datetime.now().isoformat()
            })

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    return len(data)


_scan_msg_id = 0  # Track scanner message for edit-in-place

async def send_tg(text: str):
    """Send or edit scanner message to Telegram (single message, updated in place)."""
    global _scan_msg_id
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        async with aiohttp.ClientSession() as session:
            if _scan_msg_id:
                # Try to edit existing message
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "message_id": _scan_msg_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True
                }
                async with session.post(url, json=payload, timeout=10) as resp:
                    data = await resp.json()
                    if data.get("ok") or "not modified" in data.get("description", ""):
                        return  # Edit worked or content unchanged
                    # Message deleted/too old - fall through to send new

            # Send new message
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            async with session.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10) as resp:
                data = await resp.json()
                _scan_msg_id = data.get("result", {}).get("message_id", 0)
    except:
        pass


def format_signal_msg(signals: List[TokenSignal]) -> str:
    """Format signals for Telegram."""
    buys = [s for s in signals if s.signal == "BUY"]
    watches = [s for s in signals if s.signal == "WATCH"]

    if not buys and not watches:
        return ""

    ts = datetime.now().strftime("%H:%M")

    # Group BUYs by type
    quick = [s for s in buys if s.trade_type == "QUICK"]
    momentum = [s for s in buys if s.trade_type == "MOMENTUM"]
    gems = [s for s in buys if s.trade_type == "GEM"]
    ranges = [s for s in buys if s.trade_type == "RANGE"]

    lines = [f"*SCAN* {ts}"]
    lines.append(f"BUY: Q:{len(quick)} M:{len(momentum)} G:{len(gems)} R:{len(ranges)} | WATCH:{len(watches)}")

    if quick:
        lines.append("")
        lines.append("*⚡ QUICK* (8%+)")
        for s in quick[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction} | *{s.score}pts*")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    if momentum:
        lines.append("")
        lines.append("*📈 MOMENTUM* (25%+)")
        for s in momentum[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction} | *{s.score}pts*")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    if gems:
        lines.append("")
        lines.append("*💎 GEM* (100%+)")
        for s in gems[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction} | *{s.score}pts*")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    if ranges:
        lines.append("")
        lines.append("*📊 RANGE* (20%+ DCA)")
        for s in ranges[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x | 24h+ | *{s.score}pts*")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    # Show WATCH signals if no BUYs
    if not buys and watches:
        lines.append("")
        lines.append("*WATCH*")
        for s in watches[:5]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.score}pts")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    return "\n".join(lines)


async def run_scanner(interval_secs: int = 30, send_to_tg: bool = True, live_mode: bool = False):
    """Run scanner loop."""
    mode = "LIVE" if live_mode else "SIM"
    print(f"Scanner started - {interval_secs}s interval [{mode}]")
    print(f"QUICK:    ${QUICK_MC_MIN/1000:.0f}K-${QUICK_MC_MAX/1000:.0f}K  target {QUICK_TARGET}%")
    print(f"MOMENTUM: ${MOMENTUM_MC_MIN/1000:.0f}K-${MOMENTUM_MC_MAX/1000:.0f}K target {MOMENTUM_TARGET}%")
    print(f"GEM:      <${GEM_MC_MAX/1000:.0f}K         target {GEM_TARGET}%+")
    print(f"RANGE:    ${RANGE_MC_MIN/1000:.0f}K-${RANGE_MC_MAX/1000:.0f}K  target {RANGE_TARGET}% (24h+ DCA)")
    print(f"Telegram: {'ON' if send_to_tg else 'OFF'}\n")

    # Import live trader if live mode
    if live_mode:
        from live_trader import process_signal as live_buy

    while True:
        try:
            signals = await scan()

            buys = [s for s in signals if s.signal == "BUY"]
            watches = [s for s in signals if s.signal == "WATCH"]
            quick = [s for s in buys if s.trade_type == "QUICK"]
            momentum = [s for s in buys if s.trade_type == "MOMENTUM"]
            gems = [s for s in buys if s.trade_type == "GEM"]
            ranges = [s for s in buys if s.trade_type == "RANGE"]

            ts = datetime.now().strftime('%H:%M:%S')
            print(f"[{ts}] Scanned {len(signals)} | BUY: Q:{len(quick)} M:{len(momentum)} G:{len(gems)} R:{len(ranges)} | WATCH:{len(watches)}")

            save_signals(signals)

            # Print by type with chart links
            for s in quick[:3]:
                print(f"  ⚡ Q {s.symbol.ljust(10)} {s.mc_str.ljust(7)} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.reason}")
                print(f"      {s.chart}")
            for s in momentum[:3]:
                print(f"  📈 M {s.symbol.ljust(10)} {s.mc_str.ljust(7)} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.reason}")
                print(f"      {s.chart}")
            for s in gems[:3]:
                print(f"  💎 G {s.symbol.ljust(10)} {s.mc_str.ljust(7)} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.reason}")
                print(f"      {s.chart}")
            for s in ranges[:3]:
                print(f"  📊 R {s.symbol.ljust(10)} {s.mc_str.ljust(7)} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.reason}")
                print(f"      {s.chart}")

            # Also show WATCH signals
            for s in watches[:3]:
                print(f"  👀 W {s.symbol.ljust(10)} {s.mc_str.ljust(7)} | {s.buy_ratio:.1f}x {s.vol_direction} | {s.reason}")
                print(f"      {s.chart}")

            # LIVE MODE: Execute buys
            if live_mode and buys:
                for s in buys[:2]:  # Max 2 buys per scan
                    signal_data = {
                        "signal": s.signal,
                        "address": s.address,
                        "symbol": s.symbol,
                        "trade_type": s.trade_type,
                        "price": s.price,
                        "market_cap": s.mc,
                        "buy_ratio": s.buy_ratio,
                        "liquidity": s.liquidity,
                    }
                    await live_buy(signal_data)

            # Send to TG if any signals (BUY or WATCH) - edits in place
            if send_to_tg and signals:
                msg = format_signal_msg(signals)
                if msg:
                    msg += f"\n_Scanned: {datetime.now().strftime('%H:%M:%S')}_"
                    await send_tg(msg)

        except Exception as e:
            import traceback
            print(f"  Error: {e}")
            traceback.print_exc()

        await asyncio.sleep(interval_secs)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=int, default=30)
    args = parser.parse_args()

    if args.once:
        async def once():
            signals = await scan()
            print(f"Found {len(signals)} signals:\n")
            for s in signals[:20]:
                print(f"[{s.trade_type}] {s.signal} {s.symbol} {s.mc_str} | {s.reason}")
            save_signals(signals)
        asyncio.run(once())
    else:
        asyncio.run(run_scanner(args.interval))
