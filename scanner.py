"""
Token Scanner - 3 Trade Types

TYPE 1: QUICK - Low MC fast trades
  - MC: 10K-35K
  - Target: 8%+
  - Fast in/out

TYPE 2: MOMENTUM - High MC momentum plays
  - MC: 75K-135K
  - Target: 25-30%
  - Starting momentum, high volume

TYPE 3: GEM - Runner finder
  - Key signals of a runner
  - Big potential
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import json

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


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


# Trade type configs
QUICK_MC_MIN = 10_000
QUICK_MC_MAX = 35_000
QUICK_TARGET = 8

MOMENTUM_MC_MIN = 75_000
MOMENTUM_MC_MAX = 135_000
MOMENTUM_TARGET = 25

GEM_MC_MAX = 50_000  # Gems found early
GEM_TARGET = 100

MIN_LIQUIDITY = 5_000


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

        return {
            "address": address,
            "symbol": base.get("symbol", "?")[:12],
            "name": base.get("name", "?")[:25],
            "price": float(pair.get("priceUsd", 0) or 0),
            "mc": float(pair.get("marketCap", 0) or 0),
            "liquidity": float(pair.get("liquidity", {}).get("usd", 0) or 0),
            "buys_5m": int(txns.get("m5", {}).get("buys", 0) or 0),
            "sells_5m": int(txns.get("m5", {}).get("sells", 0) or 0),
            "buys_1h": int(txns.get("h1", {}).get("buys", 0) or 0),
            "sells_1h": int(txns.get("h1", {}).get("sells", 0) or 0),
            "vol_5m": float(volume.get("m5", 0) or 0),
            "vol_1h": float(volume.get("h1", 0) or 0),
            "change_5m": float(price_change.get("m5", 0) or 0),
            "change_1h": float(price_change.get("h1", 0) or 0),
            "change_6h": float(price_change.get("h6", 0) or 0),
            "change_24h": float(price_change.get("h24", 0) or 0),
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

    # Volume direction - must be UP
    if data["buys_5m"] <= data["sells_5m"]:
        return None  # Skip if selling

    buy_ratio = data["buys_5m"] / max(1, data["sells_5m"])

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

    if data["liquidity"] < MIN_LIQUIDITY * 2:  # Higher liq requirement
        return None

    score = 0
    reasons = []

    # Need high volume
    if data["vol_1h"] < 10000:
        return None  # Skip low volume

    reasons.append(f"${data['vol_1h']/1000:.0f}K vol")
    score += 20

    # Volume direction - must be UP
    buy_ratio_5m = data["buys_5m"] / max(1, data["sells_5m"])
    buy_ratio_1h = data["buys_1h"] / max(1, data["sells_1h"])

    if buy_ratio_5m < 1.0:
        return None  # Skip if current selling

    # Starting momentum - 5m stronger than 1h
    if buy_ratio_5m > buy_ratio_1h:
        score += 25
        reasons.append("momentum starting")

    # Strong current buying
    if buy_ratio_5m >= 2.0:
        score += 20
        reasons.append(f"buy {buy_ratio_5m:.1f}x")
    elif buy_ratio_5m >= 1.5:
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

    if buy_ratio_5m >= 1.5 and buy_ratio_1h >= 1.3:
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
    if gem_signals < 2:
        return None

    reasons.insert(0, f"{gem_signals} signals")

    signal = "BUY" if gem_signals >= 2 and score >= 40 else "WATCH" if gem_signals >= 2 else "SKIP"

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


async def scan() -> List[TokenSignal]:
    """Scan for all 3 trade types."""
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


async def send_tg(text: str):
    """Send message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=10)
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

    lines = [f"*SCAN* {ts}"]
    lines.append(f"BUY: Q:{len(quick)} M:{len(momentum)} G:{len(gems)} | WATCH:{len(watches)}")

    if quick:
        lines.append("")
        lines.append("*⚡ QUICK* (8%+)")
        for s in quick[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction}")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    if momentum:
        lines.append("")
        lines.append("*📈 MOMENTUM* (25%+)")
        for s in momentum[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction}")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    if gems:
        lines.append("")
        lines.append("*💎 GEM* (100%+)")
        for s in gems[:3]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction}")
            lines.append(f"  {s.reason}")
            lines.append(f"  [chart]({s.chart})")

    # Show WATCH signals if no BUYs
    if not buys and watches:
        lines.append("")
        lines.append("*WATCH*")
        for s in watches[:5]:
            lines.append(f"`{s.symbol}` {s.mc_str} | {s.buy_ratio:.1f}x {s.vol_direction}")
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

            ts = datetime.now().strftime('%H:%M:%S')
            print(f"[{ts}] Scanned {len(signals)} | BUY: Q:{len(quick)} M:{len(momentum)} G:{len(gems)} | WATCH:{len(watches)}")

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
                    }
                    await live_buy(signal_data)

            # Send to TG if any signals (BUY or WATCH)
            if send_to_tg and signals:
                msg = format_signal_msg(signals)
                if msg:
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
