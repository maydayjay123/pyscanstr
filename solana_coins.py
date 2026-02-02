"""Solana gem finder - DexScreener + Pump.fun"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import aiohttp

from config import MAX_COIN_AGE_HOURS, MIN_LIQUIDITY


@dataclass
class MemeCoin:
    name: str
    symbol: str
    address: str
    price_usd: float
    market_cap: Optional[float]
    volume_24h: float
    price_change_1h: float
    price_change_24h: float
    liquidity: float
    age_hours: Optional[float]
    dex: str
    is_early: bool
    is_pumping: bool  # NEW: positive momentum indicator
    # Links
    dexscreener_url: str
    pump_url: Optional[str]
    birdeye_url: str


def format_age(hours: Optional[float]) -> str:
    if hours is None:
        return "?"
    if hours < 1:
        return f"{int(hours * 60)}m"
    elif hours < 24:
        return f"{hours:.0f}h"
    else:
        return f"{hours / 24:.1f}d"


async def fetch_json(url: str, timeout: int = 10) -> Optional[dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
    except:
        pass
    return None


async def get_pump_fun_coins() -> list[dict]:
    """Get new coins from pump.fun"""
    # Pump.fun king of the hill / new coins
    data = await fetch_json("https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC")
    return data if data else []


def get_tiktok_trends() -> list[str]:
    """Get trending terms from TikTok scraper."""
    try:
        import database as db
        db.init_db()
        tags = db.get_rising_hashtags(hours=6, limit=10)
        words = db.get_rising_keywords(hours=6, limit=10)
        return tags + words
    except:
        return []


def get_google_trends() -> list[str]:
    """Get trending terms from Google Trends."""
    try:
        from trends import get_google_trends as fetch_google
        items = fetch_google()
        return [t.term for t in items[:10]]
    except Exception as e:
        print(f"Google Trends unavailable: {e}")
        return []


async def get_dexscreener_trending() -> list[dict]:
    """Get trending Solana pairs - combines TikTok, Google Trends, and base terms"""
    # Base terms that always work
    base_terms = ["meme", "pepe", "doge", "trump", "ai", "pump", "cat", "dog"]

    # Get trending terms from TikTok
    tiktok_trends = get_tiktok_trends()

    # Get trending terms from Google Trends
    google_trends = get_google_trends()

    # Combine: Google trends first (broader reach), then TikTok, then base
    all_terms = list(dict.fromkeys(google_trends + tiktok_trends + base_terms))[:25]

    print(f"Searching coins for: {', '.join(all_terms[:12])}...")

    all_pairs = []
    seen = set()

    for term in all_terms:
        data = await fetch_json(f"https://api.dexscreener.com/latest/dex/search?q={term}")
        if data and isinstance(data, dict):
            pairs = data.get("pairs", [])
            for p in pairs:
                if p.get("chainId") == "solana":
                    addr = p.get("baseToken", {}).get("address", "")
                    if addr and addr not in seen:
                        all_pairs.append(p)
                        seen.add(addr)
        await asyncio.sleep(0.1)

    return all_pairs


async def search_dexscreener(query: str) -> list[dict]:
    """Search DexScreener"""
    data = await fetch_json(f"https://api.dexscreener.com/latest/dex/search?q={query}")
    if data:
        return [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
    return []


def parse_dexscreener_pair(pair: dict) -> Optional[MemeCoin]:
    """Parse DexScreener pair"""
    try:
        if pair.get("chainId") != "solana":
            return None

        base = pair.get("baseToken", {})
        address = base.get("address", "")
        price_change = pair.get("priceChange", {})

        # Age
        age_hours = None
        pair_created = pair.get("pairCreatedAt")
        if pair_created:
            age_hours = (datetime.now() - datetime.fromtimestamp(pair_created / 1000)).total_seconds() / 3600

        liquidity = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        if liquidity < MIN_LIQUIDITY:
            return None

        is_early = age_hours is not None and age_hours < MAX_COIN_AGE_HOURS

        # Check if coin is pumping (positive momentum)
        change_1h = float(price_change.get("h1", 0) or 0)
        change_5m = float(price_change.get("m5", 0) or 0)
        volume_24h = float(pair.get("volume", {}).get("h24", 0) or 0)

        # Is pumping: positive 1h change AND either positive 5m or high volume
        is_pumping = change_1h > 5 and (change_5m > 0 or volume_24h > 10000)

        return MemeCoin(
            name=base.get("name", "?")[:20],
            symbol=base.get("symbol", "?")[:10],
            address=address,
            price_usd=float(pair.get("priceUsd", 0) or 0),
            market_cap=float(pair.get("marketCap") or 0) if pair.get("marketCap") else None,
            volume_24h=volume_24h,
            price_change_1h=change_1h,
            price_change_24h=float(price_change.get("h24", 0) or 0),
            liquidity=liquidity,
            age_hours=age_hours,
            dex=pair.get("dexId", "?"),
            is_early=is_early,
            is_pumping=is_pumping,
            dexscreener_url=f"https://dexscreener.com/solana/{address}",
            pump_url=f"https://pump.fun/{address}" if "pump" in pair.get("dexId", "").lower() else None,
            birdeye_url=f"https://birdeye.so/token/{address}?chain=solana",
        )
    except:
        return None


def parse_pump_coin(coin: dict) -> Optional[MemeCoin]:
    """Parse pump.fun coin"""
    try:
        address = coin.get("mint", "")
        created = coin.get("created_timestamp")

        age_hours = None
        if created:
            age_hours = (datetime.now().timestamp() - created / 1000) / 3600

        mc = float(coin.get("usd_market_cap", 0) or 0)

        return MemeCoin(
            name=coin.get("name", "?")[:20],
            symbol=coin.get("symbol", "?")[:10],
            address=address,
            price_usd=0,  # Pump.fun doesn't give price directly
            market_cap=mc if mc > 0 else None,
            volume_24h=0,
            price_change_1h=0,
            price_change_24h=0,
            liquidity=0,
            age_hours=age_hours,
            dex="pump.fun",
            is_early=True,  # All pump.fun coins are new
            is_pumping=mc > 50000,  # Pump.fun coins with >$50k mc have traction
            dexscreener_url=f"https://dexscreener.com/solana/{address}",
            pump_url=f"https://pump.fun/{address}",
            birdeye_url=f"https://birdeye.so/token/{address}?chain=solana",
        )
    except:
        return None


async def find_trending_coins() -> list[MemeCoin]:
    """Get hot coins from DexScreener + Pump.fun"""
    coins = []
    seen = set()

    # DexScreener trending
    pairs = await get_dexscreener_trending()
    for p in pairs:
        c = parse_dexscreener_pair(p)
        if c and c.address not in seen:
            coins.append(c)
            seen.add(c.address)

    # Pump.fun new coins
    pump_coins = await get_pump_fun_coins()
    for p in pump_coins[:20]:
        c = parse_pump_coin(p)
        if c and c.address not in seen:
            coins.append(c)
            seen.add(c.address)

    # Sort: pumping early coins first, then early coins, then by volume
    # Priority: is_pumping AND is_early > is_pumping > is_early > volume
    coins.sort(key=lambda x: (
        not (x.is_pumping and x.is_early),  # Pumping + early = highest priority
        not x.is_pumping,
        not x.is_early,
        -x.volume_24h
    ))
    return coins[:15]


async def find_related_coins(sound_name: str) -> list[MemeCoin]:
    """Find coins matching a meme name"""
    import re
    name = sound_name.lower()
    name = re.sub(r'[^a-z0-9\s]', ' ', name)
    words = [w for w in name.split() if len(w) > 2][:3]

    coins = []
    seen = set()

    for word in words:
        pairs = await search_dexscreener(word)
        for p in pairs[:5]:
            c = parse_dexscreener_pair(p)
            if c and c.address not in seen and c.is_early:
                coins.append(c)
                seen.add(c.address)
        await asyncio.sleep(0.2)

    coins.sort(key=lambda x: -x.volume_24h)
    return coins[:2]


def format_coin(coin: MemeCoin, rank: int = 0) -> str:
    """Clean coin format for Telegram"""
    age = format_age(coin.age_hours)

    # Status badges
    badges = []
    if coin.is_pumping and coin.is_early:
        badges.append("FIRE")  # Best signal: new AND pumping
    elif coin.is_pumping:
        badges.append("UP")
    elif coin.is_early:
        badges.append("NEW")

    badge_str = " ".join(badges) + " " if badges else ""

    # MC formatting
    if coin.market_cap and coin.market_cap > 0:
        if coin.market_cap >= 1_000_000:
            mc = f"${coin.market_cap/1_000_000:.1f}M"
        else:
            mc = f"${coin.market_cap/1000:.0f}K"
    else:
        mc = "?"

    # Price change
    if coin.price_change_1h > 10:
        trend = "UP"
    elif coin.price_change_1h > 0:
        trend = "up"
    elif coin.price_change_1h > -10:
        trend = "FLAT"
    else:
        trend = "DOWN"

    change = f"{coin.price_change_1h:+.0f}%" if coin.price_change_1h else "-"

    # Build message
    prefix = f"{rank}. " if rank else ""
    lines = [
        f"{prefix}{badge_str}*${coin.symbol}* | {age} old | {mc}",
        f"   {trend} 1h: {change} | {coin.dex}",
    ]

    # Links - one line
    links = [f"[Dex]({coin.dexscreener_url})"]
    if coin.pump_url:
        links.append(f"[Pump]({coin.pump_url})")
    links.append(f"[Bird]({coin.birdeye_url})")
    lines.append(f"   {' | '.join(links)}")

    return "\n".join(lines)


if __name__ == "__main__":
    async def test():
        coins = await find_trending_coins()
        for i, c in enumerate(coins[:5], 1):
            print(format_coin(c, i))
            print()
    asyncio.run(test())
