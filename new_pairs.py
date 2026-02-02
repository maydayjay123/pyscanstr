"""Track brand new pairs - truly fresh coins launching every minute."""

import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from config import MIN_LIQUIDITY, ENTRY_MIN_MC, ENTRY_MAX_MC


@dataclass
class FreshCoin:
    """A fresh coin with rating and safety checks."""
    name: str
    symbol: str
    address: str
    age_minutes: int
    price_usd: float
    market_cap: Optional[float]
    liquidity: float
    volume_5m: float
    volume_1h: float
    price_change_5m: float
    price_change_1h: float
    buys_5m: int
    sells_5m: int
    dex: str

    # Ratings (0-100)
    momentum_score: int
    liquidity_score: int
    activity_score: int
    tiktok_score: int
    total_score: int

    # Flags
    is_migrated: bool
    has_tiktok_match: bool
    matched_trend: str

    # Links
    dexscreener_url: str
    pump_url: Optional[str]
    birdeye_url: str

    # Safety checks
    lp_locked: Optional[bool] = None
    lp_lock_pct: float = 0.0
    has_bundles: Optional[bool] = None
    top_holder_pct: float = 0.0
    holder_count: int = 0

    # Price data
    price_high_1h: float = 0.0
    price_low_1h: float = 0.0
    ath: float = 0.0

    # Socials
    has_twitter: bool = False
    has_website: bool = False
    has_telegram: bool = False
    twitter_url: str = ""

    # Safety score
    safety_score: int = 0
    safety_warnings: list = None

    # Trade signals
    is_recovering: bool = False  # Volume recovery from bottom
    is_dumping: bool = False     # Currently dumping
    is_pumped: bool = False      # Already pumped = LATE entry
    is_early: bool = False       # True early entry opportunity
    good_entry: bool = False     # Meets entry criteria
    entry_reason: str = ""
    target_mc: float = 0.0       # Target market cap

    def __post_init__(self):
        if self.safety_warnings is None:
            self.safety_warnings = []


async def fetch_json(url: str, session: Optional[aiohttp.ClientSession] = None,
                     timeout: int = 15, retries: int = 2) -> Optional[dict | list]:
    try:
        if session is None:
            async with aiohttp.ClientSession() as temp_session:
                return await fetch_json(url, session=temp_session, timeout=timeout, retries=retries)

        for attempt in range(retries + 1):
            try:
                async with session.get(url, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception:
                if attempt < retries:
                    await asyncio.sleep(0.4 * (2 ** attempt))
                else:
                    return None
    except Exception:
        return None
    return None


def select_best_pair(pairs: list[dict]) -> Optional[dict]:
    """Pick the most reliable pair (highest liquidity, then volume)."""
    if not pairs:
        return None

    def score(p: dict) -> tuple[float, float]:
        liq = float(p.get("liquidity", {}).get("usd", 0) or 0)
        vol = float(p.get("volume", {}).get("h1", 0) or 0)
        return (liq, vol)

    return max(pairs, key=score)


# ============ SAFETY CHECKS ============

async def get_holder_data_from_rugcheck(address: str, session: Optional[aiohttp.ClientSession] = None) -> dict:
    """Get holder data from RugCheck - the most reliable free source."""
    result = {"holder_count": 0, "top_holder_pct": 0.0, "has_bundles": False, "top1_pct": 0.0}

    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{address}/report"
        data = await fetch_json(url, session=session, timeout=12)

        if data:
            # Top holders - RugCheck provides this reliably
            top_holders = data.get("topHolders", [])
            if top_holders:
                # Calculate top 5 and top 1 percentages
                top5_pct = sum(float(h.get("pct", 0) or 0) for h in top_holders[:5])
                top1_pct = float(top_holders[0].get("pct", 0) or 0) if top_holders else 0

                result["top_holder_pct"] = top5_pct
                result["top1_pct"] = top1_pct

                # Bundles = suspicious if top holder has >20% or top5 has >50%
                result["has_bundles"] = top1_pct > 20 or top5_pct > 50

                # Estimate holder count from distribution
                # If top 20 holders have less than 80%, there are likely 100+ holders
                top20_pct = sum(float(h.get("pct", 0) or 0) for h in top_holders[:20])
                if top20_pct < 50:
                    result["holder_count"] = 500  # Healthy distribution
                elif top20_pct < 70:
                    result["holder_count"] = 200
                elif top20_pct < 85:
                    result["holder_count"] = 100
                else:
                    result["holder_count"] = 50  # Concentrated

            # Direct holder count if available
            if data.get("holderCount"):
                result["holder_count"] = data.get("holderCount")

    except:
        pass

    return result


async def check_rugcheck(address: str, session: Optional[aiohttp.ClientSession] = None) -> dict:
    """Check token safety via RugCheck API."""
    result = {
        "lp_locked": None,
        "lp_lock_pct": 0.0,
        "has_bundles": None,
        "top_holder_pct": 0.0,
        "holder_count": 0,
        "safety_score": 50,
        "warnings": [],
        "top1_pct": 0.0
    }

    try:
        # RugCheck API - single call for everything
        url = f"https://api.rugcheck.xyz/v1/tokens/{address}/report"
        data = await fetch_json(url, session=session, timeout=12)

        if data:
            # LP Status
            markets = data.get("markets", [])
            if markets:
                lp = markets[0].get("lp", {})
                lp_locked_pct = float(lp.get("lpLockedPct", 0) or 0)
                result["lp_lock_pct"] = lp_locked_pct
                result["lp_locked"] = lp_locked_pct > 50

                if lp_locked_pct < 30:
                    result["warnings"].append("LP not locked")

            # Top holders - main source of holder data
            top_holders = data.get("topHolders", [])
            if top_holders:
                top5_pct = sum(float(h.get("pct", 0) or 0) for h in top_holders[:5])
                top1_pct = float(top_holders[0].get("pct", 0) or 0) if top_holders else 0
                top20_pct = sum(float(h.get("pct", 0) or 0) for h in top_holders[:20])

                result["top_holder_pct"] = top5_pct
                result["top1_pct"] = top1_pct

                # Bundles detection
                if top1_pct > 20:
                    result["warnings"].append(f"Top1 holds {top1_pct:.0f}%")
                    result["has_bundles"] = True
                elif top5_pct > 50:
                    result["warnings"].append(f"Top5 hold {top5_pct:.0f}%")
                    result["has_bundles"] = True
                else:
                    result["has_bundles"] = False

                # Estimate holder count from distribution
                if top20_pct < 50:
                    result["holder_count"] = 500
                elif top20_pct < 70:
                    result["holder_count"] = 200
                elif top20_pct < 85:
                    result["holder_count"] = 100
                else:
                    result["holder_count"] = 50

            # Direct holder count if available
            if data.get("holderCount"):
                result["holder_count"] = data.get("holderCount")

            if result["holder_count"] < 50:
                result["warnings"].append(f"Low holders")

            # Risk score from RugCheck
            risks = data.get("risks", [])
            if risks:
                high_risks = [r for r in risks if r.get("level") == "danger"]
                if high_risks:
                    result["warnings"].append(f"{len(high_risks)} high risks")
                    result["safety_score"] -= len(high_risks) * 15

            # Calculate safety score
            score = 50
            if result["lp_locked"]:
                score += 20
            if not result["has_bundles"]:
                score += 15
            if result["holder_count"] >= 100:
                score += 15
            elif result["holder_count"] >= 50:
                score += 10

            result["safety_score"] = min(100, max(0, score))

    except Exception as e:
        result["warnings"].append("Check failed")

    return result


async def get_token_socials(address: str, session: Optional[aiohttp.ClientSession] = None) -> dict:
    """Get token social links from DexScreener."""
    result = {
        "has_twitter": False,
        "has_website": False,
        "has_telegram": False,
        "twitter_url": ""
    }

    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
        data = await fetch_json(url, session=session, timeout=10)

        if data and "pairs" in data and data["pairs"]:
            pair = data["pairs"][0]
            info = pair.get("info", {})
            socials = info.get("socials", [])
            websites = info.get("websites", [])

            for s in socials:
                stype = s.get("type", "").lower()
                url = s.get("url", "")
                if "twitter" in stype or "x.com" in url:
                    result["has_twitter"] = True
                    result["twitter_url"] = url
                elif "telegram" in stype:
                    result["has_telegram"] = True

            if websites:
                result["has_website"] = True

    except:
        pass

    return result


def analyze_trade_signal(coin) -> dict:
    """
    Analyze if coin is a good entry based on criteria:
    - Entry at 12K-120K MC (not too high)
    - NOT already pumped (if MC > 500K for young coin = late)
    - NOT at ATH - should be recovering from a DIP
    - Volume recovery indicates buyers coming back
    """
    result = {
        "is_recovering": False,
        "is_dumping": False,
        "is_pumped": False,  # Already pumped = late entry
        "is_early": False,   # True early entry
        "good_entry": False,
        "entry_reason": "",
        "target_mc": 0.0,
        "sell_targets": []
    }

    mc = coin.market_cap or 0
    change_5m = coin.price_change_5m
    change_1h = coin.price_change_1h
    vol_5m = coin.volume_5m
    buys = coin.buys_5m
    sells = coin.sells_5m
    age = coin.age_minutes

    # ============ DETECT ALREADY PUMPED (LATE ENTRY) ============
    # If MC is high relative to age, it's already pumped
    # Normal progression: ~$20K at launch, grows over hours

    if mc > 1_000_000:
        # Over $1M = definitely late
        result["is_pumped"] = True
        result["entry_reason"] = f"LATE - already ${mc/1_000_000:.1f}M"
        return result

    if mc > 500_000 and age < 60:
        # $500K+ in under 1 hour = already pumped hard
        result["is_pumped"] = True
        result["entry_reason"] = f"PUMPED - ${mc/1000:.0f}K in {age}m"
        return result

    if mc > 200_000 and age < 30:
        # $200K+ in under 30 min = already pumped
        result["is_pumped"] = True
        result["entry_reason"] = f"PUMPED - ${mc/1000:.0f}K in {age}m"
        return result

    # If 1h change is huge positive, it's at/near ATH - not a dip
    if change_1h > 100:
        result["is_pumped"] = True
        result["entry_reason"] = f"AT ATH +{change_1h:.0f}% 1h"
        return result

    # ============ DETECT DUMPING ============
    if change_5m < -15 and sells > buys:
        result["is_dumping"] = True
        result["entry_reason"] = "DUMP - avoid"
        return result

    if change_1h < -50:
        result["is_dumping"] = True
        result["entry_reason"] = f"CRASHED {change_1h:.0f}% 1h"
        return result

    # ============ DETECT TRUE EARLY ENTRY ============
    # Early = low MC, young age, not pumped yet
    in_range = ENTRY_MIN_MC <= mc <= ENTRY_MAX_MC
    is_young = age < 60  # Under 1 hour old

    if mc < 50000 and is_young:
        result["is_early"] = True

    # ============ DETECT REAL RECOVERY (from a dip, not ATH) ============
    # Recovery = was down, now coming back up with volume
    # NOT recovery if already at ATH (change_1h very positive)

    is_recovering = False
    if change_5m > 0 and change_1h < 50 and buys > sells:
        # Going up but NOT at ATH (1h change not crazy high)
        is_recovering = True

    if change_5m > 5 and change_1h < 0 and buys >= sells:
        # Was down on 1h but recovering on 5m = true dip recovery
        is_recovering = True
        result["entry_reason"] = "DIP RECOVERY"

    result["is_recovering"] = is_recovering

    # ============ GOOD ENTRY CRITERIA ============
    reasons = []

    if in_range:
        reasons.append(f"MC ${mc/1000:.0f}K")

    if result["is_early"]:
        reasons.append("EARLY")

    if is_recovering and change_1h < 0:
        reasons.append("dip recovery")
    elif is_recovering:
        reasons.append("momentum")

    if buys > sells:
        ratio = buys / max(1, sells)
        if ratio > 1.5:
            reasons.append(f"buy pressure {ratio:.1f}x")

    if coin.lp_locked:
        reasons.append("LP locked")

    if coin.has_twitter:
        reasons.append("has X")

    # Calculate good entry
    if in_range and result["is_recovering"] and buys >= sells:
        result["good_entry"] = True
        result["entry_reason"] = " | ".join(reasons)

        # Set targets based on entry MC
        # Target: ~8x from entry for micro caps, less for larger
        if mc < 30000:
            result["target_mc"] = 200000  # 6-8x
            result["sell_targets"] = [
                {"mc": 50000, "pct": 25, "label": "2x - take 25%"},
                {"mc": 100000, "pct": 25, "label": "3x - take 25%"},
                {"mc": 150000, "pct": 25, "label": "5x - take 25%"},
                {"mc": 200000, "pct": 25, "label": "8x - moon bag"}
            ]
        elif mc < 60000:
            result["target_mc"] = 150000  # 2-3x
            result["sell_targets"] = [
                {"mc": 80000, "pct": 33, "label": "1.5x - take 33%"},
                {"mc": 120000, "pct": 33, "label": "2x - take 33%"},
                {"mc": 150000, "pct": 34, "label": "2.5x - moon bag"}
            ]
        else:
            result["target_mc"] = 100000  # Quick flip
            result["sell_targets"] = [
                {"mc": 100000, "pct": 50, "label": "1.5x - take 50%"},
                {"mc": 150000, "pct": 50, "label": "2x - exit"}
            ]
    else:
        if not in_range:
            result["entry_reason"] = f"MC ${mc/1000:.0f}K outside range"
        elif result["is_dumping"]:
            result["entry_reason"] = "dumping"
        elif sells > buys:
            result["entry_reason"] = "sell pressure"
        else:
            result["entry_reason"] = "weak momentum"

    return result


async def get_price_history(pair: dict) -> dict:
    """Extract price high/low from pair data."""
    result = {
        "price_high_1h": 0.0,
        "price_low_1h": 0.0,
        "ath": 0.0
    }

    try:
        price_usd = float(pair.get("priceUsd", 0) or 0)
        change_1h = float(pair.get("priceChange", {}).get("h1", 0) or 0)

        if price_usd > 0 and change_1h != 0:
            # Estimate high/low from current price and change
            if change_1h > 0:
                result["price_low_1h"] = price_usd / (1 + change_1h / 100)
                result["price_high_1h"] = price_usd
            else:
                result["price_high_1h"] = price_usd / (1 + change_1h / 100)
                result["price_low_1h"] = price_usd

            result["ath"] = result["price_high_1h"]

    except:
        pass

    return result


async def get_latest_pairs_dexscreener(session: aiohttp.ClientSession) -> list[dict]:
    """Get latest Solana pairs from DexScreener token profiles."""
    pairs = []

    # 1. Token boosts - actively promoted new tokens
    boosts = await fetch_json("https://api.dexscreener.com/token-boosts/latest/v1", session=session)
    if boosts and isinstance(boosts, list):
        for token in boosts[:30]:
            if token.get("chainId") == "solana":
                addr = token.get("tokenAddress", "")
                if addr:
                    # Get pair data for this token
                    pair_data = await fetch_json(
                        f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                        session=session
                    )
                    if pair_data and "pairs" in pair_data and pair_data["pairs"]:
                        best_pair = select_best_pair(pair_data["pairs"])
                        if best_pair:
                            pairs.append(best_pair)
                    await asyncio.sleep(0.05)

    # 2. Latest token profiles
    profiles = await fetch_json("https://api.dexscreener.com/token-profiles/latest/v1", session=session)
    if profiles and isinstance(profiles, list):
        for token in profiles[:20]:
            if token.get("chainId") == "solana":
                addr = token.get("tokenAddress", "")
                if addr and not any(p.get("baseToken", {}).get("address") == addr for p in pairs):
                    pair_data = await fetch_json(
                        f"https://api.dexscreener.com/latest/dex/tokens/{addr}",
                        session=session
                    )
                    if pair_data and "pairs" in pair_data and pair_data["pairs"]:
                        best_pair = select_best_pair(pair_data["pairs"])
                        if best_pair:
                            pairs.append(best_pair)
                    await asyncio.sleep(0.05)

    return pairs


async def get_pump_fun_new(session: aiohttp.ClientSession) -> list[dict]:
    """Get newest coins from pump.fun - these are truly fresh."""
    # Newest coins sorted by creation
    url = "https://frontend-api.pump.fun/coins?offset=0&limit=50&sort=created_timestamp&order=DESC&includeNsfw=false"
    data = await fetch_json(url, session=session, timeout=15)

    if not data or not isinstance(data, list):
        return []

    return data


async def get_pump_fun_graduating(session: aiohttp.ClientSession) -> list[dict]:
    """Get coins about to graduate/migrate from pump.fun."""
    # Coins close to bonding curve completion
    url = "https://frontend-api.pump.fun/coins?offset=0&limit=30&sort=market_cap&order=DESC&includeNsfw=false"
    data = await fetch_json(url, session=session, timeout=15)

    if not data or not isinstance(data, list):
        return []

    # Filter for coins with high market cap (close to graduating)
    graduating = [c for c in data if float(c.get("usd_market_cap", 0) or 0) > 30000]
    return graduating


def get_tiktok_trends() -> list[str]:
    """Get current TikTok trending terms from database."""
    try:
        import database as db
        db.init_db()
        tags = db.get_rising_hashtags(hours=12, limit=20)
        words = db.get_rising_keywords(hours=12, limit=20)
        return [t.lower() for t in tags + words]
    except:
        return []


def get_google_trends() -> list[str]:
    """Get Google trending terms."""
    try:
        from trends import get_google_trends as fetch_google
        items = fetch_google()
        if items:
            return [t.term.lower() for t in items[:15]]
    except:
        pass
    # Fallback: common meme coin terms that trend frequently
    return [
        "trump", "elon", "musk", "doge", "pepe", "wojak", "chad",
        "maga", "cat", "dog", "ai", "gpt", "agi", "based", "moon",
        "frog", "ape", "shib", "bonk", "wif", "popcat", "moodeng"
    ]


def check_trend_match(name: str, symbol: str, trends: list[str]) -> tuple[bool, str, int]:
    """Check if coin matches any trend (TikTok or Google)."""
    name_lower = name.lower()
    symbol_lower = symbol.lower()

    for trend in trends:
        trend_lower = trend.lower()
        # Direct match
        if trend_lower in name_lower or trend_lower in symbol_lower:
            return True, trend, 35
        if name_lower in trend_lower or symbol_lower in trend_lower:
            return True, trend, 30
        # Partial match
        if len(trend_lower) >= 4 and len(name_lower) >= 4:
            if trend_lower[:4] in name_lower or name_lower[:4] in trend_lower:
                return True, trend, 20

    return False, "", 0


def calculate_scores(pair: dict, trends: list[str]) -> dict:
    """Calculate rating scores for a pair."""
    base = pair.get("baseToken", {})
    name = base.get("name", "")
    symbol = base.get("symbol", "")

    price_change = pair.get("priceChange", {})
    txns = pair.get("txns", {})
    volume = pair.get("volume", {})

    change_5m = float(price_change.get("m5", 0) or 0)
    vol_5m = float(volume.get("m5", 0) or 0)
    vol_1h = float(volume.get("h1", 0) or 0)
    liquidity = float(pair.get("liquidity", {}).get("usd", 0) or 0)

    buys_5m = txns.get("m5", {}).get("buys", 0) or 0
    sells_5m = txns.get("m5", {}).get("sells", 0) or 0

    # Momentum (0-100)
    momentum = 0
    if change_5m > 100:
        momentum = 100
    elif change_5m > 50:
        momentum = 85
    elif change_5m > 20:
        momentum = 70
    elif change_5m > 10:
        momentum = 55
    elif change_5m > 5:
        momentum = 40
    elif change_5m > 0:
        momentum = 25

    if vol_5m > 20000:
        momentum = min(100, momentum + 15)

    # Liquidity (0-100)
    liq_score = 0
    if liquidity >= 50000:
        liq_score = 100
    elif liquidity >= 20000:
        liq_score = 75
    elif liquidity >= 10000:
        liq_score = 50
    elif liquidity >= 5000:
        liq_score = 30
    elif liquidity >= 2000:
        liq_score = 15

    # Activity (0-100)
    total_txns = buys_5m + sells_5m
    activity = 0
    if total_txns >= 50:
        activity = 100
    elif total_txns >= 30:
        activity = 80
    elif total_txns >= 15:
        activity = 60
    elif total_txns >= 8:
        activity = 40
    elif total_txns >= 3:
        activity = 20

    # Buy pressure bonus
    if total_txns > 0 and buys_5m / total_txns > 0.65:
        activity = min(100, activity + 15)

    # Trend match
    has_match, matched_trend, trend_score = check_trend_match(name, symbol, trends)

    # Total (weighted)
    total = int(
        momentum * 0.30 +
        liq_score * 0.20 +
        activity * 0.25 +
        trend_score * 0.83  # Scale 35 max to ~25 contribution
    )

    return {
        "momentum_score": momentum,
        "liquidity_score": liq_score,
        "activity_score": activity,
        "tiktok_score": trend_score,
        "total_score": min(100, total),
        "has_tiktok_match": has_match,
        "matched_trend": matched_trend,
    }


def parse_dexscreener_pair(pair: dict, trends: list[str]) -> Optional[FreshCoin]:
    """Parse DexScreener pair."""
    try:
        if pair.get("chainId") != "solana":
            return None

        base = pair.get("baseToken", {})
        address = base.get("address", "")
        if not address:
            return None

        price_change = pair.get("priceChange", {})
        txns = pair.get("txns", {})
        volume = pair.get("volume", {})

        # Calculate age
        age_minutes = 9999
        created = pair.get("pairCreatedAt", 0)
        if created:
            age_minutes = int((datetime.now().timestamp() * 1000 - created) / (1000 * 60))

        liquidity = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        if liquidity < MIN_LIQUIDITY:
            return None

        scores = calculate_scores(pair, trends)

        return FreshCoin(
            name=base.get("name", "?")[:20],
            symbol=base.get("symbol", "?")[:10],
            address=address,
            age_minutes=age_minutes,
            price_usd=float(pair.get("priceUsd", 0) or 0),
            market_cap=float(pair.get("marketCap") or 0) if pair.get("marketCap") else None,
            liquidity=liquidity,
            volume_5m=float(volume.get("m5", 0) or 0),
            volume_1h=float(volume.get("h1", 0) or 0),
            price_change_5m=float(price_change.get("m5", 0) or 0),
            price_change_1h=float(price_change.get("h1", 0) or 0),
            buys_5m=txns.get("m5", {}).get("buys", 0) or 0,
            sells_5m=txns.get("m5", {}).get("sells", 0) or 0,
            dex=pair.get("dexId", "?"),
            momentum_score=scores["momentum_score"],
            liquidity_score=scores["liquidity_score"],
            activity_score=scores["activity_score"],
            tiktok_score=scores["tiktok_score"],
            total_score=scores["total_score"],
            is_migrated=False,
            has_tiktok_match=scores["has_tiktok_match"],
            matched_trend=scores["matched_trend"],
            dexscreener_url=f"https://dexscreener.com/solana/{address}",
            pump_url=f"https://pump.fun/{address}" if "pump" in pair.get("dexId", "").lower() else None,
            birdeye_url=f"https://birdeye.so/token/{address}?chain=solana",
        )
    except:
        return None


def parse_pump_coin(coin: dict, trends: list[str]) -> Optional[FreshCoin]:
    """Parse pump.fun coin."""
    try:
        address = coin.get("mint", "")
        if not address:
            return None

        name = coin.get("name", "?")[:20]
        symbol = coin.get("symbol", "?")[:10]

        # Age
        created = coin.get("created_timestamp", 0)
        age_minutes = 9999
        if created:
            age_minutes = int((datetime.now().timestamp() * 1000 - created) / (1000 * 60))

        mc = float(coin.get("usd_market_cap", 0) or 0)

        # Check trend match
        has_match, matched_trend, trend_score = check_trend_match(name, symbol, trends)

        # Simple scores for pump.fun (no detailed metrics)
        momentum = 50 if mc > 10000 else 25
        liq_score = 30 if mc > 20000 else 15
        activity = 40  # Assume active if listed

        total = int(momentum * 0.30 + liq_score * 0.20 + activity * 0.25 + trend_score * 0.83)

        return FreshCoin(
            name=name,
            symbol=symbol,
            address=address,
            age_minutes=age_minutes,
            price_usd=0,
            market_cap=mc if mc > 0 else None,
            liquidity=mc * 0.1 if mc > 0 else 0,  # Estimate
            volume_5m=0,
            volume_1h=0,
            price_change_5m=0,
            price_change_1h=0,
            buys_5m=0,
            sells_5m=0,
            dex="pump.fun",
            momentum_score=momentum,
            liquidity_score=liq_score,
            activity_score=activity,
            tiktok_score=trend_score,
            total_score=min(100, total),
            is_migrated=bool(coin.get("raydium_pool")),
            has_tiktok_match=has_match,
            matched_trend=matched_trend,
            dexscreener_url=f"https://dexscreener.com/solana/{address}",
            pump_url=f"https://pump.fun/{address}",
            birdeye_url=f"https://birdeye.so/token/{address}?chain=solana",
        )
    except:
        return None


async def enrich_coin_safety(coin: FreshCoin, session: Optional[aiohttp.ClientSession] = None) -> FreshCoin:
    """Add safety checks and trade signals to a coin."""
    try:
        # Run checks in parallel
        safety_task = check_rugcheck(coin.address, session=session)
        social_task = get_token_socials(coin.address, session=session)

        safety, socials = await asyncio.gather(safety_task, social_task, return_exceptions=True)

        # Apply safety data
        if isinstance(safety, dict):
            coin.lp_locked = safety.get("lp_locked")
            coin.lp_lock_pct = safety.get("lp_lock_pct", 0)
            coin.has_bundles = safety.get("has_bundles")
            coin.top_holder_pct = safety.get("top_holder_pct", 0)
            coin.holder_count = safety.get("holder_count", 0)
            coin.safety_score = safety.get("safety_score", 50)
            coin.safety_warnings = safety.get("warnings", [])

        # Apply social data
        if isinstance(socials, dict):
            coin.has_twitter = socials.get("has_twitter", False)
            coin.has_website = socials.get("has_website", False)
            coin.has_telegram = socials.get("has_telegram", False)
            coin.twitter_url = socials.get("twitter_url", "")

        # Boost score if socials present
        if coin.has_twitter:
            coin.total_score = min(100, coin.total_score + 5)
        if coin.has_website:
            coin.total_score = min(100, coin.total_score + 3)

        # Analyze trade signals
        trade_signal = analyze_trade_signal(coin)
        coin.is_recovering = trade_signal["is_recovering"]
        coin.is_dumping = trade_signal["is_dumping"]
        coin.is_pumped = trade_signal.get("is_pumped", False)
        coin.is_early = trade_signal.get("is_early", False)
        coin.good_entry = trade_signal["good_entry"]
        coin.entry_reason = trade_signal["entry_reason"]
        coin.target_mc = trade_signal["target_mc"]

    except:
        pass

    return coin


async def get_fresh_coins(max_age_minutes: int = 60, run_safety_checks: bool = True) -> list[FreshCoin]:
    """Get fresh coins from all sources with trend matching and safety checks."""
    # Get trends from TikTok DB + Google
    trends = get_tiktok_trends() + get_google_trends()
    trends = list(set(trends))  # Dedupe

    coins = []
    seen = set()

    async with aiohttp.ClientSession() as session:
        # 1. DexScreener latest (boosted/profiles)
        dex_pairs = await get_latest_pairs_dexscreener(session)
        for pair in dex_pairs:
            coin = parse_dexscreener_pair(pair, trends)
            if coin and coin.address not in seen:
                if coin.age_minutes <= max_age_minutes * 2:  # Allow slightly older
                    # Get price history from pair
                    price_hist = await get_price_history(pair)
                    coin.price_high_1h = price_hist["price_high_1h"]
                    coin.price_low_1h = price_hist["price_low_1h"]
                    coin.ath = price_hist["ath"]
                    coins.append(coin)
                    seen.add(coin.address)

        # 2. Pump.fun newest
        pump_new = await get_pump_fun_new(session)
        for p in pump_new[:30]:
            coin = parse_pump_coin(p, trends)
            if coin and coin.address not in seen:
                if coin.age_minutes <= max_age_minutes:
                    coins.append(coin)
                    seen.add(coin.address)

        # 3. Pump.fun graduating (high MC)
        pump_grad = await get_pump_fun_graduating(session)
        for p in pump_grad[:15]:
            coin = parse_pump_coin(p, trends)
            if coin and coin.address not in seen:
                coin.is_migrated = True
                coins.append(coin)
                seen.add(coin.address)

    # Sort: trend matches first, then by score
    coins.sort(key=lambda x: (not x.has_tiktok_match, -x.total_score))

    # Take top 20 and run safety checks
    top_coins = coins[:20]

    if run_safety_checks and top_coins:
        # Run safety checks on top coins (limit to avoid rate limits)
        check_tasks = [enrich_coin_safety(c, session=session) for c in top_coins[:10]]
        enriched = await asyncio.gather(*check_tasks, return_exceptions=True)

        for i, result in enumerate(enriched):
            if isinstance(result, FreshCoin):
                top_coins[i] = result

    return top_coins


def format_rating(score: int) -> str:
    if score >= 80:
        return "A"
    elif score >= 60:
        return "B"
    elif score >= 40:
        return "C"
    elif score >= 20:
        return "D"
    return "F"


def md_safe(text: str) -> str:
    """Escape Markdown v1 special chars for Telegram."""
    if not text:
        return ""
    safe = text.replace("\\", "\\\\")
    for ch in ("_", "*", "[", "]", "(", ")", "`"):
        safe = safe.replace(ch, f"\\{ch}")
    return safe


def format_fresh_coin(coin: FreshCoin, rank: int = 0) -> str:
    """Format coin for display with CA, chart link, and safety info."""
    if coin.age_minutes < 60:
        age = f"{coin.age_minutes}m"
    elif coin.age_minutes < 1440:
        age = f"{coin.age_minutes // 60}h"
    else:
        age = f"{coin.age_minutes // 1440}d"

    rating = format_rating(coin.total_score)

    # Trend indicator
    trend = ""
    if coin.has_tiktok_match:
        trend = f" TT:{md_safe(coin.matched_trend[:8])}"

    # Migrated indicator
    migrated = " [GRAD]" if coin.is_migrated else ""

    # MC
    mc = "?"
    if coin.market_cap and coin.market_cap > 0:
        if coin.market_cap >= 1_000_000:
            mc = f"${coin.market_cap/1_000_000:.1f}M"
        else:
            mc = f"${coin.market_cap/1000:.0f}K"

    prefix = f"{rank}. " if rank else ""

    safe_symbol = md_safe(coin.symbol)

    # Safety indicators
    safety_icons = []
    if coin.lp_locked is True:
        safety_icons.append("LP")
    elif coin.lp_locked is False:
        safety_icons.append("!LP")

    if coin.has_bundles is True:
        safety_icons.append("!B")  # Warning: bundles
    elif coin.has_bundles is False:
        safety_icons.append("OK")

    if coin.has_twitter:
        safety_icons.append("X")
    if coin.has_website:
        safety_icons.append("W")

    safety_str = " ".join(safety_icons) if safety_icons else ""

    # Holders info
    holders = ""
    if coin.holder_count > 0:
        holders = f" | {coin.holder_count} holders"
        if coin.top_holder_pct > 30:
            holders += f" (top5: {coin.top_holder_pct:.0f}%)"

    # Price range (high/low)
    price_range = ""
    if coin.price_high_1h > 0 and coin.price_low_1h > 0:
        price_range = f" | H: ${coin.price_high_1h:.6f} L: ${coin.price_low_1h:.6f}"

    # Trade signal - show the REAL picture
    trade_signal = ""
    if hasattr(coin, 'is_pumped') and coin.is_pumped:
        trade_signal = f"\n   *LATE* - {md_safe(coin.entry_reason)}"
    elif hasattr(coin, 'is_early') and coin.is_early and coin.good_entry:
        target = f"${coin.target_mc/1000:.0f}K" if coin.target_mc else ""
        trade_signal = f"\n   *EARLY ENTRY* {md_safe(coin.entry_reason)}"
        if target:
            trade_signal += f" | Target: {target}"
    elif coin.good_entry:
        target = f"${coin.target_mc/1000:.0f}K" if coin.target_mc else ""
        trade_signal = f"\n   *ENTRY* {md_safe(coin.entry_reason)}"
        if target:
            trade_signal += f" | Target: {target}"
    elif coin.is_dumping:
        trade_signal = f"\n   *DUMP* - avoid"
    elif coin.is_recovering:
        # Only show recovering if NOT already pumped
        mc = coin.market_cap or 0
        if mc < 200000:
            trade_signal = f"\n   Recovering"
        else:
            trade_signal = f"\n   *LATE* - MC ${mc/1000:.0f}K"

    lines = [
        f"{prefix}[{rating}] *${safe_symbol}* {age} {mc}{migrated}",
        f"   +{coin.price_change_5m:.0f}% | Liq ${coin.liquidity/1000:.0f}K{trend}",
        f"   [{safety_str}]{holders}",
        f"   `{coin.address.replace('`','')}`",
        f"   [Chart]({coin.dexscreener_url})",
    ]

    # Add trade signal
    if trade_signal:
        lines.insert(3, trade_signal)

    # Add warnings if any (but not if good entry - don't clutter)
    if coin.safety_warnings and not coin.good_entry:
        warnings = ", ".join(md_safe(w) for w in coin.safety_warnings[:3])
        lines.insert(3, f"   *Warns:* {warnings}")

    return "\n".join(lines)


if __name__ == "__main__":
    async def test():
        print("Fetching fresh coins with safety checks...")
        coins = await get_fresh_coins(120, run_safety_checks=True)
        print(f"\nFound {len(coins)} coins:\n")

        tt = [c for c in coins if c.has_tiktok_match]
        print(f"TikTok matches: {len(tt)}")

        for i, c in enumerate(coins[:10], 1):
            print(format_fresh_coin(c, i))
            print(f"   Scores: M{c.momentum_score} L{c.liquidity_score} A{c.activity_score} T{c.tiktok_score}")
            print(f"   Safety: {c.safety_score}/100 | LP: {c.lp_lock_pct:.0f}% | Bundles: {c.has_bundles}")
            print()

    asyncio.run(test())
