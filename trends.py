"""External trend sources - Google Trends, DexScreener trending, etc."""

import asyncio
import aiohttp
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional
import json

# Optional: pytrends for Google Trends
try:
    from pytrends.request import TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    PYTRENDS_AVAILABLE = False
    print("pytrends not installed - run: pip install pytrends")


@dataclass
class TrendItem:
    """A trending term from any source."""
    term: str
    source: str  # "google", "dexscreener", "tiktok", etc.
    score: float  # Relative importance (0-100)
    related_terms: list[str]
    timestamp: datetime


# ============ GOOGLE TRENDS ============

def get_google_trends(geo: str = "US") -> list[TrendItem]:
    """Get current Google Trends - what people are searching for."""
    items = []

    # Try pytrends first
    if PYTRENDS_AVAILABLE:
        try:
            pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25), retries=2, backoff_factor=0.5)
            trending = pytrends.trending_searches(pn='united_states')

            for i, row in trending.head(20).iterrows():
                term = str(row[0])
                items.append(TrendItem(
                    term=term.lower(),
                    source="google",
                    score=100 - (i * 4),
                    related_terms=[],
                    timestamp=datetime.now()
                ))

            if items:
                return items
        except Exception:
            pass  # Fall through to RSS fallback

    # Fallback: Try Google Trends RSS feed
    try:
        import urllib.request
        url = "https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"
        with urllib.request.urlopen(url, timeout=10) as response:
            content = response.read().decode('utf-8')
            # Extract titles from RSS
            import re
            titles = re.findall(r'<title>([^<]+)</title>', content)
            for i, title in enumerate(titles[1:21]):  # Skip the feed title
                if title and len(title) > 2:
                    items.append(TrendItem(
                        term=title.lower().strip(),
                        source="google_rss",
                        score=100 - (i * 4),
                        related_terms=[],
                        timestamp=datetime.now()
                    ))
    except Exception:
        pass

    return items


def get_google_realtime_trends() -> list[TrendItem]:
    """Get Google real-time trending topics."""
    if not PYTRENDS_AVAILABLE:
        return []

    try:
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))

        # Real-time trending stories
        trending = pytrends.realtime_trending_searches(pn='US')

        items = []
        if trending is not None and len(trending) > 0:
            for i, row in trending.head(15).iterrows():
                title = str(row.get('title', row.get('entityNames', [''])[0] if 'entityNames' in row else ''))
                if title:
                    items.append(TrendItem(
                        term=title.lower(),
                        source="google_realtime",
                        score=100 - (i * 5),
                        related_terms=[],
                        timestamp=datetime.now()
                    ))

        print(f"Google Realtime: {len(items)} terms")
        return items

    except Exception as e:
        print(f"Google Realtime error: {e}")
        return []


def search_google_interest(keywords: list[str]) -> dict[str, int]:
    """Check interest level for specific keywords on Google."""
    if not PYTRENDS_AVAILABLE or not keywords:
        return {}

    try:
        pytrends = TrendReq(hl='en-US', tz=360, timeout=(10, 25))

        # Batch into groups of 5 (Google limit)
        results = {}
        for i in range(0, len(keywords), 5):
            batch = keywords[i:i+5]
            pytrends.build_payload(batch, timeframe='now 7-d', geo='US')
            interest = pytrends.interest_over_time()

            if not interest.empty:
                for kw in batch:
                    if kw in interest.columns:
                        # Average interest over the period
                        results[kw] = int(interest[kw].mean())

        return results

    except Exception as e:
        print(f"Google interest error: {e}")
        return {}


# ============ DEXSCREENER TRENDING ============

async def get_dexscreener_trending() -> list[TrendItem]:
    """Get trending pairs from DexScreener - what coins are hot."""
    try:
        async with aiohttp.ClientSession() as session:
            # DexScreener boosted/trending tokens
            url = "https://api.dexscreener.com/token-boosts/top/v1"
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

            items = []
            for i, token in enumerate(data[:20] if isinstance(data, list) else []):
                name = token.get("tokenName", "") or token.get("name", "")
                symbol = token.get("tokenSymbol", "") or token.get("symbol", "")

                if name:
                    # Add both name and symbol as trends
                    items.append(TrendItem(
                        term=name.lower(),
                        source="dexscreener",
                        score=100 - (i * 4),
                        related_terms=[symbol.lower()] if symbol else [],
                        timestamp=datetime.now()
                    ))

            print(f"DexScreener Trending: {len(items)} tokens")
            return items

    except Exception as e:
        print(f"DexScreener trending error: {e}")
        return []


async def get_dexscreener_gainers() -> list[TrendItem]:
    """Get top gaining Solana pairs - momentum plays."""
    try:
        async with aiohttp.ClientSession() as session:
            # Search for high-gaining Solana pairs
            url = "https://api.dexscreener.com/latest/dex/search?q=solana"
            async with session.get(url, timeout=15) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()

            pairs = data.get("pairs", [])

            # Filter for Solana and sort by 1h price change
            sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
            sol_pairs.sort(
                key=lambda x: float(x.get("priceChange", {}).get("h1", 0) or 0),
                reverse=True
            )

            items = []
            for i, pair in enumerate(sol_pairs[:15]):
                base = pair.get("baseToken", {})
                name = base.get("name", "")
                symbol = base.get("symbol", "")
                change = float(pair.get("priceChange", {}).get("h1", 0) or 0)

                if name and change > 10:  # Only if +10% in 1h
                    items.append(TrendItem(
                        term=name.lower(),
                        source="dexscreener_gainer",
                        score=min(100, change),  # Score = % gain
                        related_terms=[symbol.lower()] if symbol else [],
                        timestamp=datetime.now()
                    ))

            print(f"DexScreener Gainers: {len(items)} tokens")
            return items

    except Exception as e:
        print(f"DexScreener gainers error: {e}")
        return []


# ============ COMBINED TRENDS ============

async def get_all_trends() -> list[TrendItem]:
    """Aggregate trends from all sources."""
    all_trends = []

    # Google Trends (sync)
    all_trends.extend(get_google_trends())

    # DexScreener (async)
    dex_trending, dex_gainers = await asyncio.gather(
        get_dexscreener_trending(),
        get_dexscreener_gainers(),
        return_exceptions=True
    )

    if isinstance(dex_trending, list):
        all_trends.extend(dex_trending)
    if isinstance(dex_gainers, list):
        all_trends.extend(dex_gainers)

    print(f"Total trends: {len(all_trends)}")
    return all_trends


def extract_search_terms(trends: list[TrendItem]) -> list[str]:
    """Extract unique search terms from trends for coin searching."""
    terms = set()

    for t in trends:
        # Add main term
        term = t.term.strip()
        if len(term) >= 3:  # Skip very short terms
            terms.add(term)

        # Add related terms
        for r in t.related_terms:
            if len(r) >= 3:
                terms.add(r)

    return list(terms)


def get_trending_keywords() -> list[str]:
    """Quick sync function to get trending keywords for coin search."""
    keywords = []

    # Google Trends
    google = get_google_trends()
    keywords.extend([t.term for t in google[:10]])

    return keywords


async def get_hot_terms() -> list[str]:
    """Get combined hot terms from all sources for searching."""
    trends = await get_all_trends()

    # Deduplicate and sort by score
    term_scores = {}
    for t in trends:
        key = t.term.lower()
        if key in term_scores:
            term_scores[key] = max(term_scores[key], t.score)
        else:
            term_scores[key] = t.score

    # Sort by score and return top terms
    sorted_terms = sorted(term_scores.items(), key=lambda x: x[1], reverse=True)
    return [t[0] for t in sorted_terms[:30]]


# ============ CROSS-REFERENCE ============

def cross_reference_trends(tiktok_trends: list[str], external_trends: list[TrendItem]) -> list[dict]:
    """
    Find TikTok trends that match external trends.
    These are HIGH SIGNAL - trending on multiple platforms.
    """
    matches = []

    external_terms = {t.term.lower() for t in external_trends}
    external_terms.update({r.lower() for t in external_trends for r in t.related_terms})

    for tiktok in tiktok_trends:
        tiktok_lower = tiktok.lower()

        # Check for exact or partial match
        for ext in external_terms:
            if ext in tiktok_lower or tiktok_lower in ext:
                # Find the source
                source = next(
                    (t.source for t in external_trends if t.term.lower() == ext or ext in [r.lower() for r in t.related_terms]),
                    "external"
                )
                matches.append({
                    "term": tiktok,
                    "matched_with": ext,
                    "source": source,
                    "signal": "HIGH"  # Multi-platform = strong signal
                })
                break

    print(f"Cross-referenced: {len(matches)} multi-platform trends")
    return matches


if __name__ == "__main__":
    # Test
    async def test():
        print("\n=== Testing Trend Sources ===\n")

        # Google Trends
        print("Google Trends:")
        google = get_google_trends()
        for t in google[:5]:
            print(f"  - {t.term} ({t.score})")

        # DexScreener
        print("\nDexScreener Trending:")
        dex = await get_dexscreener_trending()
        for t in dex[:5]:
            print(f"  - {t.term} ({t.score})")

        # Gainers
        print("\nDexScreener Gainers:")
        gainers = await get_dexscreener_gainers()
        for t in gainers[:5]:
            print(f"  - {t.term} (+{t.score:.0f}%)")

        # Combined
        print("\nHot Terms:")
        hot = await get_hot_terms()
        print(f"  {', '.join(hot[:10])}")

    asyncio.run(test())
