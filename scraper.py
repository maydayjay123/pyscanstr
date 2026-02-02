"""TikTok scraper - trending videos + hashtag extraction."""

import asyncio
import re
from datetime import datetime
from collections import defaultdict
from config import VIDEOS_PER_SEARCH
import database as db



def extract_hashtags(text: str) -> list[str]:
    """Extract hashtags from description."""
    if not text:
        return []
    tags = re.findall(r'#(\w+)', text.lower())
    # Filter out common generic tags
    skip = {'fyp', 'foryou', 'foryoupage', 'viral', 'xyzbca', 'trending', 'tiktok'}
    return [t for t in tags if t not in skip and len(t) > 1]


def extract_mentions(text: str) -> list[str]:
    """Extract @mentions from description."""
    if not text:
        return []
    mentions = re.findall(r'@(\w+)', text.lower())
    skip = {'tiktok', 'fyp'}
    return [m for m in mentions if m not in skip and len(m) > 1]


def extract_keywords(text: str) -> list[str]:
    """Extract keywords from description (non-hashtag, non-mention)."""
    if not text:
        return []
    clean = re.sub(r'http\\S+', ' ', text.lower())
    clean = re.sub(r'[#@]\\w+', ' ', clean)
    words = re.findall(r'[a-z0-9]{2,}', clean)
    stop = {
        'the', 'and', 'for', 'with', 'this', 'that', 'its', 'its', 'you', 'your', 'yall',
        'are', 'was', 'were', 'been', 'have', 'has', 'had', 'not', 'but', 'out', 'get',
        'got', 'from', 'just', 'like', 'love', 'lol', 'omg', 'hey', 'yes', 'no', 'pls',
        'please', 'tiktok', 'fyp', 'foryou', 'foryoupage', 'viral', 'trend', 'trending',
    }
    uniq = []
    seen = set()
    for w in words:
        if w in stop or w in seen:
            continue
        seen.add(w)
        uniq.append(w)
        if len(uniq) >= 6:
            break
    return uniq


def extract_video_data(video) -> dict | None:
    """Extract data from TikTok video."""
    try:
        video_data = video.as_dict if hasattr(video, 'as_dict') else video

        music = video_data.get("music", {})
        if not music:
            return None

        sound_id = str(music.get("id", ""))
        if not sound_id:
            return None

        stats = video_data.get("stats", {})
        author = video_data.get("author", {})
        desc = video_data.get("desc", "") or ""
        hashtags = extract_hashtags(desc)
        mentions = extract_mentions(desc)
        keywords = extract_keywords(desc)

        return {
            "video_id": str(video_data.get("id", "")),
            "sound_id": sound_id,
            "sound_name": music.get("title", "Unknown"),
            "sound_author": music.get("authorName", "Unknown"),
            "video_author": author.get("uniqueId", "unknown"),
            "description": desc[:200],  # First 200 chars
            "hashtags": hashtags,
            "mentions": mentions,
            "keywords": keywords,
            "views": stats.get("playCount", 0),
            "likes": stats.get("diggCount", 0),
            "comments": stats.get("commentCount", 0),
            "shares": stats.get("shareCount", 0),
            "create_time": datetime.fromtimestamp(video_data.get("createTime", 0)),
            "video_url": f"https://www.tiktok.com/@{author.get('uniqueId', 'user')}/video/{video_data.get('id', '')}",
            "sound_url": f"https://www.tiktok.com/music/{sound_id}",
        }
    except:
        return None


async def scrape_trending(api) -> list[dict]:
    """Scrape trending videos."""
    videos = []
    try:
        async for video in api.trending.videos(count=VIDEOS_PER_SEARCH):
            data = extract_video_data(video)
            if data:
                videos.append(data)
    except Exception as e:
        print(f"Trending error: {e}")
    return videos


def store_videos(videos: list[dict]):
    """Store videos in database."""
    sounds = defaultdict(list)
    all_hashtags = defaultdict(lambda: {"count": 0, "views": 0})
    all_mentions = defaultdict(lambda: {"count": 0, "views": 0})
    all_keywords = defaultdict(lambda: {"count": 0, "views": 0})
    term_pairs = defaultdict(lambda: {"count": 0, "views": 0})

    for v in videos:
        sounds[v["sound_id"]].append(v)
        # Track hashtag popularity
        for tag in v.get("hashtags", []):
            all_hashtags[tag]["count"] += 1
            all_hashtags[tag]["views"] += v["views"]
        # Track mention popularity
        for handle in v.get("mentions", []):
            all_mentions[handle]["count"] += 1
            all_mentions[handle]["views"] += v["views"]
        # Track keyword popularity
        for word in v.get("keywords", []):
            all_keywords[word]["count"] += 1
            all_keywords[word]["views"] += v["views"]

        # Track co-occurrence between terms in this video
        terms = []
        terms += [f"tag:{t}" for t in v.get("hashtags", [])]
        terms += [f"at:{m}" for m in v.get("mentions", [])]
        terms += [f"kw:{w}" for w in v.get("keywords", [])]
        uniq_terms = list(dict.fromkeys(terms))
        for term in uniq_terms:
            for co_term in uniq_terms:
                if term == co_term:
                    continue
                term_pairs[(term, co_term)]["count"] += 1
                term_pairs[(term, co_term)]["views"] += v["views"]

    for sound_id, vids in sounds.items():
        first = vids[0]

        db.upsert_sound(
            sound_id=sound_id,
            name=first["sound_name"],
            author=first["sound_author"],
            tiktok_url=first["sound_url"],
        )

        creators = set()
        for v in vids:
            db.upsert_video(
                video_id=v["video_id"],
                sound_id=sound_id,
                author=v["video_author"],
                views=v["views"],
                likes=v["likes"],
                comments=v["comments"],
                shares=v["shares"],
                create_time=v["create_time"],
                video_url=v["video_url"],
            )
            creators.add(v["video_author"])

        db.add_snapshot(
            sound_id=sound_id,
            video_count=len(vids),
            total_views=sum(v["views"] for v in vids),
            total_likes=sum(v["likes"] for v in vids),
            total_comments=sum(v["comments"] for v in vids),
            unique_creators=len(creators),
        )

    # Store hashtag trends
    for tag, data in all_hashtags.items():
        db.upsert_hashtag(tag, data["count"], data["views"])
    for handle, data in all_mentions.items():
        db.upsert_mention(handle, data["count"], data["views"])
    for word, data in all_keywords.items():
        db.upsert_keyword(word, data["count"], data["views"])
    for (term, co_term), data in term_pairs.items():
        db.upsert_term_cooccurrence(term, co_term, data["count"], data["views"])

    return len(sounds)


async def run_scrape():
    """Main scrape function."""
    db.init_db()

    videos = []
    try:
        # Suppress TikTokApi logging
        import logging
        logging.getLogger("TikTokApi").setLevel(logging.CRITICAL)
        logging.getLogger("TikTokApi.tiktok").setLevel(logging.CRITICAL)

        from TikTokApi import TikTokApi

        async with TikTokApi() as api:
            await api.create_sessions(num_sessions=1, sleep_after=2, headless=True)

            print("Scraping trending...")
            videos = await scrape_trending(api)
            print(f"Found {len(videos)} videos")
    except ImportError as e:
        print(f"TikTok API not installed: {e}")
    except Exception as e:
        print(f"TikTok scrape failed: {e}")

    count = store_videos(videos) if videos else 0
    print(f"Stored {count} sounds")

    return count


if __name__ == "__main__":
    asyncio.run(run_scrape())
