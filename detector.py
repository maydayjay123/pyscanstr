"""Real-time viral signal detection with momentum tracking for early trend catching."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import database as db


@dataclass
class SignalResult:
    """Container for detection signals."""
    sound_id: str
    sound_name: str
    sound_author: str

    # Raw metrics
    video_count: int
    total_views: int
    total_likes: int
    total_comments: int
    unique_creators: int

    # Computed signals
    view_velocity: float  # Views per hour since oldest video
    engagement_ratio: float  # (likes + comments) / views
    creator_diversity: float  # Ratio of unique creators to videos
    freshness_score: float  # How new are the videos (0-1)
    virality_score: float  # Combined early viral signal (0-100)

    # Momentum signals (NEW)
    video_growth_rate: float  # New videos per hour (acceleration indicator)
    momentum_score: float  # Rate of change - catching the wave early

    # Trend direction
    trend_status: str  # "exploding", "rising", "peaking", "dying", "new", "stable"

    # URLs
    sample_video_url: Optional[str]
    sound_url: Optional[str]


def get_video_stats(sound_id: str) -> dict:
    """Get aggregated video stats for a sound."""
    conn = db.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) as video_count,
            SUM(views) as total_views,
            SUM(likes) as total_likes,
            SUM(comments) as total_comments,
            COUNT(DISTINCT author) as unique_creators,
            MIN(create_time) as oldest_video,
            MAX(create_time) as newest_video,
            AVG(views) as avg_views,
            AVG(likes) as avg_likes
        FROM videos
        WHERE sound_id = ?
    """, (sound_id,))

    result = cursor.fetchone()
    conn.close()
    return dict(result) if result else {}


def get_video_growth(sound_id: str) -> dict:
    """Get video count growth over time windows for momentum detection."""
    conn = db.get_connection()
    cursor = conn.cursor()

    # Videos in last 1 hour
    cursor.execute("""
        SELECT COUNT(*) FROM videos
        WHERE sound_id = ? AND scraped_at > datetime('now', '-1 hour')
    """, (sound_id,))
    last_1h = cursor.fetchone()[0]

    # Videos in last 3 hours
    cursor.execute("""
        SELECT COUNT(*) FROM videos
        WHERE sound_id = ? AND scraped_at > datetime('now', '-3 hours')
    """, (sound_id,))
    last_3h = cursor.fetchone()[0]

    # Videos in last 6 hours
    cursor.execute("""
        SELECT COUNT(*) FROM videos
        WHERE sound_id = ? AND scraped_at > datetime('now', '-6 hours')
    """, (sound_id,))
    last_6h = cursor.fetchone()[0]

    # Videos in last 12 hours
    cursor.execute("""
        SELECT COUNT(*) FROM videos
        WHERE sound_id = ? AND scraped_at > datetime('now', '-12 hours')
    """, (sound_id,))
    last_12h = cursor.fetchone()[0]

    # Videos in last 24 hours
    cursor.execute("""
        SELECT COUNT(*) FROM videos
        WHERE sound_id = ? AND scraped_at > datetime('now', '-24 hours')
    """, (sound_id,))
    last_24h = cursor.fetchone()[0]

    conn.close()

    return {
        "last_1h": last_1h,
        "last_3h": last_3h,
        "last_6h": last_6h,
        "last_12h": last_12h,
        "last_24h": last_24h,
    }


def calculate_view_velocity(stats: dict) -> float:
    """Calculate views per hour - high velocity = trending."""
    if not stats.get("oldest_video") or not stats.get("total_views"):
        return 0.0

    try:
        oldest = datetime.fromisoformat(str(stats["oldest_video"]))
        hours_since = max((datetime.now() - oldest).total_seconds() / 3600, 1)
        return stats["total_views"] / hours_since
    except:
        return 0.0


def calculate_freshness(stats: dict) -> float:
    """How fresh are the videos? 1.0 = all from last 6h, 0 = old."""
    if not stats.get("newest_video"):
        return 0.5

    try:
        newest = datetime.fromisoformat(str(stats["newest_video"]))
        hours_ago = (datetime.now() - newest).total_seconds() / 3600

        if hours_ago < 3:
            return 1.0
        elif hours_ago < 6:
            return 0.9
        elif hours_ago < 12:
            return 0.7
        elif hours_ago < 24:
            return 0.5
        elif hours_ago < 48:
            return 0.3
        else:
            return 0.1
    except:
        return 0.5


def calculate_momentum(growth: dict) -> tuple[float, float]:
    """
    Calculate momentum score - detecting acceleration.
    Returns (video_growth_rate, momentum_score)

    Key insight: If videos in last 1h > videos in 1h before that,
    the trend is ACCELERATING - this is the early signal we want.
    """
    last_1h = growth.get("last_1h", 0)
    last_3h = growth.get("last_3h", 0)
    last_6h = growth.get("last_6h", 0)
    last_12h = growth.get("last_12h", 0)
    last_24h = growth.get("last_24h", 0)

    # Video growth rate (videos per hour in last 3h)
    video_growth_rate = last_3h / 3 if last_3h > 0 else 0

    # Momentum = compare recent velocity to older velocity
    # If recent is faster than older, momentum is positive (accelerating)
    recent_rate = last_3h / 3 if last_3h > 0 else 0

    # Older rate: videos from 3-12h ago, per hour
    older_videos = max(0, last_12h - last_3h)
    older_rate = older_videos / 9 if older_videos > 0 else 0.1  # 9 hours window

    if older_rate > 0:
        momentum_ratio = recent_rate / older_rate
    else:
        momentum_ratio = recent_rate * 10  # New sound, high momentum if any recent activity

    # Normalize to 0-100 scale
    # ratio of 2 = double the rate = strong momentum = ~50 score
    # ratio of 5 = 5x the rate = exploding = ~100 score
    momentum_score = min(100, (momentum_ratio - 1) * 25) if momentum_ratio > 1 else 0

    return video_growth_rate, max(0, momentum_score)


def determine_trend_status(stats: dict, view_velocity: float, freshness: float,
                          momentum_score: float, video_growth_rate: float) -> str:
    """Determine if trend is exploding, rising, peaking, or dying."""
    engagement = (stats.get("total_likes", 0) + stats.get("total_comments", 0))
    views = stats.get("total_views", 1)
    engagement_rate = engagement / views if views > 0 else 0
    total_views = stats.get("total_views", 0)

    # EXPLODING: Very high momentum + fresh content
    if momentum_score > 60 and freshness > 0.7:
        return "exploding"

    # EXPLODING: High video growth rate + high engagement
    if video_growth_rate > 3 and engagement_rate > 0.05:
        return "exploding"

    # RISING: Good momentum + fresh content
    if momentum_score > 30 and freshness > 0.5:
        return "rising"

    # RISING: New sound with high engagement
    if freshness > 0.8 and engagement_rate > 0.05:
        return "rising"

    # RISING: High velocity + fresh
    if view_velocity > 10000 and freshness > 0.5:
        return "rising"

    # RISING: Moderate momentum, still fresh
    if momentum_score > 15 and freshness > 0.6:
        return "rising"

    # DYING: Very high views but low freshness = old viral
    if total_views > 1000000 and freshness < 0.3:
        return "dying"

    # PEAKING: High views, slowing momentum
    if total_views > 500000 and momentum_score < 20:
        return "peaking"

    # NEW: Fresh but low velocity = potential
    if freshness > 0.7 and momentum_score < 15:
        return "new"

    return "stable"


def calculate_virality_score(stats: dict, view_velocity: float,
                             engagement_ratio: float, freshness: float,
                             creator_diversity: float, momentum_score: float) -> float:
    """
    Calculate combined virality score (0-100).
    Prioritizes EARLY signals over already-viral content.
    Now includes momentum for catching trends as they start moving.
    """
    score = 0.0

    # Momentum (up to 30 points) - HIGHEST WEIGHT for early detection
    # This is the key to catching moving trends early
    score += min(30, momentum_score * 0.3)

    # Engagement ratio (up to 25 points) - quality signal
    # 10% engagement = 25 pts
    engagement_score = min(25, engagement_ratio * 250)
    score += engagement_score

    # Freshness (up to 20 points) - want NEW sounds
    score += freshness * 20

    # View velocity (up to 15 points)
    # 10k views/hr = 15 pts
    velocity_score = min(15, (view_velocity / 10000) * 15)
    score += velocity_score

    # Creator diversity (up to 10 points) - organic spread signal
    score += creator_diversity * 10

    # PENALTY for already-viral sounds (we want EARLY catches)
    total_views = stats.get("total_views", 0)
    if total_views > 10000000:  # 10M+ views = too late
        score *= 0.2
    elif total_views > 5000000:  # 5M+ views = late
        score *= 0.4
    elif total_views > 1000000:  # 1M+ views = late
        score *= 0.6
    elif total_views > 500000:  # 500k+ views = catching on
        score *= 0.8
    elif total_views > 100000:  # 100k+ views = still early-ish
        score *= 0.95

    # BONUS for exploding momentum on fresh content
    if momentum_score > 50 and freshness > 0.7:
        score *= 1.3

    return min(100, max(0, score))


def detect_signals(sound_id: str) -> Optional[SignalResult]:
    """Detect real-time viral signals for a sound with momentum tracking."""
    sound = db.get_sound_by_id(sound_id)
    if not sound:
        return None

    stats = get_video_stats(sound_id)
    if not stats or stats.get("video_count", 0) == 0:
        return None

    # Get growth data for momentum calculation
    growth = get_video_growth(sound_id)

    # Calculate metrics
    view_velocity = calculate_view_velocity(stats)
    freshness = calculate_freshness(stats)
    video_growth_rate, momentum_score = calculate_momentum(growth)

    views = stats.get("total_views", 1)
    engagement = stats.get("total_likes", 0) + stats.get("total_comments", 0)
    engagement_ratio = engagement / views if views > 0 else 0

    video_count = stats.get("video_count", 1)
    unique_creators = stats.get("unique_creators", 1)
    creator_diversity = unique_creators / video_count if video_count > 0 else 0

    trend_status = determine_trend_status(
        stats, view_velocity, freshness, momentum_score, video_growth_rate
    )
    virality_score = calculate_virality_score(
        stats, view_velocity, engagement_ratio, freshness,
        creator_diversity, momentum_score
    )

    sample_url = db.get_sample_video_url(sound_id)

    return SignalResult(
        sound_id=sound_id,
        sound_name=sound["name"],
        sound_author=sound["author"],
        video_count=video_count,
        total_views=stats.get("total_views", 0),
        total_likes=stats.get("total_likes", 0),
        total_comments=stats.get("total_comments", 0),
        unique_creators=unique_creators,
        view_velocity=view_velocity,
        engagement_ratio=engagement_ratio,
        creator_diversity=creator_diversity,
        freshness_score=freshness,
        virality_score=virality_score,
        video_growth_rate=video_growth_rate,
        momentum_score=momentum_score,
        trend_status=trend_status,
        sample_video_url=sample_url,
        sound_url=sound.get("tiktok_url"),
    )


def get_all_candidates() -> list[SignalResult]:
    """Get all sounds and compute their real-time signals."""
    conn = db.get_connection()
    cursor = conn.cursor()

    # Get all sounds with recent videos (expanded window to 48h)
    cursor.execute("""
        SELECT DISTINCT s.sound_id
        FROM sounds s
        JOIN videos v ON s.sound_id = v.sound_id
        WHERE v.scraped_at > datetime('now', '-48 hours')
    """)

    sound_ids = [row["sound_id"] for row in cursor.fetchall()]
    conn.close()

    candidates = []
    for sound_id in sound_ids:
        signal = detect_signals(sound_id)
        if signal and signal.virality_score > 5:  # Lower threshold to catch more early
            candidates.append(signal)

    print(f"Found {len(candidates)} candidate sounds")
    return candidates


def filter_promising(candidates: list[SignalResult]) -> list[SignalResult]:
    """Filter and categorize candidates."""
    # Separate by trend status
    exploding = [c for c in candidates if c.trend_status == "exploding"]
    rising = [c for c in candidates if c.trend_status == "rising"]
    new = [c for c in candidates if c.trend_status == "new"]
    peaking = [c for c in candidates if c.trend_status == "peaking"]
    dying = [c for c in candidates if c.trend_status == "dying"]

    # Prioritize: exploding first, then rising, then new, then peaking
    # Include dying for awareness but rank lower
    promising = exploding + rising + new + peaking + dying

    # Sort by virality score within each group
    promising.sort(key=lambda x: x.virality_score, reverse=True)

    print(f"Filtered: {len(exploding)} exploding, {len(rising)} rising, {len(new)} new, {len(peaking)} peaking, {len(dying)} dying")
    return promising
