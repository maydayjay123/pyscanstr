"""SQLite database for tracking sounds and growth over time."""

import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize database tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Sounds table - tracks unique sounds
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sounds (
            sound_id TEXT PRIMARY KEY,
            name TEXT,
            author TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tiktok_url TEXT
        )
    """)

    # Snapshots table - tracks sound metrics over time for growth detection
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sound_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            video_count INTEGER,
            total_views INTEGER,
            total_likes INTEGER,
            total_comments INTEGER,
            unique_creators INTEGER,
            FOREIGN KEY (sound_id) REFERENCES sounds(sound_id)
        )
    """)

    # Videos table - individual videos for analysis
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            sound_id TEXT,
            author TEXT,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            shares INTEGER,
            create_time TIMESTAMP,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            video_url TEXT,
            FOREIGN KEY (sound_id) REFERENCES sounds(sound_id)
        )
    """)

    # Comments table - for detecting comment clustering
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_phrases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sound_id TEXT,
            phrase TEXT,
            count INTEGER DEFAULT 1,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sound_id) REFERENCES sounds(sound_id)
        )
    """)

    # Hashtags table - tracks trending hashtags
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtags (
            tag TEXT PRIMARY KEY,
            total_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Hashtag snapshots - track hashtag growth over time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tag TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            video_count INTEGER,
            total_views INTEGER,
            FOREIGN KEY (tag) REFERENCES hashtags(tag)
        )
    """)

    # Keywords table - tracks trending words from captions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            word TEXT PRIMARY KEY,
            total_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Keyword snapshots - track keyword growth over time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keyword_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            video_count INTEGER,
            total_views INTEGER,
            FOREIGN KEY (word) REFERENCES keywords(word)
        )
    """)

    # Mentions table - tracks trending @mentions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mentions (
            handle TEXT PRIMARY KEY,
            total_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Mention snapshots - track mention growth over time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS mention_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            handle TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            video_count INTEGER,
            total_views INTEGER,
            FOREIGN KEY (handle) REFERENCES mentions(handle)
        )
    """)

    # Term co-occurrence - tracks what terms show up together
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS term_cooccurrence (
            term TEXT,
            co_term TEXT,
            total_count INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (term, co_term)
        )
    """)

    # Create indexes for performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_sound_time ON snapshots(sound_id, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_sound ON videos(sound_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_sound ON comment_phrases(sound_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hashtag_snapshots ON hashtag_snapshots(tag, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_keyword_snapshots ON keyword_snapshots(word, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mention_snapshots ON mention_snapshots(handle, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_term_cooccurrence ON term_cooccurrence(term)")

    conn.commit()
    conn.close()


def upsert_sound(sound_id: str, name: str, author: str, tiktok_url: str = ""):
    """Insert or update a sound."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO sounds (sound_id, name, author, tiktok_url)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(sound_id) DO UPDATE SET
            name = excluded.name,
            author = excluded.author,
            tiktok_url = CASE WHEN excluded.tiktok_url != '' THEN excluded.tiktok_url ELSE sounds.tiktok_url END
    """, (sound_id, name, author, tiktok_url))

    conn.commit()
    conn.close()


def add_snapshot(sound_id: str, video_count: int, total_views: int,
                 total_likes: int, total_comments: int, unique_creators: int):
    """Add a snapshot of sound metrics."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO snapshots (sound_id, video_count, total_views, total_likes, total_comments, unique_creators)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (sound_id, video_count, total_views, total_likes, total_comments, unique_creators))

    conn.commit()
    conn.close()


def upsert_video(video_id: str, sound_id: str, author: str, views: int,
                 likes: int, comments: int, shares: int, create_time: datetime, video_url: str):
    """Insert or update a video."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO videos (video_id, sound_id, author, views, likes, comments, shares, create_time, video_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            views = excluded.views,
            likes = excluded.likes,
            comments = excluded.comments,
            shares = excluded.shares,
            scraped_at = CURRENT_TIMESTAMP
    """, (video_id, sound_id, author, views, likes, comments, shares, create_time, video_url))

    conn.commit()
    conn.close()


def add_comment_phrase(sound_id: str, phrase: str):
    """Track a comment phrase for clustering detection."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO comment_phrases (sound_id, phrase, count)
        VALUES (?, ?, 1)
        ON CONFLICT DO UPDATE SET
            count = count + 1,
            last_seen = CURRENT_TIMESTAMP
    """, (sound_id, phrase.lower()))

    conn.commit()
    conn.close()


def get_sound_snapshots(sound_id: str, hours: int = 48) -> list[dict]:
    """Get snapshots for a sound within the last N hours."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(hours=hours)

    cursor.execute("""
        SELECT * FROM snapshots
        WHERE sound_id = ? AND timestamp > ?
        ORDER BY timestamp ASC
    """, (sound_id, cutoff))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_growth_rate(sound_id: str) -> Optional[float]:
    """Calculate growth rate over last 24 hours. Returns None if not enough data."""
    snapshots = get_sound_snapshots(sound_id, hours=48)

    if len(snapshots) < 2:
        return None

    # Get oldest and newest video counts
    oldest = snapshots[0]["video_count"]
    newest = snapshots[-1]["video_count"]

    if oldest == 0:
        return None

    return (newest - oldest) / oldest


def get_all_sounds_with_recent_activity(hours: int = 24) -> list[dict]:
    """Get all sounds that have been updated in the last N hours."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(hours=hours)

    cursor.execute("""
        SELECT DISTINCT s.*,
            (SELECT video_count FROM snapshots WHERE sound_id = s.sound_id ORDER BY timestamp DESC LIMIT 1) as latest_video_count,
            (SELECT total_views FROM snapshots WHERE sound_id = s.sound_id ORDER BY timestamp DESC LIMIT 1) as latest_views
        FROM sounds s
        JOIN snapshots snap ON s.sound_id = snap.sound_id
        WHERE snap.timestamp > ?
    """, (cutoff,))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_unique_creators_for_sound(sound_id: str) -> int:
    """Get count of unique creators using this sound."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(DISTINCT author) as creator_count
        FROM videos
        WHERE sound_id = ?
    """, (sound_id,))

    result = cursor.fetchone()
    conn.close()
    return result["creator_count"] if result else 0


def get_top_comment_phrases(sound_id: str, limit: int = 5) -> list[dict]:
    """Get the most common comment phrases for a sound."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT phrase, count
        FROM comment_phrases
        WHERE sound_id = ?
        ORDER BY count DESC
        LIMIT ?
    """, (sound_id, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_sample_video_url(sound_id: str) -> Optional[str]:
    """Get a sample video URL for a sound."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT video_url FROM videos
        WHERE sound_id = ? AND video_url IS NOT NULL
        ORDER BY views DESC
        LIMIT 1
    """, (sound_id,))

    result = cursor.fetchone()
    conn.close()
    return result["video_url"] if result else None


def get_sound_by_id(sound_id: str) -> Optional[dict]:
    """Get sound details by ID."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sounds WHERE sound_id = ?", (sound_id,))
    result = cursor.fetchone()
    conn.close()
    return dict(result) if result else None


def upsert_hashtag(tag: str, count: int, views: int):
    """Insert or update hashtag stats."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO hashtags (tag, total_count, total_views)
        VALUES (?, ?, ?)
        ON CONFLICT(tag) DO UPDATE SET
            total_count = total_count + excluded.total_count,
            total_views = total_views + excluded.total_views,
            last_seen = CURRENT_TIMESTAMP
    """, (tag, count, views))

    # Add snapshot for trend tracking
    cursor.execute("""
        INSERT INTO hashtag_snapshots (tag, video_count, total_views)
        VALUES (?, ?, ?)
    """, (tag, count, views))

    conn.commit()
    conn.close()


def upsert_keyword(word: str, count: int, views: int):
    """Insert or update keyword stats."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO keywords (word, total_count, total_views)
        VALUES (?, ?, ?)
        ON CONFLICT(word) DO UPDATE SET
            total_count = total_count + excluded.total_count,
            total_views = total_views + excluded.total_views,
            last_seen = CURRENT_TIMESTAMP
    """, (word, count, views))

    cursor.execute("""
        INSERT INTO keyword_snapshots (word, video_count, total_views)
        VALUES (?, ?, ?)
    """, (word, count, views))

    conn.commit()
    conn.close()


def get_trending_hashtags(hours: int = 6, limit: int = 20) -> list[dict]:
    """Get hashtags that are trending (most activity recently)."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(hours=hours)

    cursor.execute("""
        SELECT
            h.tag,
            h.total_count,
            h.total_views,
            SUM(hs.video_count) as recent_videos,
            SUM(hs.total_views) as recent_views,
            COUNT(hs.id) as snapshot_count
        FROM hashtags h
        JOIN hashtag_snapshots hs ON h.tag = hs.tag
        WHERE hs.timestamp > ?
        GROUP BY h.tag
        HAVING snapshot_count >= 1
        ORDER BY recent_views DESC, recent_videos DESC
        LIMIT ?
    """, (cutoff, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_rising_hashtags(hours: int = 6, limit: int = 15) -> list[str]:
    """Get hashtag names that are rising fast - for coin search."""
    trending = get_trending_hashtags(hours, limit)
    return [t["tag"] for t in trending if t["recent_videos"] > 0]


def get_trending_keywords(hours: int = 6, limit: int = 20) -> list[dict]:
    """Get keywords that are trending (most activity recently)."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(hours=hours)

    cursor.execute("""
        SELECT
            k.word,
            k.total_count,
            k.total_views,
            SUM(ks.video_count) as recent_videos,
            SUM(ks.total_views) as recent_views,
            COUNT(ks.id) as snapshot_count
        FROM keywords k
        JOIN keyword_snapshots ks ON k.word = ks.word
        WHERE ks.timestamp > ?
        GROUP BY k.word
        HAVING snapshot_count >= 1
        ORDER BY recent_views DESC, recent_videos DESC
        LIMIT ?
    """, (cutoff, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_rising_keywords(hours: int = 6, limit: int = 15) -> list[str]:
    """Get keywords that are rising fast - for coin search."""
    trending = get_trending_keywords(hours, limit)
    return [t["word"] for t in trending if t["recent_videos"] > 0]


def upsert_mention(handle: str, count: int, views: int):
    """Insert or update @mention stats."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO mentions (handle, total_count, total_views)
        VALUES (?, ?, ?)
        ON CONFLICT(handle) DO UPDATE SET
            total_count = total_count + excluded.total_count,
            total_views = total_views + excluded.total_views,
            last_seen = CURRENT_TIMESTAMP
    """, (handle, count, views))

    cursor.execute("""
        INSERT INTO mention_snapshots (handle, video_count, total_views)
        VALUES (?, ?, ?)
    """, (handle, count, views))

    conn.commit()
    conn.close()


def get_trending_mentions(hours: int = 6, limit: int = 15) -> list[dict]:
    """Get @mentions that are trending recently."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = datetime.now() - timedelta(hours=hours)

    cursor.execute("""
        SELECT
            m.handle,
            m.total_count,
            m.total_views,
            SUM(ms.video_count) as recent_videos,
            SUM(ms.total_views) as recent_views,
            COUNT(ms.id) as snapshot_count
        FROM mentions m
        JOIN mention_snapshots ms ON m.handle = ms.handle
        WHERE ms.timestamp > ?
        GROUP BY m.handle
        HAVING snapshot_count >= 1
        ORDER BY recent_views DESC, recent_videos DESC
        LIMIT ?
    """, (cutoff, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def upsert_term_cooccurrence(term: str, co_term: str, count: int, views: int):
    """Insert or update term co-occurrence counts."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO term_cooccurrence (term, co_term, total_count, total_views)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(term, co_term) DO UPDATE SET
            total_count = total_count + excluded.total_count,
            total_views = total_views + excluded.total_views,
            last_seen = CURRENT_TIMESTAMP
    """, (term, co_term, count, views))

    conn.commit()
    conn.close()


def get_related_terms(term: str, limit: int = 5) -> list[dict]:
    """Get terms most often used with a given term."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT co_term, total_count, total_views
        FROM term_cooccurrence
        WHERE term = ?
        ORDER BY total_views DESC, total_count DESC
        LIMIT ?
    """, (term, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]
