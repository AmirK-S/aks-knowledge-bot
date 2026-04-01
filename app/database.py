import aiosqlite
import os
from app.config import DB_PATH

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _db = await aiosqlite.connect(DB_PATH)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA foreign_keys=ON")
        await _init_tables(_db)
    return _db


async def _init_tables(db: aiosqlite.Connection):
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            platform TEXT,
            title TEXT,
            raw_transcript TEXT,
            analysis TEXT,
            key_points TEXT,
            category TEXT,
            tags TEXT DEFAULT '[]',
            language TEXT DEFAULT 'en',
            source_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
            url,
            title,
            raw_transcript,
            analysis,
            key_points,
            category,
            content='entries',
            content_rowid='id',
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE TRIGGER IF NOT EXISTS entries_ai AFTER INSERT ON entries BEGIN
            INSERT INTO entries_fts(rowid, url, title, raw_transcript, analysis, key_points, category)
            VALUES (new.id, new.url, new.title, new.raw_transcript, new.analysis, new.key_points, new.category);
        END;

        CREATE TRIGGER IF NOT EXISTS entries_ad AFTER DELETE ON entries BEGIN
            INSERT INTO entries_fts(entries_fts, rowid, url, title, raw_transcript, analysis, key_points, category)
            VALUES ('delete', old.id, old.url, old.title, old.raw_transcript, old.analysis, old.key_points, old.category);
        END;

        CREATE TRIGGER IF NOT EXISTS entries_au AFTER UPDATE ON entries BEGIN
            INSERT INTO entries_fts(entries_fts, rowid, url, title, raw_transcript, analysis, key_points, category)
            VALUES ('delete', old.id, old.url, old.title, old.raw_transcript, old.analysis, old.key_points, old.category);
            INSERT INTO entries_fts(rowid, url, title, raw_transcript, analysis, key_points, category)
            VALUES (new.id, new.url, new.title, new.raw_transcript, new.analysis, new.key_points, new.category);
        END;
    """)
    await db.commit()


async def insert_entry(
    url: str,
    platform: str | None = None,
    title: str | None = None,
    raw_transcript: str | None = None,
    analysis: str | None = None,
    key_points: str | None = None,
    category: str | None = None,
    tags: str = "[]",
    language: str = "en",
    source_type: str | None = None,
) -> int:
    db = await get_db()
    cur = await db.execute(
        """INSERT INTO entries (url, platform, title, raw_transcript, analysis, key_points, category, tags, language, source_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(url) DO UPDATE SET
             title=excluded.title,
             raw_transcript=excluded.raw_transcript,
             analysis=excluded.analysis,
             key_points=excluded.key_points,
             category=excluded.category,
             tags=excluded.tags,
             language=excluded.language,
             source_type=excluded.source_type
        """,
        (url, platform, title, raw_transcript, analysis, key_points, category, tags, language, source_type),
    )
    await db.commit()
    return cur.lastrowid


async def search_entries(query: str, limit: int = 10) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        """SELECT e.*, rank
           FROM entries_fts fts
           JOIN entries e ON e.id = fts.rowid
           WHERE entries_fts MATCH ?
           ORDER BY rank
           LIMIT ?""",
        (query, limit),
    )
    return [dict(r) for r in rows]


async def get_entries_by_category(category: str, limit: int = 20) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM entries WHERE category LIKE ? ORDER BY created_at DESC LIMIT ?",
        (f"%{category}%", limit),
    )
    return [dict(r) for r in rows]


async def get_all_categories() -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT category, COUNT(*) as cnt FROM entries WHERE category IS NOT NULL AND category != '' GROUP BY category ORDER BY cnt DESC"
    )
    return [dict(r) for r in rows]


async def get_stats() -> dict:
    db = await get_db()
    row = await db.execute_fetchall("SELECT COUNT(*) as total FROM entries")
    total = row[0]["total"]
    row2 = await db.execute_fetchall(
        "SELECT platform, COUNT(*) as cnt FROM entries GROUP BY platform ORDER BY cnt DESC"
    )
    platforms = {r["platform"]: r["cnt"] for r in row2}
    row3 = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM entries WHERE created_at >= datetime('now', '-7 days')"
    )
    this_week = row3[0]["cnt"]
    return {"total": total, "platforms": platforms, "this_week": this_week}


async def get_recent_entries(days: int = 7, limit: int = 50) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM entries WHERE created_at >= datetime('now', ? || ' days') ORDER BY created_at DESC LIMIT ?",
        (f"-{days}", limit),
    )
    return [dict(r) for r in rows]


async def get_random_entry() -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM entries ORDER BY RANDOM() LIMIT 1"
    )
    return dict(rows[0]) if rows else None


async def get_entry_by_url(url: str) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM entries WHERE url = ?", (url,)
    )
    return dict(rows[0]) if rows else None


async def close_db():
    global _db
    if _db:
        await _db.close()
        _db = None
