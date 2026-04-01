"""Clean up migrated data — fix categories and fetch missing titles."""
from __future__ import annotations

import asyncio
import json
import logging
import re

from app.database import get_db

log = logging.getLogger(__name__)


async def cleanup_categories():
    """Parse JSON category blobs into clean category names."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, category FROM entries WHERE category IS NOT NULL AND category != ''"
    )
    updated = 0
    for row in rows:
        cat = row["category"]
        # Skip already clean categories
        if not cat.startswith("{") and not cat.startswith("["):
            continue

        clean = _extract_category(cat)
        if clean and clean != cat:
            await db.execute(
                "UPDATE entries SET category = ? WHERE id = ?", (clean, row["id"])
            )
            updated += 1

    if updated:
        await db.commit()
        log.info("Cleaned %d categories", updated)


def _extract_category(raw: str) -> str:
    """Extract a clean category name from various JSON formats."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw.strip()

    if isinstance(data, dict):
        # Format: {"business": {"present": true, ...}, "philosophie_de_vie": ...}
        # Find the first key with present=true or highest confidence
        best = None
        best_conf = -1
        for key, val in data.items():
            if isinstance(val, dict):
                if val.get("present"):
                    conf = val.get("confidence", 0.5)
                    if conf > best_conf:
                        best = key
                        best_conf = conf
            elif isinstance(val, str):
                return val  # Simple {"category": "business"} format

        if best:
            return best.replace("_", " ").strip()

        # Fallback: first key
        first_key = next(iter(data), None)
        if first_key and isinstance(first_key, str):
            return first_key.replace("_", " ").strip()

    if isinstance(data, str):
        return data.strip()

    return raw[:50].strip()


async def fetch_missing_titles():
    """Fetch titles for entries that don't have one, using yt-dlp."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, url, platform FROM entries WHERE (title IS NULL OR title = '') AND url IS NOT NULL"
    )
    if not rows:
        return

    log.info("Fetching titles for %d entries...", len(rows))
    updated = 0

    for row in rows:
        url = row["url"]
        platform = row["platform"]

        try:
            title = await _get_title(url, platform)
            if title:
                await db.execute(
                    "UPDATE entries SET title = ? WHERE id = ?", (title, row["id"])
                )
                updated += 1
                if updated % 20 == 0:
                    await db.commit()
                    log.info("Fetched %d/%d titles", updated, len(rows))
        except Exception:
            pass  # Skip failures silently

        # Small delay to be nice to APIs
        await asyncio.sleep(0.3)

    await db.commit()
    log.info("Fetched %d titles total", updated)


async def _get_title(url: str, platform: str | None) -> str | None:
    """Get title using yt-dlp --get-title."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "--get-title", "--no-warnings", "--no-download", url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        title = stdout.decode().strip().split("\n")[0] if stdout else None
        return title if title and len(title) > 1 else None
    except (asyncio.TimeoutError, Exception):
        return None


async def run_cleanup():
    """Run all cleanup tasks."""
    log.info("Running data cleanup...")
    await cleanup_categories()
    # Fetch titles in background to not block startup
    asyncio.create_task(_fetch_titles_bg())


async def _fetch_titles_bg():
    """Background task to fetch titles."""
    try:
        await fetch_missing_titles()
    except Exception:
        log.exception("Title fetch failed")
