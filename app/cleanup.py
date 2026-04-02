"""Clean up migrated data — fix categories and fetch missing titles."""
from __future__ import annotations

import asyncio
import json
import logging
import re

from app.database import get_db

log = logging.getLogger(__name__)


CATEGORY_MAP = {
    "business": "business",
    "entrepreneurship": "business",
    "entrepreneuriat": "business",
    "finance": "finance",
    "investing": "finance",
    "investissement": "finance",
    "crypto": "crypto",
    "bitcoin": "crypto",
    "real estate": "real estate",
    "immobilier": "real estate",
    "marketing": "marketing",
    "branding": "marketing",
    "sales": "marketing",
    "self-improvement": "self-improvement",
    "mindset": "mindset",
    "philosophie": "mindset",
    "philosophie de vie": "mindset",
    "relationships": "relationships",
    "relations": "relationships",
    "dating": "relationships",
    "health": "health",
    "fitness": "health",
    "politics": "politics",
    "politique": "politics",
    "taxes": "finance",
    "tax": "finance",
    "tech": "tech",
    "technology": "tech",
    "ai": "tech",
}


async def cleanup_categories():
    """Parse JSON category blobs and long strings into clean category names."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, category, raw_transcript, analysis FROM entries WHERE category IS NOT NULL AND category != ''"
    )
    updated = 0
    for row in rows:
        cat = row["category"]
        clean = _extract_category(cat)

        # If still a long sentence, try to infer from content
        if len(clean) > 30:
            clean = _infer_category(
                (dict(row).get("raw_transcript") or ""),
                (dict(row).get("analysis") or ""),
            )

        if clean != cat:
            await db.execute(
                "UPDATE entries SET category = ? WHERE id = ?", (clean, row["id"])
            )
            updated += 1

    # Also fix entries with no category
    empty = await db.execute_fetchall(
        "SELECT id, raw_transcript, analysis FROM entries WHERE category IS NULL OR category = ''"
    )
    for row in empty:
        r = dict(row)
        cat = _infer_category(
            r.get("raw_transcript") or "",
            r.get("analysis") or "",
        )
        await db.execute("UPDATE entries SET category = ? WHERE id = ?", (cat, row["id"]))
        updated += 1

    if updated:
        await db.commit()
        log.info("Cleaned %d categories", updated)


def _extract_category(raw: str) -> str:
    """Extract a clean category name from various formats."""
    # Try JSON
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            # Format: {"business": {"present": true, ...}, ...}
            best = None
            best_conf = -1
            for key, val in data.items():
                if isinstance(val, dict) and val.get("present"):
                    conf = val.get("confidence", 0.5)
                    if isinstance(conf, (int, float)) and conf > best_conf:
                        best = key
                        best_conf = conf
            if best:
                clean = best.replace("_", " ").strip().lower()
                return CATEGORY_MAP.get(clean, clean)
        if isinstance(data, str):
            return data.strip().lower()
    except (json.JSONDecodeError, TypeError):
        pass

    # Already a short clean string?
    stripped = raw.strip().lower()
    if len(stripped) <= 30 and stripped in CATEGORY_MAP:
        return CATEGORY_MAP[stripped]
    if len(stripped) <= 25:
        return stripped

    # Long sentence — not a real category
    return _infer_from_text(stripped)


def _infer_from_text(text: str) -> str:
    """Infer category from text content using keyword matching."""
    text = text.lower()
    keywords = [
        (["business", "entrepreneur", "startup", "company", "revenue", "profit"], "business"),
        (["invest", "stock", "portfolio", "dividend", "wealth", "bank", "tax", "fiscal"], "finance"),
        (["crypto", "bitcoin", "blockchain", "defi", "token"], "crypto"),
        (["real estate", "immobilier", "property", "rental", "landlord"], "real estate"),
        (["marketing", "brand", "sales", "funnel", "conversion", "ads", "content creation"], "marketing"),
        (["mindset", "discipline", "stoic", "philosophy", "mental", "resilience", "grind"], "mindset"),
        (["relationship", "dating", "women", "men", "attraction", "couple", "marriage", "love"], "relationships"),
        (["health", "fitness", "workout", "diet", "nutrition", "gym", "muscle"], "health"),
        (["politi", "government", "election", "law", "regulation"], "politics"),
        (["tech", "software", "ai ", "artificial", "programming", "code", "saas"], "tech"),
    ]
    for words, cat in keywords:
        if any(w in text for w in words):
            return cat
    return "other"


def _infer_category(transcript: str, analysis: str) -> str:
    """Infer category from transcript and analysis content."""
    text = (transcript[:2000] + " " + analysis[:2000]).lower()
    return _infer_from_text(text)


async def fetch_missing_titles():
    """Fetch titles for entries that don't have one, using yt-dlp in parallel."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, url, platform FROM entries WHERE (title IS NULL OR title = '') AND url IS NOT NULL"
    )
    if not rows:
        return

    log.info("Fetching titles for %d entries...", len(rows))
    updated = 0

    # Process in batches of 10 concurrent
    batch_size = 10
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        tasks = [_get_title(row["url"], row["platform"]) for row in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for row, result in zip(batch, results):
            if isinstance(result, str) and result:
                await db.execute(
                    "UPDATE entries SET title = ? WHERE id = ?", (result, row["id"])
                )
                updated += 1

        if updated > 0 and i % 50 == 0:
            await db.commit()
            log.info("Fetched %d/%d titles", updated, len(rows))

    await db.commit()
    log.info("Fetched %d titles total", updated)


async def _get_title(url: str, platform: str | None) -> str | None:
    """Get title using yt-dlp --get-title."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "--get-title", "--no-warnings", "--no-download", url,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
        title = stdout.decode().strip().split("\n")[0] if stdout else None
        if title and len(title) > 1 and title != "watch":
            return title
        return None
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
