"""Clean up migrated data — fix categories, dates, duplicates, and fetch missing titles."""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime

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


async def generate_missing_titles():
    """Fetch real titles via YouTube oEmbed API or extract from content."""
    import httpx

    db = await get_db()
    # Fetch all YouTube titles (oEmbed is fast and free) + missing titles for others
    rows = await db.execute_fetchall(
        """SELECT id, url, platform, analysis, raw_transcript, category FROM entries
           WHERE url IS NOT NULL AND (
             platform = 'youtube'
             OR title IS NULL OR title = ''
           )"""
    )
    if not rows:
        return

    log.info("Fetching titles for %d entries...", len(rows))
    updated = 0

    async with httpx.AsyncClient(timeout=10) as client:
        for row in rows:
            r = dict(row)
            url = r.get("url", "")
            platform = r.get("platform", "")
            title = None

            # YouTube: use oEmbed API (free, no auth)
            if platform == "youtube":
                title = await _youtube_title(client, url)

            # Fallback: extract from analysis
            if not title:
                title = _extract_title_from_content(
                    r.get("analysis") or "",
                    r.get("raw_transcript") or "",
                    url, platform,
                    r.get("category") or "",
                )

            if title:
                await db.execute("UPDATE entries SET title = ? WHERE id = ?", (title, r["id"]))
                updated += 1

    await db.commit()
    log.info("Fetched %d titles", updated)


async def _youtube_title(client, url: str) -> str | None:
    """Get YouTube video title via oEmbed API."""
    try:
        resp = await client.get(
            "https://www.youtube.com/oembed",
            params={"url": url, "format": "json"},
        )
        if resp.status_code == 200:
            data = resp.json()
            title = data.get("title", "")
            if title and len(title) > 1:
                return title
    except Exception:
        pass
    return None


SKIP_TITLES = {
    "core value extraction", "complete breakdown", "critical caveats",
    "practical extraction", "evidence cited", "notable formulations",
    "source", "key ideas", "playbook", "critical caveats & failure modes",
    "critical caveats &amp; failure modes", "what this video is about",
    "what this video covers", "overview", "summary", "analysis",
    "introduction", "conclusion", "closing", "context",
}


def _extract_title_from_content(analysis: str, transcript: str, url: str, platform: str, category: str) -> str | None:
    """Extract a meaningful title from the analysis or transcript."""
    import re

    if analysis:
        # Remove all HTML tags, get clean text
        clean = re.sub(r"<[^>]+>", "", analysis)
        # Split into sentences
        sentences = re.split(r"[.\n]", clean)
        for s in sentences:
            s = s.strip()
            # Skip short, generic, or header-like sentences
            if len(s) < 15 or len(s) > 120:
                continue
            if s.lower() in SKIP_TITLES:
                continue
            # Skip sentences that are just headers (all words capitalized, no verbs)
            if len(s.split()) <= 3:
                continue
            # Good candidate — truncate if needed
            if len(s) > 80:
                s = s[:77] + "..."
            return s

    # Try first sentence of transcript
    if transcript:
        sentences = re.split(r"[.!?\n]", transcript[:500])
        for s in sentences:
            s = s.strip()
            if 15 < len(s) < 80:
                return s

    # Fallback
    if category and category != "other":
        return f"{category.title()} — {platform or 'video'}"

    return None


async def fix_dates():
    """Re-import real dates from Google Sheet into SQLite entries."""
    from app.sheets import read_all_from_sheet

    db = await get_db()
    updated = 0

    for tab in ("Reels", "Youtube"):
        try:
            rows = await read_all_from_sheet(tab)
        except Exception:
            log.exception("Failed to read sheet tab %s", tab)
            continue

        for row in rows:
            url = (row.get("url") or row.get("lien") or "").strip()
            date_str = (row.get("date") or "").strip()
            if not url or not date_str:
                continue

            # Parse "DD/MM/YYYY - HH:MM" format
            try:
                parsed = datetime.strptime(date_str, "%d/%m/%Y - %H:%M")
            except ValueError:
                # Try without time
                try:
                    parsed = datetime.strptime(date_str, "%d/%m/%Y")
                except ValueError:
                    log.debug("Cannot parse date '%s' for %s", date_str, url)
                    continue

            iso_date = parsed.strftime("%Y-%m-%d %H:%M:%S")
            result = await db.execute(
                "UPDATE entries SET created_at = ? WHERE url = ?",
                (iso_date, url),
            )
            if result.rowcount > 0:
                updated += 1

    if updated:
        await db.commit()
        log.info("Fixed dates for %d entries from Google Sheet", updated)
    else:
        log.info("No dates to fix (0 matches)")


async def deduplicate_entries():
    """Remove duplicate entries (same URL), keeping the one with the longest analysis."""
    db = await get_db()

    # Find URLs that appear more than once
    dupes = await db.execute_fetchall(
        "SELECT url, COUNT(*) as cnt FROM entries WHERE url IS NOT NULL GROUP BY url HAVING cnt > 1"
    )

    deleted = 0
    for row in dupes:
        url = row["url"]
        # Get all entries for this URL, ordered by analysis length desc
        entries = await db.execute_fetchall(
            "SELECT id, LENGTH(COALESCE(analysis, '')) as alen FROM entries WHERE url = ? ORDER BY alen DESC",
            (url,),
        )
        # Keep the first (longest analysis), delete the rest
        ids_to_delete = [e["id"] for e in entries[1:]]
        if ids_to_delete:
            placeholders = ",".join("?" for _ in ids_to_delete)
            await db.execute(
                f"DELETE FROM entries WHERE id IN ({placeholders})",
                ids_to_delete,
            )
            deleted += len(ids_to_delete)

    if deleted:
        await db.commit()
        log.info("Deduplicated: removed %d duplicate entries", deleted)
    else:
        log.info("No duplicate entries found")


async def run_cleanup():
    """Run all cleanup tasks."""
    log.info("Running data cleanup...")
    await cleanup_categories()
    await deduplicate_entries()
    # Fix dates and fetch titles in background to not block startup
    asyncio.create_task(_cleanup_bg())


async def _cleanup_bg():
    """Background cleanup tasks (dates + titles)."""
    try:
        await fix_dates()
    except Exception:
        log.exception("Date fix failed")
    try:
        await generate_missing_titles()
    except Exception:
        log.exception("Title generation failed")
