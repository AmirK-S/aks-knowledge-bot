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


async def generate_missing_titles():
    """Generate titles from analysis text for entries without titles."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, url, platform, analysis, raw_transcript, category FROM entries WHERE (title IS NULL OR title = '') AND url IS NOT NULL"
    )
    if not rows:
        return

    log.info("Generating titles for %d entries...", len(rows))
    updated = 0

    for row in rows:
        r = dict(row)
        title = _extract_title_from_content(
            r.get("analysis") or "",
            r.get("raw_transcript") or "",
            r.get("url") or "",
            r.get("platform") or "",
            r.get("category") or "",
        )
        if title:
            await db.execute("UPDATE entries SET title = ? WHERE id = ?", (title, r["id"]))
            updated += 1

    await db.commit()
    log.info("Generated %d titles", updated)


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


async def run_cleanup():
    """Run all cleanup tasks."""
    log.info("Running data cleanup...")
    await cleanup_categories()
    # Fetch titles in background to not block startup
    asyncio.create_task(_fetch_titles_bg())


async def _fetch_titles_bg():
    """Background task to generate titles."""
    try:
        await generate_missing_titles()
    except Exception:
        log.exception("Title generation failed")
