"""Full ingestion pipeline: URL → download → transcribe → analyze → store."""
from __future__ import annotations

import json
import logging
import os

from app.downloader import download_and_extract
from app.transcription import transcribe
from app.llm import analyze_transcript, categorize
from app.database import insert_entry, get_entry_by_url
from app.sheets import append_to_sheet

log = logging.getLogger(__name__)


async def ingest_url(
    url: str, user_prompt: str | None = None, detail_level: str = "normal"
) -> dict:
    """
    Full pipeline for a single URL.
    Returns dict with all fields + the Telegram-formatted analysis.
    """
    # 1. Download & extract
    log.info("Downloading: %s", url)
    dl = await download_and_extract(url)
    platform = dl["platform"]
    canonical_url = dl["canonical_url"]
    title = dl.get("title")
    transcript = dl.get("transcript")
    audio_path = dl.get("audio_path")
    source_type = dl.get("source_type")
    duration = dl.get("duration")

    # Auto-adapt detail level based on duration if not explicitly set
    if detail_level == "normal" and duration:
        if duration < 180:  # < 3 min (shorts, reels)
            detail_level = "short"
        elif duration > 900:  # > 15 min (long videos)
            detail_level = "detailed"

    # 2. Transcribe if needed
    if not transcript and audio_path:
        log.info("Transcribing: %s", audio_path)
        try:
            transcript = await transcribe(audio_path)
        finally:
            # Clean up audio file
            if os.path.exists(audio_path):
                os.unlink(audio_path)
    elif audio_path and os.path.exists(audio_path):
        os.unlink(audio_path)

    if not transcript:
        return {
            "success": False,
            "error": "Could not extract transcript from this content.",
            "url": canonical_url,
            "platform": platform,
        }

    # 3. Analyze with LLM
    log.info("Analyzing: %s", canonical_url)
    analysis = await analyze_transcript(
        transcript, canonical_url, title, user_prompt, detail_level
    )

    # 4. Categorize
    log.info("Categorizing: %s", canonical_url)
    cat_data = await categorize(transcript, analysis)

    category = cat_data.get("category", "uncategorized")
    tags = json.dumps(cat_data.get("tags", []))
    language = cat_data.get("language", "en")
    key_points = json.dumps(cat_data.get("key_points", []))

    # 5. Store in database
    entry_id = await insert_entry(
        url=canonical_url,
        platform=platform,
        title=title,
        raw_transcript=transcript,
        analysis=analysis,
        key_points=key_points,
        category=category,
        tags=tags,
        language=language,
        source_type=source_type,
    )

    log.info("Stored entry #%d: %s [%s]", entry_id, canonical_url, category)

    # 6. Backup to Google Sheets
    tab = "Youtube" if platform == "youtube" else "Reels"
    await append_to_sheet({
        "url": canonical_url, "platform": platform, "raw_transcript": transcript,
        "analysis": analysis, "key_points": key_points, "category": category,
        "user_prompt": user_prompt or "",
    }, tab=tab)

    return {
        "success": True,
        "id": entry_id,
        "url": canonical_url,
        "platform": platform,
        "title": title,
        "category": category,
        "tags": cat_data.get("tags", []),
        "analysis": analysis,
        "key_points": cat_data.get("key_points", []),
        "language": language,
    }
