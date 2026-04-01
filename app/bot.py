"""AKS Knowledge Brain — Telegram Bot."""
import asyncio
import json
import logging
import re
import sys

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from app.config import TELEGRAM_TOKEN, OWNER_CHAT_ID
from app.database import (
    search_entries, get_all_categories, get_entries_by_category,
    get_stats, get_recent_entries, get_random_entry, close_db,
)
from app.ingestion import ingest_url
from app.llm import query_brain, generate_recap
from app.telegram_utils import chunk_message, format_entry_short

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("aks_brain")

bot = Bot(token=TELEGRAM_TOKEN, default=types.DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

URL_RE = re.compile(r"https?://[^\s<>\"'`)\]]+", re.I)
FLAG_RE = re.compile(r"\s*--(short|s|detailed|d|raw|r)\b", re.I)


def _extract_flags(text: str) -> tuple[str, str]:
    """Extract detail level flag from message text. Returns (cleaned_text, detail_level)."""
    detail = "normal"
    flags = FLAG_RE.findall(text)
    for f in flags:
        f = f.lower()
        if f in ("short", "s"):
            detail = "short"
        elif f in ("detailed", "d"):
            detail = "detailed"
        elif f in ("raw", "r"):
            detail = "raw"
    cleaned = FLAG_RE.sub("", text).strip()
    return cleaned, detail


def _is_owner(msg: Message) -> bool:
    return msg.from_user and msg.from_user.id == OWNER_CHAT_ID


async def _send(msg: Message, text: str):
    """Send a potentially long message, chunking if needed."""
    for chunk in chunk_message(text):
        try:
            await msg.answer(chunk, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception:
            # Fallback: send without HTML parsing if tags are broken
            await msg.answer(chunk, parse_mode=None)
        await asyncio.sleep(0.3)  # Rate limiting


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@dp.message(CommandStart())
async def cmd_start(msg: Message):
    if not _is_owner(msg):
        return
    await msg.answer(
        "<b>AKS Knowledge Brain</b>\n\n"
        "Send me any video link and I'll analyze it.\n\n"
        "<b>Commands:</b>\n"
        "/search <query> — Search knowledge base\n"
        "/ask <question> — Ask your brain\n"
        "/recap — Weekly recap\n"
        "/categories — List categories\n"
        "/browse <category> — Browse category\n"
        "/stats — Stats\n"
        "/random — Random entry\n\n"
        "<b>Flags:</b> add --short, --detailed, or --raw to any message"
    )


@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not _is_owner(msg):
        return
    stats = await get_stats()
    platforms = "\n".join(f"  {p}: {c}" for p, c in stats["platforms"].items())
    await msg.answer(
        f"<b>Knowledge Base Stats</b>\n\n"
        f"Total entries: <b>{stats['total']}</b>\n"
        f"This week: <b>{stats['this_week']}</b>\n\n"
        f"<b>By platform:</b>\n{platforms}"
    )


@dp.message(Command("categories"))
async def cmd_categories(msg: Message):
    if not _is_owner(msg):
        return
    cats = await get_all_categories()
    if not cats:
        await msg.answer("No categories yet. Start sending links!")
        return
    lines = [f"  <b>{c['category']}</b> ({c['cnt']})" for c in cats]
    await msg.answer("<b>Categories</b>\n\n" + "\n".join(lines))


@dp.message(Command("browse"))
async def cmd_browse(msg: Message):
    if not _is_owner(msg):
        return
    query = msg.text.replace("/browse", "").strip()
    if not query:
        await msg.answer("Usage: /browse <category>")
        return
    entries = await get_entries_by_category(query)
    if not entries:
        await msg.answer(f"No entries found for category: {query}")
        return
    lines = [format_entry_short(e) for e in entries]
    await _send(msg, f"<b>Category: {query}</b> ({len(entries)} entries)\n\n" + "\n\n".join(lines))


@dp.message(Command("search"))
async def cmd_search(msg: Message):
    if not _is_owner(msg):
        return
    query = msg.text.replace("/search", "").strip()
    if not query:
        await msg.answer("Usage: /search <query>")
        return
    entries = await search_entries(query)
    if not entries:
        await msg.answer(f"No results for: {query}")
        return
    lines = [format_entry_short(e) for e in entries]
    await _send(msg, f"<b>Search: {query}</b> ({len(entries)} results)\n\n" + "\n\n".join(lines))


@dp.message(Command("ask"))
async def cmd_ask(msg: Message):
    if not _is_owner(msg):
        return
    raw_text = msg.text.replace("/ask", "").strip()
    if not raw_text:
        await msg.answer("Usage: /ask <question>")
        return

    question, detail = _extract_flags(raw_text)
    await msg.answer("Searching brain...")

    # Search for relevant entries
    entries = await search_entries(question, limit=8)
    if not entries:
        # Fallback: get recent entries
        entries = await get_recent_entries(days=30, limit=10)

    if not entries:
        await msg.answer("Knowledge base is empty. Send me some links first!")
        return

    answer = await query_brain(question, entries, detail)
    await _send(msg, answer)


@dp.message(Command("recap"))
async def cmd_recap(msg: Message):
    if not _is_owner(msg):
        return
    await msg.answer("Generating weekly recap...")
    entries = await get_recent_entries(days=7)
    if not entries:
        await msg.answer("No entries from the past 7 days.")
        return
    recap = await generate_recap(entries)
    await _send(msg, recap)
    await msg.answer("Recap sent!")


@dp.message(Command("random"))
async def cmd_random(msg: Message):
    if not _is_owner(msg):
        return
    entry = await get_random_entry()
    if not entry:
        await msg.answer("Knowledge base is empty.")
        return
    text = f"<b>Random entry</b>\n\n"
    text += f"<b>{entry.get('title') or 'Untitled'}</b>\n"
    text += f"Category: {entry.get('category', '?')} | Platform: {entry.get('platform', '?')}\n\n"
    if entry.get("key_points"):
        try:
            points = json.loads(entry["key_points"])
            if isinstance(points, list):
                text += "<b>Key points:</b>\n" + "\n".join(f"- {p}" for p in points) + "\n\n"
        except (json.JSONDecodeError, TypeError):
            pass
    if entry.get("analysis"):
        text += entry["analysis"]
    await _send(msg, text)


# ---------------------------------------------------------------------------
# URL handler — main ingestion
# ---------------------------------------------------------------------------

@dp.message(F.text)
async def handle_message(msg: Message):
    if not _is_owner(msg):
        return

    text = msg.text or ""
    urls = URL_RE.findall(text)

    if not urls:
        # No URL — treat as a question to the brain
        question, detail = _extract_flags(text)
        if question:
            await msg.answer("Thinking...")
            entries = await search_entries(question, limit=8)
            if not entries:
                entries = await get_recent_entries(days=30, limit=10)
            if entries:
                answer = await query_brain(question, entries, detail)
                await _send(msg, answer)
            else:
                await msg.answer("Knowledge base is empty. Send me some links first!")
        return

    # Extract prompt (text without URLs and flags)
    prompt_text = text
    for u in urls:
        prompt_text = prompt_text.replace(u, "")
    prompt_text, detail = _extract_flags(prompt_text)
    prompt_text = prompt_text.strip() or None

    for url in urls:
        # Clean URL
        url = url.rstrip(",.;:!?)")
        status_msg = await msg.answer(f"Processing: {url}")

        try:
            result = await ingest_url(url, user_prompt=prompt_text, detail_level=detail)

            if result.get("success"):
                # Send analysis
                analysis = result.get("analysis", "No analysis generated.")
                header = (
                    f"<b>{result.get('title') or 'Untitled'}</b>\n"
                    f"Category: <b>{result.get('category', '?')}</b> | "
                    f"Platform: {result.get('platform', '?')}\n"
                    f"Tags: {', '.join(result.get('tags', []))}\n\n"
                )
                await _send(msg, header + analysis)
            else:
                await msg.answer(f"Failed: {result.get('error', 'Unknown error')}")

        except Exception as e:
            log.exception("Error processing %s", url)
            await msg.answer(f"Error processing {url}: {str(e)[:200]}")

        # Delete "Processing..." message
        try:
            await status_msg.delete()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _health_server():
    """Minimal HTTP health check server for Coolify."""
    from asyncio import start_server

    async def handle(reader, writer):
        await reader.read(1024)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
        await writer.drain()
        writer.close()

    server = await start_server(handle, "0.0.0.0", 8443)
    log.info("Health check server on :8443")
    return server


async def main():
    log.info("Starting AKS Knowledge Brain bot...")
    health = await _health_server()
    try:
        await dp.start_polling(bot)
    finally:
        health.close()
        await close_db()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
