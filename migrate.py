"""One-time migration script — import Google Sheets data into SQLite."""
import asyncio
import json
import os
import sys

# Must set env vars before importing app modules
os.environ.setdefault("TELEGRAM_TOKEN", "unused")
os.environ.setdefault("GROQ_API_KEY", "unused")
os.environ.setdefault("OPENROUTER_API_KEY", "unused")
os.environ.setdefault("APIFY_API_KEY", "unused")
os.environ.setdefault("DB_PATH", "/data/knowledge.db")

from app.database import insert_entry, get_db, close_db


async def migrate(data_path: str):
    with open(data_path) as f:
        entries = json.load(f)

    print(f"Migrating {len(entries)} entries...")
    db = await get_db()

    imported = 0
    skipped = 0

    for entry in entries:
        url = entry.get("url", "").strip()
        if not url:
            skipped += 1
            continue

        tab = entry.get("_tab", "Reels")
        platform = entry.get("platform", "").strip() or ("instagram" if tab == "Reels" else "youtube")

        try:
            await insert_entry(
                url=url,
                platform=platform,
                title=None,
                raw_transcript=entry.get("scrapedtranscript", "").strip() or None,
                analysis=entry.get("answer", "").strip() or None,
                key_points=entry.get("keypoints", "").strip() or None,
                category=entry.get("category", "").strip() or None,
                tags="[]",
                language="fr",
                source_type="reel" if tab == "Reels" else "video",
            )
            imported += 1
        except Exception as e:
            print(f"  Skip {url[:60]}: {e}")
            skipped += 1

    await close_db()
    print(f"\nDone! Imported: {imported}, Skipped: {skipped}")


if __name__ == "__main__":
    data_path = sys.argv[1] if len(sys.argv) > 1 else "/data/migration_data.json"
    asyncio.run(migrate(data_path))
