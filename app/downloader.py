"""Download videos and extract subtitles/audio from various platforms."""
from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
import logging

import httpx

from app.config import APIFY_API_KEY

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL parsing helpers
# ---------------------------------------------------------------------------

_PLATFORM_PATTERNS = {
    "youtube": re.compile(r"(youtube\.com|youtu\.be)", re.I),
    "instagram": re.compile(r"instagram\.com", re.I),
    "tiktok": re.compile(r"tiktok\.com", re.I),
    "twitter_x": re.compile(r"(twitter\.com|x\.com)", re.I),
    "facebook": re.compile(r"(facebook\.com|fb\.watch)", re.I),
    "reddit": re.compile(r"reddit\.com", re.I),
    "linkedin": re.compile(r"linkedin\.com", re.I),
}


def detect_platform(url: str) -> str | None:
    for name, pat in _PLATFORM_PATTERNS.items():
        if pat.search(url):
            return name
    return None


def extract_youtube_id(url: str) -> str | None:
    patterns = [
        re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})"),
        re.compile(r"/(?:embed|shorts|live|v)/([A-Za-z0-9_-]{11})"),
    ]
    for p in patterns:
        m = p.search(url)
        if m:
            return m.group(1)
    return None


def canonicalize_url(url: str, platform: str | None = None) -> str:
    if platform == "youtube":
        vid = extract_youtube_id(url)
        if vid:
            return f"https://www.youtube.com/watch?v={vid}"
    # Strip tracking params
    for param in ("igshid", "igsh", "si", "utm_source", "utm_medium", "utm_campaign"):
        url = re.sub(rf"[?&]{param}=[^&]*", "", url)
    url = url.replace("?&", "?").rstrip("?&")
    return url


# ---------------------------------------------------------------------------
# YouTube: subtitles via yt-dlp (no download needed!)
# ---------------------------------------------------------------------------


async def _youtube_apify_transcript(video_id: str) -> tuple[str | None, str | None, int | None]:
    """Get YouTube transcript via Apify actor (reliable, works from any IP).
    Returns (transcript, title, duration)."""
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                "https://api.apify.com/v2/acts/karamelo~youtube-transcripts/run-sync-get-dataset-items",
                headers={
                    "Authorization": f"Bearer {APIFY_API_KEY}",
                    "Accept": "application/json",
                },
                json={"urls": [f"https://www.youtube.com/watch?v={video_id}"]},
            )
            if resp.status_code not in (200, 201):
                log.warning("Apify YouTube returned %s: %s", resp.status_code, resp.text[:300])
                return None, None, None
            data = resp.json()

        if not data:
            log.warning("Apify YouTube transcript returned empty for %s", video_id)
            return None, None, None

        item = data[0] if isinstance(data, list) else data
        title = item.get("title")

        # Actor returns captions as list of strings
        captions = item.get("captions", [])
        if isinstance(captions, list) and captions:
            transcript = " ".join(str(c) for c in captions)
            # Clean HTML entities
            transcript = transcript.replace("&#39;", "'").replace("&amp;", "&").replace("&quot;", '"')
        else:
            transcript = None

        duration = None  # Actor doesn't return duration

        if transcript and len(transcript) > 50:
            log.info("Got transcript via Apify for %s (%d chars)", video_id, len(transcript))
            return transcript, title, duration
        else:
            log.warning("Apify transcript too short for %s", video_id)

    except Exception as e:
        log.warning("Apify YouTube transcript failed for %s: %s", video_id, e)

    return None, None, None


async def _youtube_title_oembed(video_id: str) -> str | None:
    """Get title via oEmbed API (reliable, no bot detection)."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
            )
            if r.status_code == 200:
                return r.json().get("title")
    except Exception:
        pass
    return None


async def youtube_get_subtitles(url: str) -> dict:
    """Try to get subtitles without downloading the video. Returns {title, transcript} or {title, audio_path}."""
    canonical = canonicalize_url(url, "youtube")
    video_id = extract_youtube_id(url)

    # 1. Try Apify transcript (reliable, works from any IP)
    transcript, title, duration = await _youtube_apify_transcript(video_id)
    if transcript:
        if not title:
            title = await _youtube_title_oembed(video_id)
        return {"title": title, "transcript": transcript, "audio_path": None, "duration": duration}

    # 2. Try yt-dlp subtitles (may fail on bot-blocked IPs)
    if not title:
        title = await _youtube_title_oembed(video_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        sub_path = os.path.join(tmpdir, "subs")
        cmd = [
            "yt-dlp",
            "--no-download",
            "--write-auto-sub",
            "--write-sub",
            "--sub-lang", "en,fr,en-orig",
            "--sub-format", "srt/vtt/best",
            "--convert-subs", "srt",
            "-o", sub_path,
            "--no-warnings",
            "--extractor-args", "youtube:player_client=ios,web",
            canonical,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0 and stderr:
            log.warning("yt-dlp subs failed (rc=%s): %s", proc.returncode, stderr.decode()[:300])
        if not duration and stdout:
            # try to parse duration from yt-dlp output
            pass

        for lang in ("en", "fr", "en-orig"):
            for ext in ("srt", "vtt"):
                candidate = f"{sub_path}.{lang}.{ext}"
                if os.path.exists(candidate):
                    with open(candidate) as f:
                        raw = f.read()
                    transcript = _clean_srt(raw)
                    if transcript and len(transcript) > 50:
                        log.info("Got subtitles via yt-dlp for %s (lang=%s)", canonical, lang)
                        return {"title": title, "transcript": transcript, "audio_path": None, "duration": duration}

        # 3. Download audio for Whisper transcription
        log.info("No subtitles for %s, trying audio download", canonical)
        audio_path = os.path.join(tmpdir, "audio.m4a")
        cmd2 = [
            "yt-dlp",
            "-f", "ba[ext=m4a]/ba/b",
            "--no-playlist",
            "-o", audio_path,
            "--extractor-args", "youtube:player_client=ios,web",
            canonical,
        ]
        proc2 = await asyncio.create_subprocess_exec(
            *cmd2, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr2 = await proc2.communicate()

        if os.path.exists(audio_path):
            import shutil
            persistent = tempfile.mktemp(suffix=".m4a")
            shutil.copy2(audio_path, persistent)
            return {"title": title, "transcript": None, "audio_path": persistent, "duration": duration}

        log.warning("yt-dlp audio download failed for %s (rc=%s): %s",
                     canonical, proc2.returncode, stderr2.decode()[:500] if stderr2 else "no stderr")

        return {"title": title, "transcript": None, "audio_path": None, "duration": None}


def _clean_srt(raw: str) -> str:
    """Strip SRT formatting, return plain text."""
    lines = raw.replace("\r", "").split("\n")
    ts_re = re.compile(r"^\s*\d{1,2}:\d{2}:\d{1,2}[.,]\d{3}\s*-->")
    idx_re = re.compile(r"^\s*\d+\s*$")
    tag_re = re.compile(r"<[^>]+>")
    out = []
    for line in lines:
        if idx_re.match(line) or ts_re.match(line) or not line.strip():
            continue
        cleaned = tag_re.sub("", line).strip()
        if cleaned:
            out.append(cleaned)
    return " ".join(out).strip()


# ---------------------------------------------------------------------------
# Instagram: via Apify
# ---------------------------------------------------------------------------


async def instagram_get_audio(url: str) -> dict:
    """Use Apify to get Instagram reel video URL, download audio."""
    canonical = canonicalize_url(url, "instagram")

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            "https://api.apify.com/v2/acts/xMc5Ga1oCONPmWJIa/run-sync-get-dataset-items",
            headers={
                "Authorization": f"Bearer {APIFY_API_KEY}",
                "Accept": "application/json",
            },
            json={"includeSharesCount": False, "username": [canonical]},
        )
        resp.raise_for_status()
        data = resp.json()

    if not data:
        return {"title": None, "transcript": None, "audio_path": None, "duration": None}

    item = data[0] if isinstance(data, list) else data
    video_url = item.get("videoUrl") or item.get("video_url")
    title = item.get("caption", "")[:200] if item.get("caption") else None

    if not video_url:
        return {"title": title, "transcript": None, "audio_path": None, "duration": None, "video_url": None}

    # Download and extract audio with yt-dlp
    audio_path = tempfile.mktemp(suffix=".m4a")
    cmd = [
        "yt-dlp",
        "-f", "ba/b",
        "-o", audio_path,
        "--no-warnings",
        video_url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()

    if os.path.exists(audio_path):
        return {"title": title, "transcript": None, "audio_path": audio_path, "duration": None}

    # Fallback: download raw video and extract with ffmpeg
    video_path = tempfile.mktemp(suffix=".mp4")
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.get(video_url)
        with open(video_path, "wb") as f:
            f.write(resp.content)

    audio_path2 = tempfile.mktemp(suffix=".m4a")
    proc2 = await asyncio.create_subprocess_exec(
        "ffmpeg", "-i", video_path, "-vn", "-c:a", "aac", "-b:a", "128k", audio_path2, "-y",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc2.communicate()
    os.unlink(video_path)

    if os.path.exists(audio_path2):
        return {"title": title, "transcript": None, "audio_path": audio_path2, "duration": None}

    return {"title": title, "transcript": None, "audio_path": None, "duration": None}


# ---------------------------------------------------------------------------
# Generic: yt-dlp handles TikTok, Twitter, Facebook, etc.
# ---------------------------------------------------------------------------


async def generic_get_audio(url: str) -> dict:
    """Use yt-dlp for any supported platform."""
    audio_path = tempfile.mktemp(suffix=".m4a")
    cmd = [
        "yt-dlp",
        "-f", "ba[ext=m4a]/ba/b",
        "--no-playlist",
        "-o", audio_path,
        "--print", "%(title)s",
        "--no-warnings",
        url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    title = stdout.decode().strip().split("\n")[0] if stdout else None

    if os.path.exists(audio_path):
        return {"title": title, "transcript": None, "audio_path": audio_path, "duration": None}
    return {"title": title, "transcript": None, "audio_path": None, "duration": None}


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------


async def download_and_extract(url: str) -> dict:
    """Returns {platform, title, transcript, audio_path, canonical_url, source_type}."""
    platform = detect_platform(url)
    canonical = canonicalize_url(url, platform)

    if platform == "youtube":
        result = await youtube_get_subtitles(url)
        source_type = "short" if "/shorts/" in url else "video"
    elif platform == "instagram":
        result = await instagram_get_audio(url)
        source_type = "reel" if "/reel/" in url else "post"
    else:
        result = await generic_get_audio(url)
        source_type = "video"

    return {
        "platform": platform,
        "canonical_url": canonical,
        "source_type": source_type,
        **result,
    }
