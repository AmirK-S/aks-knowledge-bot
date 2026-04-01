"""Transcription via Groq Whisper API (free, whisper-large-v3-turbo)."""
import logging
import os

import httpx

from app.config import GROQ_API_KEY

log = logging.getLogger(__name__)

GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
MAX_FILE_SIZE = 25 * 1024 * 1024  # 25 MB limit


async def transcribe(audio_path: str) -> str:
    """Transcribe an audio file using Groq Whisper. Returns transcript text."""
    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Audio file not found: {audio_path}")

    file_size = os.path.getsize(audio_path)
    if file_size > MAX_FILE_SIZE:
        log.warning("File %s is %.1f MB, splitting", audio_path, file_size / 1e6)
        return await _transcribe_chunked(audio_path)

    return await _transcribe_single(audio_path)


async def _transcribe_single(audio_path: str) -> str:
    """Single file transcription."""
    filename = os.path.basename(audio_path)

    async with httpx.AsyncClient(timeout=300) as client:
        with open(audio_path, "rb") as f:
            resp = await client.post(
                GROQ_URL,
                headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
                files={"file": (filename, f, "audio/mp4")},
                data={
                    "model": "whisper-large-v3-turbo",
                    "response_format": "text",
                },
            )
        resp.raise_for_status()
        return resp.text.strip()


async def _transcribe_chunked(audio_path: str) -> str:
    """Split large audio files and transcribe each chunk."""
    import asyncio
    import tempfile

    chunk_dir = tempfile.mkdtemp()
    chunk_pattern = os.path.join(chunk_dir, "chunk_%03d.m4a")

    # Split into 10-minute chunks with ffmpeg
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-i", audio_path,
        "-f", "segment", "-segment_time", "600",
        "-c:a", "aac", "-b:a", "64k",
        chunk_pattern, "-y",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()

    # Transcribe each chunk
    chunks = sorted(
        f for f in os.listdir(chunk_dir) if f.startswith("chunk_")
    )

    transcripts = []
    for chunk_file in chunks:
        chunk_path = os.path.join(chunk_dir, chunk_file)
        try:
            text = await _transcribe_single(chunk_path)
            transcripts.append(text)
        finally:
            os.unlink(chunk_path)

    os.rmdir(chunk_dir)
    return " ".join(transcripts)
