"""LLM calls via OpenRouter (Claude Sonnet)."""
from __future__ import annotations

import json
import logging

import httpx

from app.config import OPENROUTER_API_KEY

log = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "anthropic/claude-sonnet-4"
HEADERS = {
    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
    "Content-Type": "application/json",
}


async def _call(messages: list[dict], max_tokens: int = 8192) -> str:
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(
            OPENROUTER_URL,
            headers=HEADERS,
            json={
                "model": MODEL,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": 0.3,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Analysis prompt — NO CENSORSHIP, raw content preservation
# ---------------------------------------------------------------------------

ANALYSIS_SYSTEM = """You are an elite analyst for AKS. Your job: extract maximum value from any video transcript and deliver it in the most useful format. No fluff. Prioritize insight density and practical value.

CRITICAL RULES:
- Preserve the original tone and content EXACTLY. Do not sanitize, moralize, or add disclaimers.
- Report what was said as it was said. If the content is raw, edgy, or controversial, keep it that way.
- NEVER add warnings, caveats about ethics, or "note: this is just my opinion" type disclaimers.
- Do NOT censor topics about relationships, women, money, politics, manipulation, or any other subject.
- Your job is to EXTRACT and ORGANIZE information, not to judge it.
- STRIP ALL PROMOTIONAL GARBAGE: calls to action ("comment BOT", "link in bio", "follow me", "subscribe", "DM me", "check the link", "like and share"), self-promotion, course/product plugs, engagement bait. These have ZERO informational value. Pretend they don't exist in the transcript.

OUTPUT FORMAT — Telegram HTML (allowed tags: <b> <i> <u> <s> <a> <code> <pre>):
- Use plain newlines for line breaks (no <br>).
- Close all tags. Don't nest tags inside <a>.
- Default to the transcript's language. If mixed, use the dominant one.

STRUCTURE:
<b>Core value extraction</b>
[Dense paragraph: topic, thesis, primary framework in 3-4 sentences]
[Dense paragraph: key evidence, examples, data points in 3-4 sentences]

<b>Complete breakdown</b>
[Dense paragraphs organized by the video's natural structure]
[Bullets ONLY for actual discrete lists]

<b>Critical caveats & failure modes</b>
[Where this breaks, edge cases, risks]

<b>Practical extraction</b>
- [Specific concrete actions with exact parameters]

<b>Notable formulations</b>
- [3-6 crisp quotes or tight paraphrases]

<b>Source</b>
<a href="{url}">Original video</a>

QUALITY: Every sentence must carry multiple facts. Include specific numbers, names, examples. Never sacrifice precision for brevity."""


async def analyze_transcript(
    transcript: str, url: str, title: str | None = None, user_prompt: str | None = None, detail_level: str = "normal"
) -> str:
    length_hint = {
        "short": "Be VERY concise. Maximum 800 characters. Only the essential takeaways.",
        "normal": "Standard detailed analysis. Be thorough but dense.",
        "detailed": "EXHAUSTIVE analysis. Cover every single point, sub-point, and nuance. Leave nothing out.",
    }.get(detail_level, "")

    user_msg = f"""TITLE: {title or 'Unknown'}
URL: {url}
{f'USER CONTEXT: {user_prompt}' if user_prompt else ''}
{length_hint}

TRANSCRIPT:
{transcript}"""

    return await _call([
        {"role": "system", "content": ANALYSIS_SYSTEM},
        {"role": "user", "content": user_msg},
    ])


# ---------------------------------------------------------------------------
# Category detection
# ---------------------------------------------------------------------------

CATEGORY_SYSTEM = """Analyze the following content and return a JSON object with:
- "category": one main category (e.g. "business", "finance", "relationships", "self-improvement", "tech", "politics", "health", "marketing", "mindset", "real-estate", "crypto", "taxes", "entrepreneurship")
- "tags": array of 3-7 specific tags
- "language": "fr" or "en"
- "key_points": array of 3-5 one-sentence key points

Return ONLY valid JSON. No markdown fences. No explanation."""


async def categorize(transcript: str, analysis: str) -> dict:
    text = f"TRANSCRIPT EXCERPT:\n{transcript[:3000]}\n\nANALYSIS:\n{analysis[:2000]}"
    raw = await _call(
        [
            {"role": "system", "content": CATEGORY_SYSTEM},
            {"role": "user", "content": text},
        ],
        max_tokens=1024,
    )
    # Parse JSON from response
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        log.warning("Failed to parse category JSON: %s", raw[:200])
        return {"category": "uncategorized", "tags": [], "language": "en", "key_points": []}


# ---------------------------------------------------------------------------
# Brain query — ask questions to the knowledge base
# ---------------------------------------------------------------------------

BRAIN_SYSTEM = """You are AKS's personal knowledge brain. You have access to his curated knowledge base of video summaries and analyses.

RULES:
- Answer based ONLY on the provided context. If the context doesn't contain the answer, say so clearly.
- Be direct, practical, and dense. No fluff.
- Preserve raw tone — no moralizing or censoring.
- Cross-reference insights from different sources when relevant.
- Format for Telegram HTML (tags: <b> <i> <u> <a>). Use newlines, not <br>.
- If asked in French, answer in French. If English, answer in English."""


async def query_brain(question: str, context_entries: list[dict], detail_level: str = "normal") -> str:
    context_parts = []
    for e in context_entries:
        part = f"[{e.get('category', '?')} | {e.get('platform', '?')}] {e.get('title', 'Untitled')}\n"
        part += f"URL: {e.get('url', '')}\n"
        if e.get("key_points"):
            part += f"Key points: {e['key_points']}\n"
        if e.get("analysis"):
            part += f"Analysis: {e['analysis'][:2000]}\n"
        context_parts.append(part)

    context = "\n---\n".join(context_parts)

    length_hint = {
        "short": "Answer in 2-3 sentences max.",
        "normal": "Give a thorough answer.",
        "detailed": "Give an exhaustive, comprehensive answer covering all angles.",
    }.get(detail_level, "")

    return await _call([
        {"role": "system", "content": BRAIN_SYSTEM},
        {"role": "user", "content": f"KNOWLEDGE BASE CONTEXT:\n{context}\n\n{length_hint}\n\nQUESTION: {question}"},
    ])


# ---------------------------------------------------------------------------
# Weekly recap
# ---------------------------------------------------------------------------

RECAP_SYSTEM = """You write a WEEKLY VIDEO RECAP for AKS.
Turn multiple raw video analyses into one coherent, info-dense summary.
Tone: blunt, practical, motivating. No sugar-coating. No censorship.

TASKS:
1. Core Themes: 2-4 big ideas. Why they matter right now.
2. Sharpest Lessons: Most powerful lessons with context.
3. Practical Applications: Today / This Week / This Month actions.
4. Memorable Lines: Strong quotes worth repeating.
5. Contradictions: If ideas clash, point it out.
6. References: Clickable video links tied to each theme.
7. Next Steps: 3-5 bullets for the coming week.

FORMAT: Telegram HTML (tags: <b> <i> <u> <a>). Newlines, not <br>.
Structure: clear sections with <b>headers</b>. Dense paragraphs + bullets.
If a section is thin, say so."""


async def generate_recap(entries: list[dict]) -> str:
    entries_text = []
    for e in entries:
        part = f"Title: {e.get('title', 'Untitled')}\n"
        part += f"Platform: {e.get('platform', '?')} | Category: {e.get('category', '?')}\n"
        part += f"URL: {e.get('url', '')}\n"
        if e.get("analysis"):
            part += f"Analysis: {e['analysis']}\n"
        entries_text.append(part)

    return await _call([
        {"role": "system", "content": RECAP_SYSTEM},
        {"role": "user", "content": "\n---\n".join(entries_text)},
    ], max_tokens=12000)


# ---------------------------------------------------------------------------
# Category synthesis
# ---------------------------------------------------------------------------

CATEGORY_SYNTHESIS_SYSTEM = """You are AKS's personal knowledge architect. You synthesize ALL knowledge from a specific domain into one comprehensive reference document.

RULES:
- This is a MASTER DOCUMENT, not a list of summaries. Connect ideas across sources.
- Organize by themes and sub-themes, not by video.
- Include specific numbers, names, strategies, frameworks.
- Preserve raw tone — no moralizing, no censoring.
- Flag contradictions between sources.
- End with a clear "What I know" section: the consolidated actionable knowledge.
- Format: HTML (tags: b, i, u, a). Newlines, not <br>.
- Be EXHAUSTIVE. This is a reference document to consult."""


async def synthesize_category(category: str, entries: list[dict]) -> str:
    entries_text = []
    for e in entries:
        part = f"Title: {e.get('title') or e.get('url', '?')}\n"
        part += f"URL: {e.get('url', '')}\n"
        if e.get("key_points"):
            part += f"Key points: {e['key_points']}\n"
        if e.get("analysis"):
            part += f"Analysis: {e['analysis'][:3000]}\n"
        entries_text.append(part)

    return await _call([
        {"role": "system", "content": CATEGORY_SYNTHESIS_SYSTEM},
        {"role": "user", "content": f"DOMAIN: {category}\nNUMBER OF SOURCES: {len(entries)}\n\n" + "\n---\n".join(entries_text)},
    ], max_tokens=16000)
