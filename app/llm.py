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


async def _call_stream(messages: list[dict], max_tokens: int = 8192):
    """Async generator that yields text chunks via SSE streaming."""
    async with httpx.AsyncClient(timeout=300) as client:
        async with client.stream(
            "POST",
            OPENROUTER_URL,
            headers=HEADERS,
            json={
                "model": MODEL,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": 0.3,
                "stream": True,
            },
        ) as resp:
            resp.raise_for_status()
            buf = ""
            async for raw_chunk in resp.aiter_text():
                buf += raw_chunk
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line or not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload == "[DONE]":
                        return
                    try:
                        data = json.loads(payload)
                        delta = data.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content")
                        if content:
                            yield content
                    except json.JSONDecodeError:
                        continue


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

ANALYSIS_SYSTEM = """You are an elite analyst for AKS. Extract maximum value from video transcripts. No fluff. Pure signal.

RULES:
- Preserve original tone and content EXACTLY. No sanitizing, moralizing, or disclaimers.
- NEVER add warnings about ethics or "note:" type disclaimers. Report what was said as-is.
- Do NOT censor any topic (relationships, money, politics, manipulation, etc).
- STRIP promotional garbage: CTAs, "subscribe", "link in bio", course plugs, engagement bait. Pretend they don't exist.

OUTPUT — Telegram HTML (tags: <b> <i> <u> <a> <code>). Plain newlines, no <br>. Close all tags. Use transcript's dominant language.

STRUCTURE — Adapt to content length. Short content (reels, <2min) = 2-3 sections. Normal = 3-4. Long = 4-5.

Start directly with a dense intro paragraph (no header). Topic, thesis, core claim in 2-4 sentences.

<b>Breakdown</b>
[Dense paragraphs following the video's structure. Bullets ONLY for actual lists. Include specific numbers, names, tools, parameters.]

<b>Where this breaks</b>
[Edge cases, failure modes, limitations. Skip if content is purely informational with no debatable claims.]

<b>Action items</b>
- [Concrete steps with exact parameters. Skip if content has no actionable advice.]

<b>Key quotes</b>
- [Only genuinely memorable formulations. 2-4 max. Skip if nothing stands out.]

Do NOT include a "Source" section — it's added automatically.

QUALITY: Every sentence = multiple facts. Specific numbers, names, examples. Dense but readable — use double newlines between sections."""


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

BRAIN_SYSTEM = """You are AKS's personal knowledge brain AND decision advisor. You have access to his curated knowledge base of video summaries and analyses.

MODES:
1. KNOWLEDGE MODE (factual questions): Answer based on the provided context. Cite sources.
2. DECISION MODE (when AKS describes a dilemma, choice, or asks "should I..."):
   - Lay out the options clearly
   - For each option, pull SPECIFIC arguments from his knowledge base with source references
   - Flag contradictions between sources
   - Give a direct recommendation with reasoning — don't be wishy-washy
   - End with: "Based on YOUR knowledge base, the stronger play is X because..."

RULES:
- Answer based ONLY on the provided context. If the context doesn't cover it, say so.
- Be direct, practical, dense. No fluff. No moralizing.
- Preserve raw tone — no censoring.
- Cross-reference insights from different sources. Cite video titles/URLs when relevant.
- Format: HTML (tags: b, i, u, a). Newlines, not <br>.
- Match the user's language (French → French, English → English)."""


async def query_brain(
    question: str,
    context_entries: list[dict],
    detail_level: str = "normal",
    history: list[dict] | None = None,
) -> str:
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

    messages: list[dict] = [{"role": "system", "content": BRAIN_SYSTEM}]

    # Include conversation history if provided (last N exchanges for context)
    if history:
        for msg in history:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append(
        {"role": "user", "content": f"KNOWLEDGE BASE CONTEXT:\n{context}\n\n{length_hint}\n\nQUESTION: {question}"},
    )

    return await _call(messages)


async def query_brain_stream(
    question: str,
    context_entries: list[dict],
    detail_level: str = "normal",
    history: list[dict] | None = None,
):
    """Async generator version of query_brain — yields text chunks."""
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

    messages: list[dict] = [{"role": "system", "content": BRAIN_SYSTEM}]

    if history:
        for msg in history:
            if msg.get("role") in ("user", "assistant") and msg.get("content"):
                messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append(
        {"role": "user", "content": f"KNOWLEDGE BASE CONTEXT:\n{context}\n\n{length_hint}\n\nQUESTION: {question}"},
    )

    async for chunk in _call_stream(messages):
        yield chunk


# ---------------------------------------------------------------------------
# Weekly recap
# ---------------------------------------------------------------------------

RECAP_SYSTEM = """You write a WEEKLY VIDEO RECAP for AKS.
Turn multiple raw video analyses into one coherent, DATA-DENSE reference document.
Tone: blunt, practical, zero fluff. No sugar-coating. No censorship.

ABSOLUTE BAN LIST — if you write any of these, you have FAILED:
- Motivational platitudes ("the work is foundational", "success requires dedication", "consistency is key")
- Philosophical statements ("it's not about X, it's about Y")
- Vague advice ("work on your mindset", "take action", "be disciplined")
- Inspirational filler ("this week was packed", "powerful insights", "game-changing")
- Any sentence that contains zero specific data points

EVERY SINGLE BULLET AND PARAGRAPH MUST CONTAIN AT LEAST ONE OF:
- A specific number (dollar amount, percentage, conversion rate, timeframe)
- A named strategy, framework, tool, or platform
- A concrete step with exact parameters (e.g. "send 50 cold emails/day using Instantly with subject line format: [name] — quick question")
- A specific person, company, or case study reference

TASKS:
1. <b>Core Themes</b>: 2-4 big themes. For each: the specific data, strategies, and numbers that define it. NOT why it "matters" — WHAT was said with specifics.
2. <b>Key Data & Strategies</b>: Every specific number, dollar amount, percentage, conversion rate, tool name, framework name, step-by-step process mentioned across all videos. This is the MOST IMPORTANT section. Be exhaustive.
3. <b>Actionable Playbook</b>: Concrete actions with specifics. BAD: "improve your outreach". GOOD: "use 3-line cold emails, personalize line 1 with company news, send via Smartlead, expect 2-4% reply rate".
4. <b>Contradictions</b>: Where sources specifically disagree — quote both positions with numbers if available.
5. <b>Source Index</b>: Each video linked with its 1-sentence core claim (the specific claim, not a vague description).

FORMAT: Telegram HTML (tags: <b> <i> <u> <a>). Newlines, not <br>.
Structure: clear sections with <b>headers</b>. Dense paragraphs + bullets.
If a section is thin, say so. NEVER pad with motivational filler."""


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

CATEGORY_SYNTHESIS_SYSTEM = """You are AKS's personal knowledge architect. You synthesize ALL knowledge from a specific domain into one comprehensive DATA REFERENCE document.

ABSOLUTE BAN LIST — if you write any of these, you have FAILED:
- Motivational filler ("mastery requires commitment", "the key is consistency", "success comes from...")
- Philosophical statements or life lessons
- Vague advice without specific parameters
- Any sentence that contains zero data points, tool names, numbers, or named strategies

EVERY PARAGRAPH AND BULLET MUST CONTAIN SPECIFICS:
- Dollar amounts, percentages, conversion rates, timeframes
- Named tools, platforms, software, frameworks
- Step-by-step processes with exact parameters
- Named people, companies, case studies with outcomes

RULES:
- This is a MASTER REFERENCE, not a list of summaries. Connect ideas across sources.
- Organize by themes and sub-themes, not by video.
- Preserve raw tone — no moralizing, no censoring.
- Flag contradictions between sources with specific quotes/numbers from each side.
- End with a clear "What I know" section: consolidated SPECIFIC knowledge — not vague takeaways, but exact numbers, strategies, tools, and processes extracted from all sources.
- Format: HTML (tags: b, i, u, a). Newlines, not <br>.
- Be EXHAUSTIVE. This is a data reference to consult, not a motivational poster."""


MACRO_SYSTEM = """You are AKS's strategic analyst. You analyze his ENTIRE knowledge base to surface meta-patterns, evolution of thinking, blind spots, and strategic insights.

This is NOT a summary. This is a HIGH-LEVEL STRATEGIC ANALYSIS of what AKS has been consuming and learning.

ABSOLUTE BAN LIST — if you write any of these, you have FAILED:
- Motivational filler ("the work isn't optional—it's foundational", "knowledge is power", "consistency wins")
- Philosophical observations ("it's not about the destination", "true growth comes from...")
- Vague theme labels without data ("he's interested in business" — say WHAT business strategies with WHAT numbers)
- Any sentence without at least one specific: a number, a tool name, a person's name, a dollar amount, a strategy name, a company name

EVERY CLAIM MUST BE GROUNDED IN SPECIFICS from the knowledge base. Not "he's learned about marketing" but "he's consumed 12 entries on cold outreach, with specific focus on Instantly/Smartlead for email automation, 2-4% reply rate benchmarks, and Alex Hormozi's $100M framework for offer creation."

STRUCTURE:
1. <b>Knowledge Profile</b> — What AKS consumes, with specific category counts, named creators/sources, and the exact strategies/frameworks he's accumulated.
2. <b>Dominant Themes & Data</b> — The 5-7 biggest themes. For each: specific numbers, strategies, tools, and frameworks from the content. NOT vague descriptions.
3. <b>Evolution</b> — How have interests shifted over time? What's rising, what's fading? Reference specific content.
4. <b>Blind Spots</b> — What topics are conspicuously absent? What specific knowledge gaps exist?
5. <b>Contradictions</b> — Where do sources SPECIFICALLY disagree? Quote or reference conflicting positions with exact numbers/claims from each side.
6. <b>Top 10 Highest-Value Data Points</b> — The 10 most specific, actionable insights across the entire base. Each must include: the exact claim with numbers/evidence, and which source it came from. NOT "great insight about business" but "source X claims cold email at 50/day with 3-line format yields 12 meetings/month at $0 ad spend."
7. <b>Strategic Playbook</b> — Based on ALL of this, specific next actions with exact parameters. NOT "focus on marketing" but "implement the 50 cold emails/day system using [tool] targeting [specific ICP] based on the framework from [source]."

FORMAT: HTML (b, i, u, a). Newlines, not <br>. No censorship. Raw, direct, useful.
Be EXHAUSTIVE and SPECIFIC. If you catch yourself writing a sentence with no numbers, no names, and no specific strategy — DELETE IT and replace with data."""


async def generate_macro_analysis(entries: list[dict], category_summaries: list[dict]) -> str:
    """Two-pass macro analysis: uses pre-computed category summaries, not raw entries."""

    # Build compact overview of the knowledge base
    overview = f"KNOWLEDGE BASE: {len(entries)} entries total\n\n"

    # Category distribution
    cat_counts: dict[str, int] = {}
    platform_counts: dict[str, int] = {}
    for e in entries:
        c = e.get("category", "other")
        cat_counts[c] = cat_counts.get(c, 0) + 1
        p = e.get("platform", "?")
        platform_counts[p] = platform_counts.get(p, 0) + 1

    overview += "CATEGORY DISTRIBUTION:\n"
    for c, n in sorted(cat_counts.items(), key=lambda x: -x[1]):
        overview += f"  {c}: {n} entries\n"
    overview += f"\nPLATFORMS: {platform_counts}\n\n"

    # Sample titles by category (5 per category max)
    overview += "SAMPLE TITLES BY CATEGORY:\n"
    by_cat: dict[str, list[str]] = {}
    for e in entries:
        c = e.get("category", "other")
        if c not in by_cat:
            by_cat[c] = []
        if len(by_cat[c]) < 5:
            by_cat[c].append(e.get("title") or e.get("url", "?"))
    for c, titles in by_cat.items():
        overview += f"\n[{c}]\n" + "\n".join(f"  - {t}" for t in titles) + "\n"

    # Category syntheses (the real meat — already pre-computed)
    overview += "\n\nCATEGORY SYNTHESES (pre-analyzed):\n"
    for cs in category_summaries:
        overview += f"\n{'='*40}\n"
        overview += f"CATEGORY: {cs['category']} ({cs['count']} entries)\n"
        overview += f"{cs['summary'][:3000]}\n"

    return await _call([
        {"role": "system", "content": MACRO_SYSTEM},
        {"role": "user", "content": overview},
    ], max_tokens=16000)


async def synthesize_category(category: str, entries: list[dict]) -> str:
    entries_text = []
    for e in entries:
        part = f"Title: {e.get('title') or e.get('url', '?')}\n"
        part += f"URL: {e.get('url', '')}\n"
        if e.get("tags"):
            part += f"Tags: {e['tags']}\n"
        if e.get("key_points"):
            part += f"Key points: {e['key_points']}\n"
        if e.get("analysis"):
            part += f"Analysis excerpt: {e['analysis'][:800]}\n"
        entries_text.append(part)

    return await _call([
        {"role": "system", "content": CATEGORY_SYNTHESIS_SYSTEM},
        {"role": "user", "content": f"DOMAIN: {category}\nNUMBER OF SOURCES: {len(entries)}\n\n" + "\n---\n".join(entries_text)},
    ], max_tokens=16000)
