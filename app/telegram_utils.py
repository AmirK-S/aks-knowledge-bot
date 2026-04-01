"""Telegram message utilities — chunking, formatting, etc."""
import re

MAX_MSG_LEN = 4000  # Telegram limit is 4096, leave margin


def chunk_message(text: str, max_len: int = MAX_MSG_LEN) -> list[str]:
    """Split a long message into Telegram-safe chunks, preserving HTML tags."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    remaining = text

    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break

        # Find a safe break point
        break_at = max_len
        # Try double newline
        idx = remaining.rfind("\n\n", 0, max_len)
        if idx > max_len * 0.4:
            break_at = idx
        else:
            # Try single newline
            idx = remaining.rfind("\n", 0, max_len)
            if idx > max_len * 0.3:
                break_at = idx
            else:
                # Try space
                idx = remaining.rfind(" ", 0, max_len)
                if idx > max_len * 0.3:
                    break_at = idx

        # Don't break inside an HTML tag
        last_open = remaining.rfind("<", 0, break_at)
        last_close = remaining.rfind(">", 0, break_at)
        if last_open > last_close:
            break_at = max(0, last_open - 1)

        chunk = remaining[:break_at].strip()
        remaining = remaining[break_at:].strip()

        if chunk:
            # Close any unclosed tags
            chunk = _balance_tags(chunk)
            chunks.append(chunk)

    return chunks


_ALLOWED_TAGS = {"b", "i", "u", "s", "a", "code", "pre"}


def _balance_tags(html: str) -> str:
    """Close unclosed tags and reopen them wouldn't work across chunks, so just close."""
    tag_re = re.compile(r"<(/?)(\w+)([^>]*)>")
    stack = []
    for m in tag_re.finditer(html):
        is_closing = m.group(1) == "/"
        tag = m.group(2).lower()
        if tag not in _ALLOWED_TAGS:
            continue
        if is_closing:
            # Pop matching tag from stack
            for i in range(len(stack) - 1, -1, -1):
                if stack[i] == tag:
                    stack.pop(i)
                    break
        else:
            stack.append(tag)

    # Close remaining open tags in reverse order
    for tag in reversed(stack):
        html += f"</{tag}>"
    return html


def escape_html(text: str) -> str:
    """Escape text for Telegram HTML, preserving allowed tags."""
    # Only escape < that are NOT part of allowed tags
    text = text.replace("&", "&amp;")
    # Protect allowed tags
    for tag in _ALLOWED_TAGS:
        text = text.replace(f"&lt;{tag}&gt;", f"<{tag}>")
        text = text.replace(f"&lt;{tag} ", f"<{tag} ")
        text = text.replace(f"&lt;/{tag}&gt;", f"</{tag}>")
    return text


def format_entry_short(entry: dict) -> str:
    """Format an entry as a short one-liner for lists."""
    title = entry.get("title") or "Untitled"
    cat = entry.get("category") or "?"
    platform = entry.get("platform") or "?"
    url = entry.get("url", "")
    if len(title) > 60:
        title = title[:57] + "..."
    return f"<b>[{cat}]</b> {title} ({platform})\n<a href=\"{url}\">Link</a>"
