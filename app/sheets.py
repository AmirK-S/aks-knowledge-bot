"""Google Sheets sync — backup every new entry to the Sheet."""
from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import time
import urllib.parse
import urllib.request
from subprocess import run, PIPE

from app.config import DB_PATH

log = logging.getLogger(__name__)

SHEET_ID = "1yf5c_tLTSYYZ4T_6U8o980NMew5ZSgdLXbjfbujd35M"
SCOPES = "https://www.googleapis.com/auth/spreadsheets"

# Service account key is stored as env var (JSON string)
_SA_KEY: dict | None = None


def _get_sa_key() -> dict | None:
    global _SA_KEY
    if _SA_KEY is not None:
        return _SA_KEY
    # Try base64-encoded key first
    b64 = os.environ.get("GOOGLE_SA_KEY_B64")
    if b64:
        import base64
        _SA_KEY = json.loads(base64.b64decode(b64))
        return _SA_KEY
    # Try raw JSON
    raw = os.environ.get("GOOGLE_SA_KEY")
    if raw:
        _SA_KEY = json.loads(raw)
        return _SA_KEY
    # Try file path
    path = os.environ.get("GOOGLE_SA_KEY_PATH", "")
    if path and os.path.exists(path):
        with open(path) as f:
            _SA_KEY = json.load(f)
        return _SA_KEY
    return None


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _get_token() -> str | None:
    sa = _get_sa_key()
    if not sa:
        log.warning("No Google SA key configured, Sheets sync disabled")
        return None

    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    now = int(time.time())
    claims = _b64url(json.dumps({
        "iss": sa["client_email"],
        "scope": SCOPES,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode())
    signing_input = f"{header}.{claims}".encode()

    keyfile = tempfile.mktemp()
    with open(keyfile, "w") as f:
        f.write(sa["private_key"])
    try:
        result = run(
            ["openssl", "dgst", "-sha256", "-sign", keyfile],
            input=signing_input, capture_output=True,
        )
    finally:
        os.unlink(keyfile)

    sig = _b64url(result.stdout)
    jwt = f"{header}.{claims}.{sig}"

    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt,
    }).encode()
    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())["access_token"]


async def append_to_sheet(entry: dict, tab: str = "Reels") -> bool:
    """Append a row to the Google Sheet as backup."""
    try:
        token = _get_token()
        if not token:
            return False

        row = [
            entry.get("created_at", ""),
            entry.get("platform", ""),
            entry.get("url", ""),
            (entry.get("raw_transcript", "") or "")[:49000],  # Sheet limit
            entry.get("user_prompt", ""),
            entry.get("key_points", ""),
            entry.get("category", ""),
            (entry.get("analysis", "") or "")[:49000],
        ]

        body = json.dumps({"values": [row]}).encode()
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
            f"/values/{tab}!A:H:append?valueInputOption=RAW&insertDataOption=INSERT_ROWS"
        )
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req)
        log.info("Synced to Google Sheet: %s", entry.get("url", "?"))
        return True
    except Exception:
        log.exception("Failed to sync to Google Sheet")
        return False


async def read_all_from_sheet(tab: str = "Reels") -> list[dict]:
    """Read all rows from a sheet tab."""
    token = _get_token()
    if not token:
        return []

    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{tab}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read())

    rows = data.get("values", [])
    if not rows:
        return []

    headers = [h.strip().lower() for h in rows[0]]
    entries = []
    for row in rows[1:]:
        entry = {}
        for i, h in enumerate(headers):
            entry[h] = row[i] if i < len(row) else ""
        entries.append(entry)
    return entries
