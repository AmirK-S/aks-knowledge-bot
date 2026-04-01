"""Web dashboard for AKS Knowledge Brain."""
from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
from asyncio import start_server
from urllib.parse import urlparse, parse_qs, unquote

from app.database import (
    get_stats, get_all_categories, get_entries_by_category,
    search_entries, get_recent_entries, get_db,
)
from app.llm import query_brain

log = logging.getLogger(__name__)

PASSWORD = "Ihavemoney123!"
_sessions: dict[str, float] = {}  # token -> expiry


def _check_auth(headers: str) -> bool:
    for line in headers.split("\r\n"):
        if line.lower().startswith("cookie:"):
            cookies = line.split(":", 1)[1].strip()
            for c in cookies.split(";"):
                c = c.strip()
                if c.startswith("brain_session="):
                    token = c.split("=", 1)[1]
                    if token in _sessions and _sessions[token] > time.time():
                        return True
    return False


def _create_session() -> str:
    token = secrets.token_hex(32)
    _sessions[token] = time.time() + 86400 * 7  # 7 days
    return token


def _get_body(raw: bytes) -> bytes:
    parts = raw.split(b"\r\n\r\n", 1)
    return parts[1] if len(parts) > 1 else b""


def _json_response(data, status=200):
    body = json.dumps(data, default=str, ensure_ascii=False).encode()
    return (f"HTTP/1.1 {status} OK\r\nContent-Type: application/json; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n\r\n").encode() + body


def _html_response(html, status=200, headers=""):
    body = html.encode() if isinstance(html, str) else html
    return (f"HTTP/1.1 {status} OK\r\nContent-Type: text/html; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n{headers}\r\n").encode() + body


def _redirect(url, cookie=""):
    h = f"Set-Cookie: {cookie}\r\n" if cookie else ""
    return f"HTTP/1.1 302 Found\r\nLocation: {url}\r\n{h}\r\n".encode()


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

LOGIN_PAGE = """<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AKS Brain - Login</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,system-ui,sans-serif;background:#0a0a0a;color:#e8e8e8;display:flex;align-items:center;justify-content:center;min-height:100vh}.login{background:#141414;border:1px solid #2a2a2a;border-radius:16px;padding:40px;width:340px;text-align:center}h1{font-size:1.4rem;margin-bottom:8px}.sub{color:#888;font-size:.85rem;margin-bottom:24px}input{width:100%;background:#1e1e1e;border:1px solid #2a2a2a;border-radius:8px;padding:12px;color:#e8e8e8;font-size:.95rem;margin-bottom:12px;outline:none}input:focus{border-color:#3b82f6}button{width:100%;background:#3b82f6;color:#fff;border:none;border-radius:8px;padding:12px;font-size:.95rem;font-weight:600;cursor:pointer}button:hover{background:#2563eb}.err{color:#ef4444;font-size:.85rem;margin-top:8px;display:none}</style></head>
<body><div class="login"><h1>AKS Brain</h1><p class="sub">Enter password</p><form method="POST" action="/login"><input type="password" name="password" placeholder="Password" autofocus><button type="submit">Enter</button></form><p class="err" id="err">Wrong password</p></div>
<script>if(location.search.includes('err'))document.getElementById('err').style.display='block'</script></body></html>"""

DASHBOARD_HTML = r"""<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AKS Brain</title>
<style>
:root{--bg:#0a0a0a;--s1:#111;--s2:#1a1a1a;--s3:#222;--border:#2a2a2a;--text:#e8e8e8;--muted:#777;--accent:#3b82f6;--accent2:#8b5cf6;--green:#22c55e}
*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,system-ui,sans-serif;background:var(--bg);color:var(--text);line-height:1.6}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
.app{display:flex;min-height:100vh}
.sidebar{width:240px;background:var(--s1);border-right:1px solid var(--border);padding:20px;position:fixed;height:100vh;overflow-y:auto}
.sidebar h1{font-size:1.2rem;margin-bottom:4px}.sidebar .sub{color:var(--muted);font-size:.75rem;margin-bottom:24px}
.nav-section{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin:20px 0 8px}
.nav-item{display:block;padding:8px 12px;border-radius:8px;color:var(--text);font-size:.85rem;cursor:pointer;transition:background .15s;border:none;background:none;width:100%;text-align:left}
.nav-item:hover,.nav-item.active{background:var(--s2);color:var(--accent);text-decoration:none}
.nav-count{float:right;color:var(--muted);font-size:.75rem}
.main{margin-left:240px;flex:1;padding:32px;max-width:960px}
.page{display:none}.page.active{display:block}
.stats{display:flex;gap:12px;margin-bottom:28px;flex-wrap:wrap}
.stat{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:20px;flex:1;min-width:130px}
.stat-val{font-size:2rem;font-weight:700;color:var(--accent)}.stat-lbl{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.search-box{display:flex;gap:8px;margin-bottom:24px}
.search-box input{flex:1;background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:12px 16px;color:var(--text);font-size:.9rem;outline:none}
.search-box input:focus{border-color:var(--accent)}
.search-box button,.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:10px 20px;cursor:pointer;font-weight:600;font-size:.85rem}
.btn-sm{padding:6px 12px;font-size:.75rem;border-radius:6px}.btn-outline{background:transparent;border:1px solid var(--border);color:var(--text)}.btn-outline:hover{border-color:var(--accent);color:var(--accent)}
.entries{display:flex;flex-direction:column;gap:10px}
.entry-card{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:16px 20px;cursor:pointer;transition:border-color .15s}
.entry-card:hover{border-color:var(--accent)}
.entry-top{display:flex;justify-content:space-between;align-items:center;gap:12px}
.entry-title{font-weight:600;font-size:.9rem;flex:1}
.badge{font-size:.65rem;padding:3px 10px;border-radius:20px;font-weight:500}
.badge-cat{background:rgba(59,130,246,.12);color:var(--accent);border:1px solid rgba(59,130,246,.25)}
.badge-plat{background:rgba(139,92,246,.12);color:var(--accent2);border:1px solid rgba(139,92,246,.25)}
.entry-url{font-size:.75rem;color:var(--muted);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
/* Detail page */
.detail-header{margin-bottom:24px}.detail-title{font-size:1.4rem;font-weight:700;margin-bottom:8px}
.detail-meta{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.player{margin-bottom:24px;border-radius:12px;overflow:hidden;background:#000}
.player iframe{width:100%;aspect-ratio:16/9;border:none;display:block}
.section{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px}
.section-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.section-title{font-weight:600;font-size:.95rem}
.section-actions{display:flex;gap:6px}
.section-body{font-size:.85rem;line-height:1.7;color:#ccc;white-space:pre-wrap;word-break:break-word}
.section-body b{color:var(--accent)}.section-body i{color:var(--muted)}
.key-points{list-style:none;padding:0}.key-points li{padding:6px 0;border-bottom:1px solid var(--border);font-size:.85rem}.key-points li:last-child{border:none}
.key-points li::before{content:"→ ";color:var(--accent);font-weight:700}
/* Chat */
.chat-box{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px}
.chat-messages{max-height:400px;overflow-y:auto;margin-bottom:12px;display:flex;flex-direction:column;gap:8px}
.chat-msg{padding:10px 14px;border-radius:10px;font-size:.85rem;max-width:85%;line-height:1.5}
.chat-msg.user{background:var(--accent);color:#fff;align-self:flex-end}.chat-msg.bot{background:var(--s2);align-self:flex-start}
.chat-input{display:flex;gap:8px}
.chat-input input{flex:1;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--text);font-size:.85rem;outline:none}
.loading{text-align:center;padding:40px;color:var(--muted)}
@media(max-width:768px){.sidebar{display:none}.main{margin-left:0}}
</style></head>
<body>
<div class="app">
<nav class="sidebar">
  <h1>AKS Brain</h1>
  <p class="sub">Knowledge Base</p>
  <a class="nav-item active" onclick="showPage('home')" data-page="home">Home</a>
  <a class="nav-item" onclick="showPage('chat')" data-page="chat">Ask Brain</a>
  <div class="nav-section">Categories</div>
  <div id="nav-cats"></div>
  <div class="nav-section" style="margin-top:24px"><a href="/logout" style="color:var(--muted);font-size:.75rem">Logout</a></div>
</nav>
<div class="main">

<!-- HOME -->
<div class="page active" id="page-home">
  <div class="stats" id="stats"></div>
  <div class="search-box"><input type="text" id="searchInput" placeholder="Search your knowledge base..." onkeydown="if(event.key==='Enter')doSearch()"><button onclick="doSearch()">Search</button></div>
  <h2 style="font-size:1rem;margin-bottom:12px" id="entries-title">Recent</h2>
  <div class="entries" id="entries"><div class="loading">Loading...</div></div>
</div>

<!-- DETAIL -->
<div class="page" id="page-detail"></div>

<!-- CHAT -->
<div class="page" id="page-chat">
  <h2 style="font-size:1.1rem;margin-bottom:16px">Ask your brain</h2>
  <div class="chat-box">
    <div class="chat-messages" id="chat-messages"></div>
    <div class="chat-input"><input type="text" id="chatInput" placeholder="What do you want to know?" onkeydown="if(event.key==='Enter')sendChat()"><button class="btn" onclick="sendChat()">Send</button></div>
  </div>
</div>

</div></div>

<script>
async function api(path,opts){const r=await fetch('/api'+path,opts);return r.json()}

let currentPage='home';
function showPage(p,skipNav){
  document.querySelectorAll('.page').forEach(el=>el.classList.remove('active'));
  document.getElementById('page-'+p).classList.add('active');
  if(!skipNav){document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));document.querySelector('[data-page="'+p+'"]')?.classList.add('active')}
  currentPage=p;
}

async function loadStats(){
  const s=await api('/stats');
  document.getElementById('stats').innerHTML=`
    <div class="stat"><div class="stat-val">${s.total}</div><div class="stat-lbl">Entries</div></div>
    <div class="stat"><div class="stat-val">${s.this_week}</div><div class="stat-lbl">This week</div></div>
    <div class="stat"><div class="stat-val">${Object.keys(s.platforms).length}</div><div class="stat-lbl">Platforms</div></div>`;
}

async function loadCategories(){
  const cats=await api('/categories');
  document.getElementById('nav-cats').innerHTML=cats.map(c=>
    `<a class="nav-item" onclick="loadCategory('${c.category.replace(/'/g,"\\'")}')" data-page="cat-${c.category}">${c.category}<span class="nav-count">${c.cnt}</span></a>`
  ).join('');
}

function entryTitle(e){
  if(e.title) return e.title;
  try{const u=new URL(e.url);return u.pathname.split('/').filter(Boolean).pop()||u.hostname}catch(ex){return e.url?.slice(0,50)||'Untitled'}
}

function renderEntries(entries,title){
  document.getElementById('entries-title').textContent=title||'Results';
  if(!entries.length){document.getElementById('entries').innerHTML='<div class="loading">No entries found</div>';return}
  document.getElementById('entries').innerHTML=entries.map(e=>`
    <div class="entry-card" onclick="loadDetail(${e.id})">
      <div class="entry-top">
        <div class="entry-title">${entryTitle(e)}</div>
        <div style="display:flex;gap:6px">
          ${e.category?`<span class="badge badge-cat">${e.category}</span>`:''}
          ${e.platform?`<span class="badge badge-plat">${e.platform}</span>`:''}
        </div>
      </div>
      <div class="entry-url">${e.url||''}</div>
    </div>`).join('');
}

async function loadRecent(){renderEntries(await api('/recent?days=30&limit=50'),'Recent')}
async function loadCategory(cat){
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.remove('active'));
  document.querySelector('[data-page="cat-'+cat+'"]')?.classList.add('active');
  showPage('home',true);
  renderEntries(await api('/category/'+encodeURIComponent(cat)),'Category: '+cat);
}
async function doSearch(){
  const q=document.getElementById('searchInput').value.trim();if(!q)return;
  renderEntries(await api('/search?q='+encodeURIComponent(q)),'Search: '+q);
}

function getPlayer(url,platform){
  if(platform==='youtube'){
    const m=url.match(/(?:v=|youtu\.be\/)([A-Za-z0-9_-]{11})/);
    if(m)return `<div class="player"><iframe src="https://www.youtube.com/embed/${m[1]}" allowfullscreen></iframe></div>`;
  }
  if(platform==='instagram'){
    const m=url.match(/\/reel\/([^\/\?]+)/)||url.match(/\/p\/([^\/\?]+)/);
    if(m)return `<div class="player"><iframe src="https://www.instagram.com/reel/${m[1]}/embed/" style="aspect-ratio:9/16;max-height:600px" allowfullscreen></iframe></div>`;
  }
  return `<div style="margin-bottom:16px"><a href="${url}" target="_blank" class="btn btn-outline">Open original</a></div>`;
}

function parseKP(raw){
  try{const arr=JSON.parse(raw);if(Array.isArray(arr))return arr}catch(e){}
  if(typeof raw==='string'&&raw.trim())return[raw];return[]
}

function copyText(text,btn){navigator.clipboard.writeText(text);btn.textContent='Copied!';setTimeout(()=>btn.textContent='Copy',1500)}
function downloadText(text,name){const a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(text);a.download=name;a.click()}

async function loadDetail(id){
  showPage('detail');
  const el=document.getElementById('page-detail');
  el.innerHTML='<div class="loading">Loading...</div>';
  const e=await api('/entry/'+id);
  const kp=parseKP(e.key_points);
  const analysis=(e.analysis||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/&lt;b&gt;/g,'<b>').replace(/&lt;\/b&gt;/g,'</b>')
    .replace(/&lt;i&gt;/g,'<i>').replace(/&lt;\/i&gt;/g,'</i>')
    .replace(/&lt;u&gt;/g,'<u>').replace(/&lt;\/u&gt;/g,'</u>')
    .replace(/&lt;a /g,'<a ').replace(/&lt;\/a&gt;/g,'</a>').replace(/&gt;/g,'>');
  const transcript=e.raw_transcript||'';

  el.innerHTML=`
    <a onclick="showPage('home')" style="cursor:pointer;font-size:.8rem;color:var(--muted);display:inline-block;margin-bottom:16px">← Back</a>
    <div class="detail-header">
      <div class="detail-title">${entryTitle(e)}</div>
      <div class="detail-meta">
        ${e.category?`<span class="badge badge-cat">${e.category}</span>`:''}
        ${e.platform?`<span class="badge badge-plat">${e.platform}</span>`:''}
        ${e.source_type?`<span class="badge" style="background:var(--s2);border:1px solid var(--border);color:var(--muted)">${e.source_type}</span>`:''}
        ${e.created_at?`<span style="color:var(--muted);font-size:.75rem">${e.created_at}</span>`:''}
      </div>
    </div>

    ${getPlayer(e.url,e.platform)}

    ${kp.length?`<div class="section"><div class="section-header"><div class="section-title">Key Points</div></div>
      <ul class="key-points">${kp.map(p=>'<li>'+p+'</li>').join('')}</ul></div>`:''}

    <div class="section">
      <div class="section-header"><div class="section-title">Analysis</div>
        <div class="section-actions">
          <button class="btn btn-sm btn-outline" onclick="copyText(\`${analysis.replace(/`/g,'\\`').replace(/\$/g,'\\$')}\`,this)">Copy</button>
          <button class="btn btn-sm btn-outline" onclick="downloadText(document.getElementById('analysis-raw').textContent,'analysis.txt')">Download</button>
        </div>
      </div>
      <div class="section-body" id="analysis-text">${analysis||'<span style="color:var(--muted)">No analysis available</span>'}</div>
      <pre id="analysis-raw" style="display:none">${(e.analysis||'').replace(/</g,'&lt;')}</pre>
    </div>

    ${transcript?`<div class="section">
      <div class="section-header"><div class="section-title">Transcript</div>
        <div class="section-actions">
          <button class="btn btn-sm btn-outline" onclick="copyText(document.getElementById('transcript-text').textContent,this)">Copy</button>
          <button class="btn btn-sm btn-outline" onclick="downloadText(document.getElementById('transcript-text').textContent,'transcript.txt')">Download</button>
        </div>
      </div>
      <div class="section-body" id="transcript-text" style="max-height:300px;overflow-y:auto;font-size:.8rem;color:var(--muted)">${transcript.replace(/</g,'&lt;')}</div>
    </div>`:''}

    <div class="section">
      <div class="section-header"><div class="section-title">Rewrite</div></div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'short')">Short version</button>
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'pragmatic')">Pragmatic</button>
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'bullets')">Bullet points</button>
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'detailed')">Full detailed</button>
      </div>
      <div id="rewrite-result" style="margin-top:12px"></div>
    </div>`;
}

async function rewrite(id,style){
  const el=document.getElementById('rewrite-result');
  el.innerHTML='<div class="loading">Rewriting...</div>';
  const r=await api('/rewrite/'+id+'?style='+style);
  el.innerHTML=`<div class="section-body" style="margin-top:8px">${r.text||r.error||'Error'}</div>
    <div style="margin-top:8px"><button class="btn btn-sm btn-outline" onclick="copyText(document.getElementById('rewrite-result').querySelector('.section-body').textContent,this)">Copy</button></div>`;
}

// Chat
async function sendChat(){
  const input=document.getElementById('chatInput');
  const q=input.value.trim();if(!q)return;input.value='';
  const msgs=document.getElementById('chat-messages');
  msgs.innerHTML+=`<div class="chat-msg user">${q}</div>`;
  msgs.innerHTML+=`<div class="chat-msg bot" id="chat-loading">Thinking...</div>`;
  msgs.scrollTop=msgs.scrollHeight;
  const r=await api('/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q})});
  document.getElementById('chat-loading').outerHTML=`<div class="chat-msg bot">${r.answer||r.error||'Error'}</div>`;
  msgs.scrollTop=msgs.scrollHeight;
}

loadStats();loadCategories();loadRecent();
</script>
</body></html>"""


async def _get_entry_by_id(entry_id: int) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM entries WHERE id = ?", (entry_id,))
    return dict(rows[0]) if rows else None


async def handle_request(reader, writer):
    try:
        raw = await reader.read(65536)
        if not raw:
            writer.close()
            return

        request_line = raw.decode(errors="replace").split("\r\n")[0]
        parts = request_line.split(" ")
        if len(parts) < 2:
            writer.close()
            return
        method, full_path = parts[0], parts[1]
        headers_str = raw.decode(errors="replace").split("\r\n\r\n")[0]
        path = full_path.split("?")[0]
        authed = _check_auth(headers_str)

        # Login
        if path == "/login" and method == "POST":
            body = _get_body(raw).decode(errors="replace")
            params = dict(p.split("=", 1) for p in body.split("&") if "=" in p)
            pw = unquote(params.get("password", "").replace("+", " "))
            if pw == PASSWORD:
                token = _create_session()
                writer.write(_redirect("/", f"brain_session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800"))
            else:
                writer.write(_redirect("/login?err=1"))
            await writer.drain()
            writer.close()
            return

        if path == "/logout":
            writer.write(_redirect("/", "brain_session=; Path=/; Max-Age=0"))
            await writer.drain()
            writer.close()
            return

        if path == "/login" or (not authed and not path.startswith("/api/")):
            writer.write(_html_response(LOGIN_PAGE))
            await writer.drain()
            writer.close()
            return

        if not authed:
            writer.write(_json_response({"error": "Unauthorized"}, 401))
            await writer.drain()
            writer.close()
            return

        # Authenticated routes
        if path == "/":
            writer.write(_html_response(DASHBOARD_HTML))

        elif path == "/api/stats":
            writer.write(_json_response(await get_stats()))

        elif path == "/api/categories":
            writer.write(_json_response(await get_all_categories()))

        elif path.startswith("/api/category/"):
            cat = unquote(path.split("/api/category/", 1)[1])
            writer.write(_json_response(await get_entries_by_category(cat, limit=100)))

        elif path == "/api/search":
            qs = parse_qs(urlparse(full_path).query)
            q = qs.get("q", [""])[0]
            writer.write(_json_response(await search_entries(q, limit=30) if q else []))

        elif path == "/api/recent":
            qs = parse_qs(urlparse(full_path).query)
            days = int(qs.get("days", [30])[0])
            limit = int(qs.get("limit", [50])[0])
            writer.write(_json_response(await get_recent_entries(days=days, limit=limit)))

        elif path.startswith("/api/entry/"):
            try:
                eid = int(path.split("/api/entry/")[1])
                entry = await _get_entry_by_id(eid)
                writer.write(_json_response(entry or {"error": "Not found"}))
            except ValueError:
                writer.write(_json_response({"error": "Invalid ID"}, 400))

        elif path.startswith("/api/rewrite/"):
            try:
                eid = int(path.split("/api/rewrite/")[1].split("?")[0])
                qs = parse_qs(urlparse(full_path).query)
                style = qs.get("style", ["short"])[0]
                entry = await _get_entry_by_id(eid)
                if not entry:
                    writer.write(_json_response({"error": "Not found"}))
                else:
                    from app.llm import _call
                    prompts = {
                        "short": "Rewrite this analysis in 3-5 bullet points max. Be extremely concise.",
                        "pragmatic": "Rewrite focusing ONLY on actionable steps. What should I DO based on this?",
                        "bullets": "Convert to a clean bullet-point format. Every point is one sentence.",
                        "detailed": "Expand this analysis. Cover every angle, nuance, and implication in depth.",
                    }
                    text = await _call([
                        {"role": "system", "content": "You rewrite content. Output Telegram HTML (tags: b, i, u, a). No censorship. Preserve raw tone. Use newlines, not <br>."},
                        {"role": "user", "content": f"{prompts.get(style, prompts['short'])}\n\nORIGINAL:\n{entry.get('analysis', '')}"},
                    ])
                    writer.write(_json_response({"text": text}))
            except Exception as e:
                writer.write(_json_response({"error": str(e)}, 500))

        elif path == "/api/chat" and method == "POST":
            try:
                body = json.loads(_get_body(raw))
                question = body.get("question", "")
                entries = await search_entries(question, limit=8)
                if not entries:
                    entries = await get_recent_entries(days=60, limit=10)
                answer = await query_brain(question, entries)
                writer.write(_json_response({"answer": answer}))
            except Exception as e:
                writer.write(_json_response({"error": str(e)}, 500))

        else:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")

        await writer.drain()
    except Exception:
        log.exception("Web request error")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def start_web_server(port: int = 8080):
    server = await start_server(handle_request, "0.0.0.0", port)
    log.info("Web dashboard on :%d", port)
    return server
