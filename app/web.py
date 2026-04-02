"""Web dashboard for AKS Knowledge Brain — v2."""
from __future__ import annotations

import json
import logging
import secrets
import time
from asyncio import start_server
from urllib.parse import urlparse, parse_qs, unquote

from app.database import (
    get_stats, get_all_categories, get_entries_by_category,
    search_entries, get_recent_entries, get_db,
    get_entries_by_platform, get_category_entries_for_summary,
    get_all_weeks, get_entries_by_week, save_recap, get_all_recaps,
    save_macro_analysis, get_latest_macro,
)
from app.llm import query_brain, generate_recap, synthesize_category, generate_macro_analysis

log = logging.getLogger(__name__)

PASSWORD = "Ihavemoney123!"
_sessions: dict[str, float] = {}


def _check_auth(headers: str) -> bool:
    for line in headers.split("\r\n"):
        if line.lower().startswith("cookie:"):
            for c in line.split(":", 1)[1].strip().split(";"):
                c = c.strip()
                if c.startswith("brain_session="):
                    token = c.split("=", 1)[1]
                    if token in _sessions and _sessions[token] > time.time():
                        return True
    return False


def _create_session() -> str:
    token = secrets.token_hex(32)
    _sessions[token] = time.time() + 86400 * 7
    return token


def _get_body(raw: bytes) -> bytes:
    parts = raw.split(b"\r\n\r\n", 1)
    return parts[1] if len(parts) > 1 else b""


def _resp(body, content_type="text/html; charset=utf-8", status=200, extra_headers=""):
    if isinstance(body, str):
        body = body.encode()
    h = f"HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\n{extra_headers}\r\n"
    return h.encode() + body


def _json_resp(data, status=200):
    return _resp(json.dumps(data, default=str, ensure_ascii=False), "application/json; charset=utf-8", status)


def _redirect(url, cookie=""):
    h = f"Set-Cookie: {cookie}\r\n" if cookie else ""
    return f"HTTP/1.1 302 Found\r\nLocation: {url}\r\n{h}\r\n".encode()


LOGIN_PAGE = """<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AKS Brain</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,system-ui,sans-serif;background:#0a0a0a;color:#e8e8e8;display:flex;align-items:center;justify-content:center;min-height:100vh}.login{background:#141414;border:1px solid #2a2a2a;border-radius:16px;padding:40px;width:340px;text-align:center}h1{font-size:1.4rem;margin-bottom:8px}.sub{color:#888;font-size:.85rem;margin-bottom:24px}input{width:100%;background:#1e1e1e;border:1px solid #2a2a2a;border-radius:8px;padding:12px;color:#e8e8e8;font-size:.95rem;margin-bottom:12px;outline:none}input:focus{border-color:#3b82f6}button{width:100%;background:#3b82f6;color:#fff;border:none;border-radius:8px;padding:12px;font-size:.95rem;font-weight:600;cursor:pointer}button:hover{background:#2563eb}.err{color:#ef4444;font-size:.85rem;margin-top:8px;display:none}</style></head>
<body><div class="login"><h1>AKS Brain</h1><p class="sub">Enter password</p><form method="POST" action="/login"><input type="password" name="password" placeholder="Password" autofocus><button type="submit">Enter</button></form><p class="err" id="err">Wrong password</p></div>
<script>if(location.search.includes('err'))document.getElementById('err').style.display='block'</script></body></html>"""

DASHBOARD = r"""<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>AKS Brain</title>
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
.stats{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}
.stat{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:16px 20px;flex:1;min-width:100px}
.stat-val{font-size:1.8rem;font-weight:700;color:var(--accent)}.stat-lbl{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.toolbar{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center}
.search-box{display:flex;gap:8px;flex:1;min-width:200px}
.search-box input{flex:1;background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:10px 14px;color:var(--text);font-size:.85rem;outline:none}
.search-box input:focus{border-color:var(--accent)}
.btn{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:8px 16px;cursor:pointer;font-weight:600;font-size:.8rem;white-space:nowrap}
.btn-sm{padding:6px 12px;font-size:.75rem;border-radius:6px}
.btn-outline{background:transparent;border:1px solid var(--border);color:var(--text)}.btn-outline:hover{border-color:var(--accent);color:var(--accent)}
.btn-outline.active{border-color:var(--accent);color:var(--accent);background:rgba(59,130,246,.1)}
.filter-group{display:flex;gap:4px}
.entries{display:flex;flex-direction:column;gap:8px;margin-top:12px}
.entry-card{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px 18px;cursor:pointer;transition:border-color .15s}
.entry-card:hover{border-color:var(--accent)}
.entry-top{display:flex;justify-content:space-between;align-items:center;gap:12px}
.entry-title{font-weight:600;font-size:.88rem;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badges{display:flex;gap:6px;flex-shrink:0}
.badge{font-size:.65rem;padding:3px 10px;border-radius:20px;font-weight:500}
.badge-cat{background:rgba(59,130,246,.12);color:var(--accent);border:1px solid rgba(59,130,246,.25)}
.badge-plat{background:rgba(139,92,246,.12);color:var(--accent2);border:1px solid rgba(139,92,246,.25)}
.entry-url{font-size:.7rem;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.section-title{font-size:1rem;font-weight:600;margin-bottom:12px}
/* Detail */
.detail-title{font-size:1.3rem;font-weight:700;margin-bottom:8px}
.detail-meta{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.player{margin-bottom:20px;border-radius:12px;overflow:hidden;background:#000}
.player iframe{width:100%;aspect-ratio:16/9;border:none;display:block}
.section{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:14px}
.section-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.section-body{font-size:.85rem;line-height:1.7;color:#ccc;white-space:pre-wrap;word-break:break-word}
.section-body b{color:var(--accent)}.section-body i{color:var(--muted)}
.key-points{list-style:none;padding:0}.key-points li{padding:5px 0;border-bottom:1px solid var(--border);font-size:.85rem}.key-points li:last-child{border:none}
.key-points li::before{content:"\2192 ";color:var(--accent);font-weight:700}
/* Chat */
.chat-messages{max-height:400px;overflow-y:auto;margin-bottom:12px;display:flex;flex-direction:column;gap:8px}
.chat-msg{padding:10px 14px;border-radius:10px;font-size:.85rem;max-width:85%;line-height:1.5}
.chat-msg.user{background:var(--accent);color:#fff;align-self:flex-end}.chat-msg.bot{background:var(--s2);align-self:flex-start}
.chat-input{display:flex;gap:8px}
.chat-input input{flex:1;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--text);font-size:.85rem;outline:none}
.synthesis-body{font-size:.85rem;line-height:1.7;color:#ccc;white-space:pre-wrap;word-break:break-word}
.synthesis-body b{color:var(--accent)}
.loading{text-align:center;padding:40px;color:var(--muted)}
@media(max-width:768px){.sidebar{display:none}.main{margin-left:0}}
</style></head>
<body>
<div class="app">
<nav class="sidebar">
  <h1>AKS Brain</h1><p class="sub">Knowledge Base</p>
  <a class="nav-item active" onclick="showPage('home')" data-page="home">Home</a>
  <a class="nav-item" onclick="showPage('chat')" data-page="chat">Ask Brain</a>
  <a class="nav-item" onclick="showPage('recap')" data-page="recap">Weekly Recap</a>
  <div class="nav-section">Categories</div>
  <div id="nav-cats"></div>
  <div class="nav-section" style="margin-top:20px"><a href="/logout" style="color:var(--muted);font-size:.75rem">Logout</a></div>
</nav>
<div class="main">

<!-- HOME -->
<div class="page active" id="page-home">
  <div class="stats" id="stats"></div>
  <div class="toolbar">
    <div class="search-box"><input type="text" id="searchInput" placeholder="Search..." onkeydown="if(event.key==='Enter')doSearch()"><button class="btn" onclick="doSearch()">Search</button></div>
    <div class="filter-group">
      <button class="btn btn-sm btn-outline active" onclick="filterPlatform('all',this)">All</button>
      <button class="btn btn-sm btn-outline" onclick="filterPlatform('instagram',this)">Instagram</button>
      <button class="btn btn-sm btn-outline" onclick="filterPlatform('youtube',this)">YouTube</button>
    </div>
  </div>
  <div class="section-title" id="entries-title">Recent</div>
  <div class="entries" id="entries"><div class="loading">Loading...</div></div>
</div>

<!-- CATEGORY -->
<div class="page" id="page-category">
  <a onclick="showPage('home')" style="cursor:pointer;font-size:.8rem;color:var(--muted)">← Back</a>
  <div style="display:flex;justify-content:space-between;align-items:center;margin:16px 0">
    <div class="section-title" id="cat-title" style="margin:0"></div>
    <button class="btn" onclick="synthesizeCat()">Summarize all</button>
  </div>
  <div id="cat-synthesis" style="margin-bottom:16px"></div>
  <div class="toolbar">
    <div class="filter-group">
      <button class="btn btn-sm btn-outline active" onclick="filterCatPlatform('all',this)">All</button>
      <button class="btn btn-sm btn-outline" onclick="filterCatPlatform('instagram',this)">Instagram</button>
      <button class="btn btn-sm btn-outline" onclick="filterCatPlatform('youtube',this)">YouTube</button>
    </div>
  </div>
  <div class="entries" id="cat-entries"></div>
</div>

<!-- DETAIL -->
<div class="page" id="page-detail"></div>

<!-- CHAT -->
<div class="page" id="page-chat">
  <div class="section-title">Ask your brain</div>
  <div class="section">
    <div class="chat-messages" id="chat-messages"></div>
    <div class="chat-input"><input type="text" id="chatInput" placeholder="What do you want to know?" onkeydown="if(event.key==='Enter')sendChat()"><button class="btn" onclick="sendChat()">Send</button></div>
  </div>
</div>

<!-- RECAP -->
<div class="page" id="page-recap">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
    <div class="section-title" style="margin:0">Recaps & Analysis</div>
    <div style="display:flex;gap:8px">
      <button class="btn" onclick="generateAllRecaps()">Generate all past recaps</button>
      <button class="btn" style="background:var(--accent2)" onclick="generateMacro()">Macro Analysis</button>
    </div>
  </div>
  <div id="macro-content"></div>
  <div id="recap-list"><div class="loading">Loading recaps...</div></div>
</div>

</div></div>

<script>
async function api(path,opts){const r=await fetch('/api'+path,opts);return r.json()}
let currentCat='',allCatEntries=[];

function showPage(p,skipNav){
  document.querySelectorAll('.page').forEach(el=>el.classList.remove('active'));
  document.getElementById('page-'+p).classList.add('active');
  if(!skipNav)document.querySelectorAll('.sidebar .nav-item').forEach(n=>n.classList.remove('active'));
  if(!skipNav){const el=document.querySelector('[data-page="'+p+'"]');if(el)el.classList.add('active')}
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
  if(e.title&&e.title.length>2&&e.title!=='watch'&&!e.title.match(/^[A-Za-z0-9_-]{8,15}$/))return e.title;
  // Fallback: clean URL
  try{
    const u=new URL(e.url);
    if(u.hostname.includes('instagram'))return 'Instagram Reel';
    if(u.hostname.includes('youtu')){const v=u.searchParams.get('v')||u.pathname.split('/').pop();return 'YouTube: '+v}
    return u.hostname.replace('www.','')
  }catch(ex){return e.url?.slice(0,50)||'Untitled'}
}

function renderEntries(entries,containerId){
  const el=document.getElementById(containerId||'entries');
  if(!entries.length){el.innerHTML='<div class="loading">No entries</div>';return}
  el.innerHTML=entries.map(e=>`
    <div class="entry-card" onclick="loadDetail(${e.id})">
      <div class="entry-top">
        <div class="entry-title">${entryTitle(e)}</div>
        <div class="badges">
          ${e.category?`<span class="badge badge-cat">${e.category}</span>`:''}
          ${e.platform?`<span class="badge badge-plat">${e.platform}</span>`:''}
        </div>
      </div>
      <div class="entry-url">${e.url||''}</div>
    </div>`).join('');
}

let allHomeEntries=[];
async function loadRecent(){
  allHomeEntries=await api('/recent?days=9999&limit=500');
  renderEntries(allHomeEntries);
  document.getElementById('entries-title').textContent='All entries ('+allHomeEntries.length+')';
}
function filterPlatform(p,btn){
  document.querySelectorAll('#page-home .filter-group .btn-outline').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  const filtered=p==='all'?allHomeEntries:allHomeEntries.filter(e=>e.platform===p);
  renderEntries(filtered);
  document.getElementById('entries-title').textContent=(p==='all'?'All':p)+' ('+filtered.length+')';
}
async function doSearch(){
  const q=document.getElementById('searchInput').value.trim();if(!q)return;
  const r=await api('/search?q='+encodeURIComponent(q));
  renderEntries(r);
  document.getElementById('entries-title').textContent='Search: '+q+' ('+r.length+')';
}

// Category page
async function loadCategory(cat){
  currentCat=cat;
  showPage('category');
  document.getElementById('cat-title').textContent=cat+' (loading...)';
  document.getElementById('cat-synthesis').innerHTML='';
  allCatEntries=await api('/category/'+encodeURIComponent(cat)+'?limit=200');
  document.getElementById('cat-title').textContent=cat+' ('+allCatEntries.length+' entries)';
  renderEntries(allCatEntries,'cat-entries');
}
function filterCatPlatform(p,btn){
  document.querySelectorAll('#page-category .filter-group .btn-outline').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  const filtered=p==='all'?allCatEntries:allCatEntries.filter(e=>e.platform===p);
  renderEntries(filtered,'cat-entries');
}
async function synthesizeCat(){
  const el=document.getElementById('cat-synthesis');
  el.innerHTML='<div class="section"><div class="loading">Generating comprehensive synthesis of '+currentCat+'... (this may take a minute)</div></div>';
  const r=await api('/synthesize/'+encodeURIComponent(currentCat));
  el.innerHTML=`<div class="section"><div class="section-header"><div class="section-title">Complete synthesis: ${currentCat}</div>
    <div><button class="btn btn-sm btn-outline" onclick="copyText(this.closest('.section').querySelector('.synthesis-body').textContent,this)">Copy</button></div></div>
    <div class="synthesis-body">${r.text||r.error||'Error'}</div></div>`;
}

// Detail page
function getPlayer(url,platform){
  if(platform==='youtube'){const m=url.match(/(?:v=|youtu\.be\/)([A-Za-z0-9_-]{11})/);if(m)return`<div class="player"><iframe src="https://www.youtube.com/embed/${m[1]}" allowfullscreen></iframe></div>`}
  if(platform==='instagram'){const m=url.match(/\/reel\/([^\/\?]+)/)||url.match(/\/p\/([^\/\?]+)/);if(m)return`<div class="player"><iframe src="https://www.instagram.com/reel/${m[1]}/embed/" style="aspect-ratio:9/16;max-height:500px" allowfullscreen></iframe></div>`}
  return`<div style="margin-bottom:16px"><a href="${url}" target="_blank" class="btn btn-outline">Open original</a></div>`;
}
function parseKP(raw){try{const a=JSON.parse(raw);if(Array.isArray(a))return a}catch(e){}if(typeof raw==='string'&&raw.trim())return[raw];return[]}
function copyText(t,btn){navigator.clipboard.writeText(t);btn.textContent='Copied!';setTimeout(()=>btn.textContent='Copy',1500)}
function downloadText(t,n){const a=document.createElement('a');a.href='data:text/plain;charset=utf-8,'+encodeURIComponent(t);a.download=n;a.click()}

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
  const title=entryTitle(e);

  el.innerHTML=`
    <a onclick="history.back?history.back():showPage('home')" style="cursor:pointer;font-size:.8rem;color:var(--muted)">← Back</a>
    <div style="margin-top:12px"><div class="detail-title">${title}</div>
      <div class="detail-meta">
        ${e.category?`<span class="badge badge-cat">${e.category}</span>`:''}
        ${e.platform?`<span class="badge badge-plat">${e.platform}</span>`:''}
        ${e.source_type?`<span class="badge" style="background:var(--s2);border:1px solid var(--border);color:var(--muted)">${e.source_type}</span>`:''}
        <span style="color:var(--muted);font-size:.75rem">${e.created_at||''}</span>
        <a href="${e.url}" target="_blank" style="font-size:.75rem">Open original</a>
      </div>
    </div>
    ${getPlayer(e.url,e.platform)}
    ${kp.length?`<div class="section"><div class="section-title">Key Points</div><ul class="key-points">${kp.map(p=>'<li>'+p+'</li>').join('')}</ul></div>`:''}
    <div class="section">
      <div class="section-header"><div class="section-title">Analysis</div><div class="badges">
        <button class="btn btn-sm btn-outline" onclick="copyText(document.getElementById('a-raw').textContent,this)">Copy</button>
        <button class="btn btn-sm btn-outline" onclick="downloadText(document.getElementById('a-raw').textContent,'analysis.txt')">Download</button>
      </div></div>
      <div class="section-body">${analysis||'<span style="color:var(--muted)">No analysis</span>'}</div>
      <pre id="a-raw" style="display:none">${(e.analysis||'').replace(/</g,'&lt;')}</pre>
    </div>
    ${e.raw_transcript?`<div class="section">
      <div class="section-header"><div class="section-title">Transcript</div><div class="badges">
        <button class="btn btn-sm btn-outline" onclick="copyText(document.getElementById('t-raw').textContent,this)">Copy</button>
        <button class="btn btn-sm btn-outline" onclick="downloadText(document.getElementById('t-raw').textContent,'transcript.txt')">Download</button>
      </div></div>
      <div class="section-body" id="t-raw" style="max-height:250px;overflow-y:auto;font-size:.8rem;color:var(--muted)">${e.raw_transcript.replace(/</g,'&lt;')}</div>
    </div>`:''}
    <div class="section">
      <div class="section-title">Rewrite</div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'short')">Short</button>
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'pragmatic')">Pragmatic</button>
        <button class="btn btn-sm btn-outline" onclick="rewrite(${id},'bullets')">Bullets</button>
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
    <button class="btn btn-sm btn-outline" style="margin-top:8px" onclick="copyText(this.previousElementSibling.textContent,this)">Copy</button>`;
}

// Chat
async function sendChat(){
  const input=document.getElementById('chatInput');const q=input.value.trim();if(!q)return;input.value='';
  const msgs=document.getElementById('chat-messages');
  msgs.innerHTML+=`<div class="chat-msg user">${q}</div><div class="chat-msg bot" id="chat-loading">Thinking...</div>`;
  msgs.scrollTop=msgs.scrollHeight;
  const r=await api('/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({question:q})});
  document.getElementById('chat-loading').outerHTML=`<div class="chat-msg bot">${r.answer||r.error||'Error'}</div>`;
  msgs.scrollTop=msgs.scrollHeight;
}

// Recaps
async function loadRecaps(){
  const el=document.getElementById('recap-list');
  const recaps=await api('/recaps');
  if(!recaps.length){el.innerHTML='<div class="loading">No recaps yet. Click "Generate all past recaps".</div>';return}
  el.innerHTML=recaps.map(r=>`
    <div class="section" style="margin-bottom:12px">
      <div class="section-header">
        <div class="section-title">Week of ${r.week_start} (${r.entry_count} entries)</div>
        <button class="btn btn-sm btn-outline" onclick="copyText(this.closest('.section').querySelector('.synthesis-body').textContent,this)">Copy</button>
      </div>
      <div class="synthesis-body">${r.recap||'Empty'}</div>
    </div>`).join('');
}
async function generateAllRecaps(){
  const el=document.getElementById('recap-list');
  el.innerHTML='<div class="section"><div class="loading">Generating recaps for all past weeks... This will take several minutes. Don\'t close this page.</div></div>';
  const r=await api('/generate-all-recaps');
  el.innerHTML=`<div class="loading">${r.message||r.error}</div>`;
  setTimeout(loadRecaps,2000);
}
async function generateMacro(){
  const el=document.getElementById('macro-content');
  el.innerHTML='<div class="section"><div class="loading">Generating macro analysis across all 450+ entries... This may take 1-2 minutes.</div></div>';
  const r=await api('/generate-macro');
  el.innerHTML=`<div class="section" style="margin-bottom:20px">
    <div class="section-header"><div class="section-title">Macro Analysis (${r.entry_count||'?'} entries)</div>
      <button class="btn btn-sm btn-outline" onclick="copyText(this.closest('.section').querySelector('.synthesis-body').textContent,this)">Copy</button></div>
    <div class="synthesis-body">${r.text||r.error||'Error'}</div></div>`;
}
async function loadMacro(){
  const r=await api('/macro');
  if(r&&r.analysis){
    document.getElementById('macro-content').innerHTML=`<div class="section" style="margin-bottom:20px">
      <div class="section-header"><div class="section-title">Macro Analysis (${r.entry_count||'?'} entries) — ${r.created_at||''}</div>
        <div class="badges"><button class="btn btn-sm btn-outline" onclick="copyText(this.closest('.section').querySelector('.synthesis-body').textContent,this)">Copy</button>
        <button class="btn btn-sm" style="background:var(--accent2)" onclick="generateMacro()">Refresh</button></div></div>
      <div class="synthesis-body">${r.analysis}</div></div>`;
  }
}

loadStats();loadCategories();loadRecent();loadRecaps();loadMacro();
</script></body></html>"""


async def _generate_macro_bg():
    """Two-pass macro analysis: category summaries → global analysis."""
    try:
        log.info("Macro analysis: starting pass 1 (category summaries)...")
        cats = await get_all_categories()
        category_summaries = []

        for cat in cats:
            cat_name = cat["category"]
            if cat["cnt"] < 2:
                continue
            entries = await get_category_entries_for_summary(cat_name)
            if not entries:
                continue
            log.info("Macro pass 1: summarizing %s (%d entries)", cat_name, len(entries))
            summary = await synthesize_category(cat_name, entries)
            category_summaries.append({
                "category": cat_name,
                "count": len(entries),
                "summary": summary,
            })

        log.info("Macro analysis: starting pass 2 (global synthesis from %d categories)...", len(category_summaries))
        all_entries = await get_recent_entries(days=9999, limit=500)
        text = await generate_macro_analysis(all_entries, category_summaries)
        await save_macro_analysis(text, len(all_entries))
        log.info("Macro analysis complete")
    except Exception:
        log.exception("Macro analysis failed")


async def _generate_all_recaps_bg():
    """Generate recaps for all past weeks in background."""
    try:
        weeks = await get_all_weeks()
        log.info("Generating recaps for %d weeks...", len(weeks))
        for w in weeks:
            ws, we = w["week_start"], w["week_end"]
            entries = await get_entries_by_week(ws, we)
            if not entries:
                continue
            log.info("Recap for week %s (%d entries)", ws, len(entries))
            text = await generate_recap(entries)
            await save_recap(ws, we, text, len(entries))
        log.info("All recaps generated")
    except Exception:
        log.exception("Recap generation failed")


async def _get_entry(eid: int) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM entries WHERE id = ?", (eid,))
    return dict(rows[0]) if rows else None


async def handle_request(reader, writer):
    try:
        raw = await reader.read(65536)
        if not raw:
            writer.close()
            return
        req_line = raw.decode(errors="replace").split("\r\n")[0]
        parts = req_line.split(" ")
        if len(parts) < 2:
            writer.close()
            return
        method, full_path = parts[0], parts[1]
        headers_str = raw.decode(errors="replace").split("\r\n\r\n")[0]
        path = full_path.split("?")[0]
        authed = _check_auth(headers_str)

        # Login/logout
        if path == "/login" and method == "POST":
            body = _get_body(raw).decode(errors="replace")
            params = dict(p.split("=", 1) for p in body.split("&") if "=" in p)
            pw = unquote(params.get("password", "").replace("+", " "))
            writer.write(_redirect("/", f"brain_session={_create_session()}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800") if pw == PASSWORD else _redirect("/login?err=1"))
            await writer.drain(); writer.close(); return
        if path == "/logout":
            writer.write(_redirect("/", "brain_session=; Path=/; Max-Age=0"))
            await writer.drain(); writer.close(); return
        if not authed and not path.startswith("/api/"):
            writer.write(_resp(LOGIN_PAGE))
            await writer.drain(); writer.close(); return
        if not authed:
            writer.write(_json_resp({"error": "Unauthorized"}, 401))
            await writer.drain(); writer.close(); return

        # Routes
        if path == "/":
            writer.write(_resp(DASHBOARD))
        elif path == "/api/stats":
            writer.write(_json_resp(await get_stats()))
        elif path == "/api/categories":
            writer.write(_json_resp(await get_all_categories()))
        elif path.startswith("/api/category/"):
            cat = unquote(path.split("/api/category/", 1)[1])
            qs = parse_qs(urlparse(full_path).query)
            limit = int(qs.get("limit", [100])[0])
            writer.write(_json_resp(await get_entries_by_category(cat, limit=limit)))
        elif path == "/api/search":
            qs = parse_qs(urlparse(full_path).query)
            q = qs.get("q", [""])[0]
            writer.write(_json_resp(await search_entries(q, limit=30) if q else []))
        elif path == "/api/recent":
            qs = parse_qs(urlparse(full_path).query)
            days = int(qs.get("days", [9999])[0])
            limit = int(qs.get("limit", [500])[0])
            writer.write(_json_resp(await get_recent_entries(days=days, limit=limit)))
        elif path.startswith("/api/entry/"):
            eid = int(path.split("/api/entry/")[1])
            writer.write(_json_resp(await _get_entry(eid) or {"error": "Not found"}))
        elif path.startswith("/api/platform/"):
            p = path.split("/api/platform/")[1]
            writer.write(_json_resp(await get_entries_by_platform(p)))
        elif path.startswith("/api/rewrite/"):
            eid = int(path.split("/api/rewrite/")[1].split("?")[0])
            qs = parse_qs(urlparse(full_path).query)
            style = qs.get("style", ["short"])[0]
            entry = await _get_entry(eid)
            if not entry:
                writer.write(_json_resp({"error": "Not found"}))
            else:
                from app.llm import _call
                prompts = {"short": "Rewrite in 3-5 bullet points max.", "pragmatic": "ONLY actionable steps. What should I DO?", "bullets": "Clean bullet-point format. One sentence per point.", "detailed": "Expand. Cover every angle and nuance."}
                text = await _call([{"role": "system", "content": "Rewrite content. HTML (b,i,u,a). No censorship. Newlines, not <br>."}, {"role": "user", "content": f"{prompts.get(style, prompts['short'])}\n\nORIGINAL:\n{entry.get('analysis', '')}"}])
                writer.write(_json_resp({"text": text}))
        elif path.startswith("/api/synthesize/"):
            cat = unquote(path.split("/api/synthesize/")[1])
            entries = await get_category_entries_for_summary(cat)
            if not entries:
                writer.write(_json_resp({"error": "No entries in this category"}))
            else:
                text = await synthesize_category(cat, entries)
                writer.write(_json_resp({"text": text}))
        elif path == "/api/recaps":
            writer.write(_json_resp(await get_all_recaps()))

        elif path == "/api/macro":
            macro = await get_latest_macro()
            writer.write(_json_resp(macro or {}))

        elif path == "/api/generate-all-recaps":
            import asyncio as _aio
            _aio.create_task(_generate_all_recaps_bg())
            writer.write(_json_resp({"message": "Generating recaps in background. They will appear shortly."}))

        elif path == "/api/generate-macro":
            import asyncio as _aio
            _aio.create_task(_generate_macro_bg())
            writer.write(_json_resp({"text": "Macro analysis started in background. Generating category summaries first, then global analysis. Refresh in 2-3 minutes.", "entry_count": 0}))

        elif path == "/api/generate-recap":
            entries = await get_recent_entries(days=7, limit=50)
            if not entries:
                writer.write(_json_resp({"text": "No entries from the past 7 days."}))
            else:
                text = await generate_recap(entries)
                writer.write(_json_resp({"text": text}))
        elif path == "/api/chat" and method == "POST":
            body = json.loads(_get_body(raw))
            question = body.get("question", "")
            entries = await search_entries(question, limit=8)
            if not entries:
                entries = await get_recent_entries(days=60, limit=10)
            answer = await query_brain(question, entries)
            writer.write(_json_resp({"answer": answer}))
        else:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")
        await writer.drain()
    except Exception:
        log.exception("Web error")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def start_web_server(port: int = 8080):
    server = await start_server(handle_request, "0.0.0.0", port)
    log.info("Web dashboard on :%d", port)
    return server
