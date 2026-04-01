"""Lightweight web dashboard for AKS Knowledge Brain."""
from __future__ import annotations

import json
import logging
from asyncio import start_server

from app.database import (
    get_stats, get_all_categories, get_entries_by_category,
    search_entries, get_recent_entries, get_db,
)

log = logging.getLogger(__name__)

HTML_PAGE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AKS Brain</title>
<style>
:root { --bg: #0a0a0a; --surface: #141414; --surface2: #1e1e1e; --border: #2a2a2a; --text: #e8e8e8; --muted: #888; --accent: #3b82f6; --accent2: #8b5cf6; }
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); line-height: 1.6; }
.container { max-width: 900px; margin: 0 auto; padding: 20px; }
h1 { font-size: 1.5rem; margin-bottom: 4px; }
.subtitle { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
.stats { display: flex; gap: 12px; margin-bottom: 24px; flex-wrap: wrap; }
.stat { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 20px; flex: 1; min-width: 120px; }
.stat-value { font-size: 1.8rem; font-weight: 700; color: var(--accent); }
.stat-label { font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
.search-bar { display: flex; gap: 8px; margin-bottom: 24px; }
.search-bar input { flex: 1; background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; color: var(--text); font-size: 0.95rem; outline: none; }
.search-bar input:focus { border-color: var(--accent); }
.search-bar button { background: var(--accent); color: #fff; border: none; border-radius: 8px; padding: 12px 20px; cursor: pointer; font-weight: 600; }
.tabs { display: flex; gap: 4px; margin-bottom: 16px; border-bottom: 1px solid var(--border); padding-bottom: 8px; flex-wrap: wrap; }
.tab { background: none; border: none; color: var(--muted); padding: 8px 16px; cursor: pointer; border-radius: 6px; font-size: 0.85rem; }
.tab.active { background: var(--surface2); color: var(--text); }
.tab:hover { background: var(--surface); color: var(--text); }
.entries { display: flex; flex-direction: column; gap: 12px; }
.entry { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 20px; cursor: pointer; transition: border-color 0.2s; }
.entry:hover { border-color: var(--accent); }
.entry-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px; gap: 12px; }
.entry-title { font-weight: 600; font-size: 0.95rem; }
.entry-meta { display: flex; gap: 8px; flex-shrink: 0; }
.badge { font-size: 0.7rem; padding: 3px 8px; border-radius: 20px; background: var(--surface2); color: var(--muted); border: 1px solid var(--border); }
.badge.cat { background: rgba(59,130,246,0.15); color: var(--accent); border-color: rgba(59,130,246,0.3); }
.badge.platform { background: rgba(139,92,246,0.15); color: var(--accent2); border-color: rgba(139,92,246,0.3); }
.entry-analysis { color: var(--muted); font-size: 0.85rem; line-height: 1.5; max-height: 0; overflow: hidden; transition: max-height 0.3s; }
.entry.open .entry-analysis { max-height: 5000px; }
.entry-analysis-content { padding-top: 12px; border-top: 1px solid var(--border); margin-top: 12px; color: var(--text); }
.entry-analysis-content b { color: var(--accent); }
.entry-url { font-size: 0.75rem; color: var(--muted); margin-top: 8px; word-break: break-all; }
.entry-url a { color: var(--accent); text-decoration: none; }
.loading { text-align: center; padding: 40px; color: var(--muted); }
.empty { text-align: center; padding: 40px; color: var(--muted); }
.cat-list { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 24px; }
.cat-btn { background: var(--surface); border: 1px solid var(--border); border-radius: 20px; padding: 6px 14px; color: var(--text); cursor: pointer; font-size: 0.8rem; transition: all 0.2s; }
.cat-btn:hover, .cat-btn.active { background: rgba(59,130,246,0.15); border-color: var(--accent); color: var(--accent); }
.cat-count { color: var(--muted); font-size: 0.75rem; }
@media (max-width: 600px) { .stats { flex-direction: column; } .stat { min-width: auto; } }
</style>
</head>
<body>
<div class="container">
  <h1>AKS Brain</h1>
  <p class="subtitle">Personal Knowledge Base</p>

  <div class="stats" id="stats"><div class="loading">Loading...</div></div>

  <div class="search-bar">
    <input type="text" id="searchInput" placeholder="Search your brain..." onkeydown="if(event.key==='Enter')doSearch()">
    <button onclick="doSearch()">Search</button>
  </div>

  <div class="cat-list" id="categories"></div>

  <div class="tabs" id="tabs">
    <button class="tab active" onclick="loadRecent()" data-tab="recent">Recent</button>
    <button class="tab" onclick="loadAll()" data-tab="all">All</button>
  </div>

  <div class="entries" id="entries"><div class="loading">Loading...</div></div>
</div>

<script>
const API = '';

async function api(path) {
  const r = await fetch('/api' + path);
  return r.json();
}

async function loadStats() {
  const s = await api('/stats');
  document.getElementById('stats').innerHTML = `
    <div class="stat"><div class="stat-value">${s.total}</div><div class="stat-label">Total entries</div></div>
    <div class="stat"><div class="stat-value">${s.this_week}</div><div class="stat-label">This week</div></div>
    <div class="stat"><div class="stat-value">${Object.keys(s.platforms).length}</div><div class="stat-label">Platforms</div></div>
  `;
}

async function loadCategories() {
  const cats = await api('/categories');
  document.getElementById('categories').innerHTML = cats.map(c =>
    `<button class="cat-btn" onclick="loadCategory('${c.category}')">${c.category} <span class="cat-count">${c.cnt}</span></button>`
  ).join('');
}

function renderEntries(entries) {
  if (!entries.length) {
    document.getElementById('entries').innerHTML = '<div class="empty">No entries found</div>';
    return;
  }
  document.getElementById('entries').innerHTML = entries.map(e => {
    const title = e.title || e.url?.replace(/https?:\\/\\//, '').slice(0, 60) || 'Untitled';
    const analysis = (e.analysis || '').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/&lt;b&gt;/g, '<b>').replace(/&lt;\\/b&gt;/g, '</b>')
      .replace(/&lt;i&gt;/g, '<i>').replace(/&lt;\\/i&gt;/g, '</i>')
      .replace(/&lt;a /g, '<a ').replace(/&lt;\\/a&gt;/g, '</a>').replace(/&gt;/g, '>');
    let keyPoints = '';
    try {
      const kp = JSON.parse(e.key_points || '[]');
      if (Array.isArray(kp) && kp.length) keyPoints = '<br>' + kp.map(p => '• ' + p).join('<br>');
    } catch(ex) {}
    return `<div class="entry" onclick="this.classList.toggle('open')">
      <div class="entry-header">
        <div class="entry-title">${title}</div>
        <div class="entry-meta">
          ${e.category ? `<span class="badge cat">${e.category}</span>` : ''}
          ${e.platform ? `<span class="badge platform">${e.platform}</span>` : ''}
        </div>
      </div>
      <div class="entry-url"><a href="${e.url}" target="_blank">${e.url || ''}</a></div>
      <div class="entry-analysis"><div class="entry-analysis-content">${keyPoints}${analysis ? '<br><br>' + analysis : ''}</div></div>
    </div>`;
  }).join('');
}

async function loadRecent() {
  setTab('recent');
  document.getElementById('entries').innerHTML = '<div class="loading">Loading...</div>';
  const entries = await api('/recent?days=30&limit=50');
  renderEntries(entries);
}

async function loadAll() {
  setTab('all');
  document.getElementById('entries').innerHTML = '<div class="loading">Loading...</div>';
  const entries = await api('/recent?days=9999&limit=200');
  renderEntries(entries);
}

async function loadCategory(cat) {
  document.querySelectorAll('.cat-btn').forEach(b => b.classList.toggle('active', b.textContent.includes(cat)));
  document.getElementById('entries').innerHTML = '<div class="loading">Loading...</div>';
  const entries = await api('/category/' + encodeURIComponent(cat));
  renderEntries(entries);
}

async function doSearch() {
  const q = document.getElementById('searchInput').value.trim();
  if (!q) return;
  document.getElementById('entries').innerHTML = '<div class="loading">Searching...</div>';
  const entries = await api('/search?q=' + encodeURIComponent(q));
  renderEntries(entries);
}

function setTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
}

loadStats();
loadCategories();
loadRecent();
</script>
</body>
</html>"""


async def handle_request(reader, writer):
    """Handle HTTP requests for the web dashboard."""
    try:
        raw = await reader.read(4096)
        request_line = raw.decode().split("\r\n")[0]
        parts = request_line.split(" ")
        if len(parts) < 2:
            writer.close()
            return
        method, path = parts[0], parts[1]

        if path == "/" or path == "":
            body = HTML_PAGE.encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        elif path == "/api/stats":
            stats = await get_stats()
            body = json.dumps(stats).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        elif path == "/api/categories":
            cats = await get_all_categories()
            body = json.dumps(cats).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        elif path.startswith("/api/category/"):
            cat = path.split("/api/category/")[1]
            from urllib.parse import unquote
            cat = unquote(cat)
            entries = await get_entries_by_category(cat, limit=50)
            body = json.dumps(entries, default=str).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        elif path.startswith("/api/search"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(path).query)
            q = qs.get("q", [""])[0]
            if q:
                entries = await search_entries(q, limit=20)
            else:
                entries = []
            body = json.dumps(entries, default=str).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        elif path.startswith("/api/recent"):
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(path).query)
            days = int(qs.get("days", [7])[0])
            limit = int(qs.get("limit", [50])[0])
            entries = await get_recent_entries(days=days, limit=limit)
            body = json.dumps(entries, default=str).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

        else:
            writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")

        await writer.drain()
    except Exception as e:
        log.exception("Web request error")
    finally:
        writer.close()


async def start_web_server(port: int = 8080):
    server = await start_server(handle_request, "0.0.0.0", port)
    log.info("Web dashboard running on :%d", port)
    return server
