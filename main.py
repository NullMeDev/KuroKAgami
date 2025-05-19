#!/usr/bin/env python3
"""
Enhanced Privacy- & Tech-Deal Scraper with JSON metrics output
- Scheduled RSS/HTML/Reddit per config.refresh_times
- Per-source TTL, hot thresholds, JS fallback flags, tags
- CLI dry-run & source filtering
- Category grouping multi-embed support
- Error capture with clickable Discord spoiler logs
- Writes metrics.json with valid JSON for downstream steps
"""
import argparse
import asyncio
import datetime
import json
import os
import re
import sqlite3
import sys

import feedparser
import httpx
import yaml
from bs4 import BeautifulSoup
from rapidfuzz.fuzz import token_set_ratio
from tenacity import retry, stop_after_attempt, wait_exponential

# ───────────────────────── CONFIG ─────────────────────────
ROOT = os.path.dirname(__file__)
CONF = yaml.safe_load(open(os.path.join(ROOT, "config/sources.yaml")))
DB_PATH = os.path.join(ROOT, "data/deals.sqlite")

WEBHOOKS = [w.strip() for w in os.getenv("DISCORD_WEBHOOKS", "").split(",") if w.strip()]

# util
class Deal(dict):
    """Deal: {title, body, url, source, fetched, tags:list[str]}"""
    pass

# ─────────────────── ARGPARSE CLI ─────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Run scraper")
    p.add_argument("--dry-run", action="store_true", help="Print parsed deals, don’t post to Discord")
    p.add_argument("--source", type=str, help="Only process one source by name/subreddit")
    p.add_argument("--force", choices=["rss","html","reddit","all"], help="Ignore schedule and force-fetch specific type")
    return p.parse_args()

# ─────────────────── DB FUNCTIONS ─────────────────────────
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen (sig TEXT PRIMARY KEY, posted_at TEXT)")
    return conn

# ────────────────── FETCH UTILITIES ───────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_html(url, client: httpx.AsyncClient):
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    return r.text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_json(url, client: httpx.AsyncClient):
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

# ─────────────────── PARSERS ─────────────────────────────
async def scrape_reddit(sub, client):
    data = await fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=20", client)
    out = []
    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        out.append(Deal({
            "title": d.get("title",""),
            "body": f"score {d.get('score', 0)} · {d.get('num_comments', 0)} comments",
            "url": f"https://reddit.com{d.get('permalink','')}",
            "source": f"r/{sub}",
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": [],
        }))
    return out

async def scrape_html(entry, client):
    html = await fetch_html(entry['url'], client)
    soup = BeautifulSoup(html, "lxml")
    out = []
    for b in soup.select(entry['selector']):
        title = (b.get('alt') or b.get_text() or entry['name'])[:120]
        body = " ".join(b.stripped_strings)[:200]
        link = entry['url']
        a = b.select_one('a[href]')
        if a:
            link = a['href']
        out.append(Deal({
            "title": title,
            "body": body,
            "url": link,
            "source": entry['name'],
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get('tags', []),
            "min_hot": entry.get('min_hot_score', CONF.get('global_min_hot', 0.3)),
            "ttl_days": entry.get('ttl_days', CONF.get('ttl_default_days', 30)),
            "needs_js": entry.get('needs_js', False),
        }))
    return out

async def scrape_rss(entry):
    fp = feedparser.parse(entry['url'])
    out = []
    for e in fp.entries[:20]:
        out.append(Deal({
            "title": e.get('title','')[:120],
            "body": e.get('summary','')[:200],
            "url": e.get('link',''),
            "source": entry['name'],
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get('tags', []),
            "min_hot": entry.get('min_hot_score', CONF.get('global_min_hot',0.3)),
            "ttl_days": entry.get('ttl_days', CONF.get('ttl_default_days',30)),
        }))
    return out

# ────────────────── HOT & DEDUPE LOGIC ─────────────────────
def score_pct(text):
    m = re.search(r"(\d{1,3})%", text)
    return int(m.group(1))/100 if m else 0

def hot_score(deal):
    base = score_pct(deal['title'] + deal['body'])
    return max(base, deal.get('min_hot', CONF.get('global_min_hot',0.3)))

def make_sig(deal):
    return re.sub(r"\W+", "", deal['title'].lower())[:80]

def fuzzy_seen(conn, title):
    rows = conn.execute("SELECT sig FROM seen").fetchall()
    return any(token_set_ratio(r[0], title)>90 for r in rows)

# ─────────────────── DISCORD POST & LOG ───────────────────
def post_embed(embed, errors):
    import requests
    if errors:
        count = sum(len(es) for es in errors.values())
        details = []
        for src, es in errors.items():
            for e in es:
                details.append(f"[{src}] {e}")
        spoiler = "||\n" + "\n".join(details) + "\n||"
        embed['fields'].append({
            'name': f"Errors ({count})",
            'value': spoiler,
            'inline': False
        })
    else:
        embed['fields'].append({
            'name': 'Errors',
            'value': '_No errors_',
            'inline': False
        })
    for url in WEBHOOKS:
        requests.post(url, json=embed, timeout=15)

# ────────────────────── MAIN ───────────────────────────────
async def main():
    args = parse_args()
    now = datetime.datetime.utcnow()
    tasks = []
    sources = []

    async with httpx.AsyncClient(timeout=20) as client:
        for e in CONF.get('html', []):
            if args.source and args.source != e['name']:
                continue
            if e.get('needs_js'):
                continue
            tasks.append(scrape_html(e, client))
            sources.append(e['name'])
        for e in CONF.get('rss', []):
            if args.source and args.source != e['name']:
                continue
            tasks.append(scrape_rss(e))
            sources.append(e['name'])
        for cat in CONF.get('categories', {}).values():
            for sub in cat.get('reddit', []):
                if args.source and args.source != sub:
                    continue
                tasks.append(scrape_reddit(sub, client))
                sources.append(f"r/{sub}")

    results = await asyncio.gather(*tasks, return_exceptions=True)

    deals = []
    errors = {}
    for src, res in zip(sources, results):
        if isinstance(res, Exception):
            errors.setdefault(src, []).append(str(res))
        else:
            deals.extend(res)

    conn = db_connect()
    fresh = []
    for d in deals:
        sig = make_sig(d)
        row = conn.execute("SELECT posted_at FROM seen WHERE sig=?", (sig,)).fetchone()
        expired = False
        if row:
            dt = datetime.datetime.fromisoformat(row[0])
            expired = (now - dt).days > d.get('ttl_days', 30)
        if (not row or expired) and not fuzzy_seen(conn, d['title']) and hot_score(d) >= d.get('min_hot', 0):
            fresh.append(d)
            conn.execute("REPLACE INTO seen VALUES(?,?)", (sig, now.isoformat()))
    conn.commit()
    conn.close()

    # Write metrics.json for workflow
    metrics = {
        "total_scraped": len(deals),
        "fresh_deals": [
            {"source": d['source'], "title": d['title'], "hot_score": hot_score(d)}
            for d in fresh
        ],
        "errors": errors
    }
    with open('metrics.json', 'w') as mf:
        json.dump(metrics, mf)

    if not args.dry_run:
        for cat, spec in CONF.get('categories', {}).items():
            cat_deals = [d for d in fresh if cat in d.get('tags', [])]
            if cat_deals:
                embed = {
                    "title": f"{cat.replace('_', ' ').title()} Deals – {now.strftime('%Y-%m-%d %H:%M UTC')}",
                    "fields": []
                }
                for d in cat_deals[:10]:
                    embed['fields'].append({
                        'name': d['title'],
                        'value': f"{d['body']}\n{d['url']}",
                        'inline': False
                    })
                post_embed(embed, errors)
    else:
        for d in fresh:
            print(f"[{d['source']}] {d['title']} – {d['url']}")
        if errors:
            print(f"Errors ({sum(len(es) for es in errors.values())}):")
            for src, es in errors.items():
                for e in es:
                    print(f"  [{src}] {e}")
        else:
            print("_No errors_")

if __name__ == "__main__":
    asyncio.run(main())
