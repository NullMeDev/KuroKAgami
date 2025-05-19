#!/usr/bin/env python3
"""
Enhanced Privacy- & Tech-Deal Scraper with JSON metrics, color embeds, and fallback
"""
import argparse
import asyncio
import datetime
import json
import os
import re
import sqlite3

import feedparser
import httpx
import yaml
from bs4 import BeautifulSoup
from rapidfuzz.fuzz import token_set_ratio
from tenacity import retry, stop_after_attempt, wait_exponential

# CONFIGURATION
ROOT = os.path.dirname(__file__)
CONF = yaml.safe_load(open(os.path.join(ROOT, "config/sources.yaml")))
DB_PATH = os.path.join(ROOT, "data/deals.sqlite")

# Support multiple webhooks via DISCORD_WEBHOOKS or single via DISCORD_WEBHOOK
env_hooks = os.getenv("DISCORD_WEBHOOKS", "")
single_hook = os.getenv("DISCORD_WEBHOOK", "")
if env_hooks:
    WEBHOOKS = [h.strip() for h in env_hooks.split(",") if h.strip()]
elif single_hook:
    WEBHOOKS = [single_hook]
else:
    WEBHOOKS = []

class Deal(dict):
    """Hold deal data"""
    pass

# CLI ARGUMENTS
def parse_args():
    parser = argparse.ArgumentParser(description="Run scraper")
    parser.add_argument("--dry-run", action="store_true", help="Print deals without posting")
    parser.add_argument("--source", type=str, help="Filter by a single source/subreddit")
    parser.add_argument(
        "--force",
        choices=["rss", "html", "reddit", "all"],
        help="Force fetch specific type"
    )
    return parser.parse_args()

# DATABASE
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS seen (sig TEXT PRIMARY KEY, posted_at TEXT)")
    return conn

# FETCH UTILITIES
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_html(url, client: httpx.AsyncClient):
    response = await client.get(url, timeout=20)
    response.raise_for_status()
    return response.text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_json(url, client: httpx.AsyncClient):
    response = await client.get(url, timeout=20)
    response.raise_for_status()
    return response.json()

# PARSERS
async def scrape_reddit(sub, client):
    data = await fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=20", client)
    results = []
    for item in data.get("data", {}).get("children", []):
        d = item["data"]
        results.append(Deal({
            "title": d.get("title", ""),
            "body": f"score {d.get('score',0)} · {d.get('num_comments',0)} comments",
            "url": f"https://reddit.com{d.get('permalink','')}",
            "source": f"r/{sub}",
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": [],
        }))
    return results

async def scrape_html(entry, client):
    html = await fetch_html(entry["url"], client)
    soup = BeautifulSoup(html, "lxml")
    deals = []
    for elem in soup.select(entry["selector"]):
        title = (elem.get("alt") or elem.get_text() or entry["name"])[:120]
        body = " ".join(elem.stripped_strings)[:200]
        link = entry["url"]
        a = elem.select_one("a[href]")
        if a:
            link = a["href"]
        deals.append(Deal({
            "title": title,
            "body": body,
            "url": link,
            "source": entry["name"],
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get("tags", []),
            "min_hot": entry.get("min_hot_score", CONF.get("global_min_hot", 0.3)),
            "ttl_days": entry.get("ttl_days", CONF.get("ttl_default_days", 30)),
            "needs_js": entry.get("needs_js", False),
        }))
    return deals

async def scrape_rss(entry):
    feed = feedparser.parse(entry["url"])
    deals = []
    for e in feed.entries[:20]:
        deals.append(Deal({
            "title": e.get("title","")[:120],
            "body": e.get("summary","")[:200],
            "url": e.get("link",""),
            "source": entry["name"],
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get("tags", []),
            "min_hot": entry.get("min_hot_score", CONF.get("global_min_hot", 0.3)),
            "ttl_days": entry.get("ttl_days", CONF.get("ttl_default_days", 30)),
        }))
    return deals

# SCORING & DEDUPLICATION
def score_pct(text):
    m = re.search(r"(\d{1,3})%", text)
    return int(m.group(1))/100 if m else 0

def hot_score(deal):
    base = score_pct(deal["title"] + deal["body"])
    return max(base, deal.get("min_hot", CONF.get("global_min_hot", 0.3)))

def make_sig(deal):
    return re.sub(r"\W+", "", deal["title"].lower())[:80]

def fuzzy_seen(conn, title):
    rows = conn.execute("SELECT sig FROM seen").fetchall()
    return any(token_set_ratio(r[0], title) > 90 for r in rows)

# DISCORD POST
def post_embed(embed, errors):
    import requests
    if errors:
        total = sum(len(errs) for errs in errors.values())
        details = [f"[{src}] {msg}" for src, errs in errors.items() for msg in errs]
        spoiler = "||\n" + "\n".join(details) + "\n||"
        embed["fields"].append({"name": f"Errors ({total})", "value": spoiler, "inline": False})
    else:
        embed["fields"].append({"name": "Errors", "value": "_No errors_", "inline": False})
    for hook in WEBHOOKS:
        requests.post(hook, json=embed, timeout=15)

# MAIN ENTRYPOINT
async def main():
    args = parse_args()
    now = datetime.datetime.utcnow()

    tasks = []
    sources = []
    async with httpx.AsyncClient(timeout=20) as client:
        for entry in CONF.get("html", []):
            if args.source and args.source != entry["name"]:
                continue
            if entry.get("needs_js"):
                continue
            tasks.append(scrape_html(entry, client))
            sources.append(entry["name"])
        for entry in CONF.get("rss", []):
            if args.source and args.source != entry["name"]:
                continue
            tasks.append(scrape_rss(entry))
            sources.append(entry["name"])
        for cat in CONF.get("categories", {}).values():
            for sub in cat.get("reddit", []):
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
    for deal in deals:
        sig = make_sig(deal)
        row = conn.execute("SELECT posted_at FROM seen WHERE sig=?", (sig,)).fetchone()
        expired = False
        if row:
            dt = datetime.datetime.fromisoformat(row[0])
            expired = (now - dt).days > deal.get("ttl_days", 30)
        if (not row or expired) and not fuzzy_seen(conn, deal["title"]) and hot_score(deal) >= deal.get("min_hot", 0):
            fresh.append(deal)
            conn.execute("REPLACE INTO seen VALUES(?,?)", (sig, now.isoformat()))
    conn.commit()
    conn.close()

    # Write metrics.json
    metrics = {
        "total_scraped": len(deals),
        "fresh_deals": [
            {"source": d["source"], "title": d["title"], "hot_score": hot_score(d)}
            for d in fresh
        ],
        "errors": errors
    }
    with open("metrics.json", "w") as mf:
        json.dump(metrics, mf)

    # POSTING
    if not args.dry_run:
        if not fresh:
            embed = {
                "title": "No Waifus For You Weeb 🙅‍♀️",
                "description": f"Ran at {now.strftime('%Y-%m-%d %H:%M UTC')} but found no fresh deals.",
                "fields": []
            }
            default_color = CONF.get("colors", {}).get("default")
            if default_color is not None:
                embed["color"] = default_color
            post_embed(embed, errors)
            return

        sorted_fresh = sorted(fresh, key=lambda x: x["fetched"], reverse=True)
        embed = {
            "title": f"Privacy Deals – {now.strftime('%Y-%m-%d %H:%M UTC')}",
            "fields": []
        }
        default_color = CONF.get("colors", {}).get("default")
        if default_color is not None:
            embed["color"] = default_color
        for d in sorted_fresh[:5]:
            embed["fields"].append({
                "name": d["title"],
                "value": f"{d['body']}\n{d['url']}",
                "inline": False
            })
        post_embed(embed, errors)
        return

    # DRY-RUN OUTPUT
    for d in fresh:
        print(f"[{d['source']}] {d['title']} – {d['url']}")
    if errors:
        print(f"Errors ({sum(len(es) for es in errors.values())}):")
        for src, errs in errors.items():
            for e in errs:
                print(f"  [{src}] {e}")
    else:
        print("_No errors_")

if __name__ == "__main__":
    asyncio.run(main())