#!/usr/bin/env python3 """ Enhanced Privacy- & Tech-Deal Scraper with JSON metrics, color embeds, and fallback """ import argparse import asyncio import datetime import json import os import re import sqlite3

import feedparser import httpx import yaml from bs4 import BeautifulSoup from rapidfuzz.fuzz import token_set_ratio from tenacity import retry, stop_after_attempt, wait_exponential

CONFIGURATION

ROOT = os.path.dirname(file) CONF = yaml.safe_load(open(os.path.join(ROOT, "config/sources.yaml"))) DB_PATH = os.path.join(ROOT, "data/deals.sqlite")

Support multiple webhooks via DISCORD_WEBHOOKS or single via DISCORD_WEBHOOK

env_hooks = os.getenv("DISCORD_WEBHOOKS", "") single_hook = os.getenv("DISCORD_WEBHOOK", "") if env_hooks: WEBHOOKS = [h.strip() for h in env_hooks.split(",") if h.strip()] elif single_hook: WEBHOOKS = [single_hook] else: WEBHOOKS = []

class Deal(dict): """Hold deal data""" pass

CLI ARGUMENTS

def parse_args(): parser = argparse.ArgumentParser(description="Run scraper") parser.add_argument( "--dry-run", action="store_true", help="Print deals without posting" ) parser.add_argument( "--source", type=str, help="Filter by a single source/subreddit" ) parser.add_argument( "--force", choices=["rss", "html", "reddit", "all"], help="Force fetch specific type" ) return parser.parse_args()

DATABASE

def db_connect(): conn = sqlite3.connect(DB_PATH) conn.execute( "CREATE TABLE IF NOT EXISTS seen (sig TEXT PRIMARY KEY, posted_at TEXT)" ) return conn

FETCH UTILITIES

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30)) async def fetch_html(url, client: httpx.AsyncClient): response = await client.get(url, timeout=20) response.raise_for_status() return response.text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30)) async def fetch_json(url, client: httpx.AsyncClient): response = await client.get(url, timeout=20) response.raise_for_status() return response.json()

PARSERS

async def scrape_reddit(sub, client): data = await fetch_json( f"https://www.reddit.com/r/{sub}/new.json?limit=20", client ) results = [] for item in data.get("data", {}).get("children", []): d = item["data"] results.append( Deal({ "title": d.get("title", ""), "body": ( f"score {d.get('score', 0)} · {d.get('num_comments', 0)} comments" ), "url": f"https://reddit.com{d.get('permalink', '')}", "source": f"r/{sub}", "fetched": datetime.datetime.utcnow().isoformat(), "tags": [], }) ) return results

async def scrape_html(entry, client): html = await fetch_html(entry["url"], client) soup = BeautifulSoup(html, "lxml") deals = [] for elem in soup.select(entry.get("selector", "")): title = (elem.get("alt") or elem.get_text() or entry.get("name", ""))[:120] body = " ".join(elem.stripped_strings)[:200] link = entry.get("url", "") anchor = elem.select_one("a[href]") if anchor and anchor.get("href"): link = anchor.get("href") deals.append( Deal({ "title": title, "body": body, "url": link, "source": entry.get("name", ""), "fetched": datetime.datetime.utcnow().isoformat(), "tags": entry.get("tags", []), "min_hot": entry.get( "min_hot_score", CONF.get("global_min_hot", 0.3) ), "ttl_days": entry.get( "ttl_days", CONF.get("ttl_default_days", 30) ), "needs_js": entry.get("needs_js", False), }) ) return deals

async def scrape_rss(entry): feed = feedparser.parse(entry.get("url", "")) deals = [] for e in feed.entries[:20]: deals.append( Deal({ "title": e.get("title", "")[:120], "body": e.get("summary", "")[:200], "url": e.get("link", ""), "source": entry.get("name", ""), "fetched": datetime.datetime.utcnow().isoformat(), "tags": entry.get("tags", []), "min_hot": entry.get( "min_hot_score", CONF.get("global_min_hot", 0.3) ), "ttl_days": entry.get( "ttl_days", CONF.get("ttl_default_days", 30) ), }) ) return deals

SCORING & DEDUPLICATION

def score_pct(text): match = re.search(r"(\d{1,3})%", text) return int(match.group(1)) / 100 if match else 0

def hot_score(deal): base = score_pct(deal.get("title", "") + deal.get("body", "")) return max(base, deal.get("min_hot", CONF.get("global_min_hot", 0.3)))

def make_sig(deal): return re.sub(r"\W+", "", deal.get("title", "").lower())[:80]

def fuzzy_seen(conn, title): rows = conn.execute("SELECT sig FROM seen").fetchall() return any(token_set_ratio(r[0], title) > 90 for r in rows)

DISCORD POST & LOGGING

def post_embed(embed, errors): print(f"Posting to {len(WEBHOOKS)} webhook(s): {WEBHOOKS}") import requests

if errors:
    count = sum(len(es) for es in errors.values())
    details = [f"[{src}] {msg}" for src, es in errors.items() for msg in es]
    spoiler = "||\n" + "\n".join(details) + "\n||"
    embed["fields"].append(
        {"name": f"Errors ({count})", "value": spoiler, "inline": False}
    )
else:
    embed["fields"].append({
        "name": "Errors", "value": "_No errors_", "inline": False
    })

payload = {"embeds": [embed]}
for hook in WEBHOOKS:
    resp = requests.post(hook, json=payload, timeout=15)
    resp.raise_for_status()

MAIN FUNCTION

def main(): args = parse_args() now = datetime.datetime.utcnow()

loop = asyncio.new_event_loop()
 asyncio.set_event_loop(loop)
 client = httpx.AsyncClient(timeout=20)

 tasks, sources = [], []
 for e in CONF.get("html",[]):
     if args.source and args.source!=e.get("name"): continue
     if e.get("needs_js"): continue
     tasks.append(scrape_html(e, client)); sources.append(e.get("name"))
 for e in CONF.get("rss",[]):
     if args.source and args.source!=e.get("name"): continue
     tasks.append(scrape_rss(e)); sources.append(e.get("name"))
 for cat in CONF.get("categories",{}).values():
     for sub in cat.get("reddit",[]):
         if args.source and args.source!=sub: continue
         tasks.append(scrape_reddit(sub, client)); sources.append(f"r/{sub}")

 results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
 loop.run_until_complete(client.aclose())

 deals, errors = [], {}
 from tenacity import RetryError
 for src, res in zip(sources, results):
     if isinstance(res, Exception):
         # Unwrap RetryError to get the underlying HTTP error
         if isinstance(res, RetryError) and hasattr(res, 'last_attempt'):
             cause = res.last_attempt.exception()
             msg = f"{type(cause).__name__}: {cause}"
         else:
             msg = f"{type(res).__name__}: {res}"
         errors.setdefault(src, []).append(msg)
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
         expired = (now - dt).days > d.get("ttl_days", 30)
     if (not row or expired) and not fuzzy_seen(conn, d.get("title", "")) and hot_score(d) >= d.get("min_hot", 0):
         fresh.append(d)
         conn.execute("REPLACE INTO seen VALUES(?,?)", (sig, now.isoformat()))
 conn.commit(); conn.close()

 # write metrics.json
 out = {
     "total_scraped": len(deals),
     "fresh_deals": [
         {"source": d["source"], "title": d["title"], "hot_score": hot_score(d)} for d in fresh
     ],
     "errors": errors
 }
 with open("metrics.json", "w") as mf:
     json.dump(out, mf)

 if not args.dry_run:
     if not fresh:
         emb = {
             "title": "No Waifus For You Weeb 🙅‍♀️",
             "description": f"Ran at {now.strftime('%Y-%m-%d %H:%M UTC')} but found no fresh deals.",
             "fields": []
         }
         default_color = CONF.get("colors", {}).get("default")
         if default_color is not None:
             emb["color"] = default_color
         post_embed(emb, errors)
         return

     sorted_fresh = sorted(fresh, key=lambda x: x.get("fetched"), reverse=True)
     emb = {
         "title": f"Privacy Deals – {now.strftime('%Y-%m-%d %H:%M UTC')}",
         "fields": []
     }
     default_color = CONF.get("colors", {}).get("default")
     if default_color is not None:
         emb["color"] = default_color
     for d in sorted_fresh[:5]:
         emb["fields"].append({"name": d.get("title"), "value": f"{d['body']}

{d['url']}", "inline": False}) post_embed(emb, errors) return

# dry-run output
 for d in fresh:
     print(f"[{d.get('source')}] {d.get('title')} – {d.get('url')}")
 if errors:
     print(f"Errors ({sum(len(es) for es in errors.values())}):")
     for src, es in errors.items():
         for e in es:
             print(f"  [{src}] {e}")
 else:
     print("_No errors_")

if name == "main": main() == "main": main()

