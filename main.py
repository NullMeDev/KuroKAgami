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
import sys
import logging
import time
import requests

import feedparser
import httpx
import yaml

from bs4 import BeautifulSoup
from rapidfuzz.fuzz import token_set_ratio
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ───────────────────────── CONFIGURATION ─────────────────────────
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

logger.info(f"Configured webhooks: {WEBHOOKS}")

class Deal(dict):
    """Hold deal data"""
    pass

def verify_webhook_config():
    """Verify webhook configuration"""
    logger.info("Verifying webhook configuration...")
    if not WEBHOOKS:
        logger.error("No webhooks configured!")
        return False
        
    for hook in WEBHOOKS:
        try:
            test_payload = {
                "content": "Test message - webhook verification",
                "embeds": [{
                    "title": "Webhook Test",
                    "description": "This is a test message to verify webhook configuration",
                    "color": CONF.get("colors", {}).get("default", 0)
                }]
            }
            
            logger.info(f"Testing webhook (first 20 chars): {hook[:20]}...")
            resp = requests.post(hook, json=test_payload, timeout=15)
            resp.raise_for_status()
            logger.info(f"Webhook test successful! Response: {resp.status_code}")
            return True
        except Exception as e:
            logger.error(f"Webhook test failed: {str(e)}")
            return False

def parse_args():
    parser = argparse.ArgumentParser(description="Run scraper")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print deals without posting"
    )
    parser.add_argument(
        "--source",
        type=str,
        help="Filter by a single source/subreddit"
    )
    parser.add_argument(
        "--force",
        choices=["rss", "html", "reddit", "all"],
        help="Force fetch specific type"
    )
    return parser.parse_args()

def db_connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS seen (sig TEXT PRIMARY KEY, posted_at TEXT)"
    )
    return conn

def make_sig(deal):
    """Create a unique signature for a deal to prevent duplicates"""
    return re.sub(r"\W+", "", deal.get("title", "").lower())[:80]

def fuzzy_seen(conn, title):
    """Check if a similar title has been seen before"""
    rows = conn.execute("SELECT sig FROM seen").fetchall()
    return any(token_set_ratio(r[0], title) > 90 for r in rows)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_html(url, client: httpx.AsyncClient):
    logger.info(f"Fetching HTML from: {url}")
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    return r.text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=30))
async def fetch_json(url, client: httpx.AsyncClient):
    logger.info(f"Fetching JSON from: {url}")
    r = await client.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def scrape_rss(entry):
    """Synchronous RSS scraping with enhanced logging"""
    logger.info(f"Processing RSS feed: {entry.get('url', '')}")
    feed = feedparser.parse(entry.get("url",""))
    
    if feed.bozo:
        logger.error(f"RSS Feed Error: {feed.bozo_exception}")
        return []
        
    logger.info(f"Found {len(feed.entries)} entries in feed")
    out = []
    for e in feed.entries[:20]:
        title = e.get("title", "")[:120]
        body = e.get("summary", "")[:200]
        link = e.get("link", "")
        
        logger.info(f"Processing RSS entry: {title}")
        
        # Check if entry has required fields
        if not all([title, link]):
            logger.warning(f"Skipping RSS entry due to missing required fields: {title}")
            continue
            
        deal = Deal({
            "title": title,
            "body": body,
            "url": link,
            "source": entry.get("name",""),
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get("tags", []),
            "min_hot": entry.get("min_hot_score", CONF.get("global_min_hot", 0.05)),
            "ttl_days": entry.get("ttl_days", CONF.get("ttl_default_days",30)),
        })
        out.append(deal)
        logger.info(f"Added deal: {deal['title']}")
    
    logger.info(f"Processed {len(out)} valid entries from RSS feed")
    return out

async def scrape_html(entry, client):
    html = await fetch_html(entry["url"], client)
    soup = BeautifulSoup(html, "lxml")
    out = []
    for b in soup.select(entry.get("selector","")):
        title = (b.get("alt") or b.get_text() or entry.get("name",""))[:120]
        body = " ".join(b.stripped_strings)[:200]
        link = entry.get("url","")
        a = b.select_one("a[href]")
        if a and a.get("href"):
            link = a["href"]
        out.append(Deal({
            "title": title,
            "body": body,
            "url": link,
            "source": entry.get("name",""),
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": entry.get("tags", []),
            "min_hot": entry.get("min_hot_score", CONF.get("global_min_hot",0.05)),
            "ttl_days": entry.get("ttl_days", CONF.get("ttl_default_days",30)),
            "needs_js": entry.get("needs_js", False),
        }))
    return out

async def scrape_reddit(sub, client):
    data = await fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=20", client)
    out = []
    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        out.append(Deal({
            "title": d.get("title",""),
            "body": f"score {d.get('score',0)} · {d.get('num_comments',0)} comments",
            "url": f"https://reddit.com{d.get('permalink','')}",
            "source": f"r/{sub}",
            "fetched": datetime.datetime.utcnow().isoformat(),
            "tags": [],
        }))
    return out

def post_deals_to_discord(deals, timestamp):
    """Post deals to Discord with better error handling and logging"""
    if not WEBHOOKS:
        logger.error("No webhooks configured! Please set DISCORD_WEBHOOK in GitHub secrets")
        return

    logger.info(f"Preparing to post {len(deals)} deals to {len(WEBHOOKS)} webhooks")
    
    sf = sorted(deals, key=lambda x: x.get("fetched"), reverse=True)
    emb = {
        "title": f"Privacy Deals – {timestamp.strftime('%Y-%m-%d %H:%M UTC')}",
        "description": f"Found {len(deals)} new deals",
        "fields": []
    }
    
    col = CONF.get("colors",{}).get("default")
    if col is not None:
        emb["color"] = col
        
    for d in sf[:5]:
        logger.info(f"Adding deal to embed: {d['title']}")
        emb["fields"].append({
            "name": d["title"],
            "value": f"{d.get('body', 'No description')}\n{d['url']}",
            "inline": False
        })

    payload = {"embeds":[emb]}
    logger.info(f"Payload prepared: {json.dumps(payload, indent=2)}")
    
    for hook in WEBHOOKS:
        try:
            logger.info(f"Attempting to post to webhook (first 20 chars): {hook[:20]}...")
            resp = requests.post(hook, json=payload, timeout=15)
            resp.raise_for_status()
            logger.info(f"Successfully posted to webhook. Response: {resp.status_code}")
            # Add a small delay between webhook calls
            time.sleep(1)
        except Exception as e:
            logger.error(f"Failed to post to webhook: {str(e)}")
            logger.error(f"Response content: {getattr(resp, 'text', 'No response content')}")

def post_no_deals_message(timestamp):
    """Post a message when no deals are found"""
    if not WEBHOOKS:
        return
        
    emb = {
        "title": "No New Deals Found",
        "description": f"Ran at {timestamp.strftime('%Y-%m-%d %H:%M UTC')} but found no fresh deals.",
        "fields": []
    }
    
    col = CONF.get("colors",{}).get("default")
    if col is not None:
        emb["color"] = col
        
    payload = {"embeds":[emb]}
    
    for hook in WEBHOOKS:
        try:
            resp = requests.post(hook, json=payload, timeout=15)
            resp.raise_for_status()
            logger.info("Posted no-deals message successfully")
        except Exception as e:
            logger.error(f"Failed to post no-deals message: {e}")

def main():
    args = parse_args()
    now = datetime.datetime.utcnow()
    
    logger.info("Starting scraper run")
    logger.info(f"Webhooks configured: {WEBHOOKS}")
    
    # Verify webhook configuration
    if not args.dry_run:
        if not verify_webhook_config():
            logger.error("Webhook verification failed! Check your configuration.")
            # Continue anyway to generate metrics

    # Initialize metrics
    metrics = {
        "total_scraped": 0,
        "fresh_deals": [],
        "timestamp": now.isoformat(),
        "errors": []
    }

    try:
        # Process RSS feeds synchronously first
        rss_results = []
        for entry in CONF.get("rss",[]):
            if args.source and args.source != entry.get("name"):
                continue
            try:
                results = scrape_rss(entry)
                rss_results.extend(results)
            except Exception as e:
                error_msg = f"Error processing RSS feed {entry.get('name')}: {e}"
                logger.error(error_msg)
                metrics["errors"].append(error_msg)
        
        # Then handle async tasks
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = httpx.AsyncClient(timeout=20)

        tasks, sources = [], []
        
        # HTML tasks
        for e in CONF.get("html",[]):
            if args.source and args.source!=e.get("name"): 
                continue
            if e.get("needs_js"): 
                continue
            tasks.append(scrape_html(e, client))
            sources.append(e.get("name"))

        # Reddit tasks
        for cat in CONF.get("categories",{}).values():
            for sub in cat.get("reddit",[]):
                if args.source and args.source!=sub:
                    continue
                tasks.append(scrape_reddit(sub, client))
                sources.append(f"r/{sub}")

        results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(client.aclose())

        # Combine all results
        all_results = rss_results
        for src, res in zip(sources, results):
            if isinstance(res, Exception):
                error_msg = f"Error processing {src}: {res}"
                logger.error(error_msg)
                metrics["errors"].append(error_msg)
                continue
            all_results.extend(res)

        # Process results
        conn = db_connect()
        fresh = []
        for d in all_results:
            sig = make_sig(d)
            row = conn.execute("SELECT posted_at FROM seen WHERE sig=?", (sig,)).fetchone()
            expired = False
            if row:
                dt = datetime.datetime.fromisoformat(row[0])
                expired = (now - dt).days > d.get("ttl_days", 30)
            if ((not row) or expired) and not fuzzy_seen(conn, d.get("title","")):
                fresh.append(d)
                conn.execute("REPLACE INTO seen VALUES(?,?)",(sig,now.isoformat()))
        
        conn.commit()
        conn.close()

        # Update metrics
        metrics["total_scraped"] = len(all_results)
        metrics["fresh_deals"] = [
            {"source": d["source"], "title": d["title"]}
            for d in fresh
        ]

        logger.info(f"Found {len(fresh)} fresh deals out of {len(all_results)} total")

        if not args.dry_run and fresh:
            post_deals_to_discord(fresh, now)
        elif not args.dry_run:
            logger.info("No fresh deals to post")
            post_no_deals_message(now)
        else:
            # Print deals for dry run
            for d in fresh:
                print(f"[{d['source']}] {d['title']} – {d['url']}")

    except Exception as e:
        error_msg = f"Critical error during execution: {e}"
        logger.error(error_msg)
        metrics["errors"].append(error_msg)
        raise

    finally:
        # Always write metrics file
        with open("metrics.json", "w") as mf:
            json.dump(metrics, mf)

if __name__ == "__main__":
    main()
