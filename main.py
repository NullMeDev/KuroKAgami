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
import hashlib

import feedparser
import httpx
import yaml
import requests

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

class Item(dict):
    """Base class for deals and RSS items"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calculate_hash()
    
    def calculate_hash(self):
        """Calculate a unique hash for the item"""
        content = f"{self.get('title', '')}{self.get('url', '')}"
        self['hash'] = hashlib.md5(content.encode()).hexdigest()

def db_connect():
    """Connect to SQLite database and create tables if needed"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    # Create tables for different types of items
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_items (
            hash TEXT PRIMARY KEY,
            type TEXT,
            title TEXT,
            posted_at TEXT,
            expires_at TEXT
        )
    """)
    
    return conn

def is_item_seen(conn, item):
    """Check if an item has been seen before and is not expired"""
    now = datetime.datetime.utcnow()
    
    # Get feed type configuration
    feed_type = item.get('type', 'deals')
    type_config = CONF['feed_types'].get(feed_type, {})
    
    # Check if item exists and isn't expired
    row = conn.execute(
        "SELECT posted_at, expires_at FROM seen_items WHERE hash=?", 
        (item['hash'],)
    ).fetchone()
    
    if not row:
        return False
        
    posted_at = datetime.datetime.fromisoformat(row[0])
    expires_at = datetime.datetime.fromisoformat(row[1])
    
    # Check if expired
    if now > expires_at:
        return False
        
    # For RSS items, also check similarity to prevent duplicates
    if feed_type == 'rss':
        similar_items = conn.execute(
            "SELECT title FROM seen_items WHERE type='rss' AND hash != ?",
            (item['hash'],)
        ).fetchall()
        
        threshold = type_config.get('similarity_threshold', 95)
        for similar in similar_items:
            if token_set_ratio(item['title'], similar[0]) > threshold:
                return True
                
    return True

def store_item(conn, item):
    """Store an item in the database"""
    now = datetime.datetime.utcnow()
    
    # Get feed type configuration
    feed_type = item.get('type', 'deals')
    type_config = CONF['feed_types'].get(feed_type, {})
    
    # Calculate expiration
    if feed_type == 'rss':
        expires_at = now + datetime.timedelta(hours=type_config.get('max_age_hours', 24))
    else:
        expires_at = now + datetime.timedelta(days=type_config.get('max_age_days', 30))
    
    conn.execute(
        "INSERT OR REPLACE INTO seen_items (hash, type, title, posted_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (item['hash'], feed_type, item['title'], now.isoformat(), expires_at.isoformat())
    )
    conn.commit()

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
        body = e.get("summary", "")
        if not body and 'content' in e:
            body = e['content'][0]['value'] if e['content'] else ""
        body = body[:200]
        link = e.get("link", "")
        
        if not all([title, link]):
            continue
            
        item = Item({
            "title": title,
            "body": body,
            "url": link,
            "source": entry.get("name",""),
            "type": "rss",
            "tags": entry.get("tags", []),
            "fetched": datetime.datetime.utcnow().isoformat()
        })
        
        out.append(item)
        
    return out

async def scrape_html(entry, client):
    """Scrape HTML sources for deals"""
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
            
        item = Item({
            "title": title,
            "body": body,
            "url": link,
            "source": entry.get("name",""),
            "type": "deals",
            "tags": entry.get("tags", []),
            "fetched": datetime.datetime.utcnow().isoformat()
        })
        
        out.append(item)
        
    return out

async def scrape_reddit(sub, client):
    """Scrape Reddit for deals"""
    data = await fetch_json(f"https://www.reddit.com/r/{sub}/new.json?limit=20", client)
    out = []
    
    for post in data.get("data", {}).get("children", []):
        d = post["data"]
        
        item = Item({
            "title": d.get("title",""),
            "body": f"score {d.get('score',0)} · {d.get('num_comments',0)} comments",
            "url": f"https://reddit.com{d.get('permalink','')}",
            "source": f"r/{sub}",
            "type": "deals",
            "tags": [],
            "fetched": datetime.datetime.utcnow().isoformat()
        })
        
        out.append(item)
        
    return out

def post_to_discord(items, item_type="deals"):
    """Post items to Discord with type-specific formatting"""
    if not WEBHOOKS:
        logger.error("No webhooks configured!")
        return
        
    if not items:
        return
        
    now = datetime.datetime.utcnow()
    
    # Different formatting for RSS vs deals
    if item_type == "rss":
        payload = {
            "content": "📚 New Research Updates",
            "embeds": [{
                "title": "Latest Papers and Updates",
                "description": f"Found {len(items)} new papers/updates",
                "color": CONF.get("colors",{}).get("rss_updates", 5793266),
                "timestamp": now.isoformat(),
                "fields": []
            }]
        }
    else:
        payload = {
            "content": "🎯 New Deals Found",
            "embeds": [{
                "title": f"Privacy & Tech Deals",
                "description": f"Found {len(items)} new deals",
                "color": CONF.get("colors",{}).get("default", 15844367),
                "timestamp": now.isoformat(),
                "fields": []
            }]
        }
    
    # Add items to embed
    for item in sorted(items, key=lambda x: x.get("fetched"), reverse=True)[:10]:
        payload["embeds"][0]["fields"].append({
            "name": item["title"],
            "value": f"{item.get('body', 'No description')}\n{item['url']}",
            "inline": False
        })
    
    # Post to all webhooks
    for hook in WEBHOOKS:
        try:
            response = requests.post(hook, json=payload)
            
            if response.status_code == 204:
                logger.info(f"✅ Successfully posted {item_type} update")
            else:
                logger.error(f"❌ Failed to post {item_type} update: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
            time.sleep(1)  # Delay between posts
            
        except Exception as e:
            logger.error(f"❌ Error posting to webhook: {str(e)}")

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run scraper")
    parser.add_argument("--dry-run", action="store_true", help="Print without posting")
    parser.add_argument("--force", choices=["rss", "html", "reddit", "all"], help="Force fetch type")
    args = parser.parse_args()
    
    # Initialize metrics
    metrics = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "rss_updates": [],
        "new_deals": [],
        "errors": []
    }
    
    try:
        # Connect to database
        conn = db_connect()
        
        # Process RSS feeds
        rss_items = []
        for entry in CONF.get("rss", []):
            try:
                items = scrape_rss(entry)
                rss_items.extend([i for i in items if not is_item_seen(conn, i)])
            except Exception as e:
                error_msg = f"Error processing RSS feed {entry.get('name')}: {str(e)}"
                logger.error(error_msg)
                metrics["errors"].append(error_msg)
        
        # Process deals
        deal_items = []
        
        # Setup async client
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = httpx.AsyncClient(timeout=20)
        
        try:
            # HTML deals
            tasks = []
            for entry in CONF.get("html", []):
                tasks.append(scrape_html(entry, client))
            
            results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            
            for entry, result in zip(CONF.get("html", []), results):
                if isinstance(result, Exception):
                    error_msg = f"Error scraping {entry.get('name')}: {str(result)}"
                    logger.error(error_msg)
                    metrics["errors"].append(error_msg)
                else:
                    deal_items.extend([i for i in result if not is_item_seen(conn, i)])
            
            # Reddit deals
            tasks = []
            for cat in CONF.get("categories", {}).values():
                for sub in cat.get("reddit", []):
                    tasks.append(scrape_reddit(sub, client))
            
            results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            
            for sub, result in zip([s for cat in CONF.get("categories", {}).values() for s in cat.get("reddit", [])], results):
                if isinstance(result, Exception):
                    error_msg = f"Error scraping r/{sub}: {str(result)}"
                    logger.error(error_msg)
                    metrics["errors"].append(error_msg)
                else:
                    deal_items.extend([i for i in result if not is_item_seen(conn, i)])
                    
        finally:
            loop.run_until_complete(client.aclose())
        
        # Store new items
        for item in rss_items + deal_items:
            store_item(conn, item)
        
        # Update metrics
        metrics["rss_updates"] = [{"title": i["title"], "source": i["source"]} for i in rss_items]
        metrics["new_deals"] = [{"title": i["title"], "source": i["source"]} for i in deal_items]
        
        # Post updates if not dry run
        if not args.dry_run:
            if rss_items:
                post_to_discord(rss_items, "rss")
            if deal_items:
                post_to_discord(deal_items, "deals")
        else:
            # Print updates for dry run
            if rss_items:
                print("\nRSS Updates:")
                for i in rss_items:
                    print(f"[{i['source']}] {i['title']}")
            if deal_items:
                print("\nNew Deals:")
                for i in deal_items:
                    print(f"[{i['source']}] {i['title']}")
    
    except Exception as e:
        error_msg = f"Critical error: {str(e)}"
        logger.error(error_msg)
        metrics["errors"].append(error_msg)
        raise
        
    finally:
        # Save metrics
        with open("metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
        
        # Close database connection
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
