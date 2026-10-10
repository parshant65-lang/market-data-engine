import os
import sys
import io
import re
import time
import json
import shutil
import hashlib
import datetime
import subprocess
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

VAULT_PAT = os.environ.get("VAULT_PAT")
if not VAULT_PAT:
    print("[ERROR] VAULT_PAT environment variable is required.")
    sys.exit(1)

VAULT_REPO_URL = f"https://x-access-token:{VAULT_PAT}@github.com/parshant65-lang/market-intelligence-vault.git"
VAULT_DIR = "vault_storage"
BUFFER_DIR = "local_hourly_buffer"

SESSION_MAX_SECONDS = 5 * 3600 + 30 * 60  # Run for 5.5 hours continuously before seamless relay
POLL_INTERVAL_SECONDS = 60                 # Poll live feeds every 60 seconds
PUSH_INTERVAL_SECONDS = 3600               # Commit and push accumulated batch every 1 hour

CHANNELS = [
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "full_text": True},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html", "full_text": True},
    {"category": "coindesk", "source": "CoinDesk", "domain": "coindesk.com", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "full_text": True},
    {"category": "cointelegraph", "source": "CoinTelegraph", "domain": "cointelegraph.com", "url": "https://cointelegraph.com/rss", "full_text": True}
]

BULLISH_WORDS = {
    "surge", "surges", "jump", "jumps", "gain", "gains", "rally", "rallies",
    "record high", "profit", "profits", "beat", "beats", "boom", "booming",
    "upgrade", "upgrades", "rise", "rises", "rising", "soar", "soars",
    "positive", "growth", "rebound", "rebounds", "outperform", "optimism"
}

BEARISH_WORDS = {
    "fall", "falls", "drop", "drops", "plunge", "plunges", "slump", "slumps",
    "loss", "losses", "warning", "warnings", "probe", "probes", "war", "conflict",
    "inflation", "default", "defaults", "crisis", "crash", "downgrade", "downgrades",
    "record low", "recession", "slowdown", "weak", "weakens", "fears", "cut", "cuts"
}

BLACKLIST_PHRASES = [
    "sign up", "privacy policy", "terms of service", "livestream", "watchlist",
    "newsletter", "cookie", "advertisement", "all rights reserved", "subscribe",
    "trending now", "menu", "skip to content"
]

def clean_html(raw_html):
    if not raw_html:
        return ""
    clean = re.sub(r"<[^>]+>", "", raw_html)
    return " ".join(clean.split())

def score_sentiment(text):
    t = text.lower()
    bull = sum(1 for w in BULLISH_WORDS if w in t)
    bear = sum(1 for w in BEARISH_WORDS if w in t)
    total = bull + bear
    if total == 0:
        return 0.0, "NEUTRAL"
    score = round((bull - bear) / total, 2)
    label = "BULLISH" if score > 0.15 else ("BEARISH" if score < -0.15 else "NEUTRAL")
    return score, label

def parse_pubdate(pubdate_str):
    if not pubdate_str:
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        dt = parsedate_to_datetime(pubdate_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def extract_article_details(url, domain):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=7) as resp:
            html = resp.read().decode("utf-8", errors="ignore")

        raw_paras = re.findall(r"<p[^>]*>(.*?)</p>", html)
        clean_paras = []
        for p in raw_paras:
            txt = clean_html(p)
            if len(txt) > 75 and not any(bp in txt.lower() for bp in BLACKLIST_PHRASES):
                clean_paras.append(txt)

        raw_links = re.findall(r'href=["\'](https?://[^"\'\s<>]+|/[^"\'\s<>]+)["\']', html)
        related_links = []
        seen = set()
        for l in raw_links:
            full_url = urljoin(url, l).split("?")[0].split("#")[0]
            if full_url in seen or full_url == url:
                continue
            seen.add(full_url)
            parsed = urlparse(full_url)
            if domain in parsed.netloc:
                path = parsed.path
                if re.search(r"^/(de|es|fr|fil|id|it|ja|ko|ru|zh|pt|ar)/", path):
                    continue
                if any(skip in path for skip in ["/video/", "/watch/", "/pro/", "/live-tv/", "/author/", "/tag/", "/privacy", "/terms", "/amp/", "/podcast/"]):
                    continue
                if re.search(r"/\d{4}/\d{2}/", path) or ("-" in path and len(path.strip("/").split("/")) >= 2):
                    related_links.append(full_url)

        return clean_paras, related_links[:5]
    except Exception:
        return [], []

def fetch_feed_items(ch):
    name = ch["category"]
    url = ch["url"]
    source = ch["source"]
    domain = ch["domain"]
    can_full_text = ch["full_text"]

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            content = resp.read()
        root = ET.fromstring(content)
        items = []
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for it in root.findall(".//item"):
            title = clean_html(it.findtext("title", ""))
            link = it.findtext("link", "").strip()
            desc = clean_html(it.findtext("description", ""))
            pub_date = it.findtext("pubDate", "")
            pub_utc = parse_pubdate(pub_date)

            if not title or not link:
                continue

            unique_seed = f"{link}::{title}"
            item_id = hashlib.md5(unique_seed.encode("utf-8")).hexdigest()
            score, label = score_sentiment(f"{title} {desc}")

            items.append({
                "id": item_id,
                "published_utc": pub_utc,
                "source": source,
                "category": name,
                "domain": domain,
                "title": title,
                "description": desc,
                "url": link,
                "can_full_text": can_full_text,
                "full_content": [],
                "related_links": [],
                "sentiment": {
                    "score": score,
                    "label": label
                },
                "ingested_utc": now_str
            })
        return items
    except Exception:
        return []

def flush_buffer_to_vault():
    buffer_file = os.path.join(BUFFER_DIR, "buffered_items.json")
    if not os.path.exists(buffer_file):
        print("[HOURLY SYNC] Buffer is empty. Zero new items to push.")
        return

    try:
        with open(buffer_file, "r", encoding="utf-8") as f:
            buffered_items = json.load(f)
    except Exception:
        buffered_items = []

    if not buffered_items:
        print("[HOURLY SYNC] Buffer contains 0 items. Skipping push.")
        return

    print(f"\n[HOURLY SYNC] Initiating 1-Hour Flush: {len(buffered_items)} accumulated items to private vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)

    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[HOURLY SYNC ERROR] Failed to connect to vault repository.")
        return

    # Update Link Registry
    link_reg_path = os.path.join(VAULT_DIR, "data", "link_registry.json")
    link_reg = {"total_discovered": 0, "total_ingested": 0, "links": {}}
    if os.path.exists(link_reg_path):
        try:
            with open(link_reg_path, "r", encoding="utf-8") as f:
                link_reg = json.load(f)
        except Exception:
            pass

    # Group by target JSON file
    file_groups = {}
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for item in buffered_items:
        category = item["category"]
        pub_dt = item["published_utc"].split(" ")[0]
        try:
            dt_obj = datetime.datetime.strptime(pub_dt, "%Y-%m-%d")
            year, month = str(dt_obj.year), f"{dt_obj.month:02d}"
        except Exception:
            today = datetime.datetime.now(datetime.timezone.utc)
            year, month = str(today.year), f"{today.month:02d}"

        target_folder = os.path.join(VAULT_DIR, "data", category, year, month)
        os.makedirs(target_folder, exist_ok=True)
        file_path = os.path.join(target_folder, f"{category}_{pub_dt}.json")

        if file_path not in file_groups:
            existing = []
            known_ids = set()
            known_urls = set()
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
                    for e in existing:
                        known_ids.add(e.get("id"))
                        known_urls.add(e.get("url"))
                except Exception:
                    existing = []
            file_groups[file_path] = (existing, known_ids, known_urls)

        ex_records, known_ids, known_urls = file_groups[file_path]
        if item["id"] not in known_ids and item["url"] not in known_urls:
            ex_records.append(item)
            known_ids.add(item["id"])
            known_urls.add(item["url"])

        # Update link registry
        link_reg["links"][item["url"]] = {
            "title": item["title"],
            "source": item["source"],
            "category": item["category"],
            "published_utc": item["published_utc"],
            "discovered_utc": now_str,
            "status": "ingested",
            "has_full_content": len(item.get("full_content", [])) > 0
        }
        for r_link in item.get("related_links", []):
            if r_link not in link_reg["links"]:
                link_reg["links"][r_link] = {
                    "source": item["source"],
                    "category": item["category"],
                    "parent_url": item["url"],
                    "discovered_utc": now_str,
                    "status": "discovered"
                }

    # Save data files
    for file_path, (records, _, _) in file_groups.items():
        if records:
            records.sort(key=lambda x: x["published_utc"], reverse=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)

    link_reg["total_discovered"] = len(link_reg["links"])
    link_reg["total_ingested"] = sum(1 for v in link_reg["links"].values() if v.get("status") == "ingested")

    with open(link_reg_path, "w", encoding="utf-8") as f:
        json.dump(link_reg, f, indent=2, ensure_ascii=False)

    # Git Commit and Push to Vault
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "add", "data/"], cwd=VAULT_DIR, check=True)
    commit_res = subprocess.run(
        ["git", "commit", "-m", f"Hourly sync: {len(buffered_items)} articles + Link Registry ({link_reg['total_discovered']} links) [skip ci]"],
        cwd=VAULT_DIR, capture_output=True, text=True
    )
    if commit_res.returncode == 0:
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        if push_res.returncode == 0:
            print(f"[HOURLY SYNC] Successfully synced {len(buffered_items)} articles to private vault.")
        else:
            print(f"[HOURLY SYNC ERROR] Push failed: {push_res.stderr.strip()}")

    # Cleanup local vault and buffer
    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("[HOURLY SYNC] Local buffer deleted. Runner memory clean.")

def trigger_next_relay():
    url = "https://api.github.com/repos/parshant65-lang/market-data-engine/actions/workflows/harvest_intelligence.yml/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {VAULT_PAT}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Continuous-Relay"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("[SEAMLESS RELAY] Successfully handed off and triggered next continuous session.")
    except Exception as e:
        print(f"[RELAY DISPATCH ERROR] {e}")

def main():
    print("=" * 70)
    print("🚀 CONTINUOUS 24x7 MARKET INTELLIGENCE ENGINE INITIALIZED")
    print(f"   Target Duration: {SESSION_MAX_SECONDS // 3600}h {SESSION_MAX_SECONDS % 3600 // 60}m")
    print(f"   Feed Check Cadence: Every {POLL_INTERVAL_SECONDS}s")
    print(f"   Vault Push Cadence: Every {PUSH_INTERVAL_SECONDS // 3600} Hour (with auto-delete)")
    print("=" * 70)

    os.makedirs(BUFFER_DIR, exist_ok=True)
    seen_ids_in_session = set()
    start_time = time.time()
    last_push_time = time.time()

    while (time.time() - start_time) < SESSION_MAX_SECONDS:
        current_time = time.time()
        now_dt = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")

        # 1. Fetch live items from feeds
        feed_items = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(fetch_feed_items, ch) for ch in CHANNELS]
            for f in as_completed(futures):
                feed_items.extend(f.result())

        # 2. Identify brand-new items unseen in this session
        new_items = []
        for it in feed_items:
            if it["id"] not in seen_ids_in_session:
                new_items.append(it)
                seen_ids_in_session.add(it["id"])

        # 3. Enrich new items with full text and related links
        if new_items:
            print(f"[{now_dt}] Discovered {len(new_items)} fresh articles. Extracting full content...")
            def enrich(item):
                domain = item.pop("domain", "")
                if item.pop("can_full_text", False):
                    paras, rel_links = extract_article_details(item["url"], domain)
                    item["full_content"] = paras
                    item["related_links"] = rel_links
                    if paras:
                        full_sample = " ".join(paras[:5])
                        sc, lb = score_sentiment(f"{item['title']} {item['description']} {full_sample}")
                        item["sentiment"] = {"score": sc, "label": lb}
                else:
                    item.pop("can_full_text", None)
                    item["full_content"] = [item["description"]] if item["description"] else []
                    item["related_links"] = []
                return item

            with ThreadPoolExecutor(max_workers=6) as executor:
                enriched = list(executor.map(enrich, new_items))

            # Append to local hourly buffer
            buffer_file = os.path.join(BUFFER_DIR, "buffered_items.json")
            existing_buffer = []
            if os.path.exists(buffer_file):
                try:
                    with open(buffer_file, "r", encoding="utf-8") as f:
                        existing_buffer = json.load(f)
                except Exception:
                    existing_buffer = []

            existing_buffer.extend(enriched)
            os.makedirs(BUFFER_DIR, exist_ok=True)
            with open(buffer_file, "w", encoding="utf-8") as f:
                json.dump(existing_buffer, f, ensure_ascii=False)

            print(f"[{now_dt}] Buffered {len(enriched)} articles. Current hour buffer size: {len(existing_buffer)}.")

        # 4. Check if 1 hour has elapsed for push & delete
        if (current_time - last_push_time) >= PUSH_INTERVAL_SECONDS:
            flush_buffer_to_vault()
            last_push_time = time.time()

        # Sleep before next poll
        time.sleep(POLL_INTERVAL_SECONDS)

    # End of session: Flush any remaining items, dispatch next relay, clean up
    print("\n[SESSION COMPLETE] Flushing final buffer before handoff...")
    flush_buffer_to_vault()
    trigger_next_relay()
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("🏁 Runner finished session cleanly. Next continuous runner dispatched.")

if __name__ == "__main__":
    main()
