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
import html as html_lib
import urllib.request
import xml.etree.ElementTree as ET
from collections import deque
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime

VAULT_PAT = os.environ.get("VAULT_PAT")
if not VAULT_PAT:
    print("[ERROR] VAULT_PAT environment variable is required.")
    sys.exit(1)

VAULT_REPO_URL = f"https://x-access-token:{VAULT_PAT}@github.com/parshant65-lang/market-intelligence-vault.git"
VAULT_DIR = "vault_storage"
BUFFER_DIR = "local_hourly_buffer"

SESSION_MAX_SECONDS = 5 * 3600 + 30 * 60  # 5.5 hours continuous session before handoff
PUSH_INTERVAL_SECONDS = 3600               # Flush, commit, push & delete every 1 hour (3600s)
CRAWL_DELAY_SECONDS = 1.5                  # Polite delay between page requests

CHANNELS = [
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html"},
    {"category": "coindesk", "source": "CoinDesk", "domain": "coindesk.com", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"category": "cointelegraph", "source": "CoinTelegraph", "domain": "cointelegraph.com", "url": "https://cointelegraph.com/rss"}
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
    clean = html_lib.unescape(clean)
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

def extract_page_data(url, domain):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode("utf-8", errors="ignore")

        # 1. Title
        title = ""
        og_t = re.search(r"""<meta\s+(?:property|name)=["\x27]og:title["\x27]\s+content=["\x27](.*?)["\x27]""", html)
        if og_t:
            title = clean_html(og_t.group(1))
        if not title:
            h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
            if h1:
                title = clean_html(h1.group(1))
        if not title:
            t = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
            if t:
                title = clean_html(t.group(1))

        # 2. Date
        pub_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        og_time = re.search(r"""<meta\s+(?:property|name)=["\x27](?:article:published_time|date)["\x27]\s+content=["\x27](.*?)["\x27]""", html)
        if og_time:
            pub_utc = parse_pubdate(og_time.group(1))

        # 3. Clean Paragraphs
        raw_paras = re.findall(r"<p[^>]*>(.*?)</p>", html)
        clean_paras = []
        for p in raw_paras:
            txt = clean_html(p)
            if len(txt) > 75 and not any(bp in txt.lower() for bp in BLACKLIST_PHRASES):
                clean_paras.append(txt)

        # 4. Exponential Related Links (Strict Real Article Filter)
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
                path = parsed.path.lower()
                if any(path.endswith(ext) for ext in [".woff", ".woff2", ".ttf", ".png", ".jpg", ".jpeg", ".svg", ".ico", ".css", ".js", ".json"]):
                    continue
                if re.search(r"^/(de|es|fr|fil|id|it|ja|ko|ru|zh|pt|pt-br|nl|uk|ar|tr|pl)/", path):
                    continue
                if any(skip in path for skip in ["/api/", "/favicons/", "/amp/", "/video/", "/watch/", "/pro/", "/live-tv/", "/author/", "/tag/", "/privacy", "/terms", "/investingclub/"]):
                    continue
                if "cnbc.com" in domain:
                    if path.endswith(".html") and (re.search(r"/\d{4}/\d{2}/", path) or "/markets/" in path or "/economy/" in path):
                        related_links.append(full_url)
                else:
                    if any(cat in path for cat in ["/markets/", "/business/", "/policy/", "/tech/", "/news/"]) and len(path.strip("/").split("/")) >= 3:
                        related_links.append(full_url)

        return title, pub_utc, clean_paras, related_links[:5]
    except Exception:
        return "", "", [], []

def get_feed_seeds():
    seeds = []
    for ch in CHANNELS:
        try:
            req = urllib.request.Request(ch["url"], headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                root = ET.fromstring(r.read())
            for it in root.findall(".//item")[:5]:
                link = it.findtext("link", "").strip()
                title = clean_html(it.findtext("title", ""))
                if link:
                    seeds.append({
                        "url": link,
                        "title": title,
                        "source": ch["source"],
                        "category": ch["category"],
                        "domain": ch["domain"]
                    })
        except Exception:
            pass
    return seeds

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

    print(f"\n[HOURLY SYNC] Initiating 1-Hour Flush: {len(buffered_items)} accumulated articles to vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)

    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[HOURLY SYNC ERROR] Failed to connect to vault repository.")
        return

    link_reg_path = os.path.join(VAULT_DIR, "data", "link_registry.json")
    link_reg = {"total_discovered": 0, "total_ingested": 0, "links": {}}
    if os.path.exists(link_reg_path):
        try:
            with open(link_reg_path, "r", encoding="utf-8") as f:
                link_reg = json.load(f)
        except Exception:
            pass

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

        # Update Master Link Registry
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

    for file_path, (records, _, _) in file_groups.items():
        if records:
            records.sort(key=lambda x: x["published_utc"], reverse=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)

    link_reg["total_discovered"] = len(link_reg["links"])
    link_reg["total_ingested"] = sum(1 for v in link_reg["links"].values() if v.get("status") == "ingested")

    with open(link_reg_path, "w", encoding="utf-8") as f:
        json.dump(link_reg, f, indent=2, ensure_ascii=False)

    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "add", "data/"], cwd=VAULT_DIR, check=True)
    commit_res = subprocess.run(
        ["git", "commit", "-m", f"Hourly sync: {len(buffered_items)} BFS articles + Link Registry ({link_reg['total_discovered']} links) [skip ci]"],
        cwd=VAULT_DIR, capture_output=True, text=True
    )
    if commit_res.returncode == 0:
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        if push_res.returncode == 0:
            print(f"[HOURLY SYNC] Successfully synced {len(buffered_items)} articles to vault.")
        else:
            print(f"[HOURLY SYNC ERROR] Push failed: {push_res.stderr.strip()}")

    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("[HOURLY SYNC] Local buffer flushed & deleted. Storage 100% clean.")

def trigger_next_relay():
    url = "https://api.github.com/repos/parshant65-lang/market-data-engine/actions/workflows/harvest_intelligence.yml/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {VAULT_PAT}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Continuous-BFS-Spider"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("[SEAMLESS RELAY] Next continuous BFS spider dispatched successfully.")
    except Exception as e:
        print(f"[RELAY DISPATCH ERROR] {e}")

def main():
    print("=" * 70)
    print("🕷️ CONTINUOUS 24x7 EXPONENTIAL BFS TREE SPIDER INITIALIZED")
    print("   Growth Architecture: 1 Article ➡️ 4 Links ➡️ 16 Links ➡️ 64 Links...")
    print(f"   Max Session Time: {SESSION_MAX_SECONDS // 3600}h {SESSION_MAX_SECONDS % 3600 // 60}m")
    print(f"   Hourly Flush Cadence: Every {PUSH_INTERVAL_SECONDS // 3600} Hour (with auto-delete)")
    print("=" * 70)

    os.makedirs(BUFFER_DIR, exist_ok=True)
    crawl_queue = deque()
    visited_urls = set()

    # Seed initial queue
    initial_seeds = get_feed_seeds()
    for s in initial_seeds:
        crawl_queue.append(s)
        visited_urls.add(s["url"])
    print(f"[SEED] Initialized BFS Queue with {len(crawl_queue)} starting articles.")

    start_time = time.time()
    last_push_time = time.time()
    articles_ingested_session = 0

    while (time.time() - start_time) < SESSION_MAX_SECONDS:
        current_time = time.time()

        # If queue gets low, inject fresh seeds
        if len(crawl_queue) < 4:
            new_seeds = get_feed_seeds()
            for s in new_seeds:
                if s["url"] not in visited_urls:
                    crawl_queue.append(s)
                    visited_urls.add(s["url"])

        if crawl_queue:
            current_target = crawl_queue.popleft()
            url = current_target["url"]
            source = current_target["source"]
            category = current_target["category"]
            domain = current_target["domain"]

            title, pub_utc, paras, related_links = extract_page_data(url, domain)

            if paras:
                articles_ingested_session += 1
                unique_seed = f"{url}::{title}"
                item_id = hashlib.md5(unique_seed.encode("utf-8")).hexdigest()
                sample_text = " ".join(paras[:5])
                score, label = score_sentiment(f"{title} {sample_text}")

                article_record = {
                    "id": item_id,
                    "published_utc": pub_utc,
                    "source": source,
                    "category": category,
                    "title": title,
                    "description": paras[0] if paras else "",
                    "url": url,
                    "full_content": paras,
                    "related_links": related_links,
                    "sentiment": {
                        "score": score,
                        "label": label
                    },
                    "ingested_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                }

                # Save to local hourly buffer
                buffer_file = os.path.join(BUFFER_DIR, "buffered_items.json")
                buf = []
                if os.path.exists(buffer_file):
                    try:
                        with open(buffer_file, "r", encoding="utf-8") as f:
                            buf = json.load(f)
                    except Exception:
                        buf = []
                buf.append(article_record)
                os.makedirs(BUFFER_DIR, exist_ok=True)
                with open(buffer_file, "w", encoding="utf-8") as f:
                    json.dump(buf, f, ensure_ascii=False)

                now_ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
                print(f"[{now_ts}] [BFS #{articles_ingested_session}] {title[:55]}... ({len(paras)} paras) -> +{len(related_links)} new links (Q: {len(crawl_queue)})")

                # Push newly discovered links into BFS Queue (1 -> 4 -> 16 -> 64 expansion!)
                for r_url in related_links:
                    if r_url not in visited_urls:
                        visited_urls.add(r_url)
                        crawl_queue.append({
                            "url": r_url,
                            "source": source,
                            "category": category,
                            "domain": domain
                        })

            time.sleep(CRAWL_DELAY_SECONDS)

        # Check 1-Hour Push Cadence
        if (current_time - last_push_time) >= PUSH_INTERVAL_SECONDS:
            flush_buffer_to_vault()
            last_push_time = time.time()

    # End of Session Handoff
    print("\n[SESSION COMPLETE] Flushing remaining buffer and launching next relay...")
    flush_buffer_to_vault()
    trigger_next_relay()
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("🏁 BFS Spider relay handover complete.")

if __name__ == "__main__":
    main()
