import os
import sys
import io
import re
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

        # 1. Extract clean paragraphs
        raw_paras = re.findall(r"<p[^>]*>(.*?)</p>", html)
        clean_paras = []
        for p in raw_paras:
            txt = clean_html(p)
            if len(txt) > 75 and not any(bp in txt.lower() for bp in BLACKLIST_PHRASES):
                clean_paras.append(txt)

        # 2. Extract related links (Spider Graph)
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

def main():
    print("[1/6] Ingesting multi-channel market feeds...")
    all_raw_items = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_feed_items, ch) for ch in CHANNELS]
        for f in as_completed(futures):
            all_raw_items.extend(f.result())
    print(f"      Fetched {len(all_raw_items)} items across {len(CHANNELS)} channels.")

    print("[2/6] Connecting to private intelligence vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)

    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[ERROR] Failed to clone vault repository.")
        sys.exit(1)

    # Load master link registry
    link_registry_path = os.path.join(VAULT_DIR, "data", "link_registry.json")
    link_registry = {"total_discovered": 0, "total_ingested": 0, "links": {}}
    if os.path.exists(link_registry_path):
        try:
            with open(link_registry_path, "r", encoding="utf-8") as f:
                link_registry = json.load(f)
        except Exception:
            pass

    print("[3/6] Deduplicating and identifying new articles...")
    items_to_process = []
    known_existing = {}

    for item in all_raw_items:
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

        if file_path not in known_existing:
            existing_records = []
            known_ids = set()
            known_urls = set()
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        existing_records = json.load(f)
                    for e in existing_records:
                        known_ids.add(e.get("id"))
                        known_urls.add(e.get("url"))
                except Exception:
                    existing_records = []
            known_existing[file_path] = (existing_records, known_ids, known_urls)

        ex_records, known_ids, known_urls = known_existing[file_path]
        if item["id"] not in known_ids and item["url"] not in known_urls:
            items_to_process.append((file_path, item))
            known_ids.add(item["id"])
            known_urls.add(item["url"])

    print(f"      Identified {len(items_to_process)} brand-new articles to process.")

    print("[4/6] Extracting body paragraphs and related links (Spider Crawler)...")
    def enrich_item(entry):
        file_path, item = entry
        domain = item.pop("domain", "")
        if item.pop("can_full_text", False):
            paras, rel_links = extract_article_details(item["url"], domain)
            item["full_content"] = paras
            item["related_links"] = rel_links
            if paras:
                full_text_sample = " ".join(paras[:5])
                sc, lb = score_sentiment(f"{item['title']} {item['description']} {full_text_sample}")
                item["sentiment"] = {"score": sc, "label": lb}
        else:
            item.pop("can_full_text", None)
            item["full_content"] = [item["description"]] if item["description"] else []
            item["related_links"] = []
        return file_path, item

    enriched_items = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(enrich_item, entry) for entry in items_to_process]
        for f in as_completed(futures):
            enriched_items.append(f.result())

    # Update Link Registry with both ingested and discovered related links
    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for file_path, item in enriched_items:
        # Register main article as ingested
        link_registry["links"][item["url"]] = {
            "title": item["title"],
            "source": item["source"],
            "category": item["category"],
            "published_utc": item["published_utc"],
            "discovered_utc": now_str,
            "status": "ingested",
            "related_count": len(item.get("related_links", []))
        }
        # Register discovered related links
        for r_link in item.get("related_links", []):
            if r_link not in link_registry["links"]:
                link_registry["links"][r_link] = {
                    "source": item["source"],
                    "category": item["category"],
                    "parent_url": item["url"],
                    "discovered_utc": now_str,
                    "status": "discovered"
                }

    link_registry["total_discovered"] = len(link_registry["links"])
    link_registry["total_ingested"] = sum(1 for v in link_registry["links"].values() if v.get("status") == "ingested")

    # Save link_registry.json
    os.makedirs(os.path.dirname(link_registry_path), exist_ok=True)
    with open(link_registry_path, "w", encoding="utf-8") as f:
        json.dump(link_registry, f, indent=2, ensure_ascii=False)

    for file_path, item in enriched_items:
        ex_records, _, _ = known_existing[file_path]
        ex_records.append(item)

    for file_path, (records, _, _) in known_existing.items():
        if records:
            records.sort(key=lambda x: x["published_utc"], reverse=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)

    print("[5/6] Syncing updates and Link Registry to private vault...")
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "add", "data/"], cwd=VAULT_DIR, check=True)
    commit_res = subprocess.run(
        ["git", "commit", "-m", f"Ingest: {len(enriched_items)} articles + Link Registry updated ({link_registry['total_discovered']} total links) [skip ci]"],
        cwd=VAULT_DIR, capture_output=True, text=True
    )
    if commit_res.returncode == 0:
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        if push_res.returncode == 0:
            print(f"      Successfully synced articles and link registry to vault.")
        else:
            print(f"[ERROR] Push failed: {push_res.stderr.strip()}")
    else:
        print("      Vault already up to date. Zero changes.")

    print("[6/6] Sanitizing runner environment...")
    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    print(f"[SUCCESS] Spider cycle complete. Registry contains {link_registry['total_discovered']} tracked links.")

if __name__ == "__main__":
    main()
