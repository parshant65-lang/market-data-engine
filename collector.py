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
from email.utils import parsedate_to_datetime

VAULT_PAT = os.environ.get("VAULT_PAT")
if not VAULT_PAT:
    print("[ERROR] VAULT_PAT environment variable is required.")
    sys.exit(1)

VAULT_REPO_URL = f"https://x-access-token:{VAULT_PAT}@github.com/parshant65-lang/market-intelligence-vault.git"
VAULT_DIR = "vault_storage"

CHANNELS = {
    "fastft": "https://www.ft.com/fastft?format=rss",
    "markets": "https://www.ft.com/rss/markets",
    "global_economy": "https://www.ft.com/global-economy?format=rss",
    "cryptofinance": "https://www.ft.com/cryptofinance?format=rss",
    "alphaville": "https://www.ft.com/alphaville?format=rss",
    "companies": "https://www.ft.com/companies?format=rss",
    "technology": "https://www.ft.com/technology?format=rss",
    "world": "https://www.ft.com/rss/world"
}

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

def fetch_feed(name, url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
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
                "category": name,
                "title": title,
                "description": desc,
                "url": link,
                "sentiment": {
                    "score": score,
                    "label": label
                },
                "ingested_utc": now_str
            })
        return items
    except Exception as e:
        return []

def main():
    print("[1/5] Ingesting real-time market intelligence feeds...")
    all_fetched = {}
    total_raw = 0
    for name, url in CHANNELS.items():
        records = fetch_feed(name, url)
        all_fetched[name] = records
        total_raw += len(records)
    print(f"      Fetched {total_raw} raw items across {len(CHANNELS)} channels.")

    print("[2/5] Connecting to private intelligence vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)
        
    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[ERROR] Failed to clone vault repository.")
        sys.exit(1)

    print("[3/5] Deduplicating and partitioning data...")
    new_records_count = 0
    
    for category, items in all_fetched.items():
        for item in items:
            pub_dt = item["published_utc"].split(" ")[0] # YYYY-MM-DD
            try:
                dt_obj = datetime.datetime.strptime(pub_dt, "%Y-%m-%d")
                year = str(dt_obj.year)
                month = f"{dt_obj.month:02d}"
                day_str = pub_dt
            except Exception:
                today = datetime.datetime.now(datetime.timezone.utc)
                year = str(today.year)
                month = f"{today.month:02d}"
                day_str = today.strftime("%Y-%m-%d")

            target_folder = os.path.join(VAULT_DIR, "data", category, year, month)
            os.makedirs(target_folder, exist_ok=True)
            file_path = os.path.join(target_folder, f"{category}_{day_str}.json")

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

            if item["id"] not in known_ids and item["url"] not in known_urls:
                existing.append(item)
                existing.sort(key=lambda x: x["published_utc"], reverse=True)
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(existing, f, indent=2, ensure_ascii=False)
                new_records_count += 1

    print(f"      Deduplication complete: {new_records_count} brand-new records added.")

    print("[4/5] Syncing updates to private vault...")
    if new_records_count > 0:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
        subprocess.run(["git", "add", "data/"], cwd=VAULT_DIR, check=True)
        commit_res = subprocess.run(
            ["git", "commit", "-m", f"Ingest: {new_records_count} new market intelligence records [skip ci]"],
            cwd=VAULT_DIR, capture_output=True, text=True
        )
        if commit_res.returncode == 0:
            push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
            if push_res.returncode == 0:
                print("      Successfully committed and pushed updates to vault.")
            else:
                print(f"[ERROR] Push failed: {push_res.stderr.strip()}")
    else:
        print("      Vault is already up to date. Zero new changes.")

    print("[5/5] Sanitizing runner environment...")
    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    print(f"[SUCCESS] Pipeline cycle complete. Total new items archived: {new_records_count}.")

if __name__ == "__main__":
    main()
