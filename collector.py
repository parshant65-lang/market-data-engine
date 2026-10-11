import os
import sys
import re
import time
import json
import shutil
import datetime
import subprocess
import html as html_lib
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

VAULT_PAT = os.environ.get("VAULT_PAT")
if not VAULT_PAT:
    print("[ERROR] VAULT_PAT environment variable is required.")
    sys.exit(1)

VAULT_REPO_URL = f"https://x-access-token:{VAULT_PAT}@github.com/parshant65-lang/market-intelligence-vault.git"
VAULT_DIR = "vault_spider_storage"
BUFFER_DIR = "local_spider_buffer"

SESSION_MAX_SECONDS = 5 * 3600 + 30 * 60   # 5.5 Hours continuous session
PUSH_INTERVAL_SECONDS = 900                 # Exactly 15 MINUTES sync cadence
NUM_WORKERS = 20                            # 20 parallel spider threads

CHANNELS = [
    # CNBC Feeds (US Macro & Global)
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html"},
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/10001147/device/rss/rss.html"},
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/19854910/device/rss/rss.html"},
    {"category": "cnbc_markets", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/15839069/device/rss/rss.html"},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html"},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/10000115/device/rss/rss.html"},
    {"category": "cnbc_economy", "source": "CNBC", "domain": "cnbc.com", "url": "https://www.cnbc.com/id/21324812/device/rss/rss.html"},

    # Crypto & Digital Assets
    {"category": "coindesk", "source": "CoinDesk", "domain": "coindesk.com", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"category": "cointelegraph", "source": "CoinTelegraph", "domain": "cointelegraph.com", "url": "https://cointelegraph.com/rss"},
    {"category": "decrypt", "source": "Decrypt", "domain": "decrypt.co", "url": "https://decrypt.co/feed"},

    # US Stocks & Breaking Equities
    {"category": "benzinga", "source": "Benzinga", "domain": "benzinga.com", "url": "https://www.benzinga.com/feed"},
    {"category": "yahoo_finance", "source": "Yahoo Finance", "domain": "finance.yahoo.com", "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^DJI,^IXIC,AAPL,MSFT,NVDA,GOOGL,AMZN,TSLA&region=US&lang=en-US"},

    # Indian Equities & Macro Policy
    {"category": "livemint", "source": "Livemint", "domain": "livemint.com", "url": "https://www.livemint.com/rss/markets"},
    {"category": "livemint", "source": "Livemint", "domain": "livemint.com", "url": "https://www.livemint.com/rss/companies"},
    {"category": "moneycontrol", "source": "Moneycontrol", "domain": "moneycontrol.com", "url": "https://www.moneycontrol.com/rss/MCtopnews.xml"},
    {"category": "moneycontrol", "source": "Moneycontrol", "domain": "moneycontrol.com", "url": "https://www.moneycontrol.com/rss/latestnews.xml"},
    {"category": "moneycontrol", "source": "Moneycontrol", "domain": "moneycontrol.com", "url": "https://www.moneycontrol.com/rss/business.xml"},
    {"category": "economic_times", "source": "Economic Times", "domain": "economictimes.indiatimes.com", "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"},
    {"category": "economic_times", "source": "Economic Times", "domain": "economictimes.indiatimes.com", "url": "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"}
]

CNBC_HUBS = [
    ("https://www.cnbc.com/markets/", "cnbc_markets"),
    ("https://www.cnbc.com/economy/", "cnbc_economy"),
    ("https://www.cnbc.com/investing/", "cnbc_markets"),
    ("https://www.cnbc.com/technology/", "cnbc_markets"),
    ("https://www.cnbc.com/finance/", "cnbc_markets"),
    ("https://www.cnbc.com/world/", "cnbc_economy")
]

def clean_html(raw_html):
    if not raw_html:
        return ""
    clean = re.sub(r"<[^>]+>", "", raw_html)
    clean = html_lib.unescape(clean)
    return " ".join(clean.split())

def extract_spider_links(url, domain):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            html = resp.read().decode("utf-8", errors="ignore")

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
                if any(skip in path for skip in ["/api/", "/favicons/", "/video/", "/watch/", "/pro/", "/live-tv/", "/author/", "/tag/", "/privacy", "/terms", "/investingclub/"]):
                    continue
                if "cnbc.com" in domain:
                    if "/amp/" in full_url:
                        full_url = full_url.replace("/amp/", "/")
                    if path.endswith(".html") and not any(skip in path for skip in ["/privacy", "/terms", "/disclaimer", "/site-map", "/help"]):
                        if re.search(r"/\d{4}/\d{2}/", path) or any(cat in path for cat in ["/markets", "/economy", "/finance", "/investing", "/wealth", "/business", "/technology", "/real-estate", "/personal-finance", "/world"]):
                            related_links.append(full_url)
                elif "benzinga.com" in domain:
                    if any(cat in path for cat in ["/money/", "/markets/", "/news/", "/trading/"]) and len(path.strip("/").split("/")) >= 2:
                        related_links.append(full_url)
                elif "livemint.com" in domain:
                    if any(cat in path for cat in ["/market/", "/companies/", "/money/", "/industry/", "/economy/"]) and len(path.strip("/").split("/")) >= 2:
                        related_links.append(full_url)
                elif "moneycontrol.com" in domain:
                    if any(cat in path for cat in ["/news/business/", "/news/market/", "/news/economy/", "/news/local-markets/"]) or ("news" in path and ".html" in path):
                        related_links.append(full_url)
                elif "indiatimes.com" in domain:
                    if "/markets/" in path and ("articleshow" in path or ".cms" in path or len(path.strip("/").split("/")) >= 2):
                        related_links.append(full_url)
                elif "decrypt.co" in domain:
                    if re.search(r"/\d+/", path) or any(cat in path for cat in ["/news/", "/market/"]):
                        related_links.append(full_url)
                elif "yahoo.com" in domain:
                    if "/news/" in path or len(path.strip("/").split("/")) >= 2:
                        related_links.append(full_url)
                else:
                    if any(cat in path for cat in ["/markets/", "/business/", "/policy/", "/tech/", "/news/"]) and len(path.strip("/").split("/")) >= 3:
                        related_links.append(full_url)

        return related_links[:35]
    except Exception:
        return []

def get_feed_seeds():
    seeds = []
    seen = set()
    # 1. RSS Feeds
    for ch in CHANNELS:
        try:
            req = urllib.request.Request(ch["url"], headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            with urllib.request.urlopen(req, timeout=10) as r:
                root = ET.fromstring(r.read())
            for it in root.findall(".//item")[:25]:
                link = it.findtext("link", "").strip()
                title = clean_html(it.findtext("title", ""))
                if link and link not in seen:
                    seen.add(link)
                    seeds.append({
                        "url": link,
                        "title": title,
                        "source": ch["source"],
                        "category": ch["category"],
                        "domain": ch["domain"]
                    })
        except Exception:
            pass

    # 2. CNBC Category Hubs
    for hub_url, category in CNBC_HUBS:
        try:
            req = urllib.request.Request(hub_url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            with urllib.request.urlopen(req, timeout=10) as r:
                html = r.read().decode("utf-8", errors="ignore")
            raw_links = re.findall(r'href=["\'](https?://[^"\'\s<>]+|/[^"\'\s<>]+)["\']', html)
            for l in raw_links:
                full = urljoin(hub_url, l).split("?")[0].split("#")[0]
                if "/amp/" in full:
                    full = full.replace("/amp/", "/")
                if "cnbc.com" in full and full.endswith(".html") and re.search(r"/\d{4}/\d{2}/", full):
                    if full not in seen:
                        seen.add(full)
                        seeds.append({
                            "url": full,
                            "title": "",
                            "source": "CNBC",
                            "category": category,
                            "domain": "cnbc.com"
                        })
        except Exception:
            pass

    return seeds

def sync_spider_links_to_vault():
    buffer_file = os.path.join(BUFFER_DIR, "spider_new_links.json")
    if not os.path.exists(buffer_file):
        return

    try:
        with open(buffer_file, "r", encoding="utf-8") as f:
            new_links = json.load(f)
    except Exception:
        new_links = {}

    if not new_links:
        return

    print(f"\n[SPIDER SYNC] Synchronizing {len(new_links)} discovered links to master vault...")
    if os.path.exists(VAULT_DIR):
        shutil.rmtree(VAULT_DIR)

    clone_res = subprocess.run(
        ["git", "clone", "--depth", "1", VAULT_REPO_URL, VAULT_DIR],
        capture_output=True, text=True
    )
    if clone_res.returncode != 0:
        print("[SPIDER SYNC ERROR] Failed to connect to vault.")
        return

    link_reg_path = os.path.join(VAULT_DIR, "data", "link_registry.json")
    link_reg = {"total_discovered": 0, "total_ingested": 0, "links": {}}
    if os.path.exists(link_reg_path):
        try:
            with open(link_reg_path, "r", encoding="utf-8") as f:
                link_reg = json.load(f)
        except Exception:
            pass

    added_count = 0
    for url, meta in new_links.items():
        if url not in link_reg["links"]:
            link_reg["links"][url] = meta
            added_count += 1
        # If url already in link_reg, NEVER overwrite status if already ingested
        elif link_reg["links"][url].get("status") == "discovered" and meta.get("title"):
            link_reg["links"][url]["title"] = meta["title"]

    link_reg["total_discovered"] = len(link_reg["links"])
    link_reg["total_ingested"] = sum(1 for v in link_reg["links"].values() if v.get("status") == "ingested")

    with open(link_reg_path, "w", encoding="utf-8") as f:
        json.dump(link_reg, f, indent=2, ensure_ascii=False)

    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=VAULT_DIR, check=True)
    subprocess.run(["git", "add", "data/link_registry.json"], cwd=VAULT_DIR, check=True)
    commit_res = subprocess.run(
        ["git", "commit", "-m", f"Spider sync: +{added_count} discovered links (Total: {link_reg['total_discovered']}) [skip ci]"],
        cwd=VAULT_DIR, capture_output=True, text=True
    )
    if commit_res.returncode == 0:
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=VAULT_DIR, capture_output=True, text=True)
        if push_res.returncode == 0:
            print(f"[SPIDER SYNC] Successfully synced +{added_count} links to vault registry.")
        else:
            print(f"[SPIDER SYNC ERROR] Push failed: {push_res.stderr.strip()}")

    shutil.rmtree(VAULT_DIR, ignore_errors=True)
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)

def trigger_next_relay():
    url = "https://api.github.com/repos/parshant65-lang/market-data-engine/actions/workflows/harvest_intelligence.yml/dispatches"
    req = urllib.request.Request(
        url,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {VAULT_PAT}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Continuous-Spider-Engine"
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print("[SEAMLESS RELAY] Next continuous spider session triggered successfully.")
    except Exception as e:
        print(f"[RELAY DISPATCH ERROR] {e}")

def main():
    print("=" * 70)
    print("🕷️ AUTONOMOUS HIGH-SPEED LINK SPIDER INITIALIZED")
    print(f"   Target Portals: 9 Financial Networks (20 Feeds + 6 Hubs)")
    print(f"   Sync Cadence: Exactly Every {PUSH_INTERVAL_SECONDS // 60} Minutes")
    print(f"   Max Session Time: {SESSION_MAX_SECONDS // 3600}h {SESSION_MAX_SECONDS % 3600 // 60}m")
    print("=" * 70)

    os.makedirs(BUFFER_DIR, exist_ok=True)
    start_time = time.time()
    last_push_time = time.time()
    discovered_in_session = 0

    while (time.time() - start_time) < SESSION_MAX_SECONDS:
        now_ts = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
        print(f"\n[{now_ts}] Scanning 9 Financial Feeds and Section Hubs...")

        # 1. Fetch fresh seeds from all feeds & hubs
        seeds = get_feed_seeds()
        buffer_file = os.path.join(BUFFER_DIR, "spider_new_links.json")
        buffered_links = {}
        if os.path.exists(buffer_file):
            try:
                with open(buffer_file, "r", encoding="utf-8") as f:
                    buffered_links = json.load(f)
            except Exception:
                buffered_links = {}

        now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        added_this_loop = 0
        for s in seeds:
            url = s["url"]
            if url not in buffered_links:
                buffered_links[url] = {
                    "source": s["source"],
                    "category": s["category"],
                    "title": s.get("title", ""),
                    "discovered_utc": now_iso,
                    "status": "discovered"
                }
                added_this_loop += 1

        # 2. Deep Spider: Pick sample of newly discovered URLs and extract related links
        spider_targets = list(buffered_links.items())[-25:]
        if spider_targets:
            def spider_url(target):
                u, meta = target
                parsed = urlparse(u)
                rel_links = extract_spider_links(u, parsed.netloc)
                return u, meta, rel_links

            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                futures = [executor.submit(spider_url, t) for t in spider_targets]
                for f in as_completed(futures):
                    parent_u, parent_meta, rel_links = f.result()
                    for rl in rel_links:
                        if rl not in buffered_links:
                            buffered_links[rl] = {
                                "source": parent_meta["source"],
                                "category": parent_meta["category"],
                                "parent_url": parent_u,
                                "discovered_utc": now_iso,
                                "status": "discovered"
                            }
                            added_this_loop += 1

        discovered_in_session += added_this_loop
        os.makedirs(BUFFER_DIR, exist_ok=True)
        with open(buffer_file, "w", encoding="utf-8") as f:
            json.dump(buffered_links, f, ensure_ascii=False)

        print(f"[{now_ts}] Spider Crawl Finished: +{added_this_loop} links added. (Buffered: {len(buffered_links)} links)")

        # Sync every 15 minutes
        if (time.time() - last_push_time) >= PUSH_INTERVAL_SECONDS:
            sync_spider_links_to_vault()
            last_push_time = time.time()

        time.sleep(30)

    print("\n[SESSION COMPLETE] Flushing final spider buffer and triggering next relay...")
    sync_spider_links_to_vault()
    trigger_next_relay()
    shutil.rmtree(BUFFER_DIR, ignore_errors=True)
    print("🏁 Spider session handover complete.")

if __name__ == "__main__":
    main()
