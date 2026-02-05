#!/usr/bin/env python3
"""
NHS Page Scraper via Wayback Machine
Designed for ralph loop execution - reads state, does work, updates state.
"""

import json
import os
import sys
import time
import random
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "data"
STATE_FILE = DATA_DIR / "scraper-state.json"
OUTPUT_FILE = DATA_DIR / "nhs-500-sample.json"

# Config
TARGET_PAGES = 500
TARGET_DUPLICATES = 30
MIN_WORD_COUNT = 150  # Lowered from 250 to get more pages
PAGES_PER_ITERATION = 15  # How many pages to fetch per ralph loop iteration
REQUEST_DELAY = 3  # Seconds between requests (higher to avoid rate limiting)

# Create session with retry logic
def get_session():
    """Create a requests session with retry logic for SSL errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # 2, 4, 8 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (compatible; NHSResearchBot/1.0; educational research)'
    })
    return session

SESSION = None

def get_http_session():
    """Get or create the HTTP session."""
    global SESSION
    if SESSION is None:
        SESSION = get_session()
    return SESSION

DOMAINS = [
    "england.nhs.uk",
    "digital.nhs.uk",
    "hee.nhs.uk"
]

# Specific paths to query (wildcards are too slow)
DOMAIN_PATHS = {
    "england.nhs.uk": [
        "england.nhs.uk/publications/*",
        "england.nhs.uk/commissioning/*",
        "england.nhs.uk/primary-care/*",
        "england.nhs.uk/mental-health/*",
        "england.nhs.uk/long-read/*",
        "england.nhs.uk/blog/*",
        "england.nhs.uk/about/*",
        "england.nhs.uk/statistics/*",
        "england.nhs.uk/ourwork/*",
        "england.nhs.uk/improvement-hub/*",
        "england.nhs.uk/patient-safety/*",
    ],
    "digital.nhs.uk": [
        "digital.nhs.uk/services/*",
        "digital.nhs.uk/data-and-information/*",
        "digital.nhs.uk/about-nhs-digital/*",
        "digital.nhs.uk/developer/*",
        "digital.nhs.uk/news/*",
        "digital.nhs.uk/binaries/*",
        "digital.nhs.uk/coronavirus/*",
        "digital.nhs.uk/programmes/*",
    ],
    "hee.nhs.uk": [
        "hee.nhs.uk/our-work/*",
        "hee.nhs.uk/news/*",
        "hee.nhs.uk/about/*",
        "hee.nhs.uk/sites/*",
        "hee.nhs.uk/education-training/*",
        "hee.nhs.uk/careers/*",
    ]
}

# URL patterns to exclude
EXCLUDE_PATTERNS = [
    r'/assets/', r'/images/', r'/css/', r'/js/',
    r'\.pdf$', r'\.jpg$', r'\.png$', r'\.gif$',
    r'/login', r'/signin', r'/search',
    r'/feed/', r'/rss/', r'\.xml$'
]


def load_state():
    """Load current scraper state."""
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        "phase": "url_discovery",  # url_discovery, scraping, duplicates, complete
        "discovered_urls": {},  # domain -> [urls]
        "scraped_count": 0,
        "failed_urls": [],
        "duplicate_pairs_created": 0,
        "last_updated": None
    }


def save_state(state):
    """Save current scraper state."""
    state["last_updated"] = datetime.now().isoformat()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_output():
    """Load current output data."""
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r') as f:
            return json.load(f)
    return {
        "metadata": {
            "created": datetime.now().isoformat()[:10],
            "total_pages": 0,
            "sources": DOMAINS,
            "duplicate_pairs": 0
        },
        "pages": [],
        "duplicate_pairs": []
    }


def save_output(data):
    """Save output data."""
    data["metadata"]["total_pages"] = len([p for p in data["pages"] if not p.get("is_duplicate_of")])
    data["metadata"]["duplicate_pairs"] = len(data["duplicate_pairs"])
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def query_cdx_api(domain, limit=500):
    """Query Wayback Machine CDX API for archived URLs using specific paths."""
    print(f"Querying CDX API for {domain}...")

    # Get specific paths for this domain
    paths = DOMAIN_PATHS.get(domain, [f"{domain}/*"])

    all_urls = []
    session = get_http_session()

    for path in paths:
        print(f"  Querying path: {path}...")

        # CDX API endpoint
        url = "https://web.archive.org/cdx/search/cdx"
        params = {
            "url": path,
            "output": "json",
            "filter": "mimetype:text/html",
            "collapse": "urlkey",  # Dedupe by URL
            "limit": 100,  # Smaller per-path limit
            "fl": "timestamp,original,mimetype,statuscode"
        }

        try:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not data or len(data) < 2:
                continue

            # First row is headers
            headers = data[0]

            for row in data[1:]:
                record = dict(zip(headers, row))
                original_url = record.get("original", "")
                timestamp = record.get("timestamp", "")

                # Filter out unwanted patterns
                if any(re.search(p, original_url, re.I) for p in EXCLUDE_PATTERNS):
                    continue

                # Avoid duplicates
                if any(u["url"] == original_url for u in all_urls):
                    continue

                all_urls.append({
                    "url": original_url,
                    "timestamp": timestamp,
                    "wayback_url": f"https://web.archive.org/web/{timestamp}/{original_url}"
                })

            print(f"    Got {len(data)-1} URLs from {path}")
            time.sleep(1)  # Be nice to the API

        except Exception as e:
            print(f"    Error querying {path}: {e}")
            continue

        # Stop if we have enough URLs for this domain
        if len(all_urls) >= limit // len(DOMAINS):
            break

    print(f"  Total: {len(all_urls)} URLs for {domain}")
    return all_urls


def extract_content(html, url):
    """Extract main text content from HTML."""
    soup = BeautifulSoup(html, 'lxml')

    # Remove unwanted elements
    for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'noscript']):
        tag.decompose()

    # Try to find main content area
    main = soup.find('main') or soup.find('article') or soup.find(class_=re.compile(r'content|main|article', re.I))

    if main:
        text = main.get_text(separator=' ', strip=True)
    else:
        # Fallback to body
        body = soup.find('body')
        text = body.get_text(separator=' ', strip=True) if body else soup.get_text(separator=' ', strip=True)

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Get title
    title_tag = soup.find('title')
    title = title_tag.get_text(strip=True) if title_tag else urlparse(url).path.split('/')[-1]

    return title, text


def fetch_page(wayback_url, original_url):
    """Fetch a page from Wayback Machine and extract content."""
    try:
        session = get_http_session()
        response = session.get(wayback_url, timeout=30)
        response.raise_for_status()

        title, content = extract_content(response.text, original_url)
        word_count = len(content.split())

        if word_count < MIN_WORD_COUNT:
            return None, f"Too short ({word_count} words)"

        return {
            "title": title,
            "content": content,
            "word_count": word_count
        }, None

    except Exception as e:
        return None, str(e)


def get_category(url):
    """Extract category from URL path."""
    path = urlparse(url).path.lower()
    parts = [p for p in path.split('/') if p]

    if parts:
        return parts[0]
    return "root"


def do_url_discovery(state):
    """Phase 1: Discover URLs from Wayback Machine."""
    print("\n=== URL Discovery Phase ===")

    for domain in DOMAINS:
        if domain not in state["discovered_urls"] or len(state["discovered_urls"][domain]) < 200:
            urls = query_cdx_api(domain, limit=300)
            state["discovered_urls"][domain] = urls
            time.sleep(2)  # Be nice to the API

    total_urls = sum(len(urls) for urls in state["discovered_urls"].values())
    print(f"\nTotal URLs discovered: {total_urls}")

    # Move to scraping if we have at least 300 URLs or enough to potentially reach target
    if total_urls >= 300:
        state["phase"] = "scraping"
        print("Moving to scraping phase...")

    save_state(state)
    return state


def do_scraping(state, output):
    """Phase 2: Scrape content from discovered URLs."""
    print("\n=== Scraping Phase ===")

    current_count = len([p for p in output["pages"] if not p.get("is_duplicate_of")])
    print(f"Current pages: {current_count}/{TARGET_PAGES}")

    if current_count >= TARGET_PAGES:
        state["phase"] = "duplicates"
        save_state(state)
        return state, output

    pages_fetched = 0

    for domain in DOMAINS:
        if pages_fetched >= PAGES_PER_ITERATION:
            break

        urls = state["discovered_urls"].get(domain, [])

        for url_info in urls:
            if pages_fetched >= PAGES_PER_ITERATION:
                break

            original_url = url_info["url"]
            wayback_url = url_info["wayback_url"]

            # Skip if already scraped
            if any(p["url"] == original_url for p in output["pages"]):
                continue

            # Skip if previously failed
            if original_url in state["failed_urls"]:
                continue

            print(f"  Fetching: {original_url[:60]}...")

            result, error = fetch_page(wayback_url, original_url)

            if result:
                page_id = f"page_{len(output['pages']) + 1:04d}"
                page = {
                    "id": page_id,
                    "url": original_url,
                    "source": domain,
                    "category": get_category(original_url),
                    "title": result["title"],
                    "content": result["content"],
                    "word_count": result["word_count"],
                    "scraped_at": datetime.now().isoformat(),
                    "is_duplicate_of": None
                }
                output["pages"].append(page)
                pages_fetched += 1
                print(f"    OK ({result['word_count']} words)")
            else:
                state["failed_urls"].append(original_url)
                print(f"    SKIP: {error}")

            time.sleep(REQUEST_DELAY)

    state["scraped_count"] = len([p for p in output["pages"] if not p.get("is_duplicate_of")])

    if state["scraped_count"] >= TARGET_PAGES:
        state["phase"] = "duplicates"

    save_state(state)
    save_output(output)

    print(f"\nProgress: {state['scraped_count']}/{TARGET_PAGES} pages")
    return state, output


def do_duplicates(state, output):
    """Phase 3: Create duplicate pairs."""
    print("\n=== Duplicate Creation Phase ===")

    current_pairs = len(output["duplicate_pairs"])
    print(f"Current duplicate pairs: {current_pairs}/{TARGET_DUPLICATES}")

    if current_pairs >= TARGET_DUPLICATES:
        state["phase"] = "complete"
        save_state(state)
        return state, output

    # Get source pages for duplicates (diverse selection)
    source_pages = [p for p in output["pages"] if not p.get("is_duplicate_of")]

    # Select pages for duplication (spread across sources)
    pages_per_source = (TARGET_DUPLICATES - current_pairs) // len(DOMAINS) + 1
    candidates = []

    for domain in DOMAINS:
        domain_pages = [p for p in source_pages if p["source"] == domain]
        random.shuffle(domain_pages)
        candidates.extend(domain_pages[:pages_per_source])

    # Create duplicates
    pairs_created = 0
    for i, source_page in enumerate(candidates):
        if current_pairs + pairs_created >= TARGET_DUPLICATES:
            break

        # Check if this page already has a duplicate
        if any(dp["original"] == source_page["id"] for dp in output["duplicate_pairs"]):
            continue

        dup_id = f"dup_{len(output['pages']) + 1:04d}"

        # Alternate between exact (even) and paraphrased placeholder (odd)
        is_exact = (pairs_created % 2 == 0)

        if is_exact:
            # Exact copy
            dup_content = source_page["content"]
            dup_type = "exact"
        else:
            # For paraphrased, we'd use LLM - for now mark as needing paraphrase
            dup_content = f"[NEEDS_PARAPHRASE] {source_page['content']}"
            dup_type = "paraphrase_pending"

        duplicate = {
            "id": dup_id,
            "url": f"synthetic://{source_page['source']}/duplicate/{dup_id}",
            "source": source_page["source"],
            "category": source_page["category"],
            "title": f"[DUP] {source_page['title']}",
            "content": dup_content,
            "word_count": source_page["word_count"],
            "scraped_at": datetime.now().isoformat(),
            "is_duplicate_of": source_page["id"],
            "duplicate_type": dup_type
        }

        output["pages"].append(duplicate)
        output["duplicate_pairs"].append({
            "original": source_page["id"],
            "duplicate": dup_id,
            "type": dup_type
        })

        pairs_created += 1
        print(f"  Created {dup_type} duplicate of {source_page['id']}")

    state["duplicate_pairs_created"] = len(output["duplicate_pairs"])

    if state["duplicate_pairs_created"] >= TARGET_DUPLICATES:
        state["phase"] = "complete"

    save_state(state)
    save_output(output)

    print(f"\nProgress: {state['duplicate_pairs_created']}/{TARGET_DUPLICATES} duplicate pairs")
    return state, output


def print_summary(state, output):
    """Print current progress summary."""
    print("\n" + "=" * 50)
    print("SCRAPER STATUS SUMMARY")
    print("=" * 50)
    print(f"Phase: {state['phase']}")
    print(f"URLs discovered: {sum(len(urls) for urls in state['discovered_urls'].values())}")
    print(f"Pages scraped: {len([p for p in output['pages'] if not p.get('is_duplicate_of')])}/{TARGET_PAGES}")
    print(f"Duplicate pairs: {len(output['duplicate_pairs'])}/{TARGET_DUPLICATES}")
    print(f"Failed URLs: {len(state['failed_urls'])}")

    if output["pages"]:
        print("\nPages by source:")
        for domain in DOMAINS:
            count = len([p for p in output["pages"] if p["source"] == domain and not p.get("is_duplicate_of")])
            print(f"  {domain}: {count}")

    print("=" * 50)


def main():
    """Main entry point - one iteration of the ralph loop."""
    print(f"\n{'='*50}")
    print(f"NHS SCRAPER - {datetime.now().isoformat()}")
    print(f"{'='*50}")

    # Load state and output
    state = load_state()
    output = load_output()

    print(f"Current phase: {state['phase']}")

    # Execute current phase
    if state["phase"] == "url_discovery":
        state = do_url_discovery(state)

    if state["phase"] == "scraping":
        state, output = do_scraping(state, output)

    if state["phase"] == "duplicates":
        state, output = do_duplicates(state, output)

    # Print summary
    print_summary(state, output)

    # Determine exit code
    if state["phase"] == "complete":
        print("\n*** TASK COMPLETE ***")
        print(f"Output saved to: {OUTPUT_FILE}")
        return 0
    else:
        print(f"\n*** MORE WORK NEEDED (phase: {state['phase']}) ***")
        return 1


if __name__ == "__main__":
    sys.exit(main())
