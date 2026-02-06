#!/usr/bin/env python3
"""
Retry Failed Pages — Knowledge Graph Extraction
Reprocesses pages that failed in the initial extraction run.
Uses Mistral 7B at low temperature (0.2) + improved JSON repair.
Skips binary content (.docx files scraped as raw bytes).
Designed for loop execution — reads state, does work, updates state.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_DIR))

from src.knowledge_graph.config import load_config
from src.knowledge_graph.main import process_text_in_chunks

# Paths
DATA_DIR = PROJECT_DIR / "data"
STATE_FILE = DATA_DIR / "extraction-state.json"
INPUT_FILE = DATA_DIR / "nhs-500-sample.json"
OUTPUT_FILE = DATA_DIR / "nhs-knowledge-graph.json"
CONFIG_FILE = PROJECT_DIR / "config.toml"

# Retry config
PAGES_PER_ITERATION = 1  # One at a time — each page can take 15-40 min on CPU
RETRY_TEMPERATURE = 0.2  # Low temp for deterministic JSON output


def load_json(path: Path) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def save_json(path: Path, data: dict) -> None:
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def is_binary_content(content: str) -> bool:
    """Detect binary/corrupted content (e.g. raw .docx files)."""
    if content.startswith('PK'):
        return True
    # High ratio of non-printable characters in the first 200 chars
    sample = content[:200]
    non_printable = sum(1 for c in sample if not c.isprintable() and c not in '\n\r\t')
    return non_printable > 20


def process_page(page: dict, config: dict) -> tuple[list | None, str | None]:
    """Process a single page through the KG extractor."""
    content = page.get("content", "")
    if not content or len(content.strip()) < 50:
        return None, "Content too short"

    if is_binary_content(content):
        return None, "Binary content (skipped)"

    try:
        # Disable standardisation and inference for batch speed
        config_copy = dict(config)
        config_copy["standardization"] = {"enabled": False}
        config_copy["inference"] = {"enabled": False}

        triples = process_text_in_chunks(config_copy, content, debug=False)

        if triples:
            for triple in triples:
                triple["source_page_id"] = page["id"]
                triple["source_url"] = page.get("url", "")
                triple["source_domain"] = page.get("source", "")
            return triples, None
        else:
            return None, "No triples extracted"

    except Exception as e:
        return None, str(e)


def main():
    print(f"\n{'='*50}")
    print("RETRY FAILED PAGES — Mistral 7B (low temp)")
    print(f"{datetime.now().isoformat()}")
    print(f"{'='*50}")

    # Load config and override temperature for retry
    config = load_config(str(CONFIG_FILE))
    if not config:
        print("ERROR: Failed to load config")
        return 1

    config["llm"]["model"] = "mistral-7b-instruct-v0.2"
    config["llm"]["temperature"] = RETRY_TEMPERATURE
    config["llm"]["max_tokens"] = 1024       # Enough for ~10-15 triples per chunk
    config["llm"]["timeout"] = 1200          # 20 min — CPU inference at ~0.7 tok/s needs time
    print(f"Model: mistral-7b-instruct-v0.2 | Temperature: {RETRY_TEMPERATURE} | max_tokens: 1024 | timeout: 1200s")

    # Load state and data
    state = load_json(STATE_FILE)
    input_data = load_json(INPUT_FILE)
    output = load_json(OUTPUT_FILE) if OUTPUT_FILE.exists() else {
        "metadata": {
            "created": datetime.now().isoformat()[:10],
            "source": str(INPUT_FILE),
            "total_pages_processed": 0,
            "total_triples": 0
        },
        "pages": {},
        "all_triples": []
    }

    # Build lookup of failed pages — skip binary content
    failed_ids = state.get("failed_ids", [])
    pages_by_id = {p["id"]: p for p in input_data["pages"]}

    failed_pages = []
    skipped_binary = []
    for fid in failed_ids:
        page = pages_by_id.get(fid)
        if not page:
            continue
        content = page.get("content", "")
        if is_binary_content(content):
            skipped_binary.append(fid)
        else:
            failed_pages.append(page)

    # Move binary pages out of failed_ids permanently (they'll never succeed)
    if skipped_binary:
        if "skipped_ids" not in state:
            state["skipped_ids"] = []
        for sid in skipped_binary:
            if sid in state["failed_ids"]:
                state["failed_ids"].remove(sid)
                state["skipped_ids"].append(sid)
        print(f"Skipped {len(skipped_binary)} binary pages (moved to skipped_ids)")

    print(f"Failed pages to retry: {len(failed_pages)}")
    print(f"Already processed: {len(state.get('processed_ids', []))}")
    print(f"Total triples so far: {state.get('total_triples', 0)}")

    if not failed_pages:
        print("\n*** NO FAILED PAGES TO RETRY ***")
        state["last_updated"] = datetime.now().isoformat()
        save_json(STATE_FILE, state)
        return 0

    # Process a batch
    batch = failed_pages[:PAGES_PER_ITERATION]
    succeeded = 0

    for page in batch:
        page_id = page["id"]
        title = page.get("title", "No title")[:60]
        content_len = len(page.get("content", ""))
        print(f"\nRetrying {page_id}: {title} ({content_len} chars)...")

        triples, error = process_page(page, config)

        if triples:
            # Success — move from failed to processed
            output["pages"][page_id] = {
                "id": page_id,
                "url": page.get("url", ""),
                "title": page.get("title", ""),
                "source": page.get("source", ""),
                "triple_count": len(triples)
            }
            output["all_triples"].extend(triples)

            state["processed_ids"].append(page_id)
            state["failed_ids"].remove(page_id)
            state["total_triples"] = state.get("total_triples", 0) + len(triples)
            succeeded += 1
            print(f"  OK: {len(triples)} triples extracted")
        else:
            print(f"  FAILED AGAIN: {error}")

        time.sleep(1)

    # Save progress
    state["last_updated"] = datetime.now().isoformat()
    save_json(STATE_FILE, state)

    output["metadata"]["total_pages_processed"] = len(output["pages"])
    output["metadata"]["total_triples"] = len(output["all_triples"])
    save_json(OUTPUT_FILE, output)

    # Summary
    remaining_failed = len(state.get("failed_ids", []))
    total_processed = len(state.get("processed_ids", []))

    print(f"\n{'='*50}")
    print("RETRY SUMMARY")
    print(f"{'='*50}")
    print(f"This iteration: {succeeded}/{len(batch)} succeeded")
    print(f"Total processed: {total_processed}")
    print(f"Remaining failed: {remaining_failed}")
    print(f"Total triples: {state.get('total_triples', 0)}")

    if remaining_failed > 0:
        print(f"\n*** {remaining_failed} FAILED PAGES REMAINING ***")
        return 1
    else:
        print("\n*** ALL RETRIES COMPLETE ***")
        return 0


if __name__ == "__main__":
    sys.exit(main())
