#!/usr/bin/env python3
"""
Batch Knowledge Graph Extraction
Processes NHS sample pages through the knowledge graph extractor.
Designed for ralph loop execution - reads state, does work, updates state.
"""

import json
import os
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

# Config
PAGES_PER_ITERATION = 2  # Process 2 pages per ralph loop iteration (LLM is slow)
CONFIG_FILE = PROJECT_DIR / "config.toml"


def load_state():
    """Load current extraction state."""
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        "phase": "extracting",
        "processed_ids": [],
        "failed_ids": [],
        "total_triples": 0,
        "last_updated": None
    }


def save_state(state):
    """Save current extraction state."""
    state["last_updated"] = datetime.now().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_input():
    """Load the NHS sample pages."""
    with open(INPUT_FILE, 'r') as f:
        return json.load(f)


def load_output():
    """Load current output data."""
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r') as f:
            return json.load(f)
    return {
        "metadata": {
            "created": datetime.now().isoformat()[:10],
            "source": str(INPUT_FILE),
            "total_pages_processed": 0,
            "total_triples": 0
        },
        "pages": {},  # page_id -> {page_info, triples}
        "all_triples": []  # Flat list of all triples with page_id
    }


def save_output(data):
    """Save output data."""
    data["metadata"]["total_pages_processed"] = len(data["pages"])
    data["metadata"]["total_triples"] = len(data["all_triples"])
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def process_page(page, config):
    """
    Process a single page through the knowledge graph extractor.

    Returns:
        tuple: (triples, error_message) - triples is None if failed
    """
    try:
        content = page.get("content", "")
        if not content or len(content.strip()) < 50:
            return None, "Content too short"

        # Process text through the extractor
        # Disable standardization and inference for batch processing (faster)
        config_copy = dict(config)
        config_copy["standardization"] = {"enabled": False}
        config_copy["inference"] = {"enabled": False}

        triples = process_text_in_chunks(config_copy, content, debug=False)

        if triples:
            # Add page metadata to each triple
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
    """Main entry point - one iteration of the ralph loop."""
    print(f"\n{'='*50}")
    print(f"BATCH KNOWLEDGE GRAPH EXTRACTION")
    print(f"{datetime.now().isoformat()}")
    print(f"{'='*50}")

    # Load config
    config = load_config(str(CONFIG_FILE))
    if not config:
        print("ERROR: Failed to load config")
        return 1

    # Load state and data
    state = load_state()
    input_data = load_input()
    output = load_output()

    # Get pages to process (exclude duplicates)
    all_pages = [p for p in input_data["pages"] if not p.get("is_duplicate_of")]
    total_pages = len(all_pages)

    # Filter to unprocessed pages
    processed_set = set(state["processed_ids"])
    failed_set = set(state["failed_ids"])
    pages_to_process = [p for p in all_pages
                        if p["id"] not in processed_set
                        and p["id"] not in failed_set]

    print(f"Total pages: {total_pages}")
    print(f"Processed: {len(state['processed_ids'])}")
    print(f"Failed: {len(state['failed_ids'])}")
    print(f"Remaining: {len(pages_to_process)}")
    print(f"Total triples so far: {state['total_triples']}")

    # Check if complete
    if not pages_to_process:
        print("\n*** EXTRACTION COMPLETE ***")
        print(f"Total pages processed: {len(state['processed_ids'])}")
        print(f"Total triples extracted: {state['total_triples']}")
        print(f"Output file: {OUTPUT_FILE}")
        state["phase"] = "complete"
        save_state(state)
        return 0

    # Process batch of pages
    pages_processed = 0
    for page in pages_to_process[:PAGES_PER_ITERATION]:
        page_id = page["id"]
        print(f"\nProcessing {page_id}: {page.get('title', 'No title')[:50]}...")

        triples, error = process_page(page, config)

        if triples:
            # Store results
            output["pages"][page_id] = {
                "id": page_id,
                "url": page.get("url", ""),
                "title": page.get("title", ""),
                "source": page.get("source", ""),
                "triple_count": len(triples)
            }
            output["all_triples"].extend(triples)

            state["processed_ids"].append(page_id)
            state["total_triples"] += len(triples)
            pages_processed += 1

            print(f"  OK: {len(triples)} triples extracted")
        else:
            state["failed_ids"].append(page_id)
            print(f"  FAILED: {error}")

        # Small delay between pages to not overwhelm LM Studio
        time.sleep(1)

    # Save progress
    save_state(state)
    save_output(output)

    # Print summary
    print(f"\n{'='*50}")
    print("ITERATION SUMMARY")
    print(f"{'='*50}")
    print(f"Pages processed this iteration: {pages_processed}")
    print(f"Total processed: {len(state['processed_ids'])}/{total_pages}")
    print(f"Total triples: {state['total_triples']}")
    print(f"Progress: {len(state['processed_ids'])/total_pages*100:.1f}%")

    remaining = total_pages - len(state['processed_ids']) - len(state['failed_ids'])
    if remaining > 0:
        print(f"\n*** MORE WORK NEEDED ({remaining} pages remaining) ***")
        return 1
    else:
        print("\n*** EXTRACTION COMPLETE ***")
        state["phase"] = "complete"
        save_state(state)
        return 0


if __name__ == "__main__":
    sys.exit(main())
