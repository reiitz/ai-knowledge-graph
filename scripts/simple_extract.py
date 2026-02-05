#!/usr/bin/env python3
"""
Simple Knowledge Graph Extraction - no chunking, faster processing.
Designed for ralph loop execution.
"""

import json
import os
import sys
import time
import requests
from datetime import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
STATE_FILE = DATA_DIR / "extraction-state.json"
INPUT_FILE = DATA_DIR / "nhs-500-sample.json"
OUTPUT_FILE = DATA_DIR / "nhs-knowledge-graph.json"

# Config
PAGES_PER_ITERATION = 1  # One page at a time - LLM is slow
LM_STUDIO_URL = "http://localhost:11434/v1/chat/completions"  # Ollama
MODEL = "phi3:mini"
MAX_CONTENT_WORDS = 150  # Very short to speed up processing


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        "processed_ids": [],
        "failed_ids": [],
        "total_triples": 0,
        "last_updated": None
    }


def save_state(state):
    state["last_updated"] = datetime.now().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_output():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, 'r') as f:
            return json.load(f)
    return {
        "metadata": {"created": datetime.now().isoformat()[:10]},
        "pages": {},
        "all_triples": []
    }


def save_output(data):
    data["metadata"]["total_pages"] = len(data["pages"])
    data["metadata"]["total_triples"] = len(data["all_triples"])
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def extract_triples(content, page_id):
    """Extract triples from content using LM Studio."""
    # Truncate content if too long
    words = content.split()
    if len(words) > MAX_CONTENT_WORDS:
        content = ' '.join(words[:MAX_CONTENT_WORDS]) + "..."

    prompt = f"""Extract 3-5 key facts from this NHS text as JSON triples.

TEXT:
{content}

Return ONLY a JSON array like this, nothing else:
[{{"subject": "X", "predicate": "does Y", "object": "Z"}}]

JSON:"""

    try:
        response = requests.post(
            LM_STUDIO_URL,
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 800,
                "temperature": 0.3
            },
            timeout=300  # 5 minutes for slow CPU inference
        )

        if response.status_code != 200:
            return None, f"API error: {response.status_code}"

        result_text = response.json()['choices'][0]['message']['content']

        # Extract JSON from response
        try:
            # Try to find JSON array in response
            start = result_text.find('[')
            end = result_text.rfind(']') + 1
            if start >= 0 and end > start:
                json_str = result_text[start:end]
                # Fix common issues: single quotes to double quotes
                json_str = json_str.replace("'", '"')
                triples = json.loads(json_str)

                # Validate and add metadata
                valid_triples = []
                for t in triples:
                    if isinstance(t, dict) and 'subject' in t and 'predicate' in t and 'object' in t:
                        t['source_page_id'] = page_id
                        valid_triples.append(t)

                return valid_triples, None
            else:
                return None, "No JSON array in response"
        except json.JSONDecodeError as e:
            return None, f"JSON parse error: {e}"

    except requests.Timeout:
        return None, "Request timeout"
    except Exception as e:
        return None, str(e)


def main():
    print(f"\n{'='*50}")
    print(f"SIMPLE KNOWLEDGE GRAPH EXTRACTION")
    print(f"{datetime.now().isoformat()}")
    print(f"{'='*50}")

    # Load data
    state = load_state()
    output = load_output()

    with open(INPUT_FILE, 'r') as f:
        input_data = json.load(f)

    # Get pages to process
    all_pages = [p for p in input_data["pages"] if not p.get("is_duplicate_of")]
    processed_set = set(state["processed_ids"])
    failed_set = set(state["failed_ids"])

    pages_to_process = [p for p in all_pages
                        if p["id"] not in processed_set
                        and p["id"] not in failed_set]

    print(f"Total pages: {len(all_pages)}")
    print(f"Processed: {len(state['processed_ids'])}")
    print(f"Failed: {len(state['failed_ids'])}")
    print(f"Remaining: {len(pages_to_process)}")
    print(f"Triples extracted: {state['total_triples']}")

    if not pages_to_process:
        print("\n*** EXTRACTION COMPLETE ***")
        return 0

    # Process batch
    for page in pages_to_process[:PAGES_PER_ITERATION]:
        page_id = page["id"]
        title = page.get("title", "")[:40]
        print(f"\nProcessing {page_id}: {title}...")

        triples, error = extract_triples(page["content"], page_id)

        if triples:
            output["pages"][page_id] = {
                "id": page_id,
                "title": page.get("title", ""),
                "triple_count": len(triples)
            }
            output["all_triples"].extend(triples)
            state["processed_ids"].append(page_id)
            state["total_triples"] += len(triples)
            print(f"  OK: {len(triples)} triples")
        else:
            state["failed_ids"].append(page_id)
            print(f"  FAILED: {error}")

        time.sleep(2)  # Brief pause between requests

    save_state(state)
    save_output(output)

    # Summary
    remaining = len(all_pages) - len(state['processed_ids']) - len(state['failed_ids'])
    progress = len(state['processed_ids']) / len(all_pages) * 100

    print(f"\n{'='*50}")
    print(f"Progress: {len(state['processed_ids'])}/{len(all_pages)} ({progress:.1f}%)")
    print(f"Total triples: {state['total_triples']}")

    if remaining > 0:
        print(f"\n*** MORE WORK NEEDED ({remaining} pages) ***")
        return 1
    else:
        print("\n*** COMPLETE ***")
        return 0


if __name__ == "__main__":
    sys.exit(main())
