#!/usr/bin/env python3
"""
Phi-3 Judge Script — Week 4 of TESTING_PLAN.md

Sends ambiguous cases to Phi-3 Mini (via Ollama) for independent review.
Uses an architecturally different model from Gemini to reduce correlated errors.

Two tasks:
  1. Classification: 90 AMBIGUOUS pages (coherence 0.4–0.7)
  2. Deduplication: 32 MEDIUM pairs (KG similarity 50–80%)

Output: AGREE / DISAGREE / UNCERTAIN per case.
"""

import csv
import json
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import Optional

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"

# Input files
EVAL_RESULTS = DATA_DIR / "evaluation-results.json"
SAMPLE_PAGES = DATA_DIR / "nhs-500-sample.json"
PIPELINE_CLASSIFICATIONS = PROJECT_DIR.parent / "nhse-content-pipeline" / "sample_pipeline_results.json"
PIPELINE_DUPLICATES = PROJECT_DIR.parent / "nhse-content-pipeline" / "sample_pipeline.duplicates.json"
DEDUP_SCORES = DATA_DIR / "deduplication-scores.csv"

# Output files
JUDGE_STATE = DATA_DIR / "phi3-judge-state.json"
CLASS_OUTPUT_JSON = DATA_DIR / "phi3-judge-classification.json"
CLASS_OUTPUT_CSV = DATA_DIR / "phi3-judge-classification.csv"
DEDUP_OUTPUT_JSON = DATA_DIR / "phi3-judge-deduplication.json"
DEDUP_OUTPUT_CSV = DATA_DIR / "phi3-judge-deduplication.csv"

# Ollama config
OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL = "phi3:mini"
TIMEOUT = 300  # 5 min per request — Phi-3 is small but CPU is slow

# Content truncation — keep prompts within Phi-3's 4k context
MAX_CONTENT_CHARS = 1500


def call_phi3(prompt: str) -> Optional[str]:
    """Send a prompt to Phi-3 via Ollama and return the response."""
    payload = {
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 200,
        }
    }
    try:
        resp = requests.post(OLLAMA_URL, json=payload, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()["message"]["content"].strip()
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to Ollama. Run 'ollama serve' first.")
        return None
    except requests.exceptions.Timeout:
        print(f"ERROR: Phi-3 timed out after {TIMEOUT}s")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def parse_verdict(response: str) -> str:
    """Extract AGREE/DISAGREE/UNCERTAIN from Phi-3's response."""
    upper = response.upper()
    if "AGREE" in upper and "DISAGREE" not in upper:
        return "AGREE"
    if "DISAGREE" in upper:
        return "DISAGREE"
    if "UNCERTAIN" in upper:
        return "UNCERTAIN"
    # Fallback: look for the first word
    first_word = response.split()[0].upper().strip(".:,") if response else ""
    if first_word in ("AGREE", "DISAGREE", "UNCERTAIN"):
        return first_word
    return "UNCERTAIN"


def load_state() -> dict:
    """Load judge state for resumability."""
    if JUDGE_STATE.exists():
        with open(JUDGE_STATE) as f:
            return json.load(f)
    return {"classified": [], "deduped": [], "last_updated": None}


def save_state(state: dict):
    """Save judge state."""
    state["last_updated"] = datetime.now().isoformat()
    with open(JUDGE_STATE, "w") as f:
        json.dump(state, f, indent=2)


def load_page_content() -> dict[str, dict]:
    """Load page content from sample JSON, keyed by page_id."""
    with open(SAMPLE_PAGES) as f:
        data = json.load(f)
    return {p["id"]: p for p in data["pages"]}


def load_eval_results() -> dict:
    """Load KG evaluation results."""
    with open(EVAL_RESULTS) as f:
        return json.load(f)


def load_pipeline_classifications() -> dict[str, str]:
    """Load Gemini pipeline classifications."""
    with open(PIPELINE_CLASSIFICATIONS) as f:
        data = json.load(f)
    return data.get("classifications", {})


def load_medium_dedup_pairs() -> list[dict]:
    """Load MEDIUM dedup pairs from CSV."""
    pairs = []
    with open(DEDUP_SCORES) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["classification"] == "MEDIUM":
                pairs.append(row)
    return pairs


# ─── Classification judging ────────────────────────────────────────

CLASSIFICATION_PROMPT = """You are an independent reviewer checking whether an NHS web page has been correctly classified.

PAGE TITLE: {title}

PAGE CONTENT (truncated):
{content}

GEMINI PIPELINE CLASSIFICATION: {gemini_category}

KNOWLEDGE GRAPH ENTITIES FOUND: {kg_entities}
TAXONOMY CATEGORIES MATCHED: {kg_categories}
KG COHERENCE SCORE: {coherence_score}

The coherence score is between the KG entities and the taxonomy. A score of 0.4–0.7 means the match is ambiguous.

Based on the actual page content, does the Gemini classification "{gemini_category}" seem correct?

Respond with exactly one word on the first line: AGREE, DISAGREE, or UNCERTAIN
Then give a one-sentence reason."""


def judge_classification(page_id: str, page: dict, eval_data: dict,
                         gemini_class: str) -> dict:
    """Ask Phi-3 to judge one ambiguous classification."""
    content = page.get("content", "")[:MAX_CONTENT_CHARS]
    title = page.get("title", page_id)

    prompt = CLASSIFICATION_PROMPT.format(
        title=title,
        content=content,
        gemini_category=gemini_class,
        kg_entities=", ".join(eval_data.get("matched_entities", [])[:10]),
        kg_categories=", ".join(eval_data.get("matched_categories", [])),
        coherence_score=eval_data.get("coherence_score", "N/A"),
    )

    print(f"  Judging {page_id} ({title[:50]}...) ", end="", flush=True)
    start = time.time()
    response = call_phi3(prompt)
    elapsed = time.time() - start

    if response is None:
        return {"page_id": page_id, "verdict": "ERROR", "reason": "No response",
                "elapsed_s": round(elapsed, 1)}

    verdict = parse_verdict(response)
    # Extract reason (everything after the first line)
    lines = response.strip().split("\n")
    reason = " ".join(lines[1:]).strip() if len(lines) > 1 else ""

    print(f"→ {verdict} ({elapsed:.1f}s)")
    return {
        "page_id": page_id,
        "title": title,
        "gemini_category": gemini_class,
        "coherence_score": eval_data.get("coherence_score"),
        "verdict": verdict,
        "reason": reason,
        "raw_response": response,
        "elapsed_s": round(elapsed, 1),
    }


# ─── Deduplication judging ─────────────────────────────────────────

DEDUPLICATION_PROMPT = """You are an independent reviewer checking whether two NHS web pages are duplicates.

PAGE A TITLE: {title_a}
PAGE A CONTENT (truncated):
{content_a}

PAGE B TITLE: {title_b}
PAGE B CONTENT (truncated):
{content_b}

KNOWLEDGE GRAPH SIMILARITY: {similarity} (entity overlap between the two pages)
SHARED ENTITIES: {shared_entities}

A similarity of 50–80% means the overlap is ambiguous — they might be duplicates with minor differences, or genuinely different pages on related topics.

Based on the actual content, are these two pages duplicates (same information, possibly with minor formatting/date differences)?

Respond with exactly one word on the first line: AGREE (they are duplicates), DISAGREE (they are not duplicates), or UNCERTAIN
Then give a one-sentence reason."""


def judge_dedup_pair(pair: dict, pages: dict[str, dict]) -> dict:
    """Ask Phi-3 to judge one medium-similarity dedup pair."""
    page_a_id = pair["page_a"]
    page_b_id = pair["page_b"]

    page_a = pages.get(page_a_id, {})
    page_b = pages.get(page_b_id, {})

    # Halve content per page to fit both in context
    half_limit = MAX_CONTENT_CHARS // 2
    content_a = page_a.get("content", "")[:half_limit]
    content_b = page_b.get("content", "")[:half_limit]

    prompt = DEDUPLICATION_PROMPT.format(
        title_a=pair.get("title_a", page_a_id),
        content_a=content_a,
        title_b=pair.get("title_b", page_b_id),
        content_b=content_b,
        similarity=pair.get("combined_score", "N/A"),
        shared_entities=pair.get("shared_entities", "N/A"),
    )

    pair_key = f"{page_a_id}:{page_b_id}"
    print(f"  Judging {pair_key} ", end="", flush=True)
    start = time.time()
    response = call_phi3(prompt)
    elapsed = time.time() - start

    if response is None:
        return {"pair": pair_key, "verdict": "ERROR", "reason": "No response",
                "elapsed_s": round(elapsed, 1)}

    verdict = parse_verdict(response)
    lines = response.strip().split("\n")
    reason = " ".join(lines[1:]).strip() if len(lines) > 1 else ""

    print(f"→ {verdict} ({elapsed:.1f}s)")
    return {
        "page_a": page_a_id,
        "page_b": page_b_id,
        "title_a": pair.get("title_a", ""),
        "title_b": pair.get("title_b", ""),
        "kg_similarity": pair.get("combined_score"),
        "verdict": verdict,
        "reason": reason,
        "raw_response": response,
        "elapsed_s": round(elapsed, 1),
    }


# ─── Output writers ────────────────────────────────────────────────

def write_classification_outputs(results: list[dict]):
    """Write classification judge results to JSON and CSV."""
    summary = {
        "agree": sum(1 for r in results if r["verdict"] == "AGREE"),
        "disagree": sum(1 for r in results if r["verdict"] == "DISAGREE"),
        "uncertain": sum(1 for r in results if r["verdict"] == "UNCERTAIN"),
        "error": sum(1 for r in results if r["verdict"] == "ERROR"),
        "total": len(results),
    }

    output = {
        "metadata": {
            "judged_at": datetime.now().isoformat(),
            "model": MODEL,
            "task": "classification_ambiguous",
            "total_cases": len(results),
        },
        "summary": summary,
        "results": results,
    }

    with open(CLASS_OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    with open(CLASS_OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "page_id", "title", "gemini_category", "coherence_score",
            "verdict", "reason", "elapsed_s"
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in writer.fieldnames})

    print(f"\nClassification results saved:")
    print(f"  {CLASS_OUTPUT_JSON}")
    print(f"  {CLASS_OUTPUT_CSV}")
    print(f"  Summary: {summary}")


def write_dedup_outputs(results: list[dict]):
    """Write deduplication judge results to JSON and CSV."""
    summary = {
        "agree": sum(1 for r in results if r["verdict"] == "AGREE"),
        "disagree": sum(1 for r in results if r["verdict"] == "DISAGREE"),
        "uncertain": sum(1 for r in results if r["verdict"] == "UNCERTAIN"),
        "error": sum(1 for r in results if r["verdict"] == "ERROR"),
        "total": len(results),
    }

    output = {
        "metadata": {
            "judged_at": datetime.now().isoformat(),
            "model": MODEL,
            "task": "deduplication_medium",
            "total_cases": len(results),
        },
        "summary": summary,
        "results": results,
    }

    with open(DEDUP_OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    with open(DEDUP_OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "page_a", "page_b", "title_a", "title_b",
            "kg_similarity", "verdict", "reason", "elapsed_s"
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in writer.fieldnames})

    print(f"\nDeduplication results saved:")
    print(f"  {DEDUP_OUTPUT_JSON}")
    print(f"  {DEDUP_OUTPUT_CSV}")
    print(f"  Summary: {summary}")


# ─── Main ──────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("PHI-3 JUDGE — TESTING_PLAN.md Week 4")
    print("=" * 60)

    # Check Ollama is reachable
    print(f"\nChecking Ollama at {OLLAMA_URL}...")
    test_resp = call_phi3("Respond with just the word OK.")
    if test_resp is None:
        print("\nCannot reach Ollama. Please run: ollama serve")
        return
    print(f"  Phi-3 responded: {test_resp[:50]}")

    # Load all data
    print("\nLoading data...")
    pages = load_page_content()
    eval_results = load_eval_results()
    gemini_classes = load_pipeline_classifications()
    medium_pairs = load_medium_dedup_pairs()
    state = load_state()

    # ── Part 1: Classification ──
    ambiguous_pages = {
        pid: pdata for pid, pdata in eval_results["pages"].items()
        if pdata["routing"] == "AMBIGUOUS"
    }
    remaining_class = [
        pid for pid in ambiguous_pages if pid not in state["classified"]
    ]

    print(f"\n{'='*60}")
    print(f"PART 1: CLASSIFICATION JUDGE")
    print(f"  Total AMBIGUOUS: {len(ambiguous_pages)}")
    print(f"  Already judged:  {len(state['classified'])}")
    print(f"  Remaining:       {len(remaining_class)}")
    print(f"{'='*60}")

    class_results = []
    # Load any previous results
    if CLASS_OUTPUT_JSON.exists():
        with open(CLASS_OUTPUT_JSON) as f:
            prev = json.load(f)
            class_results = prev.get("results", [])

    for i, page_id in enumerate(remaining_class):
        print(f"\n[{i+1}/{len(remaining_class)}]", end="")
        eval_data = ambiguous_pages[page_id]
        page = pages.get(page_id, {})
        gemini_cat = gemini_classes.get(page_id, "Unknown")

        result = judge_classification(page_id, page, eval_data, gemini_cat)
        class_results.append(result)
        state["classified"].append(page_id)

        # Save state after each case (resumable)
        save_state(state)
        write_classification_outputs(class_results)

    # ── Part 2: Deduplication ──
    remaining_dedup = [
        p for p in medium_pairs
        if f"{p['page_a']}:{p['page_b']}" not in state["deduped"]
    ]

    print(f"\n{'='*60}")
    print(f"PART 2: DEDUPLICATION JUDGE")
    print(f"  Total MEDIUM pairs: {len(medium_pairs)}")
    print(f"  Already judged:     {len(state['deduped'])}")
    print(f"  Remaining:          {len(remaining_dedup)}")
    print(f"{'='*60}")

    dedup_results = []
    # Load any previous results
    if DEDUP_OUTPUT_JSON.exists():
        with open(DEDUP_OUTPUT_JSON) as f:
            prev = json.load(f)
            dedup_results = prev.get("results", [])

    for i, pair in enumerate(remaining_dedup):
        print(f"\n[{i+1}/{len(remaining_dedup)}]", end="")
        result = judge_dedup_pair(pair, pages)
        dedup_results.append(result)
        state["deduped"].append(f"{pair['page_a']}:{pair['page_b']}")

        save_state(state)
        write_dedup_outputs(dedup_results)

    # ── Summary ──
    print(f"\n{'='*60}")
    print("DONE — ALL CASES JUDGED")
    print(f"{'='*60}")
    print(f"Classification: {len(class_results)} cases")
    print(f"Deduplication:  {len(dedup_results)} pairs")


if __name__ == "__main__":
    main()
