"""
Generate a calibration check sample from COHERENT classification results.

Randomly selects 30 pages routed as COHERENT for manual verification.
If more than 3 of 30 are incorrect, classification thresholds need adjusting.

Usage:
    python scripts/generate_calibration_sample.py
"""

import csv
import json
import random
from pathlib import Path

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_RESULTS = PROJECT_ROOT / "data" / "evaluation-results.json"
NHS_SAMPLE = PROJECT_ROOT / "data" / "nhs-500-sample.json"
PIPELINE_RESULTS = Path("/home/admo2/nhse-content-pipeline/sample_pipeline_results.json")
OUTPUT_CSV = PROJECT_ROOT / "data" / "calibration-sample.csv"

SAMPLE_SIZE = 30
CONTENT_EXCERPT_LEN = 500
MAX_ENTITIES_SHOWN = 10
SEED = 42


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    random.seed(SEED)

    # Load all three data sources
    eval_data = load_json(EVAL_RESULTS)
    nhs_data = load_json(NHS_SAMPLE)
    pipeline_data = load_json(PIPELINE_RESULTS)

    # Build lookup: page_id -> page content from the NHS sample
    content_by_id: dict[str, dict] = {
        page["id"]: page for page in nhs_data["pages"]
    }

    # Build lookup: page_id -> Gemini classification
    classifications: dict[str, str] = pipeline_data["classifications"]

    # Filter to COHERENT pages only
    coherent_pages = [
        (page_id, page_info)
        for page_id, page_info in eval_data["pages"].items()
        if page_info["routing"] == "COHERENT"
    ]

    print(f"Total pages in evaluation results: {len(eval_data['pages'])}")
    print(f"Pages routed COHERENT: {len(coherent_pages)}")

    if len(coherent_pages) < SAMPLE_SIZE:
        print(
            f"WARNING: Only {len(coherent_pages)} COHERENT pages available, "
            f"but {SAMPLE_SIZE} requested. Using all of them."
        )
        sample = coherent_pages
    else:
        sample = random.sample(coherent_pages, SAMPLE_SIZE)

    # Sort by page_id for tidy output
    sample.sort(key=lambda x: x[0])

    # Build CSV rows
    rows: list[dict] = []
    for page_id, info in sample:
        content_page = content_by_id.get(page_id, {})
        content_text = content_page.get("content", "")
        excerpt = content_text[:CONTENT_EXCERPT_LEN].replace("\n", " ").strip()

        matched_cats = info.get("matched_categories", [])
        matched_ents = info.get("matched_entities", [])

        rows.append({
            "page_id": page_id,
            "title": info["title"],
            "gemini_classification": classifications.get(page_id, "UNKNOWN"),
            "coherence_score": info["coherence_score"],
            "kg_matched_categories": "; ".join(matched_cats),
            "kg_matched_entities": "; ".join(matched_ents[:MAX_ENTITIES_SHOWN]),
            "content_excerpt": excerpt,
            "correct": "",
            "notes": "",
        })

    # Write CSV
    fieldnames = [
        "page_id",
        "title",
        "gemini_classification",
        "coherence_score",
        "kg_matched_categories",
        "kg_matched_entities",
        "content_excerpt",
        "correct",
        "notes",
    ]

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nCalibration sample written to: {OUTPUT_CSV}")
    print(f"Sample size: {len(rows)}")

    # Print summary table
    print(f"\n{'='*90}")
    print(f"{'#':<4} {'Page ID':<12} {'Score':<7} {'Gemini Classification':<30} {'Title'}")
    print(f"{'-'*90}")
    for i, row in enumerate(rows, 1):
        title_trunc = row["title"][:40] + ("..." if len(row["title"]) > 40 else "")
        print(
            f"{i:<4} {row['page_id']:<12} {row['coherence_score']:<7.2f} "
            f"{row['gemini_classification']:<30} {title_trunc}"
        )
    print(f"{'='*90}")

    # Distribution summary
    gemini_counts: dict[str, int] = {}
    for row in rows:
        cat = row["gemini_classification"]
        gemini_counts[cat] = gemini_counts.get(cat, 0) + 1

    print("\nGemini classification distribution in sample:")
    for cat, count in sorted(gemini_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<35} {count:>3}")

    score_values = [row["coherence_score"] for row in rows]
    print(f"\nCoherence score range: {min(score_values):.2f} - {max(score_values):.2f}")
    print(f"Mean coherence score:  {sum(score_values) / len(score_values):.2f}")

    print("\nCalibration threshold: if >3 of 30 are incorrect, adjust thresholds.")


if __name__ == "__main__":
    main()
