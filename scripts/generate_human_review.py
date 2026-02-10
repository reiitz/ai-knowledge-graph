"""
Generate human review spreadsheets for content experts.

Collects two types of cases that need manual expert review:
1. CONFLICT classification pages (coherence < 0.4) - pages where the KG
   evaluation found low coherence between Gemini's classification and
   the knowledge graph's entity/category matching.
2. Deduplication cross-reference mismatches - where the Gemini pipeline's
   duplicate detection and the KG's similarity assessment disagree.

Outputs:
- data/human-review-classification.csv
- data/human-review-deduplication.csv
"""

import csv
import json
from pathlib import Path

# ----- Paths -----
PROJECT_ROOT = Path("/home/admo2/ai-knowledge-graph")
PIPELINE_ROOT = Path("/home/admo2/nhse-content-pipeline")

EVAL_RESULTS = PROJECT_ROOT / "data" / "evaluation-results.json"
NHS_SAMPLE = PROJECT_ROOT / "data" / "nhs-500-sample.json"
PIPELINE_RESULTS = PIPELINE_ROOT / "sample_pipeline_results.json"
PIPELINE_DUPLICATES = PIPELINE_ROOT / "sample_pipeline.duplicates.json"
PIPELINE_MAPPING = PIPELINE_ROOT / "sample_pipeline.mapping.json"
DEDUP_SCORES = PROJECT_ROOT / "data" / "deduplication-scores.csv"

OUT_CLASSIFICATION = PROJECT_ROOT / "data" / "human-review-classification.csv"
OUT_DEDUPLICATION = PROJECT_ROOT / "data" / "human-review-deduplication.csv"


def load_json(path: Path) -> dict:
    """Load a JSON file and return its contents."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_content_lookup(pages: list[dict]) -> dict[str, dict]:
    """Build a lookup from page_id to page content dict."""
    return {page["id"]: page for page in pages}


def build_content_id_to_page_id(mapping: dict) -> dict[int, str]:
    """Reverse the mapping file: content_db_id -> page_id."""
    reverse = {}
    for page_id, info in mapping.items():
        reverse[info["content_db_id"]] = page_id
    return reverse


def load_kg_scores() -> dict[tuple[str, str], dict]:
    """
    Load KG deduplication scores into a dict keyed by (page_a, page_b)
    where page_a < page_b (sorted) to make lookups consistent.
    """
    scores = {}
    with open(DEDUP_SCORES, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only care about HIGH and LOW for cross-referencing
            if row["classification"] == "MEDIUM":
                continue
            pair_key = tuple(sorted([row["page_a"], row["page_b"]]))
            scores[pair_key] = {
                "combined_score": float(row["combined_score"]),
                "classification": row["classification"],
                "title_a": row["title_a"],
                "title_b": row["title_b"],
                "page_a": row["page_a"],
                "page_b": row["page_b"],
            }
    return scores


def generate_classification_review(
    eval_data: dict,
    content_lookup: dict[str, dict],
    classifications: dict[str, str],
) -> list[dict]:
    """
    Collect the CONFLICT classification pages for human review.
    These are pages with coherence < 0.4 where the KG strongly disagrees
    with the Gemini classification.
    """
    rows = []
    for page_id, page_eval in eval_data["pages"].items():
        if page_eval["routing"] != "CONFLICT":
            continue

        content_page = content_lookup.get(page_id, {})
        content_text = content_page.get("content", "")
        excerpt = content_text[:500] if content_text else ""

        rows.append({
            "page_id": page_id,
            "title": page_eval["title"],
            "gemini_classification": classifications.get(page_id, "UNKNOWN"),
            "coherence_score": page_eval["coherence_score"],
            "kg_matched_categories": "; ".join(page_eval.get("matched_categories", [])),
            "kg_matched_entities": "; ".join(page_eval.get("matched_entities", [])),
            "content_excerpt": excerpt,
            "expert_verdict": "",
            "notes": "",
        })

    # Sort by coherence score ascending (worst first)
    rows.sort(key=lambda r: r["coherence_score"])
    return rows


def generate_deduplication_review(
    gemini_pairs: list[dict],
    content_id_to_page: dict[int, str],
    kg_scores: dict[tuple[str, str], dict],
    content_lookup: dict[str, dict],
) -> list[dict]:
    """
    Cross-reference Gemini duplicate pairs against KG similarity scores.

    Logic:
    - Pipeline DUPLICATE + KG HIGH (>80%) -> AGREE (skip)
    - Pipeline DUPLICATE + KG LOW (<50%) -> CONFLICT (flag)
    - Pipeline NOT DUPLICATE + KG HIGH (>80%) -> MISSED_DUPLICATE (flag)
    - Pipeline NOT DUPLICATE + KG LOW (<50%) -> AGREE (skip)
    - KG MEDIUM (50-80%) -> Phi-3 judge handles (skip)

    For "NOT DUPLICATE" pairs: these are all page pairs that exist in the
    KG scores as HIGH but were NOT flagged by the Gemini pipeline.
    """
    rows = []

    # First, build a set of all Gemini-flagged duplicate pairs (as page_id pairs)
    gemini_dup_pairs: set[tuple[str, str]] = set()

    for pair in gemini_pairs:
        page_a = content_id_to_page.get(pair["content_id_a"])
        page_b = content_id_to_page.get(pair["content_id_b"])

        # Skip pairs where we can't map content IDs to page IDs
        if not page_a or not page_b:
            continue

        pair_key = tuple(sorted([page_a, page_b]))
        gemini_dup_pairs.add(pair_key)

        # Check this Gemini DUPLICATE pair against KG scores
        kg_entry = kg_scores.get(pair_key)

        if kg_entry and kg_entry["classification"] == "LOW":
            # CONFLICT: Pipeline says duplicate, KG says not similar
            content_a = content_lookup.get(pair_key[0], {})
            content_b = content_lookup.get(pair_key[1], {})

            rows.append({
                "page_a": pair_key[0],
                "page_b": pair_key[1],
                "title_a": content_a.get("title", kg_entry.get("title_a", "")),
                "title_b": content_b.get("title", kg_entry.get("title_b", "")),
                "mismatch_type": "CONFLICT",
                "gemini_similarity": pair["similarity"],
                "kg_similarity": kg_entry["combined_score"],
                "content_excerpt_a": content_a.get("content", "")[:300],
                "content_excerpt_b": content_b.get("content", "")[:300],
                "expert_verdict": "",
                "notes": "",
            })

        elif not kg_entry:
            # Pair exists in Gemini but not in KG scores at all (or was MEDIUM).
            # If not in KG at all, the KG didn't find meaningful overlap,
            # which is effectively LOW similarity. Flag as CONFLICT.
            content_a = content_lookup.get(pair_key[0], {})
            content_b = content_lookup.get(pair_key[1], {})

            # Confirm both pages exist in our sample
            if content_a and content_b:
                rows.append({
                    "page_a": pair_key[0],
                    "page_b": pair_key[1],
                    "title_a": content_a.get("title", ""),
                    "title_b": content_b.get("title", ""),
                    "mismatch_type": "CONFLICT",
                    "gemini_similarity": pair["similarity"],
                    "kg_similarity": 0.0,
                    "content_excerpt_a": content_a.get("content", "")[:300],
                    "content_excerpt_b": content_b.get("content", "")[:300],
                    "expert_verdict": "",
                    "notes": "",
                })

    # Now check for MISSED_DUPLICATE: KG HIGH pairs not in Gemini duplicates
    for pair_key, kg_entry in kg_scores.items():
        if kg_entry["classification"] != "HIGH":
            continue
        if pair_key in gemini_dup_pairs:
            # AGREE: both say duplicate
            continue

        # MISSED_DUPLICATE: KG says highly similar, Gemini didn't flag
        content_a = content_lookup.get(pair_key[0], {})
        content_b = content_lookup.get(pair_key[1], {})

        rows.append({
            "page_a": pair_key[0],
            "page_b": pair_key[1],
            "title_a": content_a.get("title", kg_entry.get("title_a", "")),
            "title_b": content_b.get("title", kg_entry.get("title_b", "")),
            "mismatch_type": "MISSED_DUPLICATE",
            "gemini_similarity": 0.0,
            "kg_similarity": kg_entry["combined_score"],
            "content_excerpt_a": content_a.get("content", "")[:300],
            "content_excerpt_b": content_b.get("content", "")[:300],
            "expert_verdict": "",
            "notes": "",
        })

    # Sort: CONFLICT first, then MISSED_DUPLICATE, then by KG similarity desc
    rows.sort(key=lambda r: (r["mismatch_type"] != "CONFLICT", -r["kg_similarity"]))
    return rows


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write a list of dicts to a CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    print("Loading input files...")

    eval_data = load_json(EVAL_RESULTS)
    nhs_sample = load_json(NHS_SAMPLE)
    pipeline_results = load_json(PIPELINE_RESULTS)
    pipeline_duplicates = load_json(PIPELINE_DUPLICATES)
    pipeline_mapping = load_json(PIPELINE_MAPPING)

    content_lookup = build_content_lookup(nhs_sample["pages"])
    classifications = pipeline_results["classifications"]
    content_id_to_page = build_content_id_to_page_id(pipeline_mapping["mapping"])

    print("Loading KG deduplication scores (this may take a moment)...")
    kg_scores = load_kg_scores()
    print(f"  Loaded {len(kg_scores)} non-MEDIUM KG pairs")

    # ----- 1. Classification review -----
    print("\nGenerating classification review...")
    classification_rows = generate_classification_review(
        eval_data, content_lookup, classifications
    )
    write_csv(
        OUT_CLASSIFICATION,
        classification_rows,
        fieldnames=[
            "page_id", "title", "gemini_classification", "coherence_score",
            "kg_matched_categories", "kg_matched_entities", "content_excerpt",
            "expert_verdict", "notes",
        ],
    )
    print(f"  Written {len(classification_rows)} CONFLICT pages to {OUT_CLASSIFICATION}")

    # ----- 2. Deduplication review -----
    print("\nGenerating deduplication review...")
    dedup_rows = generate_deduplication_review(
        pipeline_duplicates["pairs"],
        content_id_to_page,
        kg_scores,
        content_lookup,
    )
    write_csv(
        OUT_DEDUPLICATION,
        dedup_rows,
        fieldnames=[
            "page_a", "page_b", "title_a", "title_b", "mismatch_type",
            "gemini_similarity", "kg_similarity", "content_excerpt_a",
            "content_excerpt_b", "expert_verdict", "notes",
        ],
    )
    print(f"  Written {len(dedup_rows)} mismatch pairs to {OUT_DEDUPLICATION}")

    # ----- Summary -----
    conflict_count = sum(1 for r in dedup_rows if r["mismatch_type"] == "CONFLICT")
    missed_count = sum(1 for r in dedup_rows if r["mismatch_type"] == "MISSED_DUPLICATE")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Classification review:  {len(classification_rows)} CONFLICT pages")
    print(f"Deduplication review:   {len(dedup_rows)} mismatch pairs")
    print(f"  - CONFLICT:           {conflict_count} (pipeline=dup, KG=low)")
    print(f"  - MISSED_DUPLICATE:   {missed_count} (pipeline=not dup, KG=high)")
    print(f"\nTotal items for expert review: {len(classification_rows) + len(dedup_rows)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
