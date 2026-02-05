#!/usr/bin/env python3
"""
Deduplication Evaluation Script

Calculates knowledge graph similarity between page pairs to validate
duplicate detection. Per TESTING_PLAN.md methodology:

KG Similarity thresholds:
  > 80%:  HIGH - likely duplicates
  50-80%: MEDIUM - ambiguous, send to Phi-3
  < 50%:  LOW - likely not duplicates

Cross-reference with pipeline duplicate detection:
  Pipeline says DUPLICATE + KG HIGH    → AGREE (auto-accept)
  Pipeline says DUPLICATE + KG LOW     → CONFLICT (flag for review)
  Pipeline says NOT DUP + KG HIGH      → MISSED DUPLICATE (flag)
  Pipeline says NOT DUP + KG LOW       → AGREE (auto-accept)
  KG MEDIUM                            → AMBIGUOUS (send to Phi-3)
"""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from itertools import combinations

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
KG_OUTPUT_FILE = DATA_DIR / "nhs-knowledge-graph.json"
DEDUP_EVAL_FILE = DATA_DIR / "deduplication-evaluation.json"

# Thresholds from TESTING_PLAN.md
HIGH_SIMILARITY = 0.80
LOW_SIMILARITY = 0.50


def load_kg_output() -> dict:
    """Load extracted knowledge graph."""
    if not KG_OUTPUT_FILE.exists():
        return {'pages': {}, 'all_triples': []}

    with open(KG_OUTPUT_FILE, 'r') as f:
        return json.load(f)


def extract_entities_from_triples(triples: list[dict]) -> set[str]:
    """Extract normalised entities (subjects and objects) from triples."""
    entities = set()

    for triple in triples:
        for key in ('subject', 'object'):
            val = triple.get(key)
            if isinstance(val, str):
                entities.add(val.lower().strip())
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, str):
                        entities.add(item.lower().strip())

    return entities


def calculate_jaccard_similarity(set_a: set, set_b: set) -> float:
    """Calculate Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0

    intersection = set_a & set_b
    union = set_a | set_b

    return len(intersection) / len(union) if union else 0.0


def calculate_predicate_similarity(triples_a: list[dict], triples_b: list[dict]) -> float:
    """Calculate similarity based on predicate overlap."""
    preds_a = set(t.get('predicate', '').lower().strip() for t in triples_a)
    preds_b = set(t.get('predicate', '').lower().strip() for t in triples_b)

    return calculate_jaccard_similarity(preds_a, preds_b)


def calculate_kg_similarity(triples_a: list[dict], triples_b: list[dict]) -> dict:
    """
    Calculate comprehensive KG similarity between two pages.

    Returns dict with:
      - entity_similarity: Jaccard on entities
      - predicate_similarity: Jaccard on predicates
      - combined_score: Weighted combination
      - classification: HIGH / MEDIUM / LOW
    """
    entities_a = extract_entities_from_triples(triples_a)
    entities_b = extract_entities_from_triples(triples_b)

    entity_sim = calculate_jaccard_similarity(entities_a, entities_b)
    pred_sim = calculate_predicate_similarity(triples_a, triples_b)

    # Combined score: weight entities more heavily
    combined = (entity_sim * 0.7) + (pred_sim * 0.3)

    # Classify
    if combined >= HIGH_SIMILARITY:
        classification = 'HIGH'
    elif combined >= LOW_SIMILARITY:
        classification = 'MEDIUM'
    else:
        classification = 'LOW'

    return {
        'entity_similarity': round(entity_sim, 3),
        'predicate_similarity': round(pred_sim, 3),
        'combined_score': round(combined, 3),
        'classification': classification,
        'shared_entities': list(entities_a & entities_b)[:5]
    }


def evaluate_page_pairs(kg_data: dict) -> dict:
    """Evaluate all page pairs for potential duplicates."""
    results = {
        'metadata': {
            'evaluated_at': datetime.now().isoformat(),
            'total_pages': len(kg_data.get('pages', {})),
            'thresholds': {
                'high_similarity': HIGH_SIMILARITY,
                'low_similarity': LOW_SIMILARITY
            }
        },
        'pairs': [],
        'summary': {
            'high_similarity': 0,
            'medium_similarity': 0,
            'low_similarity': 0,
            'potential_duplicates': []
        }
    }

    # Group triples by page
    triples_by_page = defaultdict(list)
    for triple in kg_data.get('all_triples', []):
        page_id = triple.get('source_page_id')
        if page_id:
            triples_by_page[page_id].append(triple)

    pages = list(kg_data.get('pages', {}).keys())

    if len(pages) < 2:
        print("Need at least 2 pages to compare.")
        return results

    # Compare all pairs
    total_pairs = len(pages) * (len(pages) - 1) // 2
    print(f"Comparing {total_pairs} page pairs...")

    for page_a, page_b in combinations(pages, 2):
        triples_a = triples_by_page.get(page_a, [])
        triples_b = triples_by_page.get(page_b, [])

        if not triples_a or not triples_b:
            continue

        similarity = calculate_kg_similarity(triples_a, triples_b)

        pair_result = {
            'page_a': page_a,
            'page_b': page_b,
            'title_a': kg_data['pages'].get(page_a, {}).get('title', '')[:50],
            'title_b': kg_data['pages'].get(page_b, {}).get('title', '')[:50],
            **similarity
        }

        results['pairs'].append(pair_result)

        # Update summary
        if similarity['classification'] == 'HIGH':
            results['summary']['high_similarity'] += 1
            results['summary']['potential_duplicates'].append({
                'pages': [page_a, page_b],
                'score': similarity['combined_score']
            })
        elif similarity['classification'] == 'MEDIUM':
            results['summary']['medium_similarity'] += 1
        else:
            results['summary']['low_similarity'] += 1

    # Sort pairs by similarity (highest first)
    results['pairs'].sort(key=lambda x: x['combined_score'], reverse=True)

    return results


def print_report(results: dict):
    """Print deduplication evaluation report."""
    print("\n" + "=" * 60)
    print("DEDUPLICATION EVALUATION REPORT")
    print("=" * 60)
    print(f"Evaluated: {results['metadata']['evaluated_at']}")
    print(f"Pages: {results['metadata']['total_pages']}")
    print(f"Pairs analysed: {len(results['pairs'])}")
    print()

    s = results['summary']
    print("SIMILARITY DISTRIBUTION")
    print("-" * 40)
    print(f"  HIGH (>80% - likely duplicates):  {s['high_similarity']}")
    print(f"  MEDIUM (50-80% - ambiguous):      {s['medium_similarity']}")
    print(f"  LOW (<50% - not duplicates):      {s['low_similarity']}")
    print()

    if s['potential_duplicates']:
        print("POTENTIAL DUPLICATES FOUND")
        print("-" * 40)
        for dup in s['potential_duplicates'][:10]:
            print(f"  {dup['pages'][0]} <-> {dup['pages'][1]} (score: {dup['score']:.2f})")
        print()

    if results['pairs']:
        print("TOP 5 MOST SIMILAR PAIRS")
        print("-" * 40)
        for pair in results['pairs'][:5]:
            print(f"  [{pair['combined_score']:.2f}] {pair['classification']}")
            print(f"    A: {pair['title_a']}")
            print(f"    B: {pair['title_b']}")
            if pair['shared_entities']:
                print(f"    Shared: {', '.join(pair['shared_entities'][:3])}")
            print()


def main():
    print("Loading knowledge graph output...")
    kg_data = load_kg_output()
    print(f"  Found {len(kg_data.get('pages', {}))} pages, {len(kg_data.get('all_triples', []))} triples")

    if len(kg_data.get('pages', {})) < 2:
        print("\nNeed at least 2 pages to evaluate deduplication. Run extraction first.")
        return

    results = evaluate_page_pairs(kg_data)

    # Save results
    with open(DEDUP_EVAL_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {DEDUP_EVAL_FILE}")

    print_report(results)


if __name__ == "__main__":
    main()
