#!/usr/bin/env python3
"""
Classification Evaluation Script

Evaluates KG extraction quality by fuzzy-matching extracted entities
against taxonomy terms. Calculates coherence scores and routes cases
for review per TESTING_PLAN.md methodology.

Scoring thresholds:
  > 0.7  : COHERENT - auto-accept
  0.4-0.7: AMBIGUOUS - send to Phi-3 judge
  < 0.4  : CONFLICT - flag for human review
"""

import csv
import json
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher
from datetime import datetime

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
TAXONOMY_FILE = DATA_DIR / "taxonomy.json"
KG_OUTPUT_FILE = DATA_DIR / "nhs-knowledge-graph.json"
EVAL_OUTPUT_FILE = DATA_DIR / "evaluation-results.json"
EVAL_CSV_FILE = DATA_DIR / "classification-scores.csv"

# Thresholds from TESTING_PLAN.md
COHERENT_THRESHOLD = 0.7
AMBIGUOUS_THRESHOLD = 0.4


def load_taxonomy() -> dict:
    """Load taxonomy and build lookup structures."""
    with open(TAXONOMY_FILE, 'r') as f:
        taxonomy = json.load(f)

    # Build flat list of all taxonomy terms for matching
    all_terms = []
    term_to_category = {}

    for category in taxonomy.get('categories', []):
        cat_name = category['name']
        for item in category.get('items', []):
            label = item['label'].lower()
            all_terms.append(label)
            term_to_category[label] = cat_name

            # Also add individual words for partial matching
            for word in label.split():
                if len(word) > 3:  # Skip short words
                    all_terms.append(word)
                    term_to_category[word] = cat_name

    return {
        'terms': list(set(all_terms)),
        'term_to_category': term_to_category,
        'categories': [c['name'] for c in taxonomy.get('categories', [])]
    }


def load_kg_output() -> dict:
    """Load extracted knowledge graph."""
    if not KG_OUTPUT_FILE.exists():
        return {'pages': {}, 'all_triples': []}

    with open(KG_OUTPUT_FILE, 'r') as f:
        return json.load(f)


def fuzzy_match_score(text: str, terms: list[str], threshold: float = 0.6) -> tuple[float, list[str]]:
    """
    Calculate fuzzy match score between text and taxonomy terms.
    Returns (best_score, matched_terms).
    """
    text_lower = text.lower()
    matches = []

    for term in terms:
        # Direct substring match
        if term in text_lower:
            matches.append((1.0, term))
            continue

        # Fuzzy match using SequenceMatcher
        ratio = SequenceMatcher(None, text_lower, term).ratio()
        if ratio >= threshold:
            matches.append((ratio, term))

    if not matches:
        return 0.0, []

    # Return best matches
    matches.sort(reverse=True)
    best_score = matches[0][0]
    matched_terms = [m[1] for m in matches[:5]]  # Top 5 matches

    return best_score, matched_terms


def extract_entities_from_triples(triples: list[dict]) -> list[str]:
    """Extract unique entities (subjects and objects) from triples."""
    entities = set()

    for triple in triples:
        for key in ('subject', 'object'):
            val = triple.get(key)
            if isinstance(val, str):
                entities.add(val)
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, str):
                        entities.add(item)

    return list(entities)


def calculate_page_coherence(page_triples: list[dict], taxonomy: dict) -> dict:
    """
    Calculate coherence score for a page based on entity-taxonomy alignment.

    Returns dict with:
      - coherence_score: 0.0-1.0
      - matched_entities: entities that matched taxonomy
      - matched_categories: taxonomy categories found
      - routing: COHERENT / AMBIGUOUS / CONFLICT
    """
    entities = extract_entities_from_triples(page_triples)

    if not entities:
        return {
            'coherence_score': 0.0,
            'matched_entities': [],
            'matched_categories': [],
            'routing': 'CONFLICT',
            'reason': 'No entities extracted'
        }

    # Match each entity against taxonomy
    entity_scores = []
    all_matched_terms = []
    matched_categories = set()

    for entity in entities:
        score, matched_terms = fuzzy_match_score(entity, taxonomy['terms'])
        entity_scores.append(score)
        all_matched_terms.extend(matched_terms)

        # Track which categories were matched
        for term in matched_terms:
            if term in taxonomy['term_to_category']:
                matched_categories.add(taxonomy['term_to_category'][term])

    # Coherence score = average of best entity matches
    # Weight towards having at least some good matches
    if entity_scores:
        avg_score = sum(entity_scores) / len(entity_scores)
        max_score = max(entity_scores)
        # Blend average and max to reward pages with at least some strong matches
        coherence_score = (avg_score * 0.6) + (max_score * 0.4)
    else:
        coherence_score = 0.0

    # Determine routing
    if coherence_score >= COHERENT_THRESHOLD:
        routing = 'COHERENT'
    elif coherence_score >= AMBIGUOUS_THRESHOLD:
        routing = 'AMBIGUOUS'
    else:
        routing = 'CONFLICT'

    return {
        'coherence_score': round(coherence_score, 3),
        'entity_count': len(entities),
        'matched_entities': list(set(all_matched_terms))[:10],
        'matched_categories': list(matched_categories),
        'routing': routing
    }


def calculate_kg_similarity(triples_a: list[dict], triples_b: list[dict]) -> float:
    """
    Calculate knowledge graph similarity between two pages.
    Based on entity overlap (Jaccard similarity).
    """
    entities_a = set(e.lower() for e in extract_entities_from_triples(triples_a))
    entities_b = set(e.lower() for e in extract_entities_from_triples(triples_b))

    if not entities_a or not entities_b:
        return 0.0

    intersection = entities_a & entities_b
    union = entities_a | entities_b

    return len(intersection) / len(union) if union else 0.0


def evaluate_all_pages(kg_data: dict, taxonomy: dict) -> dict:
    """Evaluate all pages and generate results."""
    results = {
        'metadata': {
            'evaluated_at': datetime.now().isoformat(),
            'total_pages': len(kg_data.get('pages', {})),
            'total_triples': len(kg_data.get('all_triples', [])),
            'thresholds': {
                'coherent': COHERENT_THRESHOLD,
                'ambiguous': AMBIGUOUS_THRESHOLD
            }
        },
        'pages': {},
        'summary': {
            'coherent': 0,
            'ambiguous': 0,
            'conflict': 0
        },
        'category_coverage': defaultdict(int)
    }

    # Group triples by page
    triples_by_page = defaultdict(list)
    for triple in kg_data.get('all_triples', []):
        page_id = triple.get('source_page_id')
        if page_id:
            triples_by_page[page_id].append(triple)

    # Evaluate each page
    for page_id, page_info in kg_data.get('pages', {}).items():
        page_triples = triples_by_page.get(page_id, [])
        evaluation = calculate_page_coherence(page_triples, taxonomy)

        results['pages'][page_id] = {
            'title': page_info.get('title', ''),
            'triple_count': len(page_triples),
            **evaluation
        }

        # Update summary
        results['summary'][evaluation['routing'].lower()] += 1

        # Track category coverage
        for cat in evaluation.get('matched_categories', []):
            results['category_coverage'][cat] += 1

    # Convert defaultdict to regular dict for JSON serialization
    results['category_coverage'] = dict(results['category_coverage'])

    # Calculate percentages
    total = results['metadata']['total_pages']
    if total > 0:
        results['summary']['coherent_pct'] = round(100 * results['summary']['coherent'] / total, 1)
        results['summary']['ambiguous_pct'] = round(100 * results['summary']['ambiguous'] / total, 1)
        results['summary']['conflict_pct'] = round(100 * results['summary']['conflict'] / total, 1)

    return results


def print_report(results: dict):
    """Print evaluation report to console."""
    print("\n" + "=" * 60)
    print("CLASSIFICATION EVALUATION REPORT")
    print("=" * 60)
    print(f"Evaluated: {results['metadata']['evaluated_at']}")
    print(f"Pages: {results['metadata']['total_pages']}")
    print(f"Triples: {results['metadata']['total_triples']}")
    print()

    print("ROUTING SUMMARY")
    print("-" * 40)
    s = results['summary']
    print(f"  COHERENT (auto-accept):  {s.get('coherent', 0):>4} ({s.get('coherent_pct', 0):.1f}%)")
    print(f"  AMBIGUOUS (Phi-3 judge): {s.get('ambiguous', 0):>4} ({s.get('ambiguous_pct', 0):.1f}%)")
    print(f"  CONFLICT (human review): {s.get('conflict', 0):>4} ({s.get('conflict_pct', 0):.1f}%)")
    print()

    # Target from TESTING_PLAN.md
    print("vs. EXPECTED (from testing plan):")
    print("  COHERENT:  70-80%")
    print("  AMBIGUOUS: 15-20%")
    print("  CONFLICT:  5-10%")
    print()

    if results.get('category_coverage'):
        print("TAXONOMY CATEGORIES MATCHED")
        print("-" * 40)
        for cat, count in sorted(results['category_coverage'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {cat}: {count}")
    print()

    # Show sample pages by routing
    print("SAMPLE PAGES")
    print("-" * 40)

    for routing in ['CONFLICT', 'AMBIGUOUS', 'COHERENT']:
        pages = [(pid, p) for pid, p in results['pages'].items() if p['routing'] == routing]
        if pages:
            print(f"\n{routing} (showing up to 3):")
            for pid, p in pages[:3]:
                title = p['title'][:50] + "..." if len(p['title']) > 50 else p['title']
                print(f"  [{p['coherence_score']:.2f}] {title}")
                if p.get('matched_categories'):
                    print(f"         Categories: {', '.join(p['matched_categories'][:3])}")


def save_csv(results: dict):
    """Export all page scores and routing decisions to CSV."""
    with open(EVAL_CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'page_id', 'title', 'triple_count', 'entity_count',
            'coherence_score', 'routing', 'matched_categories'
        ])

        for page_id, page in sorted(results['pages'].items()):
            writer.writerow([
                page_id,
                page.get('title', ''),
                page.get('triple_count', 0),
                page.get('entity_count', 0),
                page.get('coherence_score', 0.0),
                page.get('routing', ''),
                '; '.join(page.get('matched_categories', []))
            ])

    print(f"CSV saved to: {EVAL_CSV_FILE}")


def main():
    print("Loading taxonomy...")
    taxonomy = load_taxonomy()
    print(f"  Loaded {len(taxonomy['terms'])} terms across {len(taxonomy['categories'])} categories")

    print("Loading knowledge graph output...")
    kg_data = load_kg_output()
    print(f"  Found {len(kg_data.get('pages', {}))} pages, {len(kg_data.get('all_triples', []))} triples")

    if not kg_data.get('pages'):
        print("\nNo pages to evaluate yet. Run extraction first.")
        return

    print("Evaluating pages...")
    results = evaluate_all_pages(kg_data, taxonomy)

    # Save results
    with open(EVAL_OUTPUT_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {EVAL_OUTPUT_FILE}")

    save_csv(results)

    # Print report
    print_report(results)


if __name__ == "__main__":
    main()
