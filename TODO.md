# NHS Knowledge Graph — Remaining Work

## Current State (7 Feb 2026)
- **Extraction**: 498/502 pages processed, 3,331 triples, 4,272 entities
- **Visualisation**: Complete (nhs-kg-complete.html — 1,264 communities)
- **4 binary .docx files** skipped (raw ZIP content, not extractable)

## To Do

### 1. Re-run evaluation on full dataset ⬅️ NOW
The classification and deduplication evaluations were last run on 362 pages / 1,060 triples.
Need to re-run on the full 498 pages / 3,331 triples.
- `python scripts/evaluate_classification.py` — fuzzy-match entities against NHS taxonomy
- `python scripts/evaluate_deduplication.py` — Jaccard similarity across all page pairs

### 2. Review evaluation results
- Check COHERENT / AMBIGUOUS / CONFLICT routing ratios
- Review the CONFLICT cases (manual review needed)
- Validate duplicate detection against the 30 synthetic pairs (15 exact, 15 paraphrase)
- Compare metrics against the earlier 362-page run

### 3. Entity standardisation (optional)
Currently disabled in batch processing for speed. Running standardisation would:
- Merge duplicate entities ("NHS England" / "NHS" / "National Health Service England")
- Reduce the 4,272 nodes to a tighter, more connected graph
- Requires LLM calls — slow on CPU hardware

### 4. Relationship inference (optional)
Also disabled for speed. Would:
- Add inferred relationships (transitive, semantic)
- Increase graph connectivity
- Requires LLM calls

### 5. Handle paraphrase duplicates
- 15 page pairs marked as "paraphrase_pending" in scraper state
- Need LLM paraphrasing pass or manual creation
- Used for testing duplicate detection accuracy

### 6. Analyse and document findings
- What NHS topics are most represented?
- Which entity types dominate the graph?
- How connected is the knowledge graph? (density, diameter, avg path length)
- Community analysis — what do the 1,264 communities represent?
- Write up for portfolio / assignment submission

### 7. Clean up binary .docx pages
- 4 pages (0006, 0007, 0008, 0009) contain raw Office XML
- Could extract text using python-docx if the original files are recoverable
- Low priority — 4 out of 502 pages

## Done
- [x] Scrape 502 NHS pages from Wayback Machine
- [x] Run initial KG extraction (362 pages, Mistral 7B, temp 0.8)
- [x] Build retry pipeline with improved JSON repair
- [x] Retry all 136 failed pages (Mistral 7B, temp 0.2)
- [x] Generate interactive HTML visualisation
- [x] Run classification evaluation (on 362-page subset)
- [x] Run deduplication evaluation (on 362-page subset)
