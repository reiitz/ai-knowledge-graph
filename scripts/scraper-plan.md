# NHS 500-Page Scraper Plan

## Goal
Collect 500 pages from NHS sites via Wayback Machine for knowledge graph testing.

## Target
- **Total pages needed**: 500
- **Duplicate pairs needed**: 30 (15 exact + 15 paraphrased)
- **Minimum content length**: 250 words per page

## Sources (via Wayback Machine)
1. england.nhs.uk (~170 pages)
2. digital.nhs.uk (~170 pages)
3. hee.nhs.uk (~170 pages)

## State File
Check `/home/admo2/ai-knowledge-graph/data/scraper-state.json` for current progress.

## Output File
Append pages to `/home/admo2/ai-knowledge-graph/data/nhs-500-sample.json`

## Each Iteration Should:
1. Read state file to see current progress
2. If URL discovery not done: query CDX API for more URLs
3. If pages < 500: fetch and extract content from next batch of URLs
4. If pages >= 500 but duplicates < 30: create duplicate pairs
5. Update state file with progress
6. Exit with code 0 if complete, code 1 if more work needed

## Completion Criteria
- 500+ pages with 250+ words each
- 30 duplicate pairs (15 exact, 15 paraphrased)
- All data saved to output JSON
- README documentation created

## Exit Codes
- 0 = Task complete
- 1 = More work needed (loop should continue)
