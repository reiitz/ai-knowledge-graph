#!/bin/bash
#
# Ralph Loop for Knowledge Graph Extraction
# Runs the extractor repeatedly until all pages are processed
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
EXTRACTOR="${SCRIPT_DIR}/batch_extract.py"
LOG_FILE="${PROJECT_DIR}/data/extraction.log"

# Set PYTHONPATH for dependencies
export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"

# Config
MAX_ITERATIONS=200  # Safety limit (502 pages / 5 per iteration = ~100 iterations needed)
SLEEP_BETWEEN=5     # Seconds between iterations

echo "========================================"
echo "Knowledge Graph Extraction Ralph Loop"
echo "========================================"
echo "Started: $(date)"
echo "Log file: $LOG_FILE"
echo ""

mkdir -p "$(dirname "$LOG_FILE")"

iteration=0
while [ $iteration -lt $MAX_ITERATIONS ]; do
    iteration=$((iteration + 1))

    echo ""
    echo "=== Iteration $iteration / $MAX_ITERATIONS ==="
    echo "$(date)" | tee -a "$LOG_FILE"

    # Run the extractor
    $PYTHON "$EXTRACTOR" 2>&1 | tee -a "$LOG_FILE"
    exit_code=${PIPESTATUS[0]}

    if [ $exit_code -eq 0 ]; then
        echo ""
        echo "========================================"
        echo "EXTRACTION COMPLETE!"
        echo "========================================"
        echo "Finished: $(date)"
        echo "Total iterations: $iteration"
        exit 0
    fi

    echo "Sleeping ${SLEEP_BETWEEN}s before next iteration..."
    sleep $SLEEP_BETWEEN
done

echo ""
echo "========================================"
echo "MAX ITERATIONS REACHED ($MAX_ITERATIONS)"
echo "========================================"
echo "Extraction incomplete - check state file for progress"
exit 1
