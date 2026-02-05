#!/bin/bash
#
# Ralph Loop for Simple Knowledge Graph Extraction
# Designed to run overnight - processes slowly but steadily
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
EXTRACTOR="${SCRIPT_DIR}/simple_extract.py"
LOG_FILE="${PROJECT_DIR}/data/extraction.log"

export PYTHONPATH="${PROJECT_DIR}:${PYTHONPATH}"

# Config - 502 pages / 3 per iteration = ~167 iterations needed
MAX_ITERATIONS=300  # Safety limit with buffer
SLEEP_BETWEEN=15    # Seconds between iterations (let LM Studio recover)

echo "========================================"
echo "Knowledge Graph Extraction Ralph Loop"
echo "========================================"
echo "Started: $(date)"
echo "Log file: $LOG_FILE"
echo "Processing 3 pages per iteration"
echo "Estimated time: ~8-12 hours for 502 pages"
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
exit 1
