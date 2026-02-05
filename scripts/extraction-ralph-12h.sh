#!/bin/bash
#
# Ralph Loop for Knowledge Graph Extraction (12-hour run)
# Runs extraction repeatedly until complete or time limit reached
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
EXTRACTOR="${SCRIPT_DIR}/simple_extract.py"
LOG_FILE="${PROJECT_DIR}/data/extraction-12h.log"

# Set PYTHONPATH for dependencies
export PYTHONPATH="${PROJECT_DIR}/venv/lib/python3.12/site-packages:${PYTHONPATH}"

# Config
MAX_HOURS=12
MAX_SECONDS=$((MAX_HOURS * 3600))  # 43200 seconds
SLEEP_BETWEEN=5  # Seconds between iterations

START_TIME=$(date +%s)

echo "========================================"
echo "Knowledge Graph Extraction - 12hr Ralph Loop"
echo "========================================"
echo "Started: $(date)"
echo "Will run for up to $MAX_HOURS hours"
echo "Model: phi3:mini via Ollama"
echo "Log file: $LOG_FILE"
echo ""

mkdir -p "$(dirname "$LOG_FILE")"

# Log start
echo "======================================" >> "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "======================================" >> "$LOG_FILE"

iteration=0
while true; do
    iteration=$((iteration + 1))

    # Check time limit
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    REMAINING=$((MAX_SECONDS - ELAPSED))
    HOURS_ELAPSED=$(echo "scale=2; $ELAPSED / 3600" | bc)

    if [ $ELAPSED -ge $MAX_SECONDS ]; then
        echo ""
        echo "========================================"
        echo "TIME LIMIT REACHED (${MAX_HOURS}h)"
        echo "========================================"
        echo "Finished: $(date)"
        echo "Total iterations: $iteration"
        echo "Time limit reached" >> "$LOG_FILE"
        exit 1
    fi

    echo ""
    echo "=== Iteration $iteration | ${HOURS_ELAPSED}h elapsed | $((REMAINING / 60))m remaining ==="
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
        echo "Hours elapsed: $HOURS_ELAPSED"
        echo "COMPLETE at $(date)" >> "$LOG_FILE"
        exit 0
    fi

    sleep $SLEEP_BETWEEN
done
