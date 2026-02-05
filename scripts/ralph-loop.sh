#!/bin/bash
#
# Ralph Loop for NHS Scraper
# Runs the scraper repeatedly until task is complete (exit code 0)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
SCRAPER="${SCRIPT_DIR}/nhs_scraper.py"
LOG_FILE="${PROJECT_DIR}/data/scraper.log"

# Set PYTHONPATH for dependencies
export PYTHONPATH="${PROJECT_DIR}/venv/lib/python3.12/site-packages:${PYTHONPATH}"

# Config
MAX_ITERATIONS=200  # Safety limit
SLEEP_BETWEEN=10    # Seconds between iterations (be nice to Wayback Machine)

echo "========================================"
echo "NHS Scraper Ralph Loop"
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

    # Run the scraper
    $PYTHON "$SCRAPER" 2>&1 | tee -a "$LOG_FILE"
    exit_code=${PIPESTATUS[0]}

    if [ $exit_code -eq 0 ]; then
        echo ""
        echo "========================================"
        echo "TASK COMPLETE!"
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
echo "Task incomplete - check state file for progress"
exit 1
