#!/bin/bash
# Retry loop — runs retry_failed.py for up to 12 hours
# Usage: nohup bash scripts/retry-loop-12h.sh > data/retry-12h.log 2>&1 &

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

DURATION_SECS=$((12 * 60 * 60))  # 12 hours
START_TIME=$(date +%s)
ITERATION=0

echo "=== RETRY LOOP STARTED ==="
echo "Start: $(date)"
echo "Duration: 12 hours"
echo "=========================="

while true; do
    ELAPSED=$(( $(date +%s) - START_TIME ))
    if [ $ELAPSED -ge $DURATION_SECS ]; then
        echo ""
        echo "=== TIME LIMIT REACHED (12h) ==="
        echo "End: $(date)"
        echo "Iterations: $ITERATION"
        break
    fi

    ITERATION=$((ITERATION + 1))
    REMAINING=$(( (DURATION_SECS - ELAPSED) / 60 ))
    echo ""
    echo "--- Iteration $ITERATION | $(date) | ${REMAINING}m remaining ---"

    python3 scripts/retry_failed.py
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        echo "=== ALL RETRIES COMPLETE ==="
        echo "End: $(date)"
        echo "Iterations: $ITERATION"
        break
    fi

    # Brief pause between iterations
    sleep 2
done
