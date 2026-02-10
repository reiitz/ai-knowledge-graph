#!/bin/bash
# Phi-3 Judge Loop — runs the judge script for up to 12 hours
# Usage: nohup bash scripts/phi3-judge-loop.sh > data/phi3-judge.log 2>&1 &

cd "$(dirname "$0")/.."

MAX_SECONDS=$((12 * 3600))
START=$(date +%s)

echo "=== Phi-3 Judge Loop ==="
echo "Started: $(date)"
echo "Max runtime: ${MAX_SECONDS}s (12 hours)"
echo ""

# Use venv python directly (no activate script in this venv)
PYTHON="venv/bin/python3"
if [ ! -f "$PYTHON" ]; then
    PYTHON="python3"
fi

export PYTHONPATH="."

# Single run — the script is resumable so we just run it once
# If it errors out, the loop retries
while true; do
    ELAPSED=$(( $(date +%s) - START ))
    if [ $ELAPSED -ge $MAX_SECONDS ]; then
        echo "Time limit reached after ${ELAPSED}s"
        break
    fi

    echo "--- Run at $(date) (${ELAPSED}s elapsed) ---"
    $PYTHON scripts/phi3_judge.py

    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Script completed successfully."
        break
    fi

    echo "Script exited with code $EXIT_CODE, retrying in 30s..."
    sleep 30
done

echo ""
echo "=== Loop finished at $(date) ==="
