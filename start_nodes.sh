#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/build/CppBedrock"
CONFIG="$SCRIPT_DIR/config/config.entities.yaml"

AGENT_FLAG=""
START_TIMESTAMP=""
NEXT_IS_TIME=0
for arg in "$@"; do
    if [[ "$arg" == "--agent" ]]; then
        AGENT_FLAG="--agent"
    elif [[ "$arg" == "--start-time" ]]; then
        NEXT_IS_TIME=1
    elif [[ "$NEXT_IS_TIME" -eq 1 ]]; then
        START_TIMESTAMP="$arg"
        NEXT_IS_TIME=0
    fi
done

[[ -z "$START_TIMESTAMP" ]] && START_TIMESTAMP=$(date +%s)
EXTRA_FLAGS="--start-time $START_TIMESTAMP"

if [[ ! -f "$BINARY" ]]; then
    echo "Binary not found at $BINARY — build first with: cmake --build build"
    exit 1
fi

# Extract entity IDs from config (lines of the form "  - id: N")
IDS=($(grep -E '^\s+-\s+id:' "$CONFIG" | awk '{print $NF}'))

if [[ ${#IDS[@]} -eq 0 ]]; then
    echo "No entities found in $CONFIG"
    exit 1
fi

echo "Starting ${#IDS[@]} nodes: ${IDS[*]}"

for id in "${IDS[@]}"; do
    osascript \
        -e 'tell application "Terminal"' \
        -e "  do script \"cd '$SCRIPT_DIR/build' && ./CppBedrock --node-id $id $AGENT_FLAG $EXTRA_FLAGS\"" \
        -e 'end tell'
done
