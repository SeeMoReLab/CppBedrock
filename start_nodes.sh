#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY="$SCRIPT_DIR/build/CppBedrock"
CONFIG="$SCRIPT_DIR/config/config.entities.yaml"

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
        -e "  do script \"cd '$SCRIPT_DIR/build' && ./CppBedrock --node-id $id\"" \
        -e 'end tell'
done
