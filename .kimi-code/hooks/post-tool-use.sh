#!/bin/bash
# Kimi Code PostToolUse hook — fire-and-forget notification/logging.
# Runs after Bash tools complete. Cannot inject model context,
# but can send desktop notifications, log events, etc.

set -e

read -r JSON
TOOL_NAME=$(echo "$JSON" | jq -r '.tool_name // empty')
COMMAND=$(echo "$JSON" | jq -r '.tool_input.command // empty')
TOOL_OUTPUT=$(echo "$JSON" | jq -r '.tool_output // empty')

# Only act for successful Bash commands
if [[ "$TOOL_NAME" != "Bash" ]]; then
    exit 0
fi

# --- gh pr create succeeded → notify / log ---
if [[ "$COMMAND" == *"gh pr create"* ]]; then
    # Extract PR URL from output if available
    PR_URL=$(echo "$TOOL_OUTPUT" | grep -oE 'https://github.com/[^ ]+/pull/[0-9]+' | head -n1)

    if [[ -n "$PR_URL" ]]; then
        echo "PR created: $PR_URL" >> ~/.kimi-code/hooks/pr-log.txt

        # Desktop notification (macOS)
        if command -v osascript &>/dev/null; then
            osascript -e "display notification \"PR created: $PR_URL\" with title \"Kimi Code\""
        fi

        # Desktop notification (Linux with notify-send)
        if command -v notify-send &>/dev/null; then
            notify-send "Kimi Code" "PR created: $PR_URL"
        fi
    fi
fi

exit 0
