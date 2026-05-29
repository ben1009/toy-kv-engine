#!/bin/bash
# Kimi Code hook — user-level, all projects
# Supports PreToolUse (inject context before Bash) and UserPromptSubmit (inject review skill on demand).

set -e

# Read full JSON from stdin (handles multi-line payloads safely)
JSON=$(cat)
EVENT=$(echo "$JSON" | jq -r '.hook_event_name // empty')
CWD=$(echo "$JSON" | jq -r '.cwd // empty')

# Safely walk up directory tree looking for a file; max 50 iterations to prevent infinite loops.
find_up() {
    local dir="$1"
    local target="$2"
    local max_iters=50
    local i=0
    while [[ "$dir" != "/" ]] && (( i < max_iters )); do
        if [[ -f "$dir/$target" ]]; then
            echo "$dir/$target"
            return 0
        fi
        local parent
        parent=$(dirname "$dir")
        if [[ "$parent" == "$dir" ]]; then
            break
        fi
        dir="$parent"
        ((i++))
    done
    return 1
}

case "$EVENT" in
    "PreToolUse")
        TOOL_NAME=$(echo "$JSON" | jq -r '.tool_name // empty')
        COMMAND=$(echo "$JSON" | jq -r '.tool_input.command // empty')

        if [[ "$TOOL_NAME" != "Bash" ]]; then
            exit 0
        fi

        # --- gh pr create: inject AGENTS.md + PR review skill ---
        if [[ "$COMMAND" == *"gh pr create"* ]]; then
            AGENTS_FOUND=$(find_up "$CWD" "AGENTS.md" || true)
            if [[ -n "$AGENTS_FOUND" ]]; then
                echo "[AGENTS.md context for PR creation]"
                head -n 80 "$AGENTS_FOUND"
                echo ""
            fi

            REVIEW_SKILL="${PR_REVIEW_SKILL:-$HOME/proj/agent-skills/pr-review/SKILL.md}"
            if [[ -f "$REVIEW_SKILL" ]]; then
                echo "[PR Review Skill Guidelines]"
                cat "$REVIEW_SKILL"
                echo ""
            fi
            exit 0
        fi

        # --- general Bash: auto-load AGENTS.md if present ---
        AGENTS_FOUND=$(find_up "$CWD" "AGENTS.md" || true)
        if [[ -n "$AGENTS_FOUND" ]]; then
            echo "[Project context from $AGENTS_FOUND]"
            head -n 50 "$AGENTS_FOUND"
            exit 0
        fi
        ;;

    "UserPromptSubmit")
        # Extract all text from the prompt array
        PROMPT=$(echo "$JSON" | jq -r '[.. | strings] | join(" ")')

        # Inject review skill when user asks to review a PR
        if [[ "$PROMPT" == *"review"* ]] && [[ "$PROMPT" == *"pr"* ]]; then
            REVIEW_SKILL="${PR_REVIEW_SKILL:-$HOME/proj/agent-skills/pr-review/SKILL.md}"
            if [[ -f "$REVIEW_SKILL" ]]; then
                echo "[PR Review Skill Guidelines]"
                cat "$REVIEW_SKILL"
            fi
        fi
        ;;
esac

exit 0
