#!/bin/bash
# Kimi Code hook — user-level, all projects
# Supports PreToolUse (inject context before Bash) and UserPromptSubmit (inject review skill on demand).

set -e

read -r JSON
EVENT=$(echo "$JSON" | jq -r '.hook_event_name // empty')
CWD=$(echo "$JSON" | jq -r '.cwd // empty')

case "$EVENT" in
    "PreToolUse")
        TOOL_NAME=$(echo "$JSON" | jq -r '.tool_name // empty')
        COMMAND=$(echo "$JSON" | jq -r '.tool_input.command // empty')

        if [[ "$TOOL_NAME" != "Bash" ]]; then
            exit 0
        fi

        # --- gh pr create: inject AGENTS.md + PR review skill ---
        if [[ "$COMMAND" == *"gh pr create"* ]]; then
            DIR="$CWD"
            while [[ "$DIR" != "/" ]]; do
                if [[ -f "$DIR/AGENTS.md" ]]; then
                    echo "[AGENTS.md context for PR creation]"
                    head -n 80 "$DIR/AGENTS.md"
                    echo ""
                    break
                fi
                DIR=$(dirname "$DIR")
            done

            REVIEW_SKILL="/home/liu/proj/agent-skills/pr-review/SKILL.md"
            if [[ -f "$REVIEW_SKILL" ]]; then
                echo "[PR Review Skill Guidelines]"
                cat "$REVIEW_SKILL"
                echo ""
            fi
            exit 0
        fi

        # --- general Bash: auto-load AGENTS.md if present ---
        DIR="$CWD"
        while [[ "$DIR" != "/" ]]; do
            if [[ -f "$DIR/AGENTS.md" ]]; then
                echo "[Project context from $DIR/AGENTS.md]"
                head -n 50 "$DIR/AGENTS.md"
                exit 0
            fi
            DIR=$(dirname "$DIR")
        done
        ;;

    "UserPromptSubmit")
        # Extract all text from the prompt array
        PROMPT=$(echo "$JSON" | jq -r '[.. | strings] | join(" ")')

        # Inject review skill when user asks to review a PR
        if [[ "$PROMPT" == *"review"* ]] && [[ "$PROMPT" == *"pr"* ]]; then
            REVIEW_SKILL="/home/liu/proj/agent-skills/pr-review/SKILL.md"
            if [[ -f "$REVIEW_SKILL" ]]; then
                echo "[PR Review Skill Guidelines]"
                cat "$REVIEW_SKILL"
            fi
        fi
        ;;
esac

exit 0
