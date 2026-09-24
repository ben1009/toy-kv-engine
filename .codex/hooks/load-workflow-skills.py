#!/usr/bin/env python3
"""Load the workspace workflow instructions for Git and pull request commands."""

import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path("/home/liu/proj")
REPOSITORY_ROOT = PROJECT_ROOT / "toy-kv-engine"
AGENTS = PROJECT_ROOT / "AGENTS.md"
SKILLS = PROJECT_ROOT / "agent-skills"
RUST_SKILL = SKILLS / "rust-skills" / "SKILL.md"


def read(path: Path) -> str:
    try:
        return path.read_text()
    except OSError:
        return ""


def main() -> None:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        return

    cwd = Path(event.get("cwd", "")).resolve()
    if cwd != REPOSITORY_ROOT and REPOSITORY_ROOT not in cwd.parents:
        return

    tool_name = event.get("tool_name", "")
    command = event.get("tool_input", {}).get("command", "")
    if not isinstance(command, str):
        return

    context = [read(AGENTS)]
    if tool_name in {"apply_patch", "Edit", "Write"}:
        context.append(read(RUST_SKILL))
    if "git " in command or command.startswith("git"):
        context.append(read(SKILLS / "git-workflow" / "SKILL.md"))

    is_pr_create = "gh pr create" in command
    is_pr_review = any(
        operation in command
        for operation in (
            "gh pr view",
            "gh pr checks",
            "gh pr comment",
            "gh pr edit",
            "gh pr merge",
        )
    )
    if is_pr_create or "git push" in command:
        context.append(read(SKILLS / "pr-create" / "SKILL.md"))
    if is_pr_create or is_pr_review:
        context.append(read(SKILLS / "git-workflow" / "SKILL.md"))
    if is_pr_review:
        context.append(read(SKILLS / "pr-review" / "SKILL.md"))

    context = [part for part in context if part]
    if len(context) == 1:
        return

    output = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "additionalContext": "\n\n".join(context),
        }
    }
    print(json.dumps(output))


if __name__ == "__main__":
    main()
