#!/usr/bin/env python3
"""Read the checked-in GCP test-environment README before cloud work."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import stat
import sys
import tempfile
import time
from pathlib import Path


README_PATH = Path("infra/gcp/test-environment/README.md")
MAX_README_BYTES = 128 * 1024
STATE_TTL_SECONDS = 30 * 60
GCP_PROMPT_PATTERN = re.compile(
    r"\bgcp\b|\bgcloud\b|google\s+cloud|google\s+compute|"
    r"cloud\s+(?:vm|benchmark|test)|test-environment",
    re.IGNORECASE,
)
CLOUD_COMMAND_PATTERN = re.compile(
    r"(?:gcp-free-trial-wal-bench(?:-remote)?|gcp_wal_profile_runner|preflight)\.(?:py|sh)$",
    re.IGNORECASE,
)
JAVASCRIPT_COMMAND_FIELD_PATTERN = re.compile(
    r"(?<![\w$])(?:cmd|command|shell_command|script_command|"
    r"\"(?:cmd|command|shell_command|script_command)\"|"
    r"'(?:cmd|command|shell_command|script_command)')\s*:\s*"
)
JAVASCRIPT_STRING_ASSIGNMENT_PATTERN = re.compile(
    r"\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*"
)
JAVASCRIPT_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_$][\w$]*")
JAVASCRIPT_CLOUD_TOOL_PATTERN = re.compile(
    r"\btools\.mcp__\w*(?:gcp|gcloud|google|cloud)\w*\b",
    re.IGNORECASE,
)
CLOUD_EXECUTABLES = {"bq", "gcloud", "gsutil", "terraform", "ssh", "scp", "sftp", "rsync"}
CLOUD_PYTHON_MODULES = {"googlecloudsdk.gcloud_main"}
SHELL_WRAPPERS = {"command", "exec", "env", "nice", "nohup", "stdbuf", "sudo", "time", "timeout"}
SHELL_INTERPRETERS = {"bash", "sh", "zsh", "dash"}
MAX_COMMAND_NESTING = 16
WRAPPER_OPTIONS_WITH_ARGUMENTS = {
    "env": {"-u", "--unset", "-C", "--chdir"},
    "nice": {"-n", "--adjustment"},
    "sudo": {"-u", "-g", "-h", "-p", "-C", "--user", "--group", "--host", "--chdir"},
    "stdbuf": {"-i", "-o", "-e", "--input", "--output", "--error"},
    "time": {"-f", "--format", "-o", "--output"},
    "timeout": {"-s", "--signal", "-k", "--kill-after"},
}
UV_RUN_OPTIONS_WITH_ARGUMENTS = {
    "--allow-insecure-host",
    "--cache-dir",
    "--color",
    "--config-file",
    "--config-setting",
    "--config-settings",
    "--config-settings-package",
    "--default-index",
    "--directory",
    "--env-file",
    "--exclude-newer",
    "--exclude-newer-package",
    "--extra",
    "--extra-index-url",
    "--find-links",
    "--fork-strategy",
    "--from",
    "--group",
    "--index",
    "--index-strategy",
    "--index-url",
    "--keyring-provider",
    "--link-mode",
    "--no-binary-package",
    "--no-build-isolation-package",
    "--no-build-package",
    "--no-editable-package",
    "--no-extra",
    "--no-group",
    "--no-sources-package",
    "--only-group",
    "--package",
    "--prerelease",
    "--prerelease-package",
    "--project",
    "--python",
    "--python-platform",
    "--refresh-package",
    "--reinstall-package",
    "--resolution",
    "--upgrade-group",
    "--upgrade-package",
    "--with",
    "--with-editable",
    "--with-requirements",
    "--trusted-host",
    "-C",
    "-f",
    "-i",
    "-p",
    "-w",
    "-P",
}


def read_event() -> dict[str, object]:
    try:
        value = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        return {}

    return value if isinstance(value, dict) else {}


def read_readme() -> tuple[str, str]:
    readme_path = Path(__file__).resolve().parents[2] / README_PATH
    data = readme_path.read_bytes()
    if len(data) > MAX_README_BYTES:
        raise ValueError(f"README exceeded {MAX_README_BYTES} bytes")

    text = data.decode("utf-8")
    if "Reusable GCP test environment" not in text:
        raise ValueError("checked-in file did not look like the expected README")

    return text, hashlib.sha256(data).hexdigest()


def session_state_path(event: dict[str, object]) -> Path:
    session_id = str(event.get("session_id") or "")
    if not session_id:
        session_id = str(event.get("transcript_path") or event.get("cwd") or "unknown")

    session_hash = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
    state_dir = Path(tempfile.gettempdir()) / f"codex-gcp-readme-hook-{os.getuid()}"
    if state_dir.is_symlink():
        raise OSError("GCP README hook state directory must not be a symlink")
    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = state_dir.stat()
    if metadata.st_uid != os.getuid():
        raise OSError("GCP README hook state directory has a different owner")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        state_dir.chmod(0o700)

    return state_dir / f"{session_hash}.json"


def save_state(path: Path, digest: str) -> None:
    payload = json.dumps(
        {"sha256": digest, "readme_path": README_PATH.as_posix(), "read_at": time.time()}
    )
    fd, temp_path = tempfile.mkstemp(prefix=f"{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as state_file:
            state_file.write(payload)
        os.replace(temp_path, path)
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def state_is_fresh(path: Path, current_digest: str) -> bool:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        read_at = float(state["read_at"])
        digest = state["sha256"]
        readme_path = state["readme_path"]
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False

    return (
        isinstance(digest, str)
        and digest == current_digest
        and readme_path == README_PATH.as_posix()
        and 0 <= time.time() - read_at <= STATE_TTL_SECONDS
    )


def prompt_is_gcp_related(event: dict[str, object]) -> bool:
    prompt = event.get("prompt")
    return isinstance(prompt, str) and GCP_PROMPT_PATTERN.search(prompt) is not None


def shell_command_segments(command: str) -> list[list[str]]:
    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()")
    lexer.whitespace_split = True
    lexer.commenters = "#"
    segments: list[list[str]] = []
    current: list[str] = []

    for token in lexer:
        if token and all(character in ";&|()" for character in token):
            if current:
                segments.append(current)
                current = []
        else:
            current.append(token)

    if current:
        segments.append(current)

    return segments


def _command_substitution_end(command: str, start: int, nesting_depth: int = 0) -> int | None:
    if nesting_depth > MAX_COMMAND_NESTING:
        raise ValueError("shell command substitution nesting limit exceeded")

    paren_depth = 1
    quote: str | None = None
    index = start + 2

    while index < len(command):
        character = command[index]
        if quote == "'":
            if character == "'":
                quote = None
            index += 1
            continue
        if character == "\\":
            index += 2
            continue
        if quote == '"':
            if character == '"':
                quote = None
                index += 1
                continue
            if command.startswith("$(", index):
                nested_end = _command_substitution_end(command, index, nesting_depth + 1)
                if nested_end is None:
                    raise ValueError("unterminated shell command substitution")
                index = nested_end + 1
                continue
            if character == "`":
                nested_end = _backtick_substitution_end(command, index)
                if nested_end is None:
                    raise ValueError("unterminated shell backtick substitution")
                index = nested_end + 1
                continue
            index += 1
            continue

        if character == "'":
            quote = "'"
        elif character == '"':
            quote = '"'
        elif command.startswith("$(", index):
            nested_end = _command_substitution_end(command, index, nesting_depth + 1)
            if nested_end is None:
                raise ValueError("unterminated shell command substitution")
            index = nested_end + 1
            continue
        elif character == "`":
            nested_end = _backtick_substitution_end(command, index)
            if nested_end is None:
                raise ValueError("unterminated shell backtick substitution")
            index = nested_end + 1
            continue
        elif character == "(":
            paren_depth += 1
        elif character == ")":
            paren_depth -= 1
            if paren_depth == 0:
                return index
        index += 1

    return None


def _backtick_substitution_end(command: str, start: int) -> int | None:
    index = start + 1
    while index < len(command):
        if command[index] == "\\":
            index += 2
        elif command[index] == "`":
            return index
        else:
            index += 1

    return None


def shell_command_substitutions(command: str) -> list[str]:
    substitutions: list[str] = []
    quote: str | None = None
    index = 0

    while index < len(command):
        character = command[index]
        if quote == "'":
            if character == "'":
                quote = None
            index += 1
            continue
        if character == "\\":
            index += 2
            continue
        if quote == '"':
            if character == '"':
                quote = None
                index += 1
                continue
            if command.startswith("$(", index):
                end = _command_substitution_end(command, index)
                if end is not None:
                    substitutions.append(command[index + 2 : end])
                    index = end + 1
                    continue
                raise ValueError("unterminated shell command substitution")
            elif character == "`":
                end = _backtick_substitution_end(command, index)
                if end is not None:
                    substitutions.append(command[index + 1 : end])
                    index = end + 1
                    continue
                raise ValueError("unterminated shell backtick substitution")
            index += 1
            continue

        if character == "'":
            quote = "'"
            index += 1
        elif character == '"':
            quote = '"'
            index += 1
        elif command.startswith("$(", index):
            end = _command_substitution_end(command, index)
            if end is not None:
                substitutions.append(command[index + 2 : end])
                index = end + 1
            else:
                raise ValueError("unterminated shell command substitution")
        elif character == "`":
            end = _backtick_substitution_end(command, index)
            if end is not None:
                substitutions.append(command[index + 1 : end])
                index = end + 1
            else:
                raise ValueError("unterminated shell backtick substitution")
        else:
            index += 1

    return substitutions


def segment_is_cloud_operation(tokens: list[str], depth: int = 0) -> bool:
    if not tokens:
        return False
    if depth > MAX_COMMAND_NESTING:
        raise ValueError("shell command nesting limit exceeded")

    index = 0
    while index < len(tokens):
        executable = Path(tokens[index]).name.lower()
        if re.fullmatch(r"[a-z_][a-z0-9_]*=.*", tokens[index], re.IGNORECASE):
            index += 1
            continue
        if executable in SHELL_WRAPPERS:
            if (
                executable == "command"
                and index + 1 < len(tokens)
                and tokens[index + 1] in {"-v", "-V"}
            ):
                return False
            index += 1
            while index < len(tokens) and tokens[index].startswith("-"):
                option = tokens[index]
                index += 1
                split_string: str | None = None
                if executable == "env":
                    if option in {"-S", "--split-string"} and index < len(tokens):
                        split_string = tokens[index]
                        index += 1
                    elif option.startswith("--split-string="):
                        split_string = option.partition("=")[2]
                    elif option.startswith("-S") and len(option) > 2:
                        split_string = option[2:]
                if split_string is not None:
                    try:
                        split_tokens = shlex.split(split_string, posix=True)
                    except ValueError:
                        split_tokens = []
                    if segment_is_cloud_operation(["env", *split_tokens], depth + 1):
                        return True
                if option in WRAPPER_OPTIONS_WITH_ARGUMENTS.get(executable, set()):
                    index += 1
            if executable == "timeout" and index < len(tokens):
                index += 1
            continue
        break

    if index >= len(tokens):
        return False

    executable = Path(tokens[index]).name.lower()
    arguments = tokens[index + 1 :]
    if executable == "eval":
        return command_is_cloud_operation(" ".join(arguments), depth + 1)
    if executable == "uv":
        return uv_run_is_cloud_operation(arguments, depth + 1) or any(
            CLOUD_COMMAND_PATTERN.search(Path(argument).name) for argument in arguments
        )
    if executable in CLOUD_EXECUTABLES:
        return True
    if CLOUD_COMMAND_PATTERN.search(executable):
        return True

    if executable in SHELL_INTERPRETERS:
        for arg_index, argument in enumerate(arguments[:-1]):
            if argument.startswith("-") and "c" in argument[1:].lower():
                return command_is_cloud_operation(arguments[arg_index + 1], depth + 1)

    if executable.startswith("python") and python_invokes_cloud_module(arguments):
        return True

    if executable.startswith("python") or executable in SHELL_INTERPRETERS:
        return any(CLOUD_COMMAND_PATTERN.search(Path(argument).name) for argument in arguments)

    return False


def uv_run_is_cloud_operation(arguments: list[str], depth: int) -> bool:
    if not arguments:
        return False
    if depth > MAX_COMMAND_NESTING:
        raise ValueError("uv command nesting limit exceeded")

    index = skip_uv_options(arguments, 0)
    if index >= len(arguments):
        return False
    if arguments[index] == "run":
        index += 1
    elif arguments[index : index + 2] == ["tool", "run"]:
        index += 2
    else:
        return False

    index = skip_uv_options(arguments, index)
    if index < len(arguments) and arguments[index] in {"-m", "--module"}:
        return index + 1 < len(arguments) and arguments[index + 1] in CLOUD_PYTHON_MODULES

    return index < len(arguments) and segment_is_cloud_operation(arguments[index:], depth)


def python_invokes_cloud_module(arguments: list[str]) -> bool:
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in {"-m", "--module"}:
            return index + 1 < len(arguments) and arguments[index + 1] in CLOUD_PYTHON_MODULES
        if argument in {"-c", "--", "-"}:
            return False
        if argument in {"-W", "-X"}:
            index += 2
            continue
        if argument.startswith("-"):
            index += 1
            continue

        return False

    return False


def skip_uv_options(arguments: list[str], index: int) -> int:
    while index < len(arguments) and arguments[index].startswith("-"):
        option = arguments[index]
        if option in {"-m", "--module"}:
            break
        index += 1
        option_name = option.partition("=")[0]
        if option_name in UV_RUN_OPTIONS_WITH_ARGUMENTS and "=" not in option:
            index += 1

    if index < len(arguments) and arguments[index] == "--":
        index += 1

    return index


def command_is_cloud_operation(command: object, depth: int = 0) -> bool:
    if not isinstance(command, str):
        return False
    if depth > MAX_COMMAND_NESTING:
        raise ValueError("shell command nesting limit exceeded")

    for substitution in shell_command_substitutions(command):
        if command_is_cloud_operation(substitution, depth + 1):
            return True

    return any(
        segment_is_cloud_operation(segment, depth)
        for segment in shell_command_segments(command)
    )


def javascript_string_value(source: str, start: int) -> tuple[str, int] | None:
    quote = source[start]
    if quote not in {"'", '"', "`"}:
        return None

    escaped_chars = {"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f", "v": "\v", "0": "\0"}
    value: list[str] = []
    index = start + 1

    while index < len(source):
        character = source[index]
        if character == quote:
            return "".join(value), index + 1
        if character in "\r\n" and quote != "`":
            return None
        if character != "\\":
            value.append(character)
            index += 1
            continue

        index += 1
        if index >= len(source):
            return None
        escaped = source[index]
        if escaped == "\n":
            index += 1
            continue
        if escaped == "\r":
            index += 1
            if index < len(source) and source[index] == "\n":
                index += 1
            continue
        if escaped == "x":
            digits = source[index + 1 : index + 3]
            if len(digits) != 2:
                return None
            try:
                value.append(chr(int(digits, 16)))
            except ValueError:
                return None
            index += 3
            continue
        if escaped == "u":
            if index + 1 < len(source) and source[index + 1] == "{":
                end = source.find("}", index + 2)
                if end < 0:
                    return None
                digits = source[index + 2 : end]
                index = end + 1
            else:
                digits = source[index + 1 : index + 5]
                if len(digits) != 4:
                    return None
                index += 5
            try:
                value.append(chr(int(digits, 16)))
            except (ValueError, OverflowError):
                return None
            continue
        value.append(escaped_chars.get(escaped, escaped))
        index += 1

    return None


def javascript_command_strings(source: str) -> list[str]:
    commands: list[str] = []
    variables: dict[str, str] = {}

    for match in JAVASCRIPT_STRING_ASSIGNMENT_PATTERN.finditer(source):
        parsed = javascript_string_value(source, match.end())
        if parsed is not None:
            value, _ = parsed
            variables[match.group(1)] = value

    for match in JAVASCRIPT_COMMAND_FIELD_PATTERN.finditer(source):
        index = match.end()
        while index < len(source) and source[index].isspace():
            index += 1
        parsed = javascript_string_value(source, index)
        if parsed is not None:
            command, _ = parsed
            commands.append(command)
            continue

        identifier = JAVASCRIPT_IDENTIFIER_PATTERN.match(source, index)
        if identifier is not None:
            command = variables.get(identifier.group())
            if command is not None:
                commands.append(command)

    return commands


def tool_requests_cloud_operation(event: dict[str, object]) -> bool:
    tool_name = str(event.get("tool_name") or "")
    if re.search(r"(?:gcp|gcloud|google.?cloud)", tool_name, re.IGNORECASE):
        return True

    tool_input = event.get("tool_input")
    try:
        if isinstance(tool_input, str):
            return (
                JAVASCRIPT_CLOUD_TOOL_PATTERN.search(tool_input) is not None
                or any(
                    command_is_cloud_operation(command)
                    for command in javascript_command_strings(tool_input)
                )
            )
        if not isinstance(tool_input, dict):
            return False

        return any(
            command_is_cloud_operation(tool_input.get(key))
            for key in ("command", "cmd", "shell_command", "script_command")
        ) or any(
            command_is_cloud_operation(command)
            for key in ("source", "script", "code")
            if isinstance(tool_input.get(key), str)
            for command in javascript_command_strings(tool_input[key])
        )
    except (RecursionError, ValueError):
        # Malformed shell syntax must not bypass the cloud-command gate.
        return True


def emit(value: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(value, ensure_ascii=False))
    sys.stdout.write("\n")


def readme_context(readme: str, digest: str) -> str:
    return (
        f"Read the checked-in `{README_PATH.as_posix()}` before GCP work. "
        f"SHA-256: `{digest}`. Follow its instructions before cloud commands.\n\n"
        f"{readme}"
    )


def on_prompt_submit(event: dict[str, object]) -> None:
    if not prompt_is_gcp_related(event):
        return

    try:
        readme, digest = read_readme()
        save_state(session_state_path(event), digest)
    except Exception as error:  # Cloud commands are blocked later if the read fails.
        emit(
            {
                "systemMessage": "Could not read the checked-in GCP README; cloud commands will be blocked.",
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": (
                        f"Read `{README_PATH.as_posix()}` before GCP work. "
                        f"The hook failed with {type(error).__name__}: {error}."
                    ),
                },
            }
        )
        return

    emit(
        {
            "hookSpecificOutput": {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": readme_context(readme, digest),
            }
        }
    )


def on_pre_tool_use(event: dict[str, object]) -> None:
    if not tool_requests_cloud_operation(event):
        return

    try:
        readme, digest = read_readme()
        state_path = session_state_path(event)
        if state_is_fresh(state_path, digest):
            return

        save_state(state_path, digest)
        reason = "The repository GCP README was just loaded into context. Review it, then retry the cloud command."
        extra_context = readme_context(readme, digest)
    except Exception as error:
        reason = "The checked-in GCP README could not be read, so this cloud command is blocked."
        extra_context = (
            f"Read `{README_PATH.as_posix()}` before GCP work. "
            f"The hook failed with {type(error).__name__}: {error}."
        )

    emit(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
                "additionalContext": extra_context,
            }
        }
    )


def main() -> None:
    event = read_event()
    event_name = event.get("hook_event_name")
    if event_name == "UserPromptSubmit":
        on_prompt_submit(event)
    elif event_name == "PreToolUse":
        on_pre_tool_use(event)


if __name__ == "__main__":
    main()
