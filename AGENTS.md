# AGENTS.md

Guidelines for LLM agents working on this repository.

## Project Overview

A key-value storage engine built on LSM-tree architecture in Rust. Supports key-value separation via a value log (vLog) and MVCC-based concurrency control.

**Repository:** `ben1009/toy-kv-engine`

## Architecture

```
kv-engine/src/
├── lsm_storage.rs      # Core KvEngine struct and LsmStorageInner
├── mem_table.rs        # MemTable (crossbeam-skiplist based)
├── block.rs            # SSTable block format
├── table.rs            # SSTable (SsTable)
├── compact.rs          # Compaction strategies
│   └── compact/        # leveled, simple_leveled, tiered
├── iterators/          # Merge, concat, two-merge iterators
├── mvcc/               # MVCC transaction support
├── manifest.rs         # Manifest file management
├── wal.rs              # Write-ahead log
├── key.rs              # Key types (KeyVec, KeyBytes, KeySlice)
├── bin/                # CLI and compaction simulator
├── tests/              # Integration tests
└── lib.rs              # Crate root with module declarations
```

## Conventions

### Naming
- The main public type is `KvEngine` (renamed from `MiniLsm`)
- Use feature-phase names in comments (e.g., "compaction", "MVCC") not curriculum weeks
- Module-level docs should be concise, not circular

### Lint Policy
- Crate-level `#![allow(dead_code)]` and `#![allow(unused_variables)]` in `lib.rs`
- Starter code and stubs are expected; do not add per-module allows

### Testing
- All tests must pass: `cargo test --lib --tests`
- Format check: `cargo fmt --check`
- Lint check: `cargo clippy --all-features`
- Integration tests are in `kv-engine/src/tests/`
- vLog tests are in `kv-engine/src/vlog_integration_tests.rs`

### CI Requirements
All PRs must pass:
- `clippy`, `fmt`, `coverage`, `typos`
- `sanitizers` (address + leak sanitizer)
- `codecov/patch` and `codecov/project` (89% minimum)
- Cross-platform: `ubuntu-latest`, `macos-14`, `macos-latest`
- `dependency-review`

### Build Toolchain
- Rust nightly-2026-05-28 (pinned in CI)
- Edition 2024, resolver v3
- GitHub Actions pinned to commit SHAs

## PR Workflow

**Always use `--repo ben1009/toy-kv-engine` with every `gh` command.**

### Creating PRs
See `/home/liu/proj/agent-skills/pr-create/SKILL.md`

### Reviewing PRs
See `/home/liu/proj/agent-skills/pr-review/SKILL.md`

Key rules:
1. **Never auto-fix without user confirmation**
2. **Never resolve a thread without replying first**
3. **Never merge with unresolved threads**
4. Reply to every review comment explaining what was fixed (or why not)
5. Resolve threads via GraphQL `resolveReviewThread` mutation
6. Request re-review after addressing all comments

### Git Workflow
See `/home/liu/proj/agent-skills/git-workflow/SKILL.md`

## Common Commands

```bash
# Run tests
cargo test --lib --tests

# Check formatting
cargo fmt --check

# Run linter
cargo clippy --all-features

# Check PR status
gh pr checks <number> --repo ben1009/toy-kv-engine

# View PR comments
gh pr view <number> --comments --repo ben1009/toy-kv-engine
```
