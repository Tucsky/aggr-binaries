# AGENTS.md — rules for making changes in aggr-binaries

## Core priorities (in order)
1) Performance & scalability (millions of files, billions of lines)
2) Determinism & correctness (same input => same output)
3) Simplicity (no needless indirection / state / config)
4) Maintainability (small files, shared utilities)

## Hard rules
- Hot paths must stay minimal: avoid extra loops, extra allocations, regex, and per-line object creation.
- Do not introduce new “state objects” or config knobs unless absolutely necessary.
  - Prefer using existing config and existing state. If you add new state, justify it in comments.
- Minimize loops: one pass whenever possible. If you add a loop, explain why it cannot be fused.
- Keep files small: **no file > 400 LOC**. Split by responsibility.
- Strict TypeScript:
  - No `any`. No implicit `unknown` without narrowing.
  - Avoid ad-hoc string unions sprinkled everywhere: use enums/constants for stable domains.
- Avoid refactors unless they directly:
  - reduce time in hot loops,
  - reduce syscalls,
  - reduce memory,
  - or remove duplicated logic that is already causing divergence/bugs.
- Minimal diffs: do not reformat or reorder unrelated code.

## Required workflow for changes
- Start by stating the invariant(s) impacted (1–5 bullets).
- Add a small reproducible fixture or a deterministic check for the behavior.
- Prefer changes that are locally verifiable (unit-level or fixture-level).

## Logging
- No per-line logs. Logs must be rate-limited and summarize counts.

## Documentation scope
- README is project documentation, not an iteration changelog.
- When asked to document "relevant changes", only update durable project-level docs when behavior/capabilities materially change for operators or developers.
- Do not add per-iteration UI details; prefer small edits to existing sections over new deep-dive subsections.

## Node / npm execution
- Agent shells do NOT load NVM or interactive shell config.
- When running Node or npm commands, always ensure the Node binary is available.

Preferred:
- Prefix commands with:
  PATH=$HOME/.nvm/versions/node/v24.13.0/bin:$PATH

Fallback:
- Call Node/npm via absolute paths if needed:
  $HOME/.nvm/versions/node/v24.13.0/bin/node
  $HOME/.nvm/versions/node/v24.13.0/bin/npm
