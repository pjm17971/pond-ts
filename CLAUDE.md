# pond-ts

TypeScript time series library. Successor to pondjs / react-timeseries-charts.

## Plan

**Read [PLAN.md](PLAN.md) at the start of every session.** It is the single
source of truth for what has shipped, what is next, and the design decisions
behind each phase.

**Update PLAN.md when meaningful work lands.** If you complete a task, add a
feature, fix a bug, or make a design decision that affects upcoming work, update
the relevant section of PLAN.md in the same pass. Move items from "remaining" to
"completed", add new design notes, or adjust phase scope as needed. Do not defer
this — a lost session should not erase the current state of the project.

## Stack

- TypeScript (strict)
- Vitest for tests
- Docusaurus for docs
- npm for packaging

## Commands

- `npx vitest run` — run all tests
- `npx vitest run test/<file>` — run a specific test file
- `npx tsc --noEmit` — type check
- `npx prettier --write .` — format

## Before opening a PR

Run `npx prettier --write .` before committing. Unformatted code will fail review.
