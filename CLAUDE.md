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

## Monorepo structure

npm workspaces with two packages:

- `packages/core` — the `pond-ts` package (batch + live time series)
- `packages/react` — the `@pond-ts/react` package (React hooks, peer-depends on React)

Root-level config (prettier, gitignore, CLAUDE.md, PLAN.md, README.md) is shared.
Docs site lives at `website/`.

## Stack

- TypeScript (strict)
- Vitest for tests
- Docusaurus for docs
- npm workspaces for packaging

## Commands

From repo root:

- `npm run build` — build all packages
- `npm test` — test all packages
- `npm run verify` — format check + build + test

For a specific package:

- `npm run build --workspace=pond-ts` — build core
- `npm test --workspace=pond-ts` — test core
- `npm run build --workspace=@pond-ts/react` — build react

From within `packages/core/`:

- `npx vitest run` — run all core tests
- `npx vitest run test/<file>` — run a specific test file
- `npx tsc --noEmit` — type check core
- `npx prettier --write .` — format core

## Before opening a PR

Run `npx prettier --write .` before committing. Unformatted code will fail review.
