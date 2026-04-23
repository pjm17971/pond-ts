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

## Publishing a release

All packages publish together under one `v*` tag via the GitHub Actions
workflow at `.github/workflows/release.yml`. npm publishes use OIDC
Trusted Publisher — no stored tokens, nothing to configure locally.

To cut a release from `main`:

1. Bump the `version` field in **every** `packages/*/package.json`. Keep
   them lock-step — the release tag covers the whole monorepo.
2. If `@pond-ts/react`'s `dependencies.pond-ts` caret needs to widen to
   the new minor (e.g. `^0.4.0` → `^0.5.0`), update it in the same pass.
3. **Add a `CHANGELOG.md` entry** under a new `## [X.Y.Z] — YYYY-MM-DD`
   heading, and update the compare-link footnotes. Group notes under
   `Added` / `Changed` / `Fixed` / `Deprecated`. Consumers upgrading
   between versions rely on this; skipping it compounds every release.
4. Commit with a message like `chore: bump to vX.Y.Z`.
5. Tag the commit: `git tag vX.Y.Z`.
6. Push the branch, then push the tag:
   ```
   git push origin main
   git push origin vX.Y.Z
   ```
   `--follow-tags` only pushes annotated tags; lightweight tags (the
   default with bare `git tag`) need an explicit push.

That's it. The `v*` tag push triggers `.github/workflows/release.yml`,
which checks out the tag, runs `npm run verify`, then
`npm publish --access public --provenance --workspaces` to publish every
workspace package in one pass. Do not run `npm publish` locally.
