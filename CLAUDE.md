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

## PR review (don't self-merge)

A PR author is the same mind that wrote the code — unlikely to catch
their own design errors, scope creep, silent breaking changes, or
"clever" code that would stump the next editor. Every PR of
meaningful size gets a two-layer review before merge.

**Layer 1 — self-review before opening the PR.** After committing,
before `gh pr create`, read the diff cold:

```
git diff main...HEAD
```

Read it as if it showed up on your desk unannounced. Specifically
look for:

- **Scope creep** — changes that don't belong to this PR's title
- **Speculative features** — options, extension points, or
  parameters with only one valid value
- **New vocabulary** that duplicates existing primitives or
  conventions — pond-ts prefers composition of small primitives
- **Test counts** in the PR description that don't match reality
- **"Strictly additive" claims** that aren't actually true (return
  type widenings, behavior shifts in common paths)
- **Names that don't match behavior** — method, option, and column
  names must describe what the code does
- **Sharp edges the PR body glosses over** — document every one you
  know about, even if not fixing

Fix obvious issues before opening the PR.

**Layer 2 — adversarial agent review after CI passes, before
merge.** Spawn a fresh code-review agent via the Agent tool with
`subagent_type: 'general-purpose'`. Give it the PR number and ask
for an adversarial read. The agent has no context from the authoring
session — that's the whole point.

Example invocation:

```
Agent({
  description: "Adversarial PR review",
  subagent_type: "general-purpose",
  prompt: `Review pond-ts PR #<N> adversarially. Read the diff via
  \`gh pr diff <N>\` and the description via \`gh pr view <N>\`.

  Flag concerns in these categories, in priority order:
  1. **Correctness** — missing edge cases, off-by-one errors,
     silent breaking changes, unhandled undefined values,
     collisions with existing column/method names.
  2. **Design** — over-engineering, scope creep, speculative
     parameters, duplication of existing primitives, inconsistency
     with the rest of the pond-ts API.
  3. **Tests** — claimed behaviors without assertions, missing
     edge cases, tests that don't actually pin the stated guarantee.
  4. **Docs** — mismatch between code behavior and doc prose,
     missing cross-references, examples that wouldn't compile.
  5. **Name quality** — does every method, option, and column
     name match what the code does?

  The PR author wrote an enthusiastic description. Don't trust it
  — verify against the diff. If you see nothing concerning, say so
  explicitly; don't invent concerns to look thorough.

  Report under 300 words.`,
})
```

Take the agent's concerns seriously. Argue back in your reply only
when you genuinely disagree — don't dismiss. If any concerns are
valid, push a fix commit before merging.

**When the reviewer can be skipped:**

- **Pure chore PRs** — CHANGELOG additions, workflow bumps,
  dependency updates, formatting, version bumps. Still self-review
  Layer 1, but Layer 2 doesn't earn its cost.
- **User has explicitly approved the exact change in conversation**
  ("yep, ship it"). Even then, prefer a quick agent pass for
  anything that touches type definitions, public method signatures,
  or runtime behavior — the user approved the intent, the agent
  catches the execution.

**When to require human approval:**

- Any PR that widens or narrows an existing public type in a way
  that could break downstream callers
- Any PR that adds or removes a method on `TimeSeries`, `LiveSeries`,
  or the React hook surface
- Any PR touching the release workflow or npm publish path

For these, the agent review is the floor, not the ceiling. Ask the
user before merging.

## Publishing a release

All packages publish together under one `v*` tag via the GitHub Actions
workflow at `.github/workflows/release.yml`. npm publishes use OIDC
Trusted Publisher — no stored tokens, nothing to configure locally.

To cut a release from `main`:

0. **Check that every PR merged since the last tag had a review.**
   `git log v<previous>..main --merges` shows them. Each one should
   either have an agent-review comment on the PR, explicit user
   approval in chat, or be a pure chore (CHANGELOG / workflow /
   deps). If any slipped through unreviewed, open a short follow-up
   to spot-check before releasing.
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
