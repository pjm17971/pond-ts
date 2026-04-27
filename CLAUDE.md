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

## Performance check for new operators on large data

When adding a new operator (or making a non-trivial impl change to
an existing one) that walks events, allocates per-event, or has any
cost path that scales with input size, run a performance check
before merging. The goal is to catch quadratic behavior, redundant
scans, and accidental allocation hotspots while the code is still
cheap to fix — and to leave a durable benchmark in the repo so
future regressions surface.

**When this applies:**

- New methods on `TimeSeries`, `LiveSeries`, or `PartitionedTimeSeries`
- Non-trivial impl changes to existing operators that touch event
  loops, bucket scans, or allocation patterns
- New code paths that scale with event count, bucket count, or
  partition count

**When it doesn't:**

- Bug fixes that don't change asymptotic behavior
- Pure type-level changes, documentation, or test additions
- Refactors that preserve the existing algorithm
- Operators that purely delegate to other operators (no new
  walking logic)

**Procedure:**

1. **Write down the complexity.** Before benchmarking, note the
   asymptotic cost in terms of input dimensions (N events, B
   buckets, C columns, etc.). Identify nested loops; distinguish
   amortized from worst-case behavior. This is what catches the
   quadratic bugs you didn't realize you wrote.

2. **Add a benchmark script** at
   `packages/core/scripts/perf-<operator>.mjs` matching the
   convention used by `perf-aggregate.mjs`, `perf-rolling.mjs`,
   etc. — `makeSeries` + `median` + `benchmark` + JSON output,
   importing from compiled `../dist/index.js`. Cover at minimum:

   - Typical workload size (e.g. 100k events on a 1s grid)
   - Per-element overhead floor (~1 event per bucket — surfaces
     per-bucket fixed costs)
   - Sparse source on dense grid (many empty / no-op cases)
   - Partitioned variant if the operator has one

3. **Run the benchmark; identify hotspots.** Common targets:
   per-iteration array allocations, redundant scans, missing
   cursor advances, post-process passes that could be
   short-circuited.

4. **Land optimizations the analysis surfaces.** Re-run the
   benchmark after each change to confirm the win. Don't ship
   optimization claims that aren't measured.

5. **Report before/after in the commit message** as a table.
   The benchmark numbers are the durable record of what the
   change cost — and what future regressions are measured against.

**Worked example:** `feat(materialize)` (PR #81) — analytical
O(N + B·C) analysis up front, `scripts/perf-materialize.mjs`
covering 5 scenarios, two optimizations identified and shipped
(–14% on bare `'first'`, –41% on partitioned variant, –26% on the
full multi-host pipeline), all results pinned in the commit
message.

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
- **Perf check** — for any new operator or non-trivial impl change
  that walks events or allocates per-event, did you do the perf
  check (analytical complexity + `scripts/perf-<operator>.mjs` +
  before/after table in the commit message)? See the dedicated
  section above.

Fix obvious issues before opening the PR.

**Layer 2 — adversarial agent review after CI passes, before
merge.** Spawn a fresh code-review agent via the Agent tool with
`subagent_type: 'general-purpose'`. Give it the PR number and ask
for an adversarial read. The agent has no context from the authoring
session — that's the whole point.

The agent posts its findings **directly to the PR as a comment** via
`gh pr comment`. That comment, together with your response comment,
becomes the durable review record attached to the PR.

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

  Keep the review under 300 words. Post it as a PR comment by
  running:

  gh pr comment <N> --body "$(cat <<'EOF'
  ## Adversarial review

  <your findings here, grouped by category>
  EOF
  )"

  Do not return the review as text — the PR comment is the
  deliverable.`,
})
```

**Responding to the review.** After the agent's comment lands, read
it and decide each concern on its merits. Fix genuine issues with a
follow-up commit on the same branch. Then post a **second PR
comment** that closes the loop — without it, the review is
unresolved:

```
gh pr comment <N> --body "$(cat <<'EOF'
## Review response

Addressed in <sha>:
- <concern> — <what changed>

Not addressed — rationale:
- <concern> — <why this is intentional / out of scope / already covered>
EOF
)"
```

Argue back only when you genuinely disagree — don't dismiss. The
response comment is the record that every concern was considered,
not just that the agent was run.

**The two comments together are the review record.** Once both
exist and genuine concerns have fix commits, agent-merge is
acceptable for this project — the PR comments are the durable
trail, not a human approval gate.

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
