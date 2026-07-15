# CI Scope and Stage A Isolation Design

- Date: 2026-07-15
- Status: design approved; written specification awaiting review
- Scope: `investintell-datalake-workers` PR workflow and Stage A reproducibility binding

## Context

PR #41 exposed two independent structural problems in the current CI:

1. A push to a `feat/**` branch starts the same workflow twice: once for `push` and once for `pull_request`.
2. The single `quant-engine` job runs the complete governance and quant test suite for every change, including isolated N-PORT worker changes.

For commit `8816ef0`, GitHub Actions created runs `29455656962` and `29455654554`. Each took about 22.5 minutes, consuming roughly 45 billed runner-minutes for one push. Dependency installation took about 23 seconds; the monolithic pytest step took 21 minutes and 50 seconds and failed only at the end, after 1,089 passing tests, because the Stage A source-tree hash was stale.

The stale hash was not evidence of an N-PORT regression. Stage A currently binds broad directory trees, including all of `src/`, so an unrelated worker module changes the Open Macro reproducibility hash.

Local N-PORT tests remain necessary for fast development feedback, but they are not a substitute for a small CI gate in a clean checkout with locked dependencies. The desired outcome is therefore scoped CI, not removal of CI coverage.

## Goals

- Start exactly one workflow run for each new commit pushed to an open PR.
- Keep the existing required check identity, `quant-engine`, stable.
- Give N-PORT changes a clean-environment gate that is focused, deterministic, and inexpensive.
- Run the expensive quant/governance suite only when its owned compute surface or contracts change.
- Reject a stale Stage A binding before starting the expensive suite.
- Bind Stage A evidence to its actual decision-compute closure, not unrelated repository trees.
- Preserve a final full recertification as the authoritative closure proof after the structural change.

## Non-goals

- Removing N-PORT tests from CI.
- Weakening Stage A reproducibility or bypassing its evidence checks.
- Changing the N-PORT classifier, database schema semantics, production data, or deployment behavior.
- Reconfiguring GitHub branch-protection settings or renaming the required check.
- Performing database, cloud, Docker registry, or deployment writes from the focused N-PORT gate.

## Design decisions

### 1. Eliminate duplicate feature-branch runs

The workflow will run on:

- `pull_request` for PR validation;
- `push` only for `main`, for post-merge validation.

It will no longer run on `push` to `feat/**`. A concurrency group keyed by workflow plus PR number, falling back to the ref outside PRs, will use `cancel-in-progress: true`. A superseded commit therefore stops consuming runner time once a newer commit enters the same PR.

### 2. Preserve one always-present required job

The workflow will continue to expose a single job/check named `quant-engine`. Path detection happens inside that job after checkout rather than at workflow-trigger level. This avoids leaving branch protection stuck on an `Expected` check when a path-filtered workflow does not start.

The detector produces two independent booleans:

- `nport_changed`
- `quant_changed`

If both are true, both lanes run in dependency order. If neither is true, the job records that no governed compute surface changed and exits successfully. The required check therefore exists for every PR without paying for irrelevant test suites.

### 3. Define an inexpensive N-PORT lane

The N-PORT lane is selected by changes to:

- `src/workers/nport_*.py`;
- `schemas/nport_*.sql`;
- `tests/test_nport_*.py`;
- shared runtime modules used directly by N-PORT, including `src/db.py`;
- the workflow, dependency manifest, or lockfile when those changes can affect the lane.

It runs only clean-environment checks relevant to this subsystem:

1. focused N-PORT pytest files;
2. Ruff on the governed N-PORT implementation and tests;
3. Python bytecode compilation for the governed modules.

The lane must not require a live database, network service, Docker daemon, private deployment credentials, or production data. Integration behavior that requires those resources remains covered by explicitly scheduled or separately authorized integration workflows, not by every N-PORT PR commit.

### 4. Fast-fail Stage A before expensive quant tests

When `quant_changed` is true, the job runs a dedicated Stage A binding precheck before the full governance/quant suite. The precheck compares the checked-in reproducibility record with the hash derived from the current certified compute manifest.

If the binding is stale, the job fails immediately with an actionable message naming the recertification command. The expensive test suite, compilation, and artifact checks do not start. If the precheck passes, the existing full quant/governance validation continues unchanged.

This ordering converts the observed 21-minute late failure into a seconds-scale failure without weakening the gate.

### 5. Replace broad Stage A trees with an explicit compute manifest

Stage A will stop hashing whole top-level directories such as `src/`, `harness/`, `packages/`, `services/`, and `scripts/`. A centralized, version-controlled manifest will enumerate the project files that can affect the direct-activation decision and its measurement.

The known production decision closure includes:

- `src/quadrant_score.py`;
- `src/macro_transforms.py`;
- `src/macro_sources.py`;
- `src/quadrant_confidence.py`;
- `src/quadrant_hysteresis.py`;
- `src/quadrant_assemble.py`;
- `src/quadrant_snapshot.py`;
- `src/quadrant_staleness.py`.

The final manifest also includes the exact direct-activation, Phase 0Q, sleeve, export-source, and measurement files used by the Stage A execution path. It excludes unrelated worker implementations such as N-PORT modules.

The manifest is the single source of truth for:

- Stage A hash computation;
- CI `quant_changed` path routing;
- tests that validate the certified surface.

A focused contract test walks project-local imports reachable from the certified entry points and fails if a new decision-affecting project file is not represented in the manifest. Standard-library and third-party imports are outside this repository manifest. This guards against narrowing the surface too far while preventing unrelated code from invalidating evidence.

### 6. Recertify once, after the structure is final

Existing Stage A artifacts are not rewritten repeatedly during implementation. After workflow routing, the explicit compute manifest, the binding precheck, and their tests are final, Stage A is run once for the full required reproduction count. The resulting evidence is committed together with the structural correction and validated by one final CI run.

Any higher-stage activation gate that depends on Stage A remains blocked until that final recertification and CI validation are green.

## Execution flow

For each PR commit:

1. GitHub starts one workflow run and cancels an older in-progress run for the same PR.
2. `quant-engine` checks out the repository and computes the merge-base diff.
3. The detector maps changed paths to the N-PORT and quant surfaces.
4. The job installs dependencies once if either lane requires them.
5. The N-PORT lane runs when `nport_changed` is true.
6. The Stage A binding precheck runs when `quant_changed` is true.
7. The full quant/governance suite runs only if the binding precheck succeeds.
8. The single required check reports the combined result.

## Failure behavior

- Invalid or ambiguous path-detector output fails closed; it must not silently skip validation.
- Workflow, dependency-manifest, and lockfile changes select all affected lanes.
- A path shared by N-PORT and quant selects both lanes.
- A stale Stage A record fails before the expensive suite and prints the local recertification command.
- A missing file referenced by the compute manifest fails the binding precheck.
- A newly reachable project-local decision import absent from the manifest fails the manifest contract test.
- Focused N-PORT failures fail the same required `quant-engine` check.

## Test strategy

Implementation follows test-first sequencing:

1. Add a workflow-contract test that initially fails against the duplicate trigger, absent concurrency policy, missing routing, or incorrect fast-fail ordering.
2. Add manifest-contract tests that initially fail while Stage A still binds whole directories.
3. Change the workflow and Stage A hashing implementation minimally to satisfy those contracts.
4. Run the focused N-PORT lane locally using the repository environment.
5. Run the Stage A binding precheck and relevant governance tests locally.
6. Run the full quant/governance suite once locally after the contracts pass.
7. Perform one final Stage A recertification.
8. Push once and observe one GitHub Actions run through completion.

Tests must assert behavior, not merely search for arbitrary YAML text. At minimum they verify trigger semantics, concurrency cancellation, stable job identity, path classification, both-lanes behavior for shared paths, and the fact that the full quant suite is gated behind a successful binding precheck.

## Rollout and compatibility

- Keep the workflow name and `quant-engine` job/check name stable so existing branch protection continues to recognize it.
- Land the workflow, manifest, tests, and newly generated Stage A evidence atomically in the feature branch.
- Do not push intermediate commits that retain the old expensive trigger behavior.
- Before pushing, compare the PR head with the local branch and confirm that unrelated dirty files are absent from the commit.
- After pushing, verify that only one PR run exists for the new SHA, that no superseded run continues unnecessarily, and that review threads remain resolved.

## Risks and mitigations

### Incorrect path routing

A missed path could skip a necessary lane. The centralized manifest, fail-closed detector, shared-path rules, and workflow-contract tests mitigate this. Workflow and dependency changes deliberately select broad validation.

### Incomplete Stage A compute closure

An overly narrow manifest could make reproducibility evidence unsound. The reachable project-import contract and explicit review of entry points mitigate this. Adding a new decision dependency requires updating the manifest and recertifying Stage A.

### Required-check incompatibility

Renaming or omitting the job could block merging. The design preserves the existing `quant-engine` identity and always creates the job.

### Stale evidence committed too early

Generating evidence before the compute surface stabilizes wastes time and creates noisy diffs. Recertification occurs exactly once after implementation and local verification are complete.

## Acceptance criteria

- One new commit on an open PR creates exactly one GitHub Actions workflow run for that SHA.
- A newer commit cancels an older in-progress run for the same PR.
- The required check remains named `quant-engine` and appears on every PR.
- An N-PORT-only diff runs focused N-PORT pytest, Ruff, and compilation, but not the full quant/governance suite.
- The N-PORT-only CI lane targets at most two billed runner-minutes under normal cache conditions, with approximately one minute as the operational objective.
- N-PORT-focused CI uses no live database, external service, Docker build, or deployment credential.
- An unrelated N-PORT source change does not change the Stage A compute hash.
- A decision-compute source change does change the Stage A compute hash.
- A newly reachable project-local decision dependency omitted from the manifest causes a focused test failure.
- A stale Stage A binding fails before the expensive quant/governance suite starts.
- Local focused gates and every quant/governance test not dependent on the new
  evidence binding pass before evidence generation; the complete suite passes after
  the final evidence is generated.
- Stage A completes its required full reproduction count once against the final compute manifest.
- The final push produces one green CI run, zero unresolved review threads, and no unrelated committed changes.
