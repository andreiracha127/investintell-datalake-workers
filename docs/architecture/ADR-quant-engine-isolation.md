# ADR: Quant Engine Isolation

Date: 2026-06-26

Status: Accepted for implementation

## Context

The current A3/A4 calibration implementation lives in a worker repository that
mixes pure macro/quadrant computation, grid orchestration, local filesystem
reports, network fetchers, database access, object-store upload helpers,
notebook/QC bridge code, and CLI entrypoints.

The target architecture is a deterministic offline quant engine:

- `quant-core`: a pure Python package containing deterministic domain logic.
- `quant-engine`: a batch container and CLI that runs `quant-core` over immutable
  bundles and writes explicit artifacts.
- Backend allocator contracts: frozen schemas and docs for the future boundary,
  with no runtime activation in this phase.

Governance must remain unchanged:

- A3 remains `open_macro_v03`.
- A4 remains `harness_ready_provisional_A3`.
- A5 remains `blocked`.
- `freeze_ready=false`.
- `runtime_activation=false`.

## Decision

We will extract deterministic A3/A4 calculation logic into `quant-core` and keep
all I/O, orchestration, bundle loading/writing, network access, database access,
object-store upload, and CLI behavior outside that package.

The only allowed input to the offline engine is an explicit immutable bundle
provided to the batch CLI/container. The only allowed output is an explicit
artifact directory containing manifests, reports, hashes, and diagnostics.

The backend will not import `quant-core` during this phase. It will receive only
contract documents and schemas that describe future integration boundaries.

## Boundaries

`quant-core` may contain:

- Typed input/config/result models.
- Feature manifest contract checks.
- A31/A32 config normalization and hash helpers.
- L2-to-L3 score panel logic.
- L4 state machine logic.
- Metric row canonicalization and logical hash calculation.
- Bundle evaluation hash calculation.
- Deterministic microgrid enumeration and result aggregation helpers.

`quant-core` must not contain:

- Postgres or `DATABASE_URL` access.
- FRED, ALFRED, Tiingo, QC History, or HTTP fetches.
- Object-store upload.
- Filesystem discovery outside caller-provided paths.
- Implicit wall-clock timestamps.
- Git state inspection.
- Production table writes.
- Runtime activation switches.

`quant-engine` may contain:

- CLI parsing.
- Bundle load/write adapters.
- Output directory management.
- Container entrypoint.
- Parity/backtest command wiring.
- Optional parallel execution orchestration, provided the logical outputs remain
  stable and sorted.

## Determinism Requirements

The extracted path must preserve the existing golden baseline before functional
changes are accepted:

- Runtime row count: `3221`.
- Counterfactual row count: `3221`.
- Metric row count: `5`.
- Parity mismatch count: `0`.
- Canonical metric hash:
  `70014a0a04fa26faf8aec88227f0f1fea381091acb6ac307fae30b77172300d3`.
- A4 status: `harness_ready_provisional_A3`.
- A5 status: `blocked`.
- `freeze_ready=false`.
- `runtime_activation=false`.

Any mismatch blocks extraction until explained by a deterministic, reviewed
contract update. Better metrics are not permission to freeze parameters or
advance A5.

## Backend Decision

The Plan C allocator remains backend-owned for this phase. Backend work is
limited to:

- Frozen API/data-contract docs.
- Schema/fixture tests that can validate future quant-engine artifacts.
- An extraction plan for allocator-engine boundaries.

No backend runtime call path may depend on the new `quant-core` package in this
phase.

## Consequences

Positive:

- A3/A4 calculation can be tested offline without live DB/network dependency.
- Golden parity can be enforced before and after extraction.
- Future batch execution can be containerized and pinned by image digest.
- Backend allocator contracts can evolve without silently activating runtime
  macro decisions.

Costs:

- Existing CLI code must be split carefully so orchestration remains outside the
  pure package.
- Current generated artifacts must be reproduced or explicitly revalidated in
  the isolated worktree before extraction.
- Some current tests may need fixture adapters while preserving existing hash
  semantics.

## Explicit Non-goals

This ADR does not authorize:

- Parameter selection, freeze, or model promotion.
- A5 advancement.
- Runtime activation.
- Production DB writes.
- Frontend changes.
- Main-branch merge.
- Replacing backend Plan C optimizer behavior.
