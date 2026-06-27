# Quant Engine — Governance & Rollout Runbook

Date: 2026-06-26
Lane: `feat/quant-engine-isolation` (worker) + `feat/quant-engine-contracts` (backend)
Gate: report "Processo de revisão, merge e rollout até A5".

This runbook is the decision/infra surface of the program. The items here are
**not** activated by this lane — they require repository-admin, CI, registry, and
runtime infrastructure decisions by the team. They are recorded so the path to A5
is explicit and quantitative.

## Current state (closed in this lane)

- Determinism gate: within-host, within-container, and cross-env all bit-a-bit
  identical (`docs/architecture/quant-engine-determinism-findings.md`).
- Finding F1a fixed (catalog hash path-independence) with golden re-captured.
- Finding F1b resolved operationally (provenance injected, not read from ambient git).
- Versioned contract bundle with verifier + fixtures + SemVer policy
  (`contracts/quant-engine/v1/`).
- Base image pinned by digest; hardened sandbox profile, verified
  (`docs/architecture/quant-engine-supply-chain-sandbox.md`).

Governance invariants held throughout: A3 `open_macro_v03`, A4
`harness_ready_provisional_A3`, A5 `blocked`, `freeze_ready=false`,
`runtime_activation=false`.

## Required CI status checks (proposed)

`.github/workflows/quant-engine-ci.yml` is a proposed, inert gate. To make it a
required status check the team must enable Actions and branch protection. Checks:

| Check | Required on PR | Required to merge |
|---|---|---|
| `pytest` (quant suite + contracts) | yes | yes |
| `contract_bundle.py verify` | yes | yes |
| Repeatability matrix (host leg) | yes (nightly for container/cross-env) | yes |
| Image build by digest | yes | yes |
| SBOM generated | draft optional | yes |
| Provenance generated | draft optional | yes |
| Signature/attestation verify | no | release only |

The repeatability matrix's container/cross-env legs need the input bundle and a
Docker runner, so they run locally / nightly, not on every generic PR runner.

## Branch protection (proposed)

- Require pull-request review (two reviewers per SLSA Source for protected branches).
- Require the status checks above to pass.
- Disallow direct pushes to `main`; no force-push.

## Two linked draft PRs

| PR | Scope allowed | Acceptance |
|---|---|---|
| Worker (`feat/quant-engine-isolation`) | comparator, outputs manifest, repeatability harness, F1a fix + golden recapture, contract bundle, hardened image/sandbox, ADRs | green CI matrix, image by digest, hardened sandbox, complete manifests |
| Backend (`feat/quant-engine-contracts`) | mirrored schemas, bundle hash verification, contract tests | all contract tests/fixtures green; **no execution endpoint**, no container invocation, no builder/allocator runtime change |

### Merge order

1. Freeze the contract bundle version (`contract_version` + `bundle_sha256`).
2. Update the backend to the final bundle digest.
3. Approve the worker PR with all technical + supply-chain gates green.
4. Approve the backend PR with the final contract bundle.
5. Start shadow mode.
6. Keep `A5=blocked` until shadow-mode targets are met.

## Shadow mode → A5 gate

Shadow mode runs the new path in parallel with no functional activation; it emits
manifests and metrics compared against the baseline. The A5 unblock decision is
quantitative, not opinion-based:

| Metric | Target |
|---|---|
| Functional equivalence | ≥ agreed threshold over a stable window |
| `missing_artifacts` / `unexpected_artifacts` | 0 |
| Latency P50/P95 | within agreed envelope |
| Memory / retries | within agreed envelope |
| Operational incidents | none in the window |

A5 is unblocked only under change control, with rollback ready, after all prior
gates and a recorded readiness review (architecture, security, QA, ops). No model
formula or candidate-selection rule changes as part of activation.

## Out of scope for this lane (unchanged)

Freeze of A3/A4 parameters, A5 advancement, runtime activation, production DB
writes, frontend changes, and merge to `main` remain explicitly out of scope.

## Deferred runtime follow-ups before shadow/A5

- `data-quality`: enforce source availability expiry for `quadrant_macro` before
  shadow/A5. The certified input-pack P0 gate must not change macro quadrant
  runtime staleness semantics; this requires a dedicated runtime-worker PR that
  propagates latest vintage `available_at`/release metadata into the expiry
  decision.
- `infra`: split regime gate advisory lock ownership and add lock-collision
  tests. The P0 input-pack gate records the existing `LOCK_REGIME_GATE` /
  `LOCK_SCREENER_METRICS` collision as accepted technical debt, but concurrent
  runtime scheduling must not proceed until the lock ids are distinct and covered
  by tests.
- `runtime-read-model`: keep historical quadrant backfills from winning
  `regime_quadrant_current_v`. The P0 input-pack gate must not change runtime
  read-model SQL; a dedicated runtime PR must order/filter by current `as_of`
  semantics so an old backfill cannot become the consumable current quadrant just
  because it was inserted most recently.
- `calibration-formula`: honor each V02 macro series' cadence and
  `freshness_limit_days` in `series_freshness`. This changes calibration
  quality inputs and must be handled in the calibration branch, not in the P0
  certified input-pack gate.
- `runtime-observability`: report actually inserted macro vintage rows instead
  of attempted rows after `ON CONFLICT DO NOTHING`. This touches productive
  worker DB-write accounting and needs a dedicated runtime-worker PR.
