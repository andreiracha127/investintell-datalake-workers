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

## Required CI status checks

GitHub Actions is not available as the trigger/status bridge for this repo:
the account currently cannot start hosted Actions runners because of a billing
lock. The required quant-engine gate therefore remains Railway-native. The
Railway service `quant-engine-ci-pr4` is connected to GitHub source
`andreiracha127/investintell-datalake-workers` on branch `main` and uses
`docker/railway-ci/Dockerfile` for the actual gate.

Platform evidence from 2026-06-27:

- service: `quant-engine-ci-pr4`
- service_id: `8f2613c1-66ce-4ec6-a2a7-b5a55290becc`
- project_id: `1cc3be4b-c600-43ee-9525-69e05818e5fa`
- production_environment_id: `745b95ea-a8bf-4f24-b202-b22b491b0f10`
- source repo: `andreiracha127/investintell-datalake-workers`
- source branch: `main`
- source-connect verification deployment:
  `ffba3b4d-07f3-438b-aa5d-a11d83cd496b = SUCCESS`
- source-connect verification image digest:
  `sha256:827876c8926b57d08a9fcf93749501d387aa8d66d3dabb44d3204c940a775eda`

To make `railway/quant-engine-ci` appear on every PR-head push without Actions,
enable Railway PR Environments or the equivalent Railway-native PR deploy/status
integration for this connected source. Do not reintroduce a GitHub Actions
runner bridge unless the billing lock is resolved and the team explicitly wants
GitHub-hosted runners as a trigger shim.

Railway PR Environments are a project-level toggle, not a per-service CLI flag:

1. Open Railway project `investintell-workers`.
2. Go to Project Settings -> Environments.
3. Enable PR Environments.
4. Enable Focused PR Environments so PRs deploy only affected services instead
   of every connected worker service.
5. Keep bot PR environments disabled unless dependency-bot CI cost is accepted.

After that toggle, each PR push should create or update an isolated Railway PR
environment for the connected repo, then Railway should report its deployment
status back to GitHub. Verify the first PR-head deployment targets the PR SHA,
not `main`, before requiring the status in branch protection.

Checks covered by the Railway gate:

| Check | Required on PR | Required to merge |
|---|---|---|
| `pytest` (input packs, quant engine/core, calibration, repeatability) | yes | yes |
| `contract_bundle.py verify` | yes | yes |
| Certified input-pack verification | yes | yes |
| Calibration artifact hash/provenance/governance validation | yes | yes |
| Railway image build by pinned Dockerfile | yes | yes |
| Railway source-connected deployment trigger | yes | yes |
| PR-head status `railway/quant-engine-ci` from Railway-native PR integration | yes | yes |
| SBOM generated | no | release only |
| Provenance generated | no | release only |
| Signature/attestation verify | no | release only |

The repeatability matrix evidence is committed as calibration artifacts and is
rechecked by the Railway Dockerfile. Future release-only SBOM/provenance/signature
work should remain separate from the PR CI executor decision unless the team
explicitly opens that governance wave.

## Branch protection (proposed)

- Require pull-request review (two reviewers per SLSA Source for protected branches).
- Require `railway/quant-engine-ci` to pass.
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
- `macro-history-coverage`: enforce each source's
  `minimum_valid_observations` before `quadrant_score.standardized_latest`
  returns an available z-score. This changes macro quadrant runtime scoring and
  coverage behavior, so it belongs in a dedicated runtime-worker PR before
  shadow/A5 rather than the P0 certified input-pack gate.
- `macro-vintage-identity`: include the actual PIT observation/vintage input
  identities, or the full PIT input set, in `quadrant_macro`'s
  `source_vintage_hash`. This changes productive snapshot identity semantics and
  must be handled in a dedicated runtime-worker PR before shadow/A5.
- `calibration-formula`: honor each V02 macro series' cadence and
  `freshness_limit_days` in `series_freshness`. This changes calibration
  quality inputs and must be handled in the calibration branch, not in the P0
  certified input-pack gate.
- `runtime-observability`: report actually inserted macro vintage rows instead
  of attempted rows after `ON CONFLICT DO NOTHING`. This touches productive
  worker DB-write accounting and needs a dedicated runtime-worker PR.
