# Bond EL — local fixed-anchor preparation

This runbook covers only the isolated local Workers changes and synthetic checks. It does not authorize or report production access, publication, Stage 7 execution, deployment or Light activation. See the append-only [owner amendment](../calibration/bond_market_implied_rating_owner_amendment_2026-09-30.md) and [owner decisions](https://github.com/andreiracha127/investintell-light/blob/feat/bond-default-review-market-el/docs/planning/bond-default-el-owner-decisions-2026-09-30.md).

## Prepared contract

- Canonical decimal-string anchor: `-0.8864114120812487`, with policy kind `fixed_authoritative`.
- New local policy digest: `4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a`.
- The measured frozen-window median is diagnostic; changed observations do not choose another anchor. Missing window observations produce a null diagnostic, not fabricated observations.
- Globally missing observed market levels, invalid numeric pins and foreign inherited pins remain typed refusals. Exact caller overrides are rejected. Publication identity binds the official pin through the policy digest.
- Historical declarations, failed round results, artifact-loader contracts and source pins are untouched. The loader is not silently repinned to the new policy.

## Synthetic verification

From `E:\investintell-datalake-workers-bde-el-execution`, with its isolated local environment:

```powershell
.venv\Scripts\python.exe -m pytest tests/test_bond_market_implied_rating.py tests/test_bond_market_implied_rating_worker.py tests/test_bond_market_implied_rating_owner_anchor.py -q
.venv\Scripts\python.exe -m ruff check tests/test_bond_market_implied_rating.py tests/test_bond_market_implied_rating_worker.py tests/test_bond_market_implied_rating_owner_anchor.py
```

Executed 2026-09-30: **212 tests passed**; test-file Ruff passed. The environment was Python 3.12.12, NumPy 2.5.3, pandas 3.0.6, psycopg 3.3.6, pytest 9.1.1 and Ruff 0.16.9. The minimal test environment is local and unversioned; it does not replace the repository dependency/provenance contract.

Full scoped default Ruff over the two source modules remains non-green with four **baseline** findings: `SIM101` in `_canonical_cell`, `S110`/`BLE001` in `_code_revision` and `RUF046` in `int(len(rows))`. Running Ruff over the untouched `HEAD` versions reproduces the same four findings; they were not fixed as unrelated cleanup. No new Ruff finding was observed. Scoped `git diff --check` passed.

The original state-machine tests use an explicitly unpinned historical synthetic policy to preserve their independent bucket expectations; the new [owner-anchor suite](../../tests/test_bond_market_implied_rating_owner_anchor.py) exercises the actual fixed policy, corrections, empty versus globally dark windows, finite validation, exact override rejection, current-pin compatibility, pointer CAS and deterministic identity. These tests use fake DB connections and do not call a real DSN or load credentials. The frozen round-002 UUID regression remains unchanged and passes.

## Not yet established

- Bounded calibration export through 2026-08 and reproduction of the historical `a8b9a3d2…` publication's actual rows digest/anchor.
- Executed G1, G9 and G10 evidence and owner acceptance of the executed round.
- Closed September witnessed volume, elected rating header/readability or current production identity.
- Republication under the new policy digest, final deployed source revision, or economic consumer activation.

Do not invoke the worker `run`/`plan` against a real database as part of these local checks. A future operational action requires its own explicit authorization and evidence. These uncommitted source changes cannot be presented as a deployed revision or an executed calibration round. The two preexisting macro fixture normalization differences are unrelated and remain untouched.
