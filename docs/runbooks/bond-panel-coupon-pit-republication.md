# Runbook — `bond_panel_v1` returns: coupon-PIT republication (audit A2-01)

The ordered procedure for republishing the historical returns surface of the
bond panel under the one coupon convention the resolver now implements
(contractual coupon first, point-in-time expanding median of the price/YTM
inversion otherwise) and the owner's default-flat rule (a bond in a confirmed
default pays no coupon: carry 0 from the default month), as a governed child
publication that extends the current head and keeps every other fact verbatim.

**Every step in §3 that touches production requires explicit owner
authorization, step by step. Nothing in this runbook is authorized by being
written here.** The engineering gates it relies on are the commits of the PR
that introduced it (`fix/bond-panel-coupon-pit`): the resolver fix, the
default-flat rule, the artifact builder, the terms export, the returns
tombstone DDL, and the emitter.

Related: the unit-repair child (T2.1) in `scripts/backfill_bond_panel_history.py`
(the transport this one mirrors), [`bond-market-implied-rating-republication.md`](bond-market-implied-rating-republication.md)
(the same "determinism before writing, digest-bound apply" discipline), the
round-2 applied-math audit report (area A, A2-01).

---

## 1. What changed and why

`src/bonds/panel_resolvers.bond_coupons` used the per-CUSIP **median over the
bond's full history** of the price/YTM coupon inversion whenever a row carried
no contractual `coupon_pct`. Month t's carry therefore used prices from months
after t, and a distressed tail moved the coupon of every earlier month.
Production measurement (read-only, 2026-10-07): every CUSIP-month through
2026-06 had `coupon_pct IS NULL`, i.e. the whole published history
(2002-08 → 2026-06, 2,801,208 returns rows) was priced off that fallback; from
2026-07 the live worker merges `bond_reference_terms.coupon_rate` into
`coupon_pct` and the fallback is inert where terms exist.

The resolver now carries the convention Light adopted for BOND-01
(`app.bond_optimizer.returns.bond_coupons`, branch
`fix/bond-optimizer-pinned-audit` @ `b61f019e`): the finite contractual
`coupon_pct` where the row carries one, else the expanding (months ≤ t) median
of the inversion per CUSIP in month order. The Stage 6 cron is invariant under
the coupon change (its input is one anchor month plus the closed month, and the
contractual coupon is present from 2026-07). It is **not** invariant under the
default-flat rule below: from the deploy on, a closed-month return of a bond in
a confirmed default known to the elected implied-rating publication carries 0
instead of `coupon / 12 / P`. That is the owner's decision applied forward; the
history needs republishing, and that is what this runbook does.

### Default-flat rule (owner decision, 2026-10-07)

A bond in default trades flat and pays no coupon. Inside a confirmed default
window the carry is 0 and the monthly return is the clean-price return alone;
before the window the contractual coupon (or the PIT fallback) applies. One
implementation, `src.bonds.panel_resolvers.default_flat_windows` /
`default_flat`, used by `monthly_returns` (live worker) and by the builder.

- **Source.** The market-implied D state of `bond_market_implied_rating_v1`,
  the same series Light's market EL consumes. The legal-evidence products
  (`bond_default_events` / `bond_credit_evidence_v1`, and the owner-evidence
  bridge) are not used: by their own policy they are diagnostic, never the
  market-PD numerator, they are human-adjudicated and sparse, and no
  historical extract of them exists to rebuild 2002 → 2026.
- **Date field: `d_event_month` of the confirmed episode.** Coupons stop at
  the missed payment or filing, which the event (candidate) month dates, not
  at the confirmation 0–3 months later (the round-002 rows: 619 episodes
  confirmed in the event month, 332 one month later, 42 two, 13 three). The
  first stored D row (the confirmation month, the one Light's market EL
  counts) stands in only where `d_event_month` is null, which the
  publication's CHECK makes unreachable (count recorded in the manifest,
  `event_month_source`). Every artifact names its field: `default_flat.date_field`.
- **What counts.** Only confirmed episodes (`d_confirmed`, i.e. `D` rows): a
  `d_candidate` month that never confirms keeps its coupon, however low the
  price. The implied model has no distressed-exchange or legal-event type, so
  an exchange counts exactly when its prices confirm D.
- **Cure.** The implied model has one: `p_cure` for `n_cure` consecutive
  witnessed months opens a rated spell. The window ends (exclusive) at the
  first witnessed rated row after the episode's last D row, so carry resumes
  at the cure. Losing the witness is not a cure: a `WITHDRAWN`
  (`default_absorbing`), `NOT_RATED` or carried row keeps the window open, so
  a thinly traded defaulted name stays flat until it is re-rated.
- **Point in time.** The live worker (Stage 6) reads the pointed
  implied-rating publication, which Stage 7 rebuilds *after* the panel from
  the previous panel: at closed month t it knows months ≤ t − 1, so month t is
  flat only under a default confirmed by then (`point_in_time` basis: from the
  confirmation month). Stage 7 is default-off; an absent or unpointed product
  leaves the carry contractual, and the publication lineage says which source
  it used (`default_flat_source`, `default_flat_policy_digest`,
  `default_flat_last_month`, `default_flat_closed_rows`).
- **Historical rebuild.** The builder applies realized events
  (`realized` basis: from `d_event_month`), since the history is rebuilt
  knowing the event; the manifest counts the rows that sit between an event
  and its confirmation (`default_flat.rows_before_confirmation`), i.e. what a
  point-in-time basis would still have priced with the coupon.
- **Typed exits** (matured/distressed/unexplained rows) keep their own basis
  and are copied verbatim.

### Stored basis of the history (what the republication replaces)

| Rows | Stored coupon basis | Reproduced by the builder |
|---|---|---|
| 2002-08 → 2025-03 (T3 base, `t3_historical_base_001`) | Light pre-BOND-01 full-history median over all OSBAP months ≤ 2025-03 per CUSIP | yes — `reconciliation` block of the artifact manifest |
| 2025-04 → 2026-06 (return-coverage repair tail, `t3_historical_base_return_coverage_repair_v1`) | the same coupon, backed out of the stored carry (`median(12·carry·prev_price)`), or the first inversion after 2025-03 for entrants | yes — same block |
| 2026-07 → head | live worker, contractual coupon from `bond_reference_terms` | untouched (copied verbatim) |

The builder refuses to write an artifact unless every observed row at or before
the cutoff reproduces BOTH its stored `price_return` (float32-noise tolerance:
the frozen pack stored prices as float32) and its stored carry under that basis.
That is the proof that the inputs are the ones the history was built from.

### What the republication changes

Only `carry_return`, `total_return` and `suspect` (and a `carry_basis` /
`default_event_month` payload key on default-flat rows) on observed rows with
`month <= 2026-06-01`, plus one returns tombstone per dropped key (below). Keys, `price_return`, typed-exit rows, the distribution
identity columns, the payload (plus a `coupon_pit_repair` marker) and every
row after the cutoff are verbatim. The DDL's generic pointer rules admit the
child (same config, same window, parent = current head, dual-series identity);
no trigger branch is added.

## 2. Artifacts, identity, consumers

### Inputs (pinned, already on the operator machine)

The v2 artifact set `C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2`
(the read-only T0.2v export under pointer `71b672c8-…`, re-derived by the unit
repair), pinned by `EXPECTED_SHA256_UNIT_REPAIR_V2`:

| File | sha256 |
|---|---|
| `bond_panel_live.parquet` (3,448,307 rows) | `2bc85774f608aebc57e8e345e49113938549f8f89ae87dee509cdcae8758aba2` |
| `bond_monthly_returns.parquet` (2,810,912 rows) | `a2778b5c723f1d4c1c91e31319dfe2618589a30ce56c60a9d7d5008482f39007` |

The rows at or before the cutoff are invariant across later heads (a Stage 6
child adds only its own closed month; the history always resolves to the
unit-repair child `65156481-…`), which is why an export taken under `71b672c8`
is still the correct input today.

The market-implied default source: the round-002 implied-rating rows
`C:\Users\andre\AppData\Local\investintell\bond_market_implied_rating_round_002\export_20260919T164115Z\round002_cache\baseline_implied_rows.parquet`
(3,375,028 rows, 2002-07 → 2026-08, sha256
`a7fde442abb529f3e6291ba41871e0d96cd3a0aa75d9654d309ec5e3857eed7a`, pinned
in `contracts/bond_market_implied_rating_round002_artifact.json`; policy digest
`28f70b9b…`, producer `8d8513be…`, publication identity `bc13a5e4-…`): 1,006
confirmed episodes on 903 CUSIPs. **Owner call:** HEAD's producer has moved
since round 002 (policy digest `4b752a3f…`; calendar-consecutive cures,
`c12f6cd`), and the round's activation receipt records no deploy or flag
operation, so these rows are not proven to be the publication Light reads
today. The D episodes do not depend on the anchor (only on price, spread,
witness and the default/cure rules), but the cure rule changed. Either accept
the pinned round-002 rows, or export the elected publication's rows read-only
and pass that file instead; the builder records whichever file it used
(sha256, policy digest) and the emitter binds it.

Plus the owner's read-only `bond_reference_terms` coupon export (§3.1) — the
only other input. On 2026-10-07 the table held
10,206 rows with a coupon (9,971 `Fixed`, 42 `Variable`, 193 untyped;
`max(loaded_at)` 2026-08-08). The builder applies `coupon_rate` without a
`coupon_type` filter, exactly as `build_db_monthly_panel` does for the live
panel (one convention), and records the `coupon_type` distribution of the
CUSIPs it used in the manifest. **Whether `Variable` coupons should be excluded
from the contractual branch is an owner call** — the live worker does not
exclude them today; excluding them here only would make the history and the
live tail disagree.

### Outputs (the v3 artifact; `scripts/build_bond_panel_coupon_pit_returns.py`)

`<unit_repair_v2>/coupon_pit_v3/`:

- `bond_monthly_returns.parquet` — same 13 columns and row order as v2;
- `coupon_basis.parquet` — per repriced row: previous price, stored coupon,
  new coupon, coupon basis (`contractual` | `pit` | `none`), `carry_basis`
  (`coupon` | `default_flat`), `default_event_month`, the coupon carry,
  carry before/after, delta in bp;
- `manifest.json` — `contract = returns_coupon_pit_repair_v2`, `mode`
  (`contractual_then_pit_default_flat`; the emitter refuses every other mode:
  `pit_only_preview[_default_flat]`, `contractual_then_pit`), input and
  output digests, the terms export and default-events digests and coverage,
  the resolver's own sha256, the `default_flat` block (rule, basis, date
  field, rows, CUSIPs, rows before confirmation, carry removed), counts
  (`rows_at_or_before_cutoff`, `repriced_rows`, `contractual_rows`,
  `pit_rows`, `default_flat_rows`, `dropped_rows_no_pit_basis`, …),
  reconciliation maxima, the delta distribution and the per-year carry sums
  (`per_year`, `per_year_digest`) the finalize gate compares against.

The default-flat rule changes the carry of every row inside a window, so it
is part of the artifact identity: contract `…_v2`, manifest version `…_v2`,
code revision `t3_returns_coupon_pit_repair_v2`, and the default-events sha256
in the child fingerprint.

### Identity (`scripts/backfill_bond_panel_coupon_pit_repair.py`)

`publication_id = uuid5(bond_panel_v1:coupon-pit-repair:<fingerprint>)` where
the fingerprint binds the contract, the head bound at plan time, the root base
and unit-repair child ids, the config hash `1863d3d5fa3a0edf`, the cutoff, the
three artifact digests, the terms export and default-events digests, the
per-year carry digest, the pinned counts, the dropped keys and the resolver
sha256. `code_revision = t3_returns_coupon_pit_repair_v2`.

**Dropped keys are tombstoned, not omitted.** A key with no coupon basis at or
before its month (and not in default) has no return under the resolver. The
served `bond_panel_current_returns_v1` overlays the ancestry by nearest depth,
so a key the child merely omitted would be served from the head, with its
look-ahead carry. The child therefore writes one `bond_panel_returns_tombstone`
row per pinned dropped key; the view hides every row of a tombstoned key at the
tombstone's depth or deeper (a later child that publishes the key again is
served normally). The frozen authorization is
`COUPON_PIT_EXPECTED_ARTIFACT` (step 3.3); while it is `None` the emitter
refuses with `coupon_pit_artifact_unpinned`.

### Downstream consumers of the historical rows

| Consumer | Reads | After republication |
|---|---|---|
| Light `backend/app/repositories/bond_panel.py` (`_HISTORY_RETURNS_SQL`) → `services/bond_quality_v1.py` (`_signal_joined`, `factor_returns` → `bond_factor_returns_v1`, `factor_returns_digest`, the `snapshot_rv_returns_rating_watermark…` identity), `services/bond_backtest.py` (`expanding_rv_beta`, folds), `services/bond_recommendation_refresh.py` | `bond_panel_current_returns_v1_mat` | **re-run the bond recommendation refresh** (quality, factor returns, backtest); the pinned factor-returns digest and the quality publication identity move |
| Workers `bond_panel_current_returns_v1_mat` | refreshed by the finalize step and by the daily chain | nothing to re-run |
| Workers implied rating (`src/bonds/implied_rating.py`), EL anchor, serving (`serving_materializer`) | `spread_final_bps`, `mod_dur`, prices — never the carry | not affected; `contracts/bond_market_implied_rating_round002_*` pin `bond_panel_live.parquet` only. The dependency runs the other way: the panel returns read the implied D state (default-flat), never the reverse, so there is no cycle |
| Workers `fund_factors` / `factor_model` | NAV returns, not bond returns | not affected |

## 3. Procedure (ordered; each production step needs its own owner authorization)

### 3.0 Preconditions (no production action)

- The PR is merged to `main` and the workers services are redeployed from it
  (merging workers `main` redeploys git-connected crons). The Stage 6 cron has
  run at least once on the new resolver with an unchanged publication
  (expected: the same-month short-circuit, or a child whose returns rows equal
  the previous run's).
- The operator can run `psql` against the private database the way the T3
  base and the unit repair were applied (`railway ssh` into the serial worker,
  `psql -X -q -v ON_ERROR_STOP=1 -f -` on stdin; see the T0.2v export manifest
  method notes), in a quiet panel-write window (no Stage 6 run, no Light
  refresh: outside 05:00–09:00 UTC).
- The v2 artifact directory above is present and its digests match (the
  builder refuses otherwise).
- The returns tombstone DDL is applied (§3.0b); prepare and finalize refuse
  otherwise (`… requires the returns tombstone DDL`).
- **The emitted SQL has been shape-tested only** (string assertions in
  `tests/test_backfill_bond_panel_coupon_pit_repair.py`); it has not yet run
  against PostgreSQL. Before step 3.5, the pin follow-up (3.3) runs the full
  sequence (`--emit-schema` from `backfill_bond_panel_history.py`, a
  synthetic head and a synthetic artifact built by the builder's test
  fixture, prepare → copies → batches → finalize) against a throwaway
  `timescale/timescaledb` container and attaches the psql transcript. The
  finalize DO block (temp tables, the per-year carry upsert, the FULL JOIN
  key gate) is the part to prove there.

### 3.0b Apply the tombstone DDL — PRODUCTION DDL, OWNER AUTHORIZATION REQUIRED

```powershell
$env:PYTHONIOENCODING = 'utf-8'   # the DDL has non-ASCII comments; a cp1252 console fails without it
python scripts\backfill_bond_panel_history.py --emit-schema | psql …
```

Idempotent and additive: creates `bond_panel_returns_tombstone` (insert-only
while its publication is prepared, never beside a returns row of the same key
in the same publication) and replaces `bond_panel_current_returns_v1` with the
same columns plus the tombstone filter. No existing publication has a
tombstone, so the served rows do not change; verify with
`SELECT count(*) FROM bond_panel_current_returns_v1` before and after (equal)
and refresh nothing. Nothing in the cron path installs this file. Proven
locally on PostgreSQL 16 (2026-10-07): the pre-change file applied, then this
one (the view is replaced under its existing `*_mat` dependent), then this one
again, all exit 0; `tests/test_bond_panel_returns_tombstone_db.py` covers the
semantics.

### 3.1 Owner export of the contractual coupons — READ-ONLY (owner runs it)

From `E:\tmp-deploy\api\backend` (the railway-linked checkout), outside
06:00–08:30 UTC:

```powershell
railway run --service risk-metrics -- uv run --no-project --with "psycopg[binary]" `
  python E:\investintell-datalake-workers-sep\scripts\export_bond_reference_terms_coupons.py `
  --out C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\terms_export_YYYYMMDD
```

The script rewrites the DSN to the public proxy, opens one
`default_transaction_read_only=on` session with a 30 s statement timeout,
backs off if a long active transaction exists, runs one LIMIT-bounded `SELECT`
(`cusip9, coupon_rate, coupon_type, maturity_date, batch_label, loaded_at`
where `coupon_rate IS NOT NULL`), refuses if the count differs from
`count(*)`, and writes `bond_reference_terms_coupons.csv` plus a `.sha256`
sidecar. Keep the printed `rows=… max_loaded_at=… sha256=…` line with the
evidence.

### 3.2 Build the v3 artifact — OFFLINE (no database)

From the merged `main` checkout, one process at a time (the build holds the
3.4M-row panel and the 2.8M-row returns in memory; several minutes):

```powershell
python scripts\build_bond_panel_coupon_pit_returns.py `
  --artifact-dir C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2 `
  --terms C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\terms_export_YYYYMMDD\bond_reference_terms_coupons.csv `
  --default-events C:\Users\andre\AppData\Local\investintell\bond_market_implied_rating_round_002\export_20260919T164115Z\round002_cache\baseline_implied_rows.parquet `
  --out C:\Users\andre\AppData\Local\investintell\bond_panel_unit_repair\export_20260918T202403Z\unit_repair_v2\coupon_pit_v3
```

Review `manifest.json` before anything else happens:

- `mode == contractual_then_pit_default_flat`; `default_flat.date_field ==
  d_event_month`, `inputs.default_events` (sha256, policy digest, episodes,
  `event_month_source` all `d_event_month`), `default_flat.rows` / `cusips`
  and `rows_before_confirmation`;
- `counts.dropped_rows_no_pit_basis` and
  `dropped_keys`: rows whose CUSIP has no finite inversion up to that month
  and no contractual coupon get no return row (the resolver's own outcome —
  the stored history priced them off later months). The PIT-only preview
  drops 16 rows, all `49306SAA4` 2004-03 … 2006-02; a contractual coupon
  for that CUSIP removes the drop. The keys are pinned (digest) and the
  finalize gate admits exactly those absences; more than 1,000 refuses;
- `price_return_reproduction.max_relative_diff` ≈ 1e-7 and
  `reconciliation.max_abs_diff` ≈ 6e-6 coupon points (float32 noise of the
  frozen pack; the reconciliation's *relative* maximum is large only on
  near-zero coupons and is covered by the absolute floor);
- `counts.cusips_with_contractual_coupon` vs `counts.cusips_in_scope`, and
  `inputs.terms_export.coupon_type_distribution_with_coupon` (the `Variable`
  question above);
- the delta distribution (`delta_bp`, `per_year`): the expectation from the
  production sample is sub-bp on ordinary names and tens to hundreds of
  bp/month on distressed names with a contractual coupon far above the
  price-implied one.

### 3.3 Pin the artifact — follow-up commit (owner review, quiet-window merge)

Fill `COUPON_PIT_EXPECTED_ARTIFACT` in
`scripts/backfill_bond_panel_coupon_pit_repair.py` from the manifest:

```python
COUPON_PIT_EXPECTED_ARTIFACT = {
    "artifact_sha256": {"bond_monthly_returns.parquet": "…", "coupon_basis.parquet": "…", "manifest.json": "…"},
    "terms_export_sha256": "…",   # inputs.terms_export.sha256
    "default_events_sha256": "…", # inputs.default_events.sha256
    "per_year_digest": "…",       # per_year_digest
    "dropped_keys_digest": "…",   # dropped_keys_digest
    "counts": {"returns_rows_out": …, "rows_at_or_before_cutoff": …, "rows_after_cutoff": …,
               "scope_rows": …, "repriced_rows": …, "exit_rows_at_or_before_cutoff": …},
}
```

and flip `test_authorization_constants_are_frozen_until_the_artifact_exists`
to assert those values. Every key above is required: a missing one refuses
with `coupon_pit_pin_keys_missing:<key>` (the template once lacked
`dropped_keys_digest`, and `--plan` raised a bare `KeyError`). Merge in the operator's quiet window (merging workers
`main` redeploys git-connected crons); nothing in the deploy reads the pins.

### 3.4 Plan — LOCAL, read-only

```powershell
python scripts\backfill_bond_panel_coupon_pit_repair.py --plan --from-head <current pointer publication_id>
```

`<current pointer>` is `SELECT publication_id FROM bond_panel_app_pointer WHERE
product = 'bond_panel_v1'` at the start of the quiet window. The evidence JSON
carries the deterministic child id; record it. If the pointer moves before
3.8 (a Stage 6 run), every emitted transaction refuses
(`… requires the expected head pointer`): re-plan from the new head, nothing
else changes.

### 3.5 Prepare — PRODUCTION WRITE, OWNER AUTHORIZATION REQUIRED

`--emit-prepare` | psql. Verifies live: pointer = head; head validated,
config `1863d3d5fa3a0edf`, window `2002-07-01 … > 2026-06-01`, open month =
closed + 1; ancestry reaches the unit-repair child and the frozen root; the
current view holds exactly `rows_at_or_before_cutoff` returns rows at or
before the cutoff. Inserts the `prepared` child with the live counts of the
three verbatim surfaces and `rows_at_or_before_cutoff − dropped + (view rows
after the cutoff)` returns rows, and one `bond_panel_returns_tombstone` row per
pinned dropped key (the tombstone set must equal the pinned keys). Idempotent;
never moves the pointer.

### 3.6 Verbatim copies — PRODUCTION WRITES, OWNER AUTHORIZATION REQUIRED

`--emit-copy snapshot`, `--emit-copy rv_signal`, `--emit-copy rating_pit`,
`--emit-copy returns` (months after the cutoff only), each | psql. Each
transaction re-checks the pointer and the prepared child, inserts from the
current view with the dual-series identity fill, and gates: count equals the
declared count, every data column equals the view row.

### 3.7 Artifact batches — PRODUCTION WRITES, OWNER AUTHORIZATION REQUIRED

```powershell
python scripts\backfill_bond_panel_coupon_pit_repair.py --emit-batch --from-head <head> --start-after 0 --limit 100000 | psql …
```

then `--start-after 100000`, … until the returned evidence says
`"done": true` (`rows_at_or_before_cutoff` rows, ≈ 28 batches at 100,000).
Each batch is one COPY transaction, idempotent (`ON CONFLICT DO NOTHING` after
an immutable-evidence preflight), replay-safe after finalize, and bound to the
artifact sha256. The artifact payload is kept and a `coupon_pit_repair`
marker is added.

### 3.8 Finalize — PRODUCTION WRITE, QUIET WINDOW, OWNER AUTHORIZATION REQUIRED

`--emit-finalize` | psql. One timed transaction (`lock_timeout 5s`,
`statement_timeout 55min`) with the four fact tables in SHARE mode: counts
per surface equal the declaration; returns span `2002-08-01 … head closed
month` contiguously; dual-series identity and coverage gates (as the unit
repair); per month, the child's returns keys equal the head projection's,
`price_return`/`exit_basis`/`exit_reason` are identical per row, rows after
the cutoff are identical in full, repriced rows carry the marker and satisfy
`total_return = price_return + carry_return` (1e-12) and
`suspect = |total| > 0.5`; per year, rows and the carry sums before AND after
equal the artifact manifest (1e-6); the child's tombstones equal the pinned
dropped keys. Then `prepared → validated`, pointer CAS head → child, and,
still inside the transaction, the served view is checked: no dropped key is
served and `count(*)` equals the child's declared `returns_rows` (otherwise
the CAS rolls back). After COMMIT the four `*_mat` refreshes in the frozen
order (own 20 min timeout; cannot undo the CAS).

### 3.9 Verification — READ-ONLY

- `bond_panel_app_pointer` points at the child; the child is `validated`;
  `bond_panel_current_returns_v1_mat` count equals the child's `returns_rows`,
  and none of the pinned dropped keys is in it.
- Spot CUSIPs from the production sample: `29078EAA3` (7.995 % contractual
  coupon, ≈ the stored one, until its default; carry 0 from its
  `d_event_month` 2024-06), `87952VAM8` (6.5 % contractual coupon until its
  default; carry 0 from its `d_event_month` 2022-11, see §4.3), `00077TAB0`
  (no terms, no default: PIT basis, unchanged within 0.01 bp).
- Run the Light bond recommendation refresh (§2) and confirm the new
  `factor_returns_digest`.

### 3.10 Rollback

**PRODUCTION WRITE, QUIET WINDOW, OWNER AUTHORIZATION REQUIRED.** Restore the
parent's data by publishing a new rollback child of the repair child. The
pointer guard requires forward ancestry: a direct `child → head` update is
rejected. The existing trigger remains enabled and unchanged for rollback.

Record the owner's approval reference, the repair child and its original head,
and the reviewed Git SHA containing the rollback script in the incident/change
record. Use the same authenticated `psql` connection as steps 3.5–3.8. From the
reviewed checkout, run the following command after replacing every `<…>` value
(the connection string belongs in the operator's environment, not in Git):

```powershell
$env:PGDATABASE = '<approved PostgreSQL connection string>'
psql -X -v ON_ERROR_STOP=1 `
  -v failed_child='<repair child publication_id>' `
  -v restore_parent='<original head publication_id>' `
  -v authorization='<owner approval or change-record reference>' `
  -v code_revision='<reviewed 40-character Git SHA>' `
  -f scripts/rollback_bond_panel_coupon_pit.sql
if ($LASTEXITCODE -ne 0) { throw 'Rollback or materialized-view refresh failed; inspect the psql output before retrying.' }
```

The checked-in script takes the pointer lock, requires the pointer still to
name the validated coupon-PIT repair child, and verifies its direct parent,
matching config/window, and recorded repair evidence. If Stage 6 has advanced
the pointer, it refuses: do not substitute the newer pointer into this procedure;
prepare a separately reviewed restoration plan for that newer window.

Within one transaction it projects the original head's complete ancestry,
including ancestor tombstones, and copies all four surfaces into a new child
of the repair child. Legacy distribution identity receives the same fill used
in step 3.6. Restored return rows take precedence over the repair's tombstones;
any returns present only on the repair are tombstoned on the rollback child.
The rollback publication records the owner approval reference, reviewed SHA,
source parent fingerprint, failed child and restoration target in immutable
lineage and gate evidence. That evidence determines its SHA-256 fingerprint
and publication UUID. It validates the child, advances the pointer
`repair child → rollback child`, and checks every served column and key against
the restored parent projection before committing. Any mismatch aborts the
entire transaction. No immutable history is changed.

After COMMIT the script refreshes the four `*_mat` views in the frozen order.
Retain the reported rollback publication ID and fingerprint with the psql log.
The operation is atomic and may be retried after a pre-commit failure. If the
pointer already names the rollback child, do **not** rerun publication: a
post-commit refresh failure cannot undo the pointer move. Run only these
refreshes using the same connection:

```powershell
@'
\set ON_ERROR_STOP on
SET ROLE worker_writer;
SET statement_timeout = '20min';
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;
RESET statement_timeout;
RESET ROLE;
'@ | psql -X
if ($LASTEXITCODE -ne 0) { throw 'Rollback materialized-view refresh failed.' }
```

Verify the pointer and all four materialized-view counts against the rollback
publication's declarations, confirm the original dropped keys are restored,
and re-run Light's recommendation refresh (§2) to replace its cached factors.

## 4. Evidence

### 4.1 Production sample (read-only, 2026-10-07 13:50–14:40 UTC, `bond_panel_current_*_v1_mat`, 7 CUSIPs, 804 snapshot rows, 728 returns rows)

Stored carry reproduced exactly (max |12·carry·prev − stored coupon| = 0.0 on
all 722 fallback rows); `before` = stored carry; `after` = contractual coupon
where `bond_reference_terms` carries one (6 of 7), else PIT.

| CUSIP | months | fallback rows | min price | terms | stored coupon | PIT range | Δcarry bp/mo mean (min … max) | cum. total return before → after |
|---|---|---|---|---|---|---|---|---|
| 29078EAA3 (distressed) | 246 | 241 | 14.41 | 7.995 Fixed | 7.9939 | 7.9939 … 7.9949 | +0.01 (+0.01 … +0.04) | 163.68 % → 163.71 % |
| 87952VAM8 (distressed) | 86 | 81 | 28.28 | 6.500 Fixed | 1.3978 | 1.3978 … 6.5012 | **+84.17 (+40.05 … +150.33)** | 15.81 % → 83.99 % |
| 00033GAA3 | 34 | 29 | 83.66 | 8.375 Fixed | 8.3805 | 8.376 … 8.391 | −0.05 | 25.56 % → 25.55 % |
| 00033GAB1 | 34 | 29 | 77.38 | 8.750 Fixed | 8.7516 | 8.745 … 8.755 | −0.01 (−0.02 … −0.01) | 23.99 % → 23.98 % |
| 00037BAC6 | 171 | 163 | 81.95 | 4.375 Fixed | 4.3748 | 4.374 … 4.375 | 0.00 | 58.61 % → 58.61 % |
| 00037BAF9 | 104 | 99 | 93.65 | 3.800 Fixed | 3.8001 | 3.799 … 3.801 | −0.00 | 29.32 % → 29.32 % |
| 00077TAB0 | 129 | 80 | 69.31 | none → PIT | 7.1245 | 7.1243 … 7.1247 | −0.00 (|max| 0.00) | 136.58 % → 136.58 % |

Reading: the audit's windowed estimate for `29078EAA3` (+9.2 bp/mo) was an
artifact of starting the median at 2023-06; over the full history the PIT
median and the contractual coupon agree with the stored one to 1 bp of
coupon. The material move is `87952VAM8`: its stored implied coupon (1.4 %)
is the inversion of a deeply distressed price, while the contractual coupon
is 6.5 % — under the owner convention its carry rises by 40–150 bp/month
(PIT-only would already add +56.5 bp/mo on average). This is the known
limitation of `coupon/12/P` carry on names whose coupons may not be paid; the
convention is the owner's, and the basis is now declared per row in
`coupon_basis.parquet`.

### 4.2 Full-scale preview (PIT-only, offline, the pinned v2 production export, 2026-10-07)

`scripts/build_bond_panel_coupon_pit_returns.py --out …` without `--terms`
(the `pit_only_preview` the emitter refuses; pandas 3.0.1 / numpy 2.2.6 /
pyarrow 23.0.1):

- scope: 2,801,208 observed rows ≤ 2026-06 (no typed-exit rows in that
  range), 64,053 CUSIPs; 9,704 rows after the cutoff verbatim; **every row's
  `price_return` reproduced (max 1.1e-7 relative) and every row's stored
  carry reproduced under the stored basis (max 6.3e-6 coupon points)**;
- 2,801,192 rows repriced on the PIT basis, 16 dropped (above);
- 2,345,970 rows move by more than 1e-4 bp/mo; **95.4 % move by < 0.1 bp/mo,
  99.3 % by < 1 bp/mo**; p50 |Δ| 0.0025 bp, p90 0.040 bp, p99 0.59 bp, mean
  +0.35 bp/mo; `suspect` rows 3,245 → 3,246;
- the tail: 5,248 rows > 10 bp, 1,333 > 100 bp, 123 > 1,000 bp (max
  +67,237 bp, min −4,512 bp). These are defaulted names at previous prices
  of 0.006–50 % of par (p50 16, p90 49): `coupon/12/P` explodes as P → 0,
  the stored full-history median had often clipped their coupon to 0 (37 %
  of the > 100 bp rows), and the first PIT months rest on one or two
  inversions. Almost all are already `suspect`. The contractual convention
  replaces the inversion, not the `coupon/12/P` form; a carry floor for
  defaulted prices is an owner policy question, not part of A2-01;
- by year, the mean move is +0.5 … +4.7 bp/mo in 2002–2005 and 2008–2010
  (the distressed cohorts) and ≤ 0.35 bp/mo elsewhere; p99 |Δ| is below 2 bp
  from 2006 on.

With the owner's terms export, rows of CUSIPs that carry a contractual
coupon take it instead of the PIT median (6 of the 7 sampled CUSIPs do); the
manifest reports `contractual_rows` / `pit_rows` and the per-year carry
sums the finalize gate compares.

### 4.3 Default-flat preview (offline, the pinned v2 export + the round-002 implied rows, 2026-10-07)

`scripts/build_bond_panel_coupon_pit_returns.py --default-events …baseline_implied_rows.parquet --out <scratch>`
without `--terms` (mode `pit_only_preview_default_flat`, a preview the emitter refuses; same
runtime as §4.2). Read-only; nothing written to production or to the v2
directory.

- Source: 1,006 confirmed episodes on 903 CUSIPs
  (`event_month_source`: d_event_month 1,006; the
  first-D-row fallback was never used); 347 cured,
  659 open (withdrawn or still D); confirmation lag after
  the event month: 0 mo: 619, 1 mo: 332, 2 mo: 42, 3 mo: 13.
- **Rows with carry 0: 14,727 on 903 CUSIPs** of
  2,801,208 in scope (none lacked a coupon basis: the 16 `49306SAA4` drops of
  §4.2 are not a defaulted name and stay dropped, tombstoned in the child);
  438 sit between their event month and the
  confirmation (what a point-in-time basis would still have priced with the
  coupon). Previous prices of the flat rows: p10 14.3,
  p50 44.93, p90 86.66.
  869 of them were already `suspect`. Carry removed
  per flat row versus PIT-only: p50 128.8, p90 426.3, p99 3183.4 bp/mo
  (the mean is meaningless: `00208JAE8` 2025-10 has a previous price of 1e-6 and a
  stored carry of 9.84e+09 bp, now 0 — a price-data defect the rule
  happens to neutralize).
- **The §4.2 tail** (1,333 rows whose PIT carry moved > 100 bp/mo from the
  stored one): 520 now carry 0 (50 of
  162 CUSIPs); 813 keep a coupon because the
  implied model never confirmed a default at that month (previous price p50
  14.491, p90 55.598; thinly traded
  names it does not witness, or months outside a D spell). Their carry p50 falls
  from 373.4 to 163.6 bp/mo.
- **Carry level** (stored → PIT-only → PIT + flat): > 1,000 bp/mo
  977 → 1,094 → **587**; > 500 bp/mo
  2,435 → 2,717 → **1,568**; > 200 bp/mo
  9,485 → 10,255 → **6,046**; previous price < 50
  with carry > 100 bp/mo 20,510 → 21,307 →
  **13,810**. `suspect`: 3,245 → 3,209.
- |Δ carry| versus the stored history is no longer a tail measure: a flat row's
  delta is its whole stored carry, so rows > 100 bp go 1,333 (PIT-only) →
  9,762 (with the rule) by construction.
- Rows after the cutoff (published live before this change, copied verbatim
  by the republication): 9,704 (2026-07-01, 2026-08-01), of
  which 18 on 16 CUSIPs sit inside a
  default window and keep their coupon carry (mean 131.7 bp/mo).
  From the deploy on, Stage 6 applies the rule to each new closed month.

The PIT-only side reproduces §4.2 exactly (5,248 / 1,333 / 123 rows above
10 / 100 / 1,000 bp): same inputs, same resolver.

Spot CUSIPs (cumulative total return over every v2 row of the CUSIP; the
contractual columns apply the coupon from §4.1 to the rows ≤ 2026-06). The
§4.1 sample was read on 2026-10-07 from a later head with more months after
2026-06, so its levels (`87952VAM8` 15.8 % → 84.0 %) differ from these:

| CUSIP | rows | event month | flat rows | prev. price when flat | stored | PIT-only | PIT + flat | contractual | contractual + flat |
|---|---|---|---|---|---|---|---|---|---|
| `87952VAM8` | 2019-10 → 2026-07 (82 rows, 1 after the cutoff verbatim) | 2022-11 | 41 | 28.28–52.38 | -12.52 % | 38.69 % | -3.69 % | 72.41 % | **-2.80 %** |
| `29078EAA3` | 2006-06 → 2026-07 (242 rows, 1 after the cutoff verbatim) | 2024-06 | 25 | 21.62–56.30 | 103.50 % | 103.51 % | 33.41 % | 103.56 % | **33.43 %** |
