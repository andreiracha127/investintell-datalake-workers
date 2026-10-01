# Result — round 003: G1 / G9 / G10 under the owner amendment (market-implied bond rating v1)

**Status at release (14:08): `needs-decision` (owner). Update 14:35: DG-4 recorded — ACCEPTED WITH
CONDITIONS (see final section; condition met in §14).** The three blocking readings were executed with a
recorded harness. **G1(a) PASS · G1(b) `NOT_REPRODUCIBLE_IN_THIS_ENVIRONMENT` · G9 PASS ·
G10 not exact (`pending-owner-decision`, D-1).** Nothing was published, pinned or moved.
This file is append-only and new: the declaration, round-001/002 results, the owner
amendment and the historical harness are untouched. Only the owner records DG-4.

- Executed: 2026-10-01, 11:15–14:06 (America/Sao_Paulo, UTC-03:00).
- Authority: `bde_g1_g9_g10_handoff_2026-10-01.md` (task scope), owner decisions
  `docs/planning/bond-default-el-owner-decisions-2026-09-30.md` (Light), owner amendment
  `docs/calibration/bond_market_implied_rating_owner_amendment_2026-09-30.md` (Workers).
- Work root (all outputs): `C:\Users\andre\AppData\Local\Temp\bde-g1-g9-g10-20261001-1115`.

## 0. Gate table

| Gate | Reading | Evidence |
| --- | --- | --- |
| **G1(a)** determinism | **PASS** | Two builds in distinct, sequential, fresh processes (pid 59636, then 66400) from the same export: identical `rows_digest` `b42b110a…ab26`, 3,375,473 rows, 34 raw 100k-row partitions `DataFrame.equals` with identical dtypes (and identical pickle-partition SHA-256s), parquet round-trip digest exact, and the two parquet files are byte-identical (SHA-256 `17713367…a902`). Same source, input fingerprint, pin and numeric stack. |
| **G1(b)** historical reproduction of `a8b9a3d2` | **`NOT_REPRODUCIBLE_IN_THIS_ENVIRONMENT`** | Rebuilt `rows_digest` `b42b110a…` ≠ historical `0aece290…`. Row count equal (3,375,473). Input fingerprint equal (`7106396a…`). Source `b8b9503d` and policy `28f70b9b…` equal. Localised in §4: no key or discrete-column difference; 637 float cells in two columns differ by ≤ 1.78e-15. No rounding, re-pin or digest substitution was applied. |
| **G9** positive control / default capacity | **PASS** | 2021-09..2026-08: 1,138 `d_confirmed` rows, 173 `d_candidate` rows (≥1 each); identical on rebuilt and stored rows. Exposure/floor table in §5. |
| **G10** anchor reproducibility | **not exact — `pending-owner-decision`** | Median of `L` over witnessed months of 2023-09..2026-08 = `-0.8864114120812479`; pin `-0.8864114120812487`; \|diff\| = 8.88e-16; 34 of 36 months observable (2026-07, 2026-08 dark). Within the code's 1e-9 tolerance, which is reported as diagnostic only. Identical on rebuilt and stored rows. |

DG-4 acceptance: **ACCEPTED WITH CONDITIONS** by the owner at 2026-10-01 14:35 -03:00 — recorded
verbatim in the final section; condition (parse-path re-run) met in §14.

## 1. Identities, environment, harness

| Item | Value |
| --- | --- |
| Producer source (G1) | Workers `b8b9503d25739b9a68a2e93f69b322e69a78cbc8`, detached worktree under the work root; `src/bonds/implied_rating.py` sha256 `3cc1e76068bb2e14a0e3d6221ff807cfbb44cf31483cf44daa31c1d66b341c99`, `…/implied_rating_materializer.py` `ab4d91dff431e57154bdb5056169774ee1b2f255fa830ca9defe251b1e0e4d7e`, `…/workers/bond_market_implied_rating.py` `1fe8ffa4296373359d4975bd3bc3c0bd4bb95228c7fc3bc10499836be83e16c0`. Git diff of the three files against the revision is empty. Two macro fixture JSONs show CRLF-only drift (`git diff --ignore-space-at-eol` is empty); unrelated. |
| Policy digest | historical `28f70b9bd8f617fedf6104deb86cd518d43307e88b3bbd1fcf53aa1fae869a3b`. The fixed-anchor digest `4b752a3f…` was **not** used. |
| Historical publication | `a8b9a3d2-03a9-5c82-bf21-f9d1b9298803`, rows_digest `0aece2902b6b28091192b251190e1d309d3b2866638158b4f99a6ac9d63aa462`, input_fingerprint `7106396a53ad4b3f8991ef10cb251b10060a59835ca26d5cd749d3099473c61a`, 3,375,473 rows (ledger read 14:05Z). |
| Inherited pin | `-0.8864114120812487` (hex `-0x1.c5d7b773615a6p-1`). Offline UUID5 proof (`identity_proof.json`): `publication_id_for(policy, FULL 40-char revision, fingerprint, inherited_l_anchor=pin)` equals `a8b9a3d2…`; omitting the pin does not. `2801b9d3` and `bddb0e8e` match the same way; `bc13a5e4` matches only without the suffix. `builds.l_anchor` itself is not readable by `app_runtime` (no SELECT), so the pin is **inferred from identity, not read**. |
| Local stack | Python 3.13.12, numpy 2.5.1, pandas 3.0.3, scipy 1.18.0, pyarrow 25.0.0 (Workers `.venv` interpreter, `-B`, no installs). Intel Core Ultra 9 275HX, 24 cores, 31.4 GB, Windows 11. |
| Production Workers stack | **UNKNOWN.** The `bond-live-daily` container was exited and was not woken; `requirements.txt` is unpinned. The API container (Python 3.12.14, numpy 2.4.6, pandas 3.0.3, scipy 1.17.1) is the API image, **not** evidence of the Workers image. |
| Harness | `replay-tool/gate_replay.py` sha256 `777580c0b98715c98484aa13b81f1e8f49cc9d86bf2874194bca87c4eb28a9d2` (44,696 B; last modified 12:01, before the 12:52 builds); tests `test_gate_replay.py` `7405c5252bb9e4715647040a9820535767d5f4ed13b42dbb16c26e5d110eb66b`. The historical `calibration_harness.py` (`e9295a4d…1b51`) was **not** executed: its CLI writes to historical paths, evaluates the old G1–G10 conjunction, runs no `l_anchor` pin and its G10 admits carried dark months. |
| Hazard code | Light `a0ba875b` pure functions (`el_hazard.py` `3f38ddda3ae83b42b519eb0ea941a21181b2f90cb308724aa13a9b26c72b71d5`, contract `e331e17080a350f3241ce8597b7a20ed90c9a96b8548ef8c1c031c1e4dffa94e`) loaded unchanged by AST, with a validated historical-policy row adapter (the production `MarketRatingRow` accepts only the new digest). Old rows were never restamped. Differential check against the normally-imported production model on 9 separately built synthetic fixtures plus PAVA: exact equality including float hex (`hazard_equivalence_check.json`). |

## 2. Export identity (read-only)

- Method: bounded `SELECT`s through `railway ssh` on the `api` service, role `app_runtime`,
  `transaction_read_only=on`, `lock_timeout=5s`, `statement_timeout=45s`, one year/dataset per
  fresh READ COMMITTED transaction; locks released before hashing/transport. No DDL, no write,
  no variable or credential read, no `worker.run()`.
- Source: `bond_panel_current_snapshot_v1_mat` (8 producer columns, `month <= 2026-08-01`) and
  `bond_market_implied_rating_v1` rows of `a8b9a3d2`. Numerics exported as exact `str(Decimal)`
  text (scale preserved), NULL as `\N`, floats as Python `repr`; no pandas parsing at export.
- Pointers sampled before (12:39) and after (12:51): panel `2bce7901-cf94-5c57-ad7c-f09cc0a8053c`
  (changed 09:50:14Z), rating `bddb0e8e-b46b-5060-bb88-cdc4cd6e4f29` (changed 10:46:56Z),
  unchanged in both samples and in the ledger read at 14:05Z. Mirror serves the captured head;
  historical header re-read both times and equal to the pinned identity.
- **Attempts:** attempt 1 died at `snapshot-2024` (`railway ssh` exit -1) after 44 slices, attempt 2
  at `snapshot-2016` (exit 1) after 28 slices; both had no remote/protocol/pointer error, were
  discarded by design (`INVALID` manifests kept as evidence) and re-run from scratch. Attempt 3 is
  `VALID`. A bounded transport-only retry (≤2 per slice) was added after attempt 2 and unit-tested;
  attempt 3 did **not** need it (`transport_retries` absent). Slice SHA-256 matched across all
  three attempts for every slice they shared.
- Result: snapshot 3,438,099 rows (equals the round-002 export row count), stored rows 3,375,473
  (equals the publication header). PIT/static benchmark exports were **not** taken: G1/G9/G10 do not
  use them.

| File | Rows | Bytes | SHA-256 |
| --- | --- | --- | --- |
| `snapshot.csv` (projected, 8 columns) | 3,438,099 | 318,020,723 | `69b045e91c97053ac76d5d6580dccfdc561f71c979b5cb7964263be4f268d78a` |
| `stored_rows.csv` (projected, 16 columns) | 3,375,473 | 686,682,573 | `8f5a40d6cf35fb4e1e7fe979726dea4b401551a520a64f9e6ac4e46f80844f4e` |
| `export_manifest.json` (merged) | | 858,267 | `3351afeac8d28dba5eaa6ad14e28fd0cfdd7cafaa8f0514e5ddee0958249ca40` |
| `source_manifest.json` (raw acquisition) | | 777,111 | `42909ca76ee883387335194690ae475efdc285e9d873ff31664fa7b7285088bc` |

The stored rows re-hash (unmodified producer `rows_digest`) to `0aece290…` with 3,375,473 rows:
`VERIFIED_STORED_ROWS`. This verifies the export of the stored rows; it is **not** an
independent build and is not counted toward G1(a).

## 3. Input tie to the historical production inputs

- The fresh export, loaded exactly like the worker (`csv.reader`, `Decimal`, `datetime.date`,
  `int|None` → `DataFrame(list_of_tuples)`), gives the unmodified `snapshot_fingerprint`
  `7106396a53ad4b3f8991ef10cb251b10060a59835ca26d5cd749d3099473c61a` — equal to `a8b9a3d2`.
  Recorded in `run-003/build_a/before_build.json` before the build started.
- The archived round-002 `snapshot.csv` (`e9602304…3031`) read the same way gives the same
  fingerprint (`historical_input_diagnostic.json`), whereas the round-002 CSV/pandas path gives
  `620760bf…` (the `bc13a5e4` value). The difference is **type canonicalisation**
  (`Decimal` → `str` keeps scale; CSV float → `repr`), not input drift, for the 8 stage columns.
- The handoff's step 3 (pandas-CSV fingerprint against `7106396a…`) cannot match by construction.

## 4. G1(b) — localisation (diagnostic; never a tolerance)

Stored rows vs rebuilt rows, per month, using the producer's own canonical cell encoding:

- 290 months; **174 differ**, first 2002-07, last 2026-05.
- Keys: 0 stored-only, 0 built-only.
- Columns with any difference: only `spread_norm_log` (320 cells, 174 months) and
  `neutralized_score` (317 cells, 173 months). Identical in every cell: `implied_bucket`,
  `market_level_l`, `witnessed`, `carry_months`, `spell_id`, `d_candidate`, `d_confirmed`,
  `d_event_month`, `recovery_observed`, `censoring`, `policy_*`.
- All 637 differing float pairs are finite with max \|Δ\| = 1.776e-15 (reported against 1e-12
  as a diagnostic only; **not** used to pass the gate).

Reading, without causal claim: the difference is confined to the two columns computed through
`log`, at the last-bit level, in ~0.01% of rows. Input fingerprint, source bytes, policy digest
and pin identity are proven equal, so an input or algorithm difference is not indicated by this
evidence. A numeric-implementation difference (numpy/CPU) is *consistent* with the pattern but
is **not established**: the production Workers stack is unknown and no run on it exists. The
provenance review (not re-run here) reproduced the round-002 anchors on two local stacks
(Py3.12/numpy 2.5.3 and Py3.13/numpy 2.5.1); that concerns the anchor, not these cells.

## 5. G9 — positive control and capacity (declared §7 contract)

Row flags over 2021-09..2026-08: `d_confirmed` rows 1,138, `d_candidate` rows 173; distinct
confirmed events in that window 102. Capacity estimator over the owner-approved 61 endpoints
2021-08..2026-08 (60 intervals): 85 confirmed episodes, 17 left-censored, 1 pending candidate.
Jeffreys `(D+0.5)/(E+1)`, weighted PAVA (weights `E+1`, fitted on `E>0`), LGD 0.60 (analytical,
not realised). HY floors (BB 0.000167, B 0.000837, CCC 0.00874) are a capacity test only,
never the principal PD.

| Bucket | E | D | PD Jeffreys | PD isotonic | `E·floor` | `E·floor ≥ 20` | capacity verified |
| --- | --- | --- | --- | --- | --- | --- | --- |
| AAA | 104,919 | 0 | 4.766e-6 | 3.750e-6 | n/a | n/a | true |
| AA | 150,007 | 0 | 3.333e-6 | 3.750e-6 | n/a | n/a | true |
| A | 145,065 | 0 | 3.447e-6 | 3.750e-6 | n/a | n/a | true |
| BBB | 90,580 | 0 | 5.520e-6 | 5.520e-6 | n/a | n/a | true |
| BB | 43,695 | 1 | 3.433e-5 | 3.433e-5 | 7.297 | false | true |
| B | 18,957 | 2 | 1.319e-4 | 1.319e-4 | 15.867 | false | true |
| CCC | 7,750 | 72 | 9.354e-3 | 9.354e-3 | 67.735 | true | true |

Reported as computed. In the reused hazard code the floor predicate only gates a HY bucket with
zero observed defaults; BB and B have observed defaults, so `capacity verified` is true although
`E·floor < 20`. Whether that reading is the intended one is for the owner. The same table results
from the rebuilt rows and the stored rows.

## 6. G10 — anchor reproducibility

| Field | Value |
| --- | --- |
| Window | 2023-09..2026-08, witnessed months only (one finite, consistent `L` per month; carried dark months excluded) |
| Observable months | 34 of 36; dark: 2026-07, 2026-08 |
| Median | `-0.8864114120812479` (hex `-0x1.c5d7b7736159ep-1`) |
| Pin | `-0.8864114120812487` (hex `-0x1.c5d7b773615a6p-1`) |
| \|diff\| | 8.881784197001252e-16 (signed: +8.88e-16) |
| Exact | **false** |
| Within 1e-9 | true — diagnostic only; **D-1 not decided** |
| Diagnostic chain median (independent build) | `-0.8864114120812479`, same |

The same median results on the rebuilt rows, the stored rows, and the chain recomputation. Using
all 36 months (carried values included) would give `-0.8956167626439901`, the wrong series.
Likely source of the 8.88e-16 (provenance review by a read-only subagent; **not re-run here**): the
round-002 default `pandas.read_csv` float parser differs from correctly rounded `float(Decimal)` in
3–28% of values per column in the 2024/2025 chunks; the worker feeds `Decimal`, giving `…479`, while
the default CSV parse gives `…487`. That review reproduced both anchors on two local stacks. The
pin `…487` is the owner's authority.

## 7. Step 7 — fixed-policy consistency: not executed

The ledger (read 14:05Z) holds no publication under `4b752a3f…`; the pointer is still `bddb0e8e`
(policy `28f70b9b…`). The comparison needs a publication that does not exist and is not claimed.
When one exists, exclude **both** `publication_id` and `policy_digest` (not only `publication_id`),
and for differing last months compare months before the earlier `last_month`, excluding `censoring`
on the boundary month.

## 8. Provenance of the 2026-09 rows (report only; not a gate reading)

- `bc13a5e4` is the frozen round-002 **local artifact** loaded by the PR #133 loader (UUID5 matches
  revision `c541c35c` with no pin suffix), not a worker build. `a8b9a3d2`, `2801b9d3`, `bddb0e8e`
  were built by the worker with the inherited pin.
- Their different rows come from code (provenance review by a read-only subagent; **numbers below
  were not re-run here**): on identical input for 2002–2003, `c541c35c` → `b8b9503d`
  changes D rows 1,270 → 1,470 (2002-08: 49 → 60; 2002-11 rows 8,093 → 8,095), traced to commit
  `d520255` (confirmed D carries; no source-exit for a confirmed D); `c12f6cd` changes 0–3 rows on
  the subsets tried. **Only subsets were run; full-history attribution of all 194 months was not.**
- Therefore "input drift under the same panel id" is refuted for the 8 stage columns (§3).

## 9. Observed outside G1/G9/G10 (reported, not acted on)

1. **September closed dark.** Rating pointer `bddb0e8e`: 2026-07, 08, 09 have 5,559 / 10,208 /
   10,208 rows and **0** witnessed rows. In `bond_panel_current_snapshot_v1_mat`, `dollar_volume`
   is non-null in **0** of 10,208 rows in each of 2026-07, -08, -09 (36,753 of 36,753 in 2026-06),
   while `trade_count` is non-null in 9,759 / 9,716 / 9,636. The cause lies in the panel/volume
   source, not the rating policy; it blocks any economic use.
2. `app_runtime` has no SELECT on `bond_market_implied_rating_v1_builds` (confirmed 14:05Z).
3. The elected pointer carries `28f70b9b…`; the fixed policy `4b752a3f…` needs a republication.
4. Privilege posture of the `bond_default_review_*` tables and `GRANT CREATE ON SCHEMA public` (owner call).

## 10. Not claimed

- Any DG-4 acceptance, any gate other than G1/G9/G10, G2–G8, or any change to a historical result.
- That G1(b) holds on the production image; that the production Workers stack equals any tested stack.
- That the 637 float differences are caused by numpy/CPU (consistent, not shown).
- That the stored `builds.l_anchor` equals the pin (inferred from identity; not readable).
- Independent verification of the production numerics, or a September witnessed-volume pass.
- A tolerance decision for G10, or that a within-tolerance difference satisfies "exactly".
- No production state changed: no publication, pointer, DDL, grant, variable, flag, deploy or
  `worker.run()`; the only database access was bounded read-only `SELECT`/`SET`. The original
  checkouts `E:\investintell-light` and `E:\investintell-datalake-workers` have a `git status`
  identical to the snapshot taken at 11:39 and re-compared at 14:06. This file is the only addition
  inside any `E:` checkout (isolated Workers clone, untracked, not committed).

## 11. Decisions reserved to the owner

- **D-1.** Is 8.88e-16 (≤ 1e-9) an acceptable reproduction of the anchor for G10, or is bit equality
  required? (Measured: not bit-equal on stored rows, rebuilt rows or chain.)
- **D-2.** G1(b) cannot be bit-reproduced locally (§4). Is an in-container read-only replay on the
  production image acceptable, given it competes with the daily cron? Or is localisation to
  last-bit float cells with zero discrete differences sufficient for the amendment's G1 reading?
- **D-3.** How to treat the `bc13a5e4` vs `a8b9a3d2` content divergence in the acceptance record
  (§8: different producer commits; `bc13a5e4` is a frozen local artifact).
- **D-4.** DG-4 acceptance itself.
- New: reading of the HY floor row for BB/B (§5), and the September-dark defect (§9.1) before any T.

_Addendum 14:35: D-1, D-2, D-3, D-4 and the BB/B floor reading were decided by the owner in the DG-4
record (final section). The September-dark defect remains a Phase 4 gate._

## 12. Verification run

| Check | Result |
| --- | --- |
| Harness tests (`test_gate_replay`) | 29 pass (24 initial; +5: exponent Decimals incl. real exporter `cell`, completed-bundle admission, incomplete/INVALID refusal, pointer/header movement, exponent fingerprint) |
| Exporter tests (`test_export`) | 26 pass (22 + 4 transport-retry) |
| Projection → admission integration | 2 pass (`test_prepare_integration.py`) |
| Producer pure tests (`b8b9503d`) | 15 pass, 89 deselected (`deterministic or fingerprint or inherited or cures or dark`) |
| Hazard bridge vs normal import | 9 synthetic fixtures + PAVA exact (Light venv: Py3.12.12, numpy 2.4.6, pandas 3.0.3, pydantic 2.13.4) |
| Independent reviews | (1) architecture/numerics advisor: bridge acceptable under validated adapters; (2) consequential review of the tools: `REQUEST_CHANGES`, 3×P2 (exponent Decimal grammar, completed-export admission, mismatch diagnostics) — all three corrected and covered by the tests above; the corrected tools were **not** re-reviewed independently. |

Durations (measured): build A 2,492 s wall (preflight 444 s, build 1,729 s, digest 172 s); build B
1,206 s (160 / 739 / 156 s); round-002 builds 1,000 s and 1,048 s, digest 196 s and 183 s on another
machine. Build A was slower than B on the same host, code and data; no cause was measured.

## 13. Artifact hashes (SHA-256)

Full list: `artifact_hashes.json` in the work root. Key entries:

| Artifact | SHA-256 |
| --- | --- |
| `run-003/evaluation.json` | `2e356aa62521618276e7873067f27e5d8fcb86d70fbe884b34e095e2330a956b` |
| `run-003/build_a/manifest.json` | `46606909f413d46c4504a0372442330329e11b816321a7a1ccb9064467fb3c0f` |
| `run-003/build_b/manifest.json` | `99beb0faddc86d0ab7ecd3c7afad6aa566d5068eb941289166015e9de5659aa1` |
| `run-003/build_a/rows.parquet` = `build_b/rows.parquet` | `17713367ef305fd2a587c07b041ed7a1b0f3a516a2179dc662918a795df2a902` |
| `run-003/build_a/before_build.json` | `eeaf36dde4bbf0f857ae86575b2450ea45a471420e18a02e9c7f40dab1892794` |
| `identity_proof.json` | `d854aaf8ca038ad2345a4d9b8b1f2e8a2b74170ebb507b2f43f170587c4733a6` |
| `historical_input_diagnostic.json` | `054b42b51ca82586a2fc64cb3b08a1f6cbc240120f529776133bb2fbc1e26f2e` |
| `hazard_equivalence_check.json` | `141814e1bd0fa0033a933fc9466874116e5604ccdf5d0ce273845d790e1b43b2` |
| `export-tool/bounded_export.py` | `bdea4902106dfd9852218cb9556f19fd9f16ff624eabcd3cf40b56d0de8ad153` |
| `export-tool/export_protocol.py` | `c3bc031d1c9bf3ba2da5066632347dc743b9ddff289306bf2d3f491330f88016` |
| `export-tool/remote_export.py` | `22619c05482f3556be8ec2ced610149785d8a72516dde54ac897692a8c08f38a` |
| `prepare_replay_export.py` | `7436ddaabbe380d2a95da5cfbf8ad3a95e5ff6da4f5ecf3e2979215c86c57013` |
| `ledger_readonly_final.log` (14:05Z) | `93a9590365784980e6d06b6829c28ae25d919174754f2f9b13343fd6e8bc18e0` |
| `stored_readings_readonly.log` (stored-rows SQL cross-check: 1,138/173, 34 months) | `ab377786e3faa7e62661de68cfc0305bddecd6c0d4fc6200d6dca025ab5e8d80` |

The stored-readings SQL cross-check (`stored_readings_readonly.py`, read-only) agrees with the
harness on G9 (1,138/173) and G10 (34 months, median `…479`); it reads stored rows and is not a
chain replay.

## 14. Owner condition — recorded re-run of the parse path (added 2026-10-01 14:37 -03:00)

Condition from DG-4 (below): "recorded re-run proving pandas-default parse yields …487".
Executed with the unmodified `b8b9503d` producer (`implied_rating.py` sha256 `3cc1e760…41c99`,
policy `28f70b9b…`), `market_anchor_for_snapshot(frame, last_closed_month=2026-08-01)`, on the
round-002 stack (Python 3.13.12, numpy 2.5.1, pandas 3.0.3), script `g10_parse_path_rerun.py`
(sha256 `4d1be9031cf4219428cae72171948d8855537c0d3a26b4a3101c800a70bb28bf`), output
`g10_parse_path_rerun.json` (`9124cb273d3f09b9a8fb0d80adc1281cfa4bd85a05c32b725db184a815f448de`),
log `g10_parse_path_rerun.log` (`375bcba68ae1745ee09e562514adab54867bada2d98f577c2d5e7b357c8a9642`).

| Arm | Input (sha256) | Parse | Anchor | hex | = pin `…487` |
| --- | --- | --- | --- | --- | --- |
| 1 | round-002 frozen `snapshot.csv` (`e9602304…3031`), 3,438,099 rows | `pd.read_csv(dtype={cusip_id:str}, low_memory=False)` — the round-002 harness `load_snapshot` | `-0.8864114120812487` | `-0x1.c5d7b773615a6p-1` | **true** |
| 2 | same file | same + `float_precision='round_trip'` (correctly rounded, equals the worker's `Decimal`→float) | `-0.8864114120812479` | `-0x1.c5d7b7736159ep-1` | false (= `…479`) |
| 3 | today's export `snapshot.csv` (`69b045e9…d78a`), 3,438,099 rows | pandas default + `na_values=['\\N']` | `-0.8864114120812487` | `-0x1.c5d7b773615a6p-1` | **true** |

Condition met: the pandas-default parse yields the pin on both the frozen round-002 input and
today's export; only the parse path separates `…487` from `…479`. Numeric columns are `float64`
in all arms (no `Decimal`). Not a gate reading; the G10 table in §6 is unchanged.

## 15. Phase 4 gate — G5/G6 re-read on the b8b9503d rows (added 2026-10-01 15:45 -03:00)

Required by DG-4 ("G5/G6 re-read on b8b9503d rows"). Executed locally at 2026-10-01 17:57Z on the
stored rows of `a8b9a3d2` (`stored_rows.csv` sha256 `8f5a40d6…4f4e`, 3,375,473 rows; producer
`b8b9503d`, policy `28f70b9b…`) against the exact frozen round-002 benchmark exports, using the
historical `gate_g5`/`gate_g6` bodies and the round-002 `NOT_EVALUABLE` wrappers compiled from an AST
allowlist (the historical harness CLI was not run). Holdout 2021-09..2023-08, 12-month outcomes
through 2024-08; thresholds unchanged (G5 Gini ≥ 0.70; G6 ≤ 25%). Python 3.13.12, numpy 2.5.1,
pandas 3.0.3, scipy 1.18.0; 17 synthetic contract checks passed.

| Gate | Population | Benchmark D matches | Reading |
| --- | --- | --- | --- |
| G5 forward default discrimination | 228,193 ordinal-scored holdout rows (AAA..CCC, D) | 0 benchmark D-positive labels | **NOT_EVALUABLE** — benchmark insufficiency (DG-D): no governed D benchmark with instrument id, event dates and complete 12-month follow-up. Gini/AUC undefined, not a measured pass or fail. |
| G6 false-D | 525 confirmed holdout rows → 36 distinct confirmed (CUSIP, event-month) events | 0 matched, 36 unmatched | **NOT_EVALUABLE** — benchmark insufficiency (DG-D): no governed static/feed D inventory exists. Unmatched is not false-D; the rate is unknown, neither 0% nor 100%. |

Benchmark: `rating_pit.csv` (1,547,178 rows, 2002-07..2025-03, 0 D, sha256 `cdf7af68…4056`) and
`rating_static.csv` (31,375 rows, 0 D, sha256 `7ca5b431…9019`), exported 2026-09-19T16:48:29Z from
panel pointer `65156481…`. A fresher production benchmark was **not checked**. Evidence (outside any
repository): `C:\Users\andre\AppData\Local\Temp\bde-phase34-20261001\g5g6\result.json` (sha256
`aa4d295ee304e125acfd38f9fddd4873e07da3e1b98d5ae208ebd2d7dccef8dc`), `README.md`
(`a244cdeb78cbd0079f7524cad0b0ddd02d4b50dddc9c074620eeead209e5a0a7`), `validation.json`
(`c8e8d739d94b198f7e9fa6440b150e5d501ad405b8b7e9aa1066bbe6b791a01f`). This is the partial reading the
owner asked for; it is not an acceptance decision.

## DG-4

Recorded by the agent verbatim from the owner's message of 2026-10-01 14:35 -03:00 (the owner's
`<data>` placeholder is filled with that message time; nothing else was altered):

> DG-4: ACCEPTED WITH CONDITIONS — 2026-10-01 14:35 -03:00, owner.
> - G1(a) PASS. G1(b) not literally reproduced; accepted by equivalence (§4: zero key/discrete
>   differences, 637 last-bit cells in log-derived columns; no gate reading depends on them).
> - G9 PASS. HY floor reading for BB/B accepted as declared §7 (rule applies only to D_b = 0).
> - G10 not exact (8 ULP, 8.88e-16); accepted by owner under the amendment (pin authoritative,
>   median diagnostic). Condition: recorded re-run proving pandas-default parse yields …487.
> - Accepted reference: b8b9503d / 28f70b9b / pin -0.8864114120812487 (a8b9a3d2 lineage) and
>   fixed policy 4b752a3f for republication. bc13a5e4 superseded, not eligible, not deleted.
> - Unlocks Phase 3 (local Light code) only. Phase 4 gated on: September-dark fix (§9.1),
>   pinned Workers deps + stack in build manifest, authorized republication under 4b752a3f
>   with G1(a) on the production image, and G5/G6 re-read on b8b9503d rows.

Condition status: **met** (§14). Phase 3 is unlocked by this record; Phase 4 remains gated on the
four items above. No Phase 3 or Phase 4 work was performed in this round.
