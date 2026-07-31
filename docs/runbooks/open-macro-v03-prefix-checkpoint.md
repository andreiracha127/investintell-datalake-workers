# open_macro_v03 prefix checkpoint — apply and verify

## What changed

Every run of the macro worker (and of the read-only monitor) re-read the **entire
certified pre-cut prefix** — all seed-series rows of `macro_observation_vintage`
and all sleeve rows of `eod_prices` from `EOD_MIN_DATE` (1998-01-01) through
`PACK_CUT` (2026-06-30) — shipped them to the worker, re-serialized them in the
certified canonical format and re-hashed them, in order to compare the digest with
a pin that is a **constant of the committed pack**.

The pre-cut window is *closed*. On a day where nothing pre-cut moved, that whole
pass reproduces a known answer.

`open_macro_v03_prefix_checkpoint` records "this pin was proven byte-for-byte at
**this signature**". The signature is a cheap aggregate computed entirely inside
PostgreSQL over the same window:

```
count(*),  max(available_at) | max(date),
sum(hashtextextended(<full row text>, 0)),
sum(hashtextextended(<full row text>, 8675309))
```

No prefix row is shipped, formatted or hashed client-side while the checkpoint
holds.

## Why the anti-tamper invariant is intact

The checkpoint decides **when** the byte-exact comparison runs, never **whether**
a divergence is tolerated.

| pre-cut change | detected by |
|---|---|
| insert | `count(*)` and both hash sums |
| delete | `count(*)` and both hash sums |
| **in-place correction** (same row count, same max date) | both hash sums, over the full row text of every column in the export projection |
| new pack cut | the pin the checkpoint attests no longer matches `prefix_pins()` |

Any of those puts the run back on `read_prefix` + `verify_prefix_hashes`, which
raises exactly as before. Two further escapes are open by construction:

- `OPEN_MACRO_V03_FULL_PREFIX_HASH=1` forces the full path for a single run;
- a checkpoint **expires** after `OPEN_MACRO_V03_PREFIX_CHECKPOINT_MAX_AGE_HOURS`
  (default **168h**), so the byte-exact re-proof runs at least weekly even if
  nothing ever moves. Set it to `0` to disable expiry.

Every skip and every re-hash is logged with its reason
(`prefix checkpoint hit` / `prefix re-hash: <reason>`), so an operator can always
see which path a run took.

### On "extend the accumulated hash by the delta"

The audit's phrasing assumed a growing series. It does not apply here: the prefix
is a **closed** window at `PACK_CUT` and does not grow, while the delta
(`> PACK_CUT`, `<= as_of`) is already read incrementally by `read_delta` and
bounded by the row-level window gate. There is nothing to extend — the cost was
re-verifying an unchanged closed window, and that is what the checkpoint removes.

## Apply (operator)

```bash
psql "$DATALAKE_DSN" -v ON_ERROR_STOP=1 -f schemas/open_macro_v03_prefix_checkpoint.sql
```

The table is deliberately **outside** the governed `open_macro_v03` catalog
(`EXPECTED_SCHEMA` / `verify_schema`): it holds no product state, only a cached
verification result. `verify_schema` therefore ignores it and needs no change.

Applying it changes nothing on its own — the first run after the migration takes
the full path (`no_checkpoint_for_...`), proves the pins byte-for-byte, and
records the checkpoint. Without the migration the worker behaves exactly as it
does today (`checkpoint_table_absent`).

The write is **best-effort**: the read-only monitor shares this code path, and a
role that may not write simply leaves the checkpoint unrecorded (logged, never
fatal) and pays the full verification next time.

## Verify

```sql
-- 1. What is recorded, and how fresh the byte-exact proof is.
SELECT prefix_table, prefix_sha256, row_count, verified_at,
       now() - verified_at AS age
FROM open_macro_v03_prefix_checkpoint
ORDER BY prefix_table;

-- 2. The recorded pins must equal the pack's SOURCE.json p1_export pins.
--    (compare against: python -c "from src.workers.open_macro_v03 import prefix_pins;
--     print(prefix_pins())")

-- 3. The recorded row_count must equal the live pre-cut window. Cheap and exact.
SELECT count(*) AS live_macro_prefix_rows
FROM macro_observation_vintage
WHERE series_id = ANY (:series_ids) AND available_at <= :pack_cut_end;

SELECT count(*) AS live_eod_prefix_rows
FROM eod_prices
WHERE ticker = ANY (:sleeve_tickers) AND date >= '1998-01-01' AND date <= :pack_cut;
```

Full re-proof on demand (the exact work the daily run no longer repeats):

```bash
OPEN_MACRO_V03_FULL_PREFIX_HASH=1 python -m src.run_worker open_macro_v03
```

## Rollback

```sql
DROP TABLE IF EXISTS open_macro_v03_prefix_checkpoint;
```

Every run then re-reads and re-hashes the whole prefix, exactly as before.
Reverting the code commit alone also works.
