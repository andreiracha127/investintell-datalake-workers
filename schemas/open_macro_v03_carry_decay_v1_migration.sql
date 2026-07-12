-- open_macro_v03 carry_decay_v1 ADDITIVE schema evolution.
-- Policy: artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json
-- (ratified by the quant_owner, Andrei Rachadel, 2026-07-11). Implements the DB
-- surface for the bounded-carry runtime: a carried position older than
-- MAX_CARRY_MONTHS = 3 calendar months degrades to the mandate-tilted CENTER book
-- and must be published HONESTLY, not disguised as a fresh/ordinary carried
-- compressed_50 row.
--
-- VOCABULARY (greppable tokens — the Light repo consumes these):
--   * decision_validity gains 'carried_expired' (alongside 'fresh' / 'carried'):
--     the consumable is a carry whose calendar age exceeds the cap. Chosen over a
--     bare boolean-only encoding so the validity column itself names the state the
--     existing CHECK vocabulary already enumerates ('fresh'/'carried' are the
--     established basis tokens; this extends that enum rather than inventing a
--     parallel one).
--   * allocations.book gains 'center_50' (alongside 'compressed_50'): the
--     cross-quadrant centroid of the four compressed_50 books, mandate-tilted —
--     the same token harness/direct_activation/carry_decay.py::evaluate emits
--     (book_id), and consistent with the compression naming convention
--     (compressed_N = N% of inter-quadrant distance retained; the centroid is the
--     full-compression limit of the compressed_50 family).
--   * nullable provenance columns: carry_age_months (calendar months from
--     carry_seed_as_of), carry_expired (age > cap). open_macro_v03_decisions
--     already carries a mandatory carry_seed_as_of; open_macro_v03_allocations
--     gains a NULLABLE carry_seed_as_of so an allocation row is self-describing
--     without a join. Old rows keep NULLs — strictly additive, nothing is touched.
--
-- APPLICATION (governance): applied by the ORCHESTRATOR in a controlled step
-- against the production DB — deliberately NOT part of the worker's ensure_schema
-- (src/workers/open_macro_v03.py::_SCHEMAS), so the worker never silently migrates
-- a production schema. The three base DDL files stay byte-identical (their
-- schema_migration_record pins hold); on a FRESH database apply the three base
-- files first, then this migration. The worker's verify_schema expects the
-- POST-migration catalog and fails loud (no writes) against an unmigrated one.
--
-- IDEMPOTENT: ADD COLUMN IF NOT EXISTS + DROP CONSTRAINT IF EXISTS / ADD
-- CONSTRAINT widen-recreate pairs; safe to re-run. Every widened CHECK keeps the
-- old vocabulary so all existing rows remain valid under the new constraints.
-- The CHECK expressions are written in the exact form pg_get_constraintdef
-- re-renders (= ANY (ARRAY[...]) / parenthesized OR arms), so the catalog strings
-- verify_schema pins are stable round-trips.

-- ---------------------------------------------------------------------------
-- open_macro_v03_decisions: provenance columns + widened validity vocabulary
-- ---------------------------------------------------------------------------
ALTER TABLE open_macro_v03_decisions
    ADD COLUMN IF NOT EXISTS carry_age_months INTEGER,
    ADD COLUMN IF NOT EXISTS carry_expired BOOLEAN;

-- widen: decision_validity ∈ {fresh, carried, carried_expired}
ALTER TABLE open_macro_v03_decisions
    DROP CONSTRAINT IF EXISTS open_macro_v03_decisions_decision_validity_check;
ALTER TABLE open_macro_v03_decisions
    ADD CONSTRAINT open_macro_v03_decisions_decision_validity_check
    CHECK ((decision_validity = ANY (ARRAY['fresh'::text, 'carried'::text, 'carried_expired'::text])));

-- widen: fresh iff seed = as_of; carried AND carried_expired both require an older seed
ALTER TABLE open_macro_v03_decisions
    DROP CONSTRAINT IF EXISTS open_macro_v03_decisions_validity_seed;
ALTER TABLE open_macro_v03_decisions
    ADD CONSTRAINT open_macro_v03_decisions_validity_seed
    CHECK ((((decision_validity = 'fresh'::text) AND (carry_seed_as_of = as_of))
         OR ((decision_validity = 'carried'::text) AND (carry_seed_as_of < as_of))
         OR ((decision_validity = 'carried_expired'::text) AND (carry_seed_as_of < as_of))));

-- the validity token and the provenance flag can never disagree (old NULL rows:
-- both sides false — valid without backfill)
ALTER TABLE open_macro_v03_decisions
    DROP CONSTRAINT IF EXISTS open_macro_v03_decisions_carry_expired_consistent;
ALTER TABLE open_macro_v03_decisions
    ADD CONSTRAINT open_macro_v03_decisions_carry_expired_consistent
    CHECK (((decision_validity = 'carried_expired'::text) = (carry_expired IS TRUE)));

-- ---------------------------------------------------------------------------
-- open_macro_v03_allocations: provenance columns + widened book vocabulary
-- ---------------------------------------------------------------------------
ALTER TABLE open_macro_v03_allocations
    ADD COLUMN IF NOT EXISTS carry_age_months INTEGER,
    ADD COLUMN IF NOT EXISTS carry_seed_as_of DATE,
    ADD COLUMN IF NOT EXISTS carry_expired BOOLEAN;

-- widen: book ∈ {compressed_50, center_50}
ALTER TABLE open_macro_v03_allocations
    DROP CONSTRAINT IF EXISTS open_macro_v03_allocations_book_check;
ALTER TABLE open_macro_v03_allocations
    ADD CONSTRAINT open_macro_v03_allocations_book_check
    CHECK ((book = ANY (ARRAY['compressed_50'::text, 'center_50'::text])));

-- the degraded book and the provenance flag can never disagree (old NULL rows:
-- both sides false — valid without backfill)
ALTER TABLE open_macro_v03_allocations
    DROP CONSTRAINT IF EXISTS open_macro_v03_allocations_center_book_consistent;
ALTER TABLE open_macro_v03_allocations
    ADD CONSTRAINT open_macro_v03_allocations_center_book_consistent
    CHECK (((book = 'center_50'::text) = (carry_expired IS TRUE)));
