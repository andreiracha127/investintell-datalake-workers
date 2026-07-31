-- Materialized caches for the amendment-aware SEC "effective" selections.
--
-- WHY: ``ncen_effective_filings`` and ``rr1_effective_facts`` are plain views over
-- a self-join of the raw landing tables plus a dense_rank()/count() OVER window
-- with no date predicate, so EVERY read expands the whole history of
-- ``ncen_raw_v2_rows`` / ``rr1_raw_v2_rows``. The daily publication chain reads
-- them only for ``max(effective_date)`` -- twice per run (discover_source_days and
-- build_watermarks) -- which means four full expansions per day for two scalars.
--
-- WHAT THIS IS NOT: these matviews are a CACHE, never the authority. The views
-- above keep the amendment/tie-break semantics; ``src/sec_effective_matviews.py``
-- only reads a matview while its recorded source signature still equals the live
-- one, and falls back to the view otherwise. Nothing here changes what a
-- publishable row is.
--
-- APPLY: this file is a migration. Workers do NOT install it (the runtime role
-- does not own the raw views -- prod 2026-07-24 recorded "ERROR: must be owner of
-- view ncen_effective_filing_candidates" from a worker-side CREATE OR REPLACE).
-- Apply it once with an owner role, then hand ownership to the runtime role so it
-- can REFRESH (see the ALTER ... OWNER block at the end).

-- --------------------------------------------------------------------------- --
-- Refresh state: one row per matview, carrying the source signature it was
-- refreshed at. A reader trusts a matview only while this signature still
-- matches the live one, so a missed refresh degrades to the view (slow but
-- correct), never to a stale answer.
-- --------------------------------------------------------------------------- --
CREATE TABLE IF NOT EXISTS sec_effective_matview_state (
    matview text PRIMARY KEY,
    source_family text NOT NULL CHECK (source_family <> ''),
    -- The signature of the validated-run surface the matview was built from:
    -- (count of validated runs, max raw_validated_at) for the family. The
    -- effective views admit a raw row only through sec_validated_raw_runs, so a
    -- family whose validated-run surface is unchanged cannot change their content
    -- by appending rows.
    source_run_count bigint NOT NULL CHECK (source_run_count >= 0),
    source_validated_at timestamptz,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    refresh_seconds double precision,
    CHECK ((source_run_count = 0) = (source_validated_at IS NULL))
);

-- --------------------------------------------------------------------------- --
-- N-CEN: a full mirror. The publishable filing set is one row per
-- (registrant_cik, effective_date) winner -- small enough to copy whole, and a
-- full mirror keeps any future consumer off the raw expansion too.
-- --------------------------------------------------------------------------- --
CREATE MATERIALIZED VIEW IF NOT EXISTS ncen_effective_filings_mv AS
SELECT * FROM ncen_effective_filings
WITH NO DATA;

-- raw_row_id is unique in ncen_effective_filings: each candidate is one
-- SUBMISSION.tsv row (no fan-out join) and only the sole rank-1 row of a
-- (registrant_cik, effective_date) partition is publishable. The UNIQUE index is
-- also what REFRESH ... CONCURRENTLY requires.
CREATE UNIQUE INDEX IF NOT EXISTS ncen_effective_filings_mv_pk
    ON ncen_effective_filings_mv (raw_row_id);
CREATE INDEX IF NOT EXISTS ncen_effective_filings_mv_effective_date_idx
    ON ncen_effective_filings_mv (effective_date DESC);

-- --------------------------------------------------------------------------- --
-- RR1: a per-date CALENDAR, deliberately NOT a full mirror.
--
-- rr1_effective_facts carries the whole typed jsonb projection of every
-- publishable num.tsv/txt.tsv fact; mirroring it would duplicate the largest
-- surviving landing table on a disk this program is trying to shrink. The only
-- live read of the relation outside the fee builder's own session is
-- ``max(effective_date)``, and the fee builder shadows the name with a
-- transaction-local table it fills itself. A one-row-per-date roll-up serves the
-- watermark, is a few thousand rows, and doubles as the equivalence surface:
-- per-date row counts must match the view exactly.
-- --------------------------------------------------------------------------- --
CREATE MATERIALIZED VIEW IF NOT EXISTS rr1_effective_fact_calendar_mv AS
SELECT effective_date,
       count(*)::bigint AS publishable_rows,
       count(DISTINCT accession_number)::bigint AS publishable_accessions
FROM rr1_effective_facts
GROUP BY effective_date
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS rr1_effective_fact_calendar_mv_pk
    ON rr1_effective_fact_calendar_mv (effective_date);

-- --------------------------------------------------------------------------- --
-- Ownership / grants: the runtime role must be able to REFRESH (refresh requires
-- ownership) and the readers must be able to SELECT. Guarded so the file stays
-- idempotent and applies unchanged on a test database with no app_runtime role.
-- --------------------------------------------------------------------------- --
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_runtime') THEN
        EXECUTE 'ALTER MATERIALIZED VIEW ncen_effective_filings_mv OWNER TO app_runtime';
        EXECUTE 'ALTER MATERIALIZED VIEW rr1_effective_fact_calendar_mv OWNER TO app_runtime';
        EXECUTE 'ALTER TABLE sec_effective_matview_state OWNER TO app_runtime';
    END IF;
END
$$;
