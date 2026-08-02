-- Freshness assertion for the out-of-band identity matview.
--
-- ``nport_holdings_snapshot_identity_v1`` is a MATERIALIZED VIEW over
-- ``sec_nport_holdings_v2`` JOIN ``sec_nport_instrument_class_bridge`` (resolved
-- rows only).  It is NOT created or refreshed by this repository: production owns
-- it as ``postgres`` and refreshes it out of band.  The app reads it through
-- ``sec_current_nport_holdings_snapshot_identity_v1`` and uses
-- ``MAX(report_date)`` per series as the ANCHOR of the fixed-income dossier
-- (light: fixed_income_analytics.py, fixed_income_holdings.py, nport_class_aware.py).
--
-- TWO FAILURE MODES, ONLY ONE OF WHICH IS ALREADY VISIBLE
-- ------------------------------------------------------
-- 1. the holdings pointer advances past the matview -> the matview holds NO rows
--    for the pinned publication -> the anchor CTE is empty -> the dossier degrades.
--    Loud, and already fail-closed at the app.
-- 2. the matview is behind WITHIN the pinned publication -> the anchor resolves an
--    OLDER report_date than the holdings the same publication would serve.  The
--    dossier then labels stale numbers with a real, current publication id.  No
--    relation is empty, nothing degrades, and nothing detects it.  That is the
--    hole this function closes.
--
-- WHY (2) IS POSSIBLE AT ALL, AND WHY IT IS BOUNDED
-- -------------------------------------------------
-- ``sec_nport_holdings_v2_write_guard`` admits INSERTs only while the parent
-- publication is ``prepared`` and forbids UPDATE/DELETE outright, so a publication
-- is FROZEN once validated.  A matview refreshed after validation is therefore
-- complete for that publication forever.  The residual window is a refresh that
-- landed DURING the prepared window: it captured a partial publication, the
-- publication was then validated and pinned, and no further refresh ever ran.
--
-- COST
-- ----
-- Four index-backed ``max()`` reads plus one ``count(*)`` over the matview's own
-- (small) slice of the publication: ~30 ms total on the mirror against a 625 k-row
-- publication.  Every one of the maxima is bounded by an index --
-- ``sec_nport_holdings_v2 (publication_id, report_date DESC, ...)`` and the
-- matview's own ``(publication_id, series_id, report_date, accession_number)``.
-- ``max(filing_date)`` is deliberately taken AT the maximum report_date rather
-- than over the whole publication: unscoped it cannot use either index and cost
-- 1.5 s of the 5.6 s an earlier version of this function spent per call.
--
-- The exact, bridge-filtered maximum costs ~15 s and is paid ONLY when the cheap
-- comparison already looks wrong -- the cheap source-side max ignores the bridge
-- filter, so it is an upper bound that can flag a matview that is merely
-- bridge-filtered rather than stale.  The confirmation removes that false
-- positive without putting its cost on every call.
--
-- WHAT THIS PROVES AND WHAT IT DOES NOT
-- -------------------------------------
-- ``fresh`` means exactly this and nothing more: the matview carries the newest
-- ``report_date`` of the pinned publication, and the newest ``filing_date`` AT
-- that report_date.  That is the coordinate the app's anchor is computed from.
--
-- It does NOT prove completeness.  ``sec_nport_holdings_v2`` is generated offline
-- (``src/nport/local_v2_generator.py`` writes ``sec_nport_holdings_v2.tsv.gz``)
-- and loaded into the target, so rows arrive in accession order, NOT in
-- report_date order.  A capture taken part-way through such a load is a PREFIX OF
-- ACCESSIONS, and a prefix can already contain the global maxima while missing a
-- large share of the publication -- this function would call that ``fresh``.  No
-- ordering argument is available to close that gap, and the publication carries
-- no recorded row count to check cardinality against (``sec_derived_publications``
-- stores none, and counting 4.5M rows per probe is not a cheap check).
--
-- What IS reported, as evidence rather than proof, is ``matview_rows`` -- the
-- matview's own row count for the pinned publication, which is cheap because the
-- matview is small.  A collapse there is visible to anyone comparing two runs.
--
-- STATES
-- ------
--   fresh           proven, as scoped above
--   stale           proven divergence: the matview is behind the pinned publication
--   behind_pointer  the matview holds NO rows for the pinned publication
--   unreadable      the anchor relation cannot be read at all (absent, or never
--                   refreshed) -- the app's read path is broken, not merely stale
--   unavailable     genuinely undecidable upstream (no pointer, no holdings
--                   surface, pinned publication carries no holdings)
--
-- ``nport_holdings_snapshot_identity_assert_fresh()`` raises on the first three.
-- ``unreadable`` raises because a caller that just ran REFRESH and gets silence
-- from a matview that does not exist has been told the opposite of the truth.

CREATE OR REPLACE FUNCTION nport_holdings_snapshot_identity_freshness()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    matview  CONSTANT text := 'nport_holdings_snapshot_identity_v1';
    pinned uuid;
    populated boolean;
    mv_report date;
    mv_filing date;
    src_report date;
    src_filing date;
    exact_report date;
    exact_filing date;
    mv_rows bigint;
BEGIN
    -- The SUBJECT is checked first, and its absence is HARD. Whatever the state of
    -- the upstream pointer, a missing or never-refreshed anchor relation means the
    -- app's read path is broken, not merely behind -- and a caller that has just
    -- run REFRESH must not be answered with silence.
    IF to_regclass(matview) IS NULL THEN
        RETURN jsonb_build_object('state', 'unreadable',
                                  'reason', 'matview_absent',
                                  'matview', matview);
    END IF;

    -- ``CREATE MATERIALIZED VIEW ... WITH NO DATA`` leaves a relation that raises
    -- on every SELECT until its first refresh.
    SELECT c.relispopulated INTO populated
    FROM pg_class c WHERE c.oid = to_regclass(matview);
    IF NOT COALESCE(populated, false) THEN
        RETURN jsonb_build_object('state', 'unreadable',
                                  'reason', 'matview_never_refreshed',
                                  'matview', matview);
    END IF;

    -- Genuinely undecidable upstream conditions stay SOFT: there is nothing for
    -- the matview to be fresh against, and that is not the matview's fault.
    IF to_regclass('sec_derived_current_pointers') IS NULL
       OR to_regclass('sec_nport_holdings_v2') IS NULL
       OR to_regclass('sec_nport_instrument_class_bridge') IS NULL THEN
        RETURN jsonb_build_object('state', 'unavailable',
                                  'reason', 'holdings_surface_absent',
                                  'matview', matview);
    END IF;

    SELECT c.publication_id INTO pinned
    FROM sec_derived_current_pointers c
    WHERE c.product = 'sec_nport_holdings_v2';
    IF pinned IS NULL THEN
        RETURN jsonb_build_object('state', 'unavailable',
                                  'reason', 'no_current_holdings_publication',
                                  'matview', matview);
    END IF;

    -- Evidence, not proof (see WHAT THIS PROVES above): the maxima cannot see a
    -- mid-load prefix of accessions that already carries them. The matview is
    -- small, so its own cardinality is cheap to report and a collapse is visible
    -- to anyone comparing two runs.
    EXECUTE format(
        'SELECT max(report_date), count(*) FROM %I WHERE publication_id = $1', matview
    ) INTO mv_report, mv_rows USING pinned;

    SELECT max(h.report_date) INTO src_report
    FROM sec_nport_holdings_v2 h
    WHERE h.publication_id = pinned;

    IF src_report IS NULL THEN
        RETURN jsonb_build_object('state', 'unavailable',
                                  'reason', 'pinned_publication_has_no_holdings',
                                  'matview', matview,
                                  'publication_id', pinned);
    END IF;

    IF mv_report IS NULL THEN
        -- The pointer moved past the matview. Already fail-closed at the app
        -- (empty anchor), reported here so the standstill is legible.
        RETURN jsonb_build_object('state', 'behind_pointer',
                                  'reason', 'matview_holds_no_rows_for_pinned_publication',
                                  'matview', matview,
                                  'publication_id', pinned,
                                  'matview_rows', mv_rows,
                                  'source_max_report_date', src_report);
    END IF;

    -- Scoped to the maximum report_date on purpose: an UNSCOPED max(filing_date)
    -- matches no index on either relation and dominated this function's cost.
    -- Scoped, it still catches the case that matters -- a matview that captured
    -- the newest report_date but missed a later-filed amendment OF that date.
    IF mv_report = src_report THEN
        EXECUTE format(
            'SELECT max(filing_date) FROM %I WHERE publication_id = $1 AND report_date = $2',
            matview
        ) INTO mv_filing USING pinned, mv_report;

        SELECT max(h.filing_date) INTO src_filing
        FROM sec_nport_holdings_v2 h
        WHERE h.publication_id = pinned AND h.report_date = src_report;

        IF mv_filing IS NOT DISTINCT FROM src_filing THEN
            RETURN jsonb_build_object('state', 'fresh',
                                      'reason', 'matview_carries_the_publication_maxima',
                                      'matview', matview,
                                      'publication_id', pinned,
                                      'matview_rows', mv_rows,
                                      'matview_max_report_date', mv_report,
                                      'matview_max_filing_date', mv_filing,
                                      'source_max_report_date', src_report,
                                      'source_max_filing_date', src_filing,
                                      'confirmed_against_bridge', false);
        END IF;
    END IF;

    -- Suspicion only. The cheap source-side maxima ignore the bridge filter the
    -- matview applies, so a legitimate matview can sit below them. Pay for the
    -- exact answer here, where it decides between "bridge-filtered" and "stale".
    SELECT max(h.report_date) INTO exact_report
    FROM sec_nport_holdings_v2 h
    WHERE h.publication_id = pinned
      AND EXISTS (
          SELECT 1 FROM sec_nport_instrument_class_bridge b
          WHERE b.publication_id = h.publication_id
            AND b.accession_number = h.accession_number
            AND b.holding_id = h.holding_id
            AND b.resolution_state = 'resolved'
            AND h.report_date >= b.valid_from
            AND (b.valid_to IS NULL OR h.report_date <= b.valid_to)
      );

    SELECT max(h.filing_date) INTO exact_filing
    FROM sec_nport_holdings_v2 h
    WHERE h.publication_id = pinned AND h.report_date = exact_report
      AND EXISTS (
          SELECT 1 FROM sec_nport_instrument_class_bridge b
          WHERE b.publication_id = h.publication_id
            AND b.accession_number = h.accession_number
            AND b.holding_id = h.holding_id
            AND b.resolution_state = 'resolved'
            AND h.report_date >= b.valid_from
            AND (b.valid_to IS NULL OR h.report_date <= b.valid_to)
      );

    EXECUTE format(
        'SELECT max(filing_date) FROM %I WHERE publication_id = $1 AND report_date = $2', matview
    ) INTO mv_filing USING pinned, mv_report;

    -- So the verdict always carries both sides, including on the branch that
    -- skipped the cheap filing_date comparison because the report dates differed.
    SELECT max(h.filing_date) INTO src_filing
    FROM sec_nport_holdings_v2 h
    WHERE h.publication_id = pinned AND h.report_date = src_report;

    IF mv_report = exact_report AND mv_filing IS NOT DISTINCT FROM exact_filing THEN
        RETURN jsonb_build_object('state', 'fresh',
                                  'reason', 'matview_carries_the_bridge_resolved_maxima',
                                  'matview', matview,
                                  'publication_id', pinned,
                                  'matview_rows', mv_rows,
                                  'matview_max_report_date', mv_report,
                                  'matview_max_filing_date', mv_filing,
                                  'source_max_report_date', src_report,
                                  'source_max_filing_date', src_filing,
                                  'bridge_resolved_max_report_date', exact_report,
                                  'bridge_resolved_max_filing_date', exact_filing,
                                  'confirmed_against_bridge', true);
    END IF;

    RETURN jsonb_build_object('state', 'stale',
                              'reason', 'matview_is_behind_the_pinned_publication',
                              'matview', matview,
                              'publication_id', pinned,
                              -- The runbook's first repair step tells the operator
                              -- to read this; it must be on the verdict that sends
                              -- them there, not only on the ones that do not.
                              'matview_rows', mv_rows,
                              'matview_max_report_date', mv_report,
                              'matview_max_filing_date', mv_filing,
                              'source_max_report_date', src_report,
                              'source_max_filing_date', src_filing,
                              'bridge_resolved_max_report_date', exact_report,
                              'bridge_resolved_max_filing_date', exact_filing,
                              'confirmed_against_bridge', true);
END $$;

COMMENT ON FUNCTION nport_holdings_snapshot_identity_freshness() IS
'Structured freshness verdict for nport_holdings_snapshot_identity_v1 against the '
'current sec_nport_holdings_v2 publication. States: '
'fresh (the matview carries the publication''s newest report_date, and the newest '
'filing_date AT that date -- it does NOT prove completeness) | '
'stale (proven divergence) | '
'behind_pointer (the matview holds no rows for the pinned publication) | '
'unreadable (the matview is absent, or exists but was never refreshed -- the app''s '
'read path is broken, not merely behind) | '
'unavailable (undecidable UPSTREAM: no pointer, no holdings surface, or a pinned '
'publication with no holdings). Never raises; use '
'nport_holdings_snapshot_identity_assert_fresh() right after an out-of-band REFRESH '
'when anything but fresh or unavailable must stop the caller.';

-- The assertion an operator (or a refresh script) runs immediately after
-- ``REFRESH MATERIALIZED VIEW [CONCURRENTLY] nport_holdings_snapshot_identity_v1``.
-- Raising is the point: a refresh that did not close the gap must not look like a
-- refresh that did.
CREATE OR REPLACE FUNCTION nport_holdings_snapshot_identity_assert_fresh()
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE verdict jsonb;
BEGIN
    verdict := nport_holdings_snapshot_identity_freshness();
    IF verdict->>'state' = 'unreadable' THEN
        -- Distinct errcode: this is not "behind", it is "not there". A caller that
        -- just ran REFRESH and gets a silent pass here has been told the opposite
        -- of the truth.
        RAISE EXCEPTION 'nport_holdings_snapshot_identity_v1 cannot be read: %', verdict::text
              USING ERRCODE = 'undefined_table';
    END IF;
    IF verdict->>'state' IN ('stale', 'behind_pointer') THEN
        RAISE EXCEPTION 'nport_holdings_snapshot_identity_v1 is not fresh for the current '
                        'sec_nport_holdings_v2 publication: %', verdict::text
              USING ERRCODE = 'data_exception';
    END IF;
    RETURN verdict;
END $$;

COMMENT ON FUNCTION nport_holdings_snapshot_identity_assert_fresh() IS
'Fail-closed wrapper over nport_holdings_snapshot_identity_freshness(). Raises '
'undefined_table on unreadable (the matview is absent or was never refreshed -- a '
'refresh alone may not fix it) and data_exception on stale or behind_pointer. '
'Returns the same structured verdict on fresh and on unavailable, which is '
'undecidable upstream rather than a fault of the matview.';
