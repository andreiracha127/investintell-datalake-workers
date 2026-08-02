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
-- Four index-backed ``max()`` reads in the happy path: ~30 ms total on the mirror
-- against a 625 k-row publication.  Every one of them is bounded by an index --
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
-- ``fresh`` means the matview saw the newest (report_date, filing_date) of the
-- pinned publication.  It does not prove per-series completeness: a refresh that
-- captured the newest rows but not some older series would still report fresh.
-- Postgres refreshes a matview atomically, so a partial capture can only come
-- from the prepared-window race above, and that race truncates the NEWEST rows --
-- which is exactly what these maxima see.

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
BEGIN
    -- Fail closed on absence: never claim fresh about a surface we cannot read.
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

    IF to_regclass(matview) IS NULL THEN
        RETURN jsonb_build_object('state', 'unavailable',
                                  'reason', 'matview_absent',
                                  'matview', matview,
                                  'publication_id', pinned);
    END IF;

    -- ``CREATE MATERIALIZED VIEW ... WITH NO DATA`` leaves a relation that raises
    -- on every SELECT until its first refresh.
    SELECT c.relispopulated INTO populated
    FROM pg_class c WHERE c.oid = to_regclass(matview);
    IF NOT COALESCE(populated, false) THEN
        RETURN jsonb_build_object('state', 'unavailable',
                                  'reason', 'matview_never_refreshed',
                                  'matview', matview,
                                  'publication_id', pinned);
    END IF;

    EXECUTE format(
        'SELECT max(report_date) FROM %I WHERE publication_id = $1', matview
    ) INTO mv_report USING pinned;

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
'current sec_nport_holdings_v2 publication. States: fresh | stale | behind_pointer | '
'unavailable. Never raises; use nport_holdings_snapshot_identity_assert_fresh() '
'right after an out-of-band REFRESH when a divergence must stop the caller.';

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
    IF verdict->>'state' IN ('stale', 'behind_pointer') THEN
        RAISE EXCEPTION 'nport_holdings_snapshot_identity_v1 is not fresh for the current '
                        'sec_nport_holdings_v2 publication: %', verdict::text
              USING ERRCODE = 'data_exception';
    END IF;
    RETURN verdict;
END $$;

COMMENT ON FUNCTION nport_holdings_snapshot_identity_assert_fresh() IS
'Fail-closed wrapper over nport_holdings_snapshot_identity_freshness(): raises on '
'stale or behind_pointer, returns the same structured verdict otherwise.';
