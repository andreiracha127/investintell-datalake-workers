-- Offline-reviewed, forward-only rollback. See the coupon-PIT runbook section 3.10.
-- psql -X -v failed_child=<uuid> -v restore_parent=<uuid>
--   -v authorization=<owner-approval-reference> -v code_revision=<reviewed-git-sha>
--   -f scripts/rollback_bond_panel_coupon_pit.sql
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL ROLE worker_writer;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '55min';
CREATE TEMP TABLE coupon_pit_rollback_request ON COMMIT DROP AS
SELECT :'failed_child'::uuid AS failed_child, :'restore_parent'::uuid AS restore_parent,
       :'authorization'::text AS authorization, :'code_revision'::text AS code_revision;
-- Hold the pointer and immutable inputs stable through projection, validation,
-- compare-and-swap and the served-view equality checks.
LOCK TABLE bond_panel_app_pointer IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE bond_panel_publications, bond_panel_snapshot, bond_panel_rv_signal,
           bond_panel_returns, bond_panel_rating_pit, bond_panel_returns_tombstone IN SHARE MODE;
DO $coupon_pit_rollback$
DECLARE
    request record;
    target bond_panel_publications%ROWTYPE;
    rollback_id uuid;
    fingerprint text;
    marker jsonb;
    surface text;
    extra_filter text;
    mismatch boolean;
    cas_rows integer;
BEGIN
    SELECT * INTO STRICT request FROM pg_temp.coupon_pit_rollback_request;
    IF btrim(request.authorization) = '' OR request.code_revision !~ '^[0-9a-f]{40}$' THEN
        RAISE EXCEPTION 'rollback requires owner authorization reference and reviewed git SHA';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM bond_panel_app_pointer
                   WHERE product = 'bond_panel_v1' AND publication_id = request.failed_child) THEN
        RAISE EXCEPTION 'rollback requires the expected repair child pointer';
    END IF;
    SELECT * INTO STRICT target FROM bond_panel_publications WHERE publication_id = request.restore_parent;
    IF NOT EXISTS (
        SELECT 1 FROM bond_panel_publications child
        WHERE child.publication_id = request.failed_child
          AND child.parent_publication_id = target.publication_id
          AND child.publication_status = 'validated' AND target.publication_status = 'validated'
          AND child.config_hash = '1863d3d5fa3a0edf' AND child.config_hash = target.config_hash
          AND (child.first_month, child.last_closed_month, child.open_month)
              IS NOT DISTINCT FROM (target.first_month, target.last_closed_month, target.open_month)
          AND child.code_revision = 't3_returns_coupon_pit_repair_v2'
          AND child.source_lineage->'coupon_pit_repair'->>'from_head_publication_id' = target.publication_id::text
          AND child.gate_evidence->'coupon_pit_repair' = child.source_lineage->'coupon_pit_repair'
    ) THEN RAISE EXCEPTION 'rollback requires the validated coupon repair and its unchanged-window parent'; END IF;
    marker := jsonb_build_object(
        'contract', 'coupon_pit_forward_rollback_v1',
        'failed_child_publication_id', request.failed_child,
        'restore_parent_publication_id', request.restore_parent,
        'restore_parent_input_fingerprint', btrim(target.input_fingerprint),
        'owner_authorization', request.authorization,
        'authorized_code_revision', request.code_revision
    );
    fingerprint := encode(sha256(convert_to(marker::text, 'UTF8')), 'hex');
    rollback_id := substr(fingerprint, 1, 32)::uuid;
    -- The restoration source is the parent's ancestry projection, not merely
    -- the rows physically stored on the parent (monthly publications are deltas).
    CREATE TEMP TABLE coupon_pit_rollback_ancestry ON COMMIT DROP AS
    WITH RECURSIVE ancestry(publication_id, parent_publication_id, depth, path, config_hash) AS (
        SELECT p.publication_id, p.parent_publication_id, 0, ARRAY[p.publication_id], p.config_hash
        FROM bond_panel_publications p WHERE p.publication_id = request.restore_parent
        UNION ALL
        SELECT p.publication_id, p.parent_publication_id, a.depth + 1, a.path || p.publication_id, p.config_hash
        FROM ancestry a JOIN bond_panel_publications p ON p.publication_id = a.parent_publication_id
        WHERE NOT p.publication_id = ANY(a.path) AND p.publication_status = 'validated'
          AND (p.config_hash = a.config_hash OR
               (a.config_hash = '1863d3d5fa3a0edf' AND p.config_hash = '0c0d78a866bc1090'))
    ) SELECT * FROM ancestry;
    FOREACH surface IN ARRAY ARRAY['snapshot', 'rv_signal', 'returns', 'rating_pit'] LOOP
        extra_filter := CASE WHEN surface = 'returns' THEN
            'WHERE NOT EXISTS (SELECT 1 FROM pg_temp.coupon_pit_rollback_ancestry ta
             JOIN bond_panel_returns_tombstone t USING (publication_id)
             WHERE t.month = f.month AND t.cusip_id = f.cusip_id AND ta.depth <= a.depth)'
            ELSE '' END;
        EXECUTE format('CREATE TEMP TABLE %I ON COMMIT DROP AS
            SELECT DISTINCT ON (f.month, f.cusip_id) f.*
            FROM pg_temp.coupon_pit_rollback_ancestry a JOIN %I f USING (publication_id)
            %s ORDER BY f.month, f.cusip_id, a.depth',
            'coupon_pit_restore_' || surface, 'bond_panel_' || surface, extra_filter);
        -- Use the same legacy dual-series identity fill as repair republication.
        EXECUTE format('UPDATE pg_temp.%I SET publication_id = $1,
            reference_cusip9 = COALESCE(reference_cusip9, cusip_id),
            distribution_decision_id = CASE WHEN distribution_rule IS NULL THEN NULL ELSE distribution_decision_id END,
            distribution_rule = COALESCE(distribution_rule, ''rule_144a'')', 'coupon_pit_restore_' || surface)
            USING rollback_id;
    END LOOP;
    INSERT INTO bond_panel_publications (
        publication_id, parent_publication_id, publication_status, config_hash,
        input_fingerprint, code_revision, first_month, last_closed_month, open_month,
        snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows, source_lineage, gate_evidence
    ) VALUES (
        rollback_id, request.failed_child, 'prepared', target.config_hash,
        fingerprint, request.code_revision, target.first_month, target.last_closed_month, target.open_month,
        (SELECT count(*) FROM pg_temp.coupon_pit_restore_snapshot),
        (SELECT count(*) FROM pg_temp.coupon_pit_restore_rv_signal),
        (SELECT count(*) FROM pg_temp.coupon_pit_restore_returns),
        (SELECT count(*) FROM pg_temp.coupon_pit_restore_rating_pit),
        target.source_lineage || jsonb_build_object('coupon_pit_rollback', marker),
        jsonb_build_object('coupon_pit_rollback', marker)
    );
    FOREACH surface IN ARRAY ARRAY['snapshot', 'rv_signal', 'returns', 'rating_pit'] LOOP
        EXECUTE format('INSERT INTO %I SELECT * FROM pg_temp.%I',
            'bond_panel_' || surface, 'coupon_pit_restore_' || surface);
    END LOOP;
    -- Preserve the parent's absences as well as its rows if the repair added keys.
    INSERT INTO bond_panel_returns_tombstone (publication_id, month, cusip_id, reason, payload)
    SELECT rollback_id, served.month, served.cusip_id, 'coupon_pit_restore_parent_absence', marker
    FROM bond_panel_current_returns_v1 served
    WHERE NOT EXISTS (SELECT 1 FROM pg_temp.coupon_pit_restore_returns restored
                      WHERE restored.month = served.month AND restored.cusip_id = served.cusip_id);
    UPDATE bond_panel_publications SET publication_status = 'validated', validated_at = now()
    WHERE publication_id = rollback_id AND publication_status = 'prepared';
    UPDATE bond_panel_app_pointer SET publication_id = rollback_id, changed_at = now()
    WHERE product = 'bond_panel_v1' AND publication_id = request.failed_child;
    GET DIAGNOSTICS cas_rows = ROW_COUNT;
    IF cas_rows <> 1 THEN RAISE EXCEPTION 'rollback pointer compare-and-swap lost'; END IF;
    -- All data columns, keys and multiplicities must match the restored parent;
    -- publication_id is necessarily the new auditable child, not the old parent.
    FOREACH surface IN ARRAY ARRAY['snapshot', 'rv_signal', 'returns', 'rating_pit'] LOOP
        EXECUTE format('SELECT EXISTS (
            (SELECT to_jsonb(s) - ''publication_id'' FROM %I s
             EXCEPT ALL SELECT to_jsonb(t) - ''publication_id'' FROM pg_temp.%I t)
            UNION ALL
            (SELECT to_jsonb(t) - ''publication_id'' FROM pg_temp.%I t
             EXCEPT ALL SELECT to_jsonb(s) - ''publication_id'' FROM %I s))',
            'bond_panel_current_' || surface || '_v1', 'coupon_pit_restore_' || surface,
            'coupon_pit_restore_' || surface, 'bond_panel_current_' || surface || '_v1') INTO mismatch;
        IF mismatch THEN RAISE EXCEPTION 'rollback served parent data mismatch:%', surface; END IF;
    END LOOP;
    RAISE NOTICE 'coupon pit rollback validated and pointed: publication_id=% input_fingerprint=% authorization=%',
        rollback_id, fingerprint, request.authorization;
END
$coupon_pit_rollback$;
COMMIT;
-- The pointer commit is durable. A refresh failure requires rerunning just
-- these four refreshes, not replaying the already committed publication.
SET ROLE worker_writer;
SET statement_timeout = '20min';
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rv_signal_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_returns_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_rating_pit_v1_mat;
REFRESH MATERIALIZED VIEW CONCURRENTLY bond_panel_current_snapshot_v1_mat;
RESET statement_timeout;
RESET ROLE;
