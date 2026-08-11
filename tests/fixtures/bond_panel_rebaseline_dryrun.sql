-- Scratch dry run for the re-baseline pointer transition.
-- Proves: (1) the move is accepted only with the declared contract, (2) it is
-- refused without it, and (3) the four served views still return BOTH the
-- re-baselined history and the live months afterwards -- i.e. the product does
-- not go dark and 2026-07/08 are not dropped.
\set ON_ERROR_STOP on
\pset pager off

CREATE OR REPLACE FUNCTION scratch_seed(
    pub uuid, parent uuid, hash text, first_m date, closed_m date, open_m date,
    months date[], evidence jsonb, closed_only boolean DEFAULT false
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE m date; n int := 0;
BEGIN
    FOREACH m IN ARRAY months LOOP n := n + 2; END LOOP;
    INSERT INTO bond_panel_publications (
        publication_id, parent_publication_id, publication_status, config_hash,
        input_fingerprint, code_revision, first_month, last_closed_month, open_month,
        snapshot_rows, rv_signal_rows, returns_rows, ratings_pit_rows,
        source_lineage, gate_evidence, validated_at
    ) VALUES (
        pub, parent, 'prepared', hash, md5(pub::text) || md5(hash), 'a'||substr(md5(pub::text),1,39),
        first_m, closed_m, open_m, n, CASE WHEN closed_only THEN 1 ELSE n/2 END, CASE WHEN closed_only THEN 1 ELSE n/2 END, n,
        jsonb_build_object('daily_observations','bond_observation_daily',
                           'distribution_rule','rule_144a_and_reg_s',
                           'distribution_mapping_snapshot_id','bond-reg-s-20260810-001'),
        evidence, NULL
    );
    FOREACH m IN ARRAY months LOOP
        INSERT INTO bond_panel_snapshot (publication_id, month, cusip_id, issuer_name,
            distribution_rule, reference_cusip9, distribution_decision_id,
            eligibility_state, eligibility_reason, spread_definition, payload, source_lineage)
        VALUES (pub, m, 'AAA000001', 'ISSUER ONE', 'rule_144a', 'AAA000001', NULL, 'included', 'eligible',
                'ytm_minus_interpolated_dgs', '{}'::jsonb, '{"daily_observations":"x"}'::jsonb),
               (pub, m, 'BBB000002', 'ISSUER TWO', 'rule_144a', 'BBB000002', NULL, 'excluded', 'unnamed_issuer',
                'ytm_minus_interpolated_dgs', '{}'::jsonb, '{"daily_observations":"x"}'::jsonb);
        IF (NOT closed_only) OR m = closed_m THEN
            INSERT INTO bond_panel_rv_signal (publication_id, month, cusip_id, eligibility_state,
                eligibility_reason, distribution_rule, reference_cusip9, distribution_decision_id, payload, source_lineage)
            VALUES (pub, m, 'AAA000001', 'included', 'eligible', 'rule_144a', 'AAA000001', NULL, '{}'::jsonb, '{"daily_observations":"x"}'::jsonb);
            INSERT INTO bond_panel_returns (publication_id, month, cusip_id, total_return, exit_basis,
                distribution_rule, reference_cusip9, distribution_decision_id, payload)
            VALUES (pub, m, 'AAA000001', 0.01, 'observed', 'rule_144a', 'AAA000001', NULL, '{}'::jsonb);
        END IF;
        INSERT INTO bond_panel_rating_pit (publication_id, month, cusip_id, rating_bucket, rating_state, rating_reason,
            distribution_rule, reference_cusip9, distribution_decision_id, payload)
        VALUES (pub, m, 'AAA000001', 'A', 'static_current', 'static_rating_current', 'rule_144a', 'AAA000001', NULL, '{}'::jsonb),
               (pub, m, 'BBB000002', 'NR', 'static_missing', 'static_rating_absent', 'rule_144a', 'BBB000002', NULL, '{}'::jsonb);
    END LOOP;
    UPDATE bond_panel_publications SET publication_status = 'validated', validated_at = now() WHERE publication_id = pub;
END $$;
