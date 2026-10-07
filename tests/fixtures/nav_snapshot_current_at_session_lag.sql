CREATE OR REPLACE FUNCTION fund_nav_snapshot_current_at_v1(
    subject_id uuid, selected_run_id uuid, evaluated_at timestamptz
) RETURNS boolean LANGUAGE sql STABLE AS $$
SELECT COALESCE((
    SELECT run.state='complete' AND $3 >= run.completed_at
       AND p.published_at <= $3
       AND $3 <= policy.valid_through
       AND policy.policy_hash = run.policy_hash
       AND policy.published_at <= $3
       AND active_policy.published_at <= $3
       AND published_risk.completed_at <= $3
       AND published_risk.policy_id = run.policy_id
       AND published_risk.policy_version = run.policy_version
       AND published_risk.policy_hash = run.policy_hash
       AND due.session_date >= run.as_of_session
       AND due_lag.sessions <= 1  -- MAX_SNAPSHOT_SESSION_LAG
       AND closed.session_date >= run.latest_closed_session
       AND closed_lag.sessions <= 1  -- MAX_SNAPSHOT_SESSION_LAG
       AND due.session_date BETWEEN policy.coverage_start AND policy.coverage_end
       AND active.evidence_id IS NOT DISTINCT FROM r.lifecycle_evidence_id
       AND COALESCE(nav_head.revision_id, 0) = r.nav_revision_id
       AND risk_pub.state = 'idle'
       AND risk_pub.revision_id = run.risk_publication_revision
       AND risk_pub.published_risk_run_id = run.published_risk_run_id
       AND published_risk.status = 'complete'
       AND published_risk.run_scope = 'current_full'
       AND (NOT r.admissible OR (
            r.risk_run_id = risk_pub.published_risk_run_id
            AND NOT EXISTS (
                SELECT 1 FROM fund_nav_reexpression_holds hold
                WHERE hold.instrument_id = r.instrument_id
                  AND hold.last_changed_date >= run.window_start
                  AND hold.first_changed_date <= run.window_end
            )
            AND EXISTS (
                SELECT 1 FROM fund_nav_feature_evidence feature
                WHERE feature.risk_run_id = r.risk_run_id
                  AND feature.instrument_id = r.instrument_id
                  AND feature.computed_at <= $3
                  AND feature.calc_date::text = split_part(r.feature_evidence_id, ':', 2)
                  AND feature.definition_version = split_part(r.feature_evidence_id, ':', 3)
                  AND feature.definition_version = published_risk.feature_definition_version
                  AND feature.feature_as_of = r.feature_as_of
                  AND feature.input_max_date = r.risk_input_max_date
                  AND feature.input_fingerprint = r.risk_input_fingerprint
            )
       ))
    FROM fund_nav_readiness_current p
    JOIN fund_nav_readiness_runs run ON run.run_id = p.run_id
    JOIN fund_nav_readiness_v1 r ON r.run_id = run.run_id AND r.instrument_id = $1
    JOIN nav_policy_versions policy
      ON policy.policy_id = run.policy_id AND policy.policy_version = run.policy_version
    JOIN nav_policy_current active_policy
      ON active_policy.readiness_profile = run.readiness_profile
     AND active_policy.policy_id = run.policy_id
     AND active_policy.policy_version = run.policy_version
    LEFT JOIN fund_nav_data_heads nav_head ON nav_head.instrument_id = r.instrument_id
    JOIN fund_nav_risk_publication risk_pub
      ON risk_pub.readiness_profile = run.readiness_profile
    JOIN fund_nav_risk_runs published_risk
      ON published_risk.risk_run_id = run.published_risk_run_id
    LEFT JOIN LATERAL (
        SELECT s.session_date FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.nav_due_at <= $3
        ORDER BY s.session_date DESC LIMIT 1
    ) due ON true
    LEFT JOIN LATERAL (
        SELECT s.session_date FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.valuation_close_at <= $3
        ORDER BY s.session_date DESC LIMIT 1
    ) closed ON true
    LEFT JOIN LATERAL (
        SELECT count(*) AS sessions FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.session_date > run.as_of_session AND s.session_date <= due.session_date
    ) due_lag ON true
    LEFT JOIN LATERAL (
        SELECT count(*) AS sessions FROM nav_valuation_schedules s
        WHERE s.calendar_id = run.calendar_id AND s.calendar_version = run.calendar_version
          AND s.session_date > run.latest_closed_session
          AND s.session_date <= closed.session_date
    ) closed_lag ON true
    LEFT JOIN LATERAL (
        SELECT e.evidence_id FROM nav_instrument_policy_evidence e
        WHERE e.instrument_id = r.instrument_id AND e.policy_id = r.policy_id
          AND e.policy_version = r.policy_version
          AND e.known_at <= $3 AND e.effective_at <= $3 AND e.recorded_at <= $3
        ORDER BY e.effective_at DESC, e.known_at DESC, e.recorded_at DESC,
                 e.evidence_id DESC
        LIMIT 1
    ) active ON true
    WHERE p.readiness_profile='current_daily_nav_v1'
      AND p.run_id=$2
    LIMIT 1
), false)
$$ SECURITY DEFINER SET search_path FROM CURRENT;
