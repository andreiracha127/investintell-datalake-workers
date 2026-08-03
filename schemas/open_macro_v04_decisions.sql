-- open_macro v4.0-rev (M-COMP4) monthly REGIME DECISION output.
--
-- Flat table (NOT a hypertable): exactly one row per MONTH-END (PK). The v4 engine
-- is monthly by construction — every sensor is PIT-lagged to what a decision maker
-- knew at the CLOSE of month-end t — so a mid-month row would be a different object
-- wearing this table's name. The month-end CHECK below refuses one.
--
-- SIBLING, NOT SUCCESSOR. This table and open_macro_v04_allocations are DISTINCT
-- from open_macro_v03_decisions / _allocations, which stay untouched. The v03 pair
-- is keyed by BUSINESS DAY and is written every day by the live certified worker;
-- dual-writing a monthly v4 decision into them would collide on the shared as_of PK
-- during the dark run. The v4 engine publishes here, alone, until the owner
-- ratifies the artifact and the Light is flipped to consume it.
--
-- WHAT IS DELIBERATELY NOT HERE
--   * w_hyg has no column in the allocations sibling: no book the router can emit
--     carries a non-zero HYG. That is structural (decision M-COMP3), not an
--     accident of the current dial — see open_macro_v04_allocations.sql.
--   * `high_pressure` (the m2_gdp_z modulator) is NOT published. It died in the
--     graveyard: an ON block of 182 consecutive months is not a regime signal, and
--     publishing an unmeasured diagnostic beside measured ones would let a reader
--     take it for one. Every claim in this table carries a measurement.
--
-- READ PATH. The sanctioned consumer filters valid_status = 'valid' AND honours
-- valid_until. valid_until is the NEXT month-end after as_of at 14:00 UTC — the
-- moment this row's successor becomes computable — so exactly one row is ever
-- "current", historical rows are visibly superseded, and the daily cron has a
-- ~14-hour window on the 1st of the month to publish the successor before the
-- current row lapses. A kill switch stamps valid_status = 'invalidated'.
CREATE TABLE IF NOT EXISTS open_macro_v04_decisions (
    as_of                DATE          PRIMARY KEY,

    -- ── L1, the fiscal router ───────────────────────────────────────────────
    -- deficit_gdp(t) = -( rolling_12m_sum(MTSDS133FMS)(t) / 1000 ) / GDP(t) * 100,
    -- run through the hysteresis band: > 5.0 ENTERS dominance, < 4.0 RETURNS to
    -- contained, and the closed band [4.0, 5.0] HOLDS the state in force.
    fiscal_state         TEXT          NOT NULL
        CHECK (fiscal_state IN ('contained', 'dominance')),
    -- TRUE inside the closed band: the state was CARRIED, not chosen. A book taken
    -- inside the band is auditable as such.
    fiscal_boundary      BOOLEAN       NOT NULL,
    -- consecutive months the CURRENT state has held; 1 on the month of entry. There
    -- is no 0: a row exists only for a month that HAS a state.
    fiscal_state_age_m   INTEGER       NOT NULL CHECK (fiscal_state_age_m >= 1),
    deficit_gdp          NUMERIC       NOT NULL,

    -- ── L3, the credit guard ────────────────────────────────────────────────
    -- alert = arm_a OR arm_b OR (coverage = 'blind'); severe additionally requires
    -- A3 (contained, age >= 3) and A8 (run_age <= 3 OR price-confirmed stress).
    guard_level          TEXT          NOT NULL
        CHECK (guard_level IN ('off', 'alert', 'severe')),
    -- Freshness is measured in NATIVE PUBLICATION PERIODS, never in months: the two
    -- arms publish on different clocks, so one month-count threshold gives opposite
    -- verdicts for the same label age. partial_a = arm A (SLOOS) is the missing one;
    -- partial_b = arm B (M2SL) is. A stale arm contributes FALSE, never its last
    -- known reading. The token is published even when the book does not move.
    guard_coverage       TEXT          NOT NULL
        CHECK (guard_coverage IN ('full', 'partial_a', 'partial_b', 'blind')),
    -- arm A: z(SUBLPDCILSLGNQ, 54m, min 36) > 0.5, AFTER the stale gate.
    arm_a                BOOLEAN       NOT NULL,
    -- arm B: M2SL YoY (%) > 12, AFTER the stale gate.
    arm_b                BOOLEAN       NOT NULL,
    -- length of the current maximal consecutive SEVERE-candidate stretch (0 = not a
    -- candidate this month).
    severe_run_age       INTEGER       NOT NULL CHECK (severe_run_age >= 0),
    -- A8: a candidate past K=3 that price has NOT confirmed. The guard DISARMED the
    -- portfolio; only price (SPY <= -8% of its trailing 12m max) re-arms it.
    severe_degraded      BOOLEAN       NOT NULL,
    stress_confirmed     BOOLEAN       NOT NULL,

    -- ── the quadrant: PUBLISHED DIAGNOSTIC, allocates nothing ────────────────
    -- Under the signed configuration contained amplitude is 0, so compressed_0(q)
    -- IS the CENTER for every q. The quadrant is recorded because a reader must be
    -- able to see the diagnostic move while the weights do not.
    quadrant             TEXT
        CHECK (quadrant IS NULL
               OR quadrant IN ('recovery', 'expansion', 'slowdown', 'contraction')),
    -- chain_fresh   the v03 decision chain read status='valid' this month
    -- chain_carry   the last valid chain reading is being carried (up to 3 months)
    -- no_signal     the chain exists but the carry expired / never seeded
    -- proxy         REPLAY-ONLY pre-chain reconstruction (CFNAI ma3 x CPI YoY accel)
    -- proxy_missing the proxy itself had no reading
    quadrant_source      TEXT          NOT NULL
        CHECK (quadrant_source IN ('chain_fresh', 'chain_carry', 'no_signal',
                                   'proxy', 'proxy_missing')),
    carry_age            INTEGER       NOT NULL CHECK (carry_age >= 0),
    -- the chain's candidate_confidence for the reading IN FORCE (carried along with
    -- a carried quadrant). NULL when no chain reading is in force.
    quadrant_confidence  NUMERIC
        CHECK (quadrant_confidence IS NULL
               OR (quadrant_confidence >= 0 AND quadrant_confidence <= 1)),

    -- ── the consumption token the Light reads ───────────────────────────────
    -- DETERMINISTIC derivation, in this order (see the CHECK below, which states it
    -- as an equation rather than as prose the code could drift from):
    --   guard_blind          coverage = 'blind'  — the guard cannot see; nothing
    --                        downstream should treat the month as observed
    --   dominance_baseline   fiscal_state = 'dominance' — the book is the fiscal
    --                        baseline and the quadrant did not choose it
    --   fresh                contained, quadrant_source = 'chain_fresh'
    --   carried              contained, quadrant_source = 'chain_carry'
    --   no_signal            contained, everything else
    decision_validity    TEXT          NOT NULL
        CHECK (decision_validity IN ('fresh', 'carried', 'dominance_baseline',
                                     'guard_blind', 'no_signal')),
    -- 'live'             this row's as_of was the last COMPLETE month-end at the
    --                    moment of the run AND the row did not exist before: a
    --                    decision the system actually lived through.
    -- 'bootstrap_replay' a retroactive month reconstructed from the CURRENT vintage
    --                    of the inputs. Honest by name: it is a replay, not a lived
    --                    decision, and the golden fixture proves the replay
    --                    reproduces the signed ledger byte for byte.
    -- NEVER DEMOTED on update: once a month was published live, a later re-run that
    -- recomputes it does not relabel it as a replay.
    decision_basis       TEXT          NOT NULL
        CHECK (decision_basis IN ('bootstrap_replay', 'live')),

    -- ── provenance ──────────────────────────────────────────────────────────
    -- sha256 over the canonical `date|%.17g` serialization of every input series
    -- plus the decision chain and the SPY month-end frame (the fixture manifest's
    -- own recipe), combined in a fixed order.
    input_digest_sha256  CHAR(64)      NOT NULL,
    -- the frozen formulation this row was computed under: the canonical sha256 of
    -- the {books, formulation, freshness, gates} block of the formulation freeze.
    -- A row cannot be read as evidence for a formula it was not produced by.
    formulation_sha256   CHAR(64)      NOT NULL,
    code_commit          CHAR(40)      NOT NULL,
    run_id               TEXT          NOT NULL,

    publish_state        TEXT          NOT NULL DEFAULT 'published'
        CHECK (publish_state IN ('publishing', 'published')),
    valid_status         TEXT          NOT NULL DEFAULT 'valid'
        CHECK (valid_status IN ('valid', 'invalidated')),
    -- the NEXT month-end after as_of at 14:00 UTC (see the header note).
    valid_until          TIMESTAMPTZ   NOT NULL,
    invalidated_at       TIMESTAMPTZ,
    invalidated_reason   TEXT,
    created_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- ── fail-loud consistency ───────────────────────────────────────────────
    -- as_of is a month-end: the day after it opens a month.
    CONSTRAINT open_macro_v04_decisions_month_end
        CHECK (as_of = (date_trunc('month', as_of::timestamp)
                        + INTERVAL '1 month' - INTERVAL '1 day')::date),
    -- A3: SEVERE is unreachable under fiscal dominance. Not a convention — the
    -- guard's defensive book is a CONTAINED-state instrument.
    CONSTRAINT open_macro_v04_decisions_severe_requires_contained
        CHECK (guard_level <> 'severe' OR fiscal_state = 'contained'),
    -- A3, second half: a SEVERE month is at least the third month of the contained
    -- state that admits it.
    CONSTRAINT open_macro_v04_decisions_severe_requires_contained_age
        CHECK (guard_level <> 'severe' OR fiscal_state_age_m >= 3),
    -- A8: a degraded candidate is exactly an ALERT. If it had emitted it would be
    -- 'severe'; if it were not a candidate it could not be degraded.
    CONSTRAINT open_macro_v04_decisions_degraded_is_alert
        CHECK (NOT severe_degraded OR guard_level = 'alert'),
    -- a candidate month is an ALERT month by construction (candidate => alert).
    CONSTRAINT open_macro_v04_decisions_candidate_is_not_off
        CHECK (severe_run_age = 0 OR guard_level <> 'off'),
    -- blind => ALERT at minimum: a guard that cannot see compresses conservatively
    -- and says so. 'off' would be a claim of "nothing to see", which is the one
    -- thing a blind guard cannot know.
    CONSTRAINT open_macro_v04_decisions_blind_is_not_off
        CHECK (guard_coverage <> 'blind' OR guard_level <> 'off'),
    -- guard OFF means neither arm fired (and, with the constraint above, that the
    -- guard was not blind).
    CONSTRAINT open_macro_v04_decisions_off_has_no_armed_arm
        CHECK (guard_level <> 'off' OR (NOT arm_a AND NOT arm_b)),
    -- a STALE arm cannot fire: arm_a survives only when arm A is fresh (coverage
    -- full or partial_b), arm_b only when arm B is fresh (full or partial_a).
    CONSTRAINT open_macro_v04_decisions_arm_a_requires_coverage
        CHECK (NOT arm_a OR guard_coverage IN ('full', 'partial_b')),
    CONSTRAINT open_macro_v04_decisions_arm_b_requires_coverage
        CHECK (NOT arm_b OR guard_coverage IN ('full', 'partial_a')),
    -- the quadrant is absent exactly when its source says it abstained.
    CONSTRAINT open_macro_v04_decisions_quadrant_source_consistent
        CHECK ((quadrant IS NULL)
               = (quadrant_source IN ('no_signal', 'proxy_missing'))),
    -- a FRESH chain reading has age 0; a CARRIED one is at least one month old.
    CONSTRAINT open_macro_v04_decisions_carry_age_consistent
        CHECK ((quadrant_source <> 'chain_fresh' OR carry_age = 0)
           AND (quadrant_source <> 'chain_carry' OR carry_age >= 1)),
    -- confidence belongs to a chain reading; the proxy has none to report.
    CONSTRAINT open_macro_v04_decisions_confidence_requires_chain
        CHECK (quadrant_confidence IS NULL
               OR quadrant_source IN ('chain_fresh', 'chain_carry')),
    -- the consumption token IS this equation. Stated here so the DB, not a comment,
    -- is what forbids a hand-written or drifted token.
    CONSTRAINT open_macro_v04_decisions_validity_derivation
        CHECK (decision_validity = CASE
            WHEN guard_coverage = 'blind'          THEN 'guard_blind'
            WHEN fiscal_state = 'dominance'        THEN 'dominance_baseline'
            WHEN quadrant_source = 'chain_fresh'   THEN 'fresh'
            WHEN quadrant_source = 'chain_carry'   THEN 'carried'
            ELSE 'no_signal' END),
    -- invalidation timestamp is present iff the row is invalidated
    CONSTRAINT open_macro_v04_decisions_invalidation_consistent
        CHECK ((valid_status = 'invalidated') = (invalidated_at IS NOT NULL))
);

-- Reader index: the current valid row by freshness horizon.
CREATE INDEX IF NOT EXISTS idx_open_macro_v04_decisions_valid
    ON open_macro_v04_decisions (valid_status, valid_until DESC);
