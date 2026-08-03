-- open_macro v4.0-rev (M-COMP4) monthly ALLOCATION output — the product.
--
-- Flat table (NOT a hypertable): exactly one row per MONTH-END (PK), FK to the
-- decision row so an allocation never exists without the regime decision that
-- produced it. Sibling of open_macro_v04_decisions; the v03 pair is untouched.
--
-- WHY THERE IS NO w_hyg
-- ---------------------
-- HYG is in the ENGINE's universe (harness/phase0q/book_router.UNIVERSE) at a base
-- weight of zero, as the high-yield sensitivity column. It is NOT in this schema
-- because no book the router can emit carries a non-zero HYG: the barbell's credit
-- destination is LQD, the compression is a convex combination of books that are all
-- zero there, and the ALERT blend of zeros is zero. That is structural under the
-- signed configuration (decision M-COMP3), not an accident of the current dial, and
-- a permanently-zero column would read as a live sleeve that simply never traded.
-- If a future dial gives HYG a weight, THIS TABLE MUST GAIN THE COLUMN — the sum
-- constraint below would otherwise silently absorb it into a broken invariant.
--
-- LQD, by contrast, DOES get a column: the dominance book carries LQD 0.155 through
-- the B60-LQD barbell, which replaces the {TLT, TIP, SHY} sleeve by {SHY, LQD} at a
-- 60/40 split. It is not decorative and it is not zero.
CREATE TABLE IF NOT EXISTS open_macro_v04_allocations (
    as_of                   DATE        PRIMARY KEY
        REFERENCES open_macro_v04_decisions (as_of),

    -- The EXACT set of ids harness/phase0q/book_router.emitted_books() can produce:
    -- the CENTER, the four compressed_0(q) ids (all equal to the CENTER under
    -- amplitude 0 — the id records which quadrant was READ), the barbelled
    -- dominance book, the SEVERE book, and the ALERT blend of each of the above.
    -- Thirteen ids, no more: an id outside this list means the router changed and
    -- the change must be ratified, not absorbed.
    book_id                 TEXT        NOT NULL
        CHECK (book_id IN (
            'center',
            'compressed_0(recovery)',
            'compressed_0(expansion)',
            'compressed_0(slowdown)',
            'compressed_0(contraction)',
            'alert50:center',
            'alert50:compressed_0(recovery)',
            'alert50:compressed_0(expansion)',
            'alert50:compressed_0(slowdown)',
            'alert50:compressed_0(contraction)',
            'expansion_c50+bb',
            'alert50:expansion_c50+bb',
            'severe:contraction_c50')),

    w_spy                   NUMERIC     NOT NULL CHECK (w_spy >= 0 AND w_spy <= 1),
    w_tlt                   NUMERIC     NOT NULL CHECK (w_tlt >= 0 AND w_tlt <= 1),
    w_tip                   NUMERIC     NOT NULL CHECK (w_tip >= 0 AND w_tip <= 1),
    w_gld                   NUMERIC     NOT NULL CHECK (w_gld >= 0 AND w_gld <= 1),
    w_dbc                   NUMERIC     NOT NULL CHECK (w_dbc >= 0 AND w_dbc <= 1),
    w_shy                   NUMERIC     NOT NULL CHECK (w_shy >= 0 AND w_shy <= 1),
    w_lqd                   NUMERIC     NOT NULL CHECK (w_lqd >= 0 AND w_lqd <= 1),

    -- The last daily session at or before as_of at which EVERY book instrument has
    -- a usable adjusted close. Not the month-end calendar date: a date one
    -- instrument never traded is not a date this portfolio can be priced on.
    priced_at               DATE        NOT NULL CHECK (priced_at <= as_of),

    -- provenance, identical in meaning to the decision row's (same run, same
    -- inputs, same frozen formulation).
    input_digest_sha256     CHAR(64)    NOT NULL,
    formulation_sha256      CHAR(64)    NOT NULL,
    code_commit             CHAR(40)    NOT NULL,
    run_id                  TEXT        NOT NULL,

    publish_state           TEXT        NOT NULL DEFAULT 'published'
        CHECK (publish_state IN ('publishing', 'published')),
    valid_status            TEXT        NOT NULL DEFAULT 'valid'
        CHECK (valid_status IN ('valid', 'invalidated')),
    -- the NEXT month-end after as_of at 14:00 UTC, same horizon as the decision.
    valid_until             TIMESTAMPTZ NOT NULL,
    invalidated_at          TIMESTAMPTZ,
    invalidated_reason      TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- ── fail-loud consistency ───────────────────────────────────────────────
    CONSTRAINT open_macro_v04_allocations_month_end
        CHECK (as_of = (date_trunc('month', as_of::timestamp)
                        + INTERVAL '1 month' - INTERVAL '1 day')::date),
    -- the seven published weights sum to 1. They can, because HYG is structurally
    -- zero (see the header): the engine's eight-column vector loses nothing here.
    CONSTRAINT open_macro_v04_allocations_weights_sum
        CHECK (abs(w_spy + w_tlt + w_tip + w_gld + w_dbc + w_shy + w_lqd - 1)
               < 1e-9),
    -- mandate invariant: the risk group SPY + DBC stays at or under 0.65.
    CONSTRAINT open_macro_v04_allocations_risk_cap
        CHECK (w_spy + w_dbc <= 0.65 + 1e-9),
    -- mandate invariant: the defensive group TLT + SHY + TIP stays at or above 0.20.
    CONSTRAINT open_macro_v04_allocations_defensive_floor
        CHECK (w_tlt + w_shy + w_tip >= 0.20 - 1e-9),
    -- invalidation timestamp is present iff the row is invalidated
    CONSTRAINT open_macro_v04_allocations_invalidation_consistent
        CHECK ((valid_status = 'invalidated') = (invalidated_at IS NOT NULL))
);

-- Reader index: the current valid row by freshness horizon.
CREATE INDEX IF NOT EXISTS idx_open_macro_v04_allocations_valid
    ON open_macro_v04_allocations (valid_status, valid_until DESC);
