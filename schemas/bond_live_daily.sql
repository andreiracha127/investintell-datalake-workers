-- Daily live-feed side tables owned by the ``bond_live_daily`` worker.
--
-- Neither is a publication: no publication_id, no lifecycle, no current
-- pointer. They are INPUTS -- the same shape ``bond_reference_terms`` already
-- established (2026-08-07): a flat, replaceable table the build reads, kept
-- honest by an explicit load stamp rather than by the derived-publication
-- protocol. The published, promoted artefacts stay ``bond_security_v1`` /
-- ``bond_metric_v1`` / ``bond_serving_v1``.
--
-- The dense price/YTM series itself is NOT here: it lives in the
-- ``bond_observation_daily`` hypertable, whose DDL is owned by the serving
-- repository (it is read on the app's request path). This worker writes into
-- that table but never creates it -- an absent hypertable is a REPORTED no-op,
-- never a silently-created plain table that would lose the time partitioning
-- and the continuous aggregate hanging off it.
--
-- Idempotent DDL (CREATE ... IF NOT EXISTS) so install_schema may re-apply it.

-- ---------------------------------------------------------------------------
-- Treasury yield curve, one row per (day, tenor).
-- ---------------------------------------------------------------------------
-- WHY: duration-targeted portfolio construction needs the curve the bond's
-- yield is measured against, and monitoring needs yesterday's curve to explain
-- today's price move. Stored per tenor rather than as one wide row so a tenor
-- the provider adds or drops changes rows, never the schema.
--
-- UNITS: ``yield_pct`` is PERCENT per annum (4.69 = 4.69%), which is what the
-- curve source publishes and what a curve is universally quoted in. Note this
-- is deliberately NOT the fraction convention of
-- ``bond_observation_daily.ytm`` -- a curve tenor is a quoted rate, a bond's
-- YTM is a solved one, and silently sharing a unit between them is how a 100x
-- seam gets introduced. The column name carries the unit so a reader cannot
-- assume the wrong one.
CREATE TABLE IF NOT EXISTS bond_yield_curve_daily (
    day        date        NOT NULL,
    tenor      text        NOT NULL CHECK (tenor <> ''),
    yield_pct  numeric,
    source     text        NOT NULL,
    loaded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, tenor)
);

COMMENT ON TABLE bond_yield_curve_daily IS
    'Daily treasury yield curve, one row per (day, tenor). yield_pct is PERCENT per annum. Not a publication.';
COMMENT ON COLUMN bond_yield_curve_daily.yield_pct IS
    'Quoted yield in PERCENT per annum (4.69 = 4.69%), NOT the fraction convention of bond_observation_daily.ytm.';

-- Curve-shape reads ("the whole curve on day D") are the access pattern the PK
-- already serves; this index serves the other one ("one tenor through time").
CREATE INDEX IF NOT EXISTS bond_yield_curve_daily_tenor_day_idx
    ON bond_yield_curve_daily (tenor, day);

-- ---------------------------------------------------------------------------
-- Trade-level daily aggregates for the liquid head of the universe.
-- ---------------------------------------------------------------------------
-- WHY: a price series says what a bond was worth; it does not say what it costs
-- to trade. Transaction cost is the input an optimizer needs before it can
-- propose a rebalance, and it can only be measured from the two-sided trade
-- tape. Aggregated to (cusip9, day) here because that is the grain every
-- consumer wants and because retaining raw ticks for 10k bonds would dwarf the
-- price history it explains.
--
-- SIDE SEMANTICS are empirical, not documented by the provider: on the trade
-- tape, side 2's daily median price exceeds side 1's in ~84% of two-sided
-- bond-days that differ, so side 2 is the dealer ASK and side 1 the BID. The
-- estimator is (median_ask - median_bid) / mid * 10000 and is computed ONLY
-- when both sides traded that day; a one-sided day carries a NULL spread, never
-- a zero (which would read as a frictionless bond).
--
-- A NEGATIVE bid_ask_bps is KEPT, not clipped: the estimator's honest condition
-- is "both sides traded", and crossing medians are a real property of a thin
-- day. Clipping would fabricate a floor the tape never showed.
CREATE TABLE IF NOT EXISTS bond_tick_daily (
    cusip9            text        NOT NULL CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    day               date        NOT NULL,
    trade_count       integer,
    par_volume        numeric,
    price_median      numeric,
    bid_price_median  numeric,
    ask_price_median  numeric,
    bid_ask_bps       numeric,
    yield_median      numeric,
    source            text        NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cusip9, day)
);

COMMENT ON TABLE bond_tick_daily IS
    'Daily two-sided trade aggregates for the liquid head (cusip9, day). '
    'bid_ask_bps is (median_ask - median_bid)/mid*10000, present only when BOTH '
    'sides traded; a one-sided day is NULL, never 0. Not a publication.';
COMMENT ON COLUMN bond_tick_daily.bid_ask_bps IS
    'Relative bid-ask in basis points; NULL when only one side traded. Negative values are kept (crossing medians on a thin day are real).';

CREATE INDEX IF NOT EXISTS bond_tick_daily_day_idx ON bond_tick_daily (day);
