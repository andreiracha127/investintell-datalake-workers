-- regime_gate worker — destination table (idempotent DDL).
--
-- COMBO Sprint 1: a LIVE debounced 2-of-3 cross-asset risk-off gate PLUS the
-- growth/inflation quadrant, ported from the validated Lean harness
-- (lean-research/TaaCvarSuite/main.py: _live_gate_riskoff / _market_stress /
-- _macro_quadrant). risk_off latches only after the raw 2-of-3 vote holds 21
-- consecutive days (dwell-time hysteresis — the robust innovation the frozen
-- regime_composite_daily LACKED, which missed the entire 2022 bear).
--
-- Votes (each computed from RAW Tiingo closes the worker fetches — SPY/HYG/IEF/TIP):
--   trend    : SPY < SMA200
--   credit   : HYG/IEF ratio < SMA60(ratio)            (the VALIDATED rule;
--              NOT credit_regime_daily's ratio < p20_5y — a different rule)
--   drawdown : SPY 63d-drawdown >= gate_dd (0.06)
-- Quadrant (growth x inflation clock): growth = SPY 126d return sign;
-- inflation = (TIP/IEF breakeven) 126d momentum sign. SLOWDOWN (growth down,
-- inflation up) routes the allocator to the gold haven (downstream sprint).
--
-- The whole series is recomputed each run (adjusted closes change retroactively
-- on dividends), so the upsert is DO UPDATE on every derived column.
--
-- Apply against the cloud:  psql "$DATABASE_URL" -f schemas/regime_gate.sql

CREATE TABLE IF NOT EXISTS regime_gate_daily (
    regime_date     date           NOT NULL,
    state           text           NOT NULL,           -- 'risk_on' | 'risk_off' (latched)
    trend_vote      boolean        NOT NULL,           -- SPY < SMA200
    credit_vote     boolean        NOT NULL,           -- HYG/IEF ratio < SMA60 (raw closes)
    drawdown_vote   boolean        NOT NULL,           -- SPY 63d-drawdown >= gate_dd
    vote_count      smallint       NOT NULL,           -- 0..3
    flip            boolean        NOT NULL DEFAULT false,
    dwell_days      integer        NOT NULL,           -- consecutive days in latched state
    growth_score    numeric(14,8),                     -- SPY 126d return (signed); NULL in warmup
    inflation_score numeric(14,8),                     -- TIP/IEF breakeven 126d momentum (signed)
    quadrant        text,                              -- recovery|expansion|slowdown|contraction|NULL
    spy_close       numeric(14,8),                     -- SPY close (provenance)
    hyg_ief_ratio   numeric(14,8),                     -- credit ratio (provenance)
    tip_ief_ratio   numeric(14,8),                     -- inflation breakeven (provenance)
    spy_dd          numeric(14,8),                     -- SPY drawdown from 63d high (provenance)
    computed_at     timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT regime_gate_daily_pkey PRIMARY KEY (regime_date),
    CONSTRAINT ck_regime_gate_state CHECK (state IN ('risk_on', 'risk_off')),
    CONSTRAINT ck_regime_gate_votes CHECK (vote_count BETWEEN 0 AND 3),
    CONSTRAINT ck_regime_gate_quadrant CHECK (
        quadrant IS NULL OR quadrant IN
        ('recovery', 'expansion', 'slowdown', 'contraction')
    )
);

CREATE INDEX IF NOT EXISTS regime_gate_daily_flip_idx
    ON regime_gate_daily (regime_date DESC) WHERE flip;
