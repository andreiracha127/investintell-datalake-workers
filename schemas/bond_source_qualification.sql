-- Phase-10 source-qualification registry (Increment 3, Task 6 — THE GATE).
--
-- The MINIMAL structure a FUTURE activation fills to record that a qualified,
-- AUTHORIZED source contract exists for a Phase-10 metric's inputs (production
-- prices / curves / ratings).  TODAY IT IS EMPTY: no such source is authorized
-- (plan Global Constraint #3), so src.bonds.phase10_gate.source_qualified(metric)
-- is False for every metric and the Phase-10 gate never passes.
--
-- This is NOT a serving/publication surface: it feeds no bond_serving_v1 surface,
-- no sec_derived_publications product and no current pointer, and no value from it
-- ever reaches the app registry or any API/frontend.  The gate READS it (SELECT
-- only); nothing in src/bonds/phase10_gate.py ever writes it.  A future
-- qualified-source contract INSERTs one ACTIVE row (qualified_to IS NULL) per
-- metric it authorizes, at which point that metric's source_qualified predicate
-- can begin to pass and the app registry contract is deliberately regenerated.
--
-- Idempotent (IF NOT EXISTS) so an installer may re-apply it.
CREATE TABLE IF NOT EXISTS bond_source_qualification (
    metric_id           text NOT NULL CHECK (metric_id <> ''),
    source_contract_ref text NOT NULL CHECK (source_contract_ref <> ''),
    qualified_from      timestamptz NOT NULL DEFAULT now(),
    -- NULL => the qualification is ACTIVE (open-ended); a timestamp retires it.
    qualified_to        timestamptz,
    CHECK (qualified_to IS NULL OR qualified_to > qualified_from),
    PRIMARY KEY (metric_id, source_contract_ref)
);
