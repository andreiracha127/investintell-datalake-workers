-- Neutral bond reference terms (bond_reference_terms).
--
-- WHAT THIS IS
--   A flat, replaceable reference table of instrument terms keyed by CUSIP9,
--   used ONLY to fill terms the publication chain does not carry. Measured on
--   the live curated cohort (2026-08-07): the chain reports coupon and maturity
--   almost completely (8 missing coupons, 0 missing maturities of 10,073) but
--   reports seniority, the secured flag, callability and amount outstanding for
--   ZERO securities. Those four are what this table exists to supply.
--
-- WHAT THIS IS NOT
--   * NOT a publication. It carries no publication_id, no lifecycle and no
--     pointer: it is an INPUT the security-master build reads, exactly like the
--     N-PORT holdings it already reads. The published, promoted artefact stays
--     bond_security_v1.
--   * NOT authoritative over the chain. ``apply_reference_terms`` fills gaps
--     only; a term the observations resolved is never overwritten.
--   * NOT a source name. The published basis token is the neutral
--     ``vendor_reference``; no serving payload ever names where terms came from.
--
-- TRANSPORT (decided 2026-08-07, documented so the next operator does not guess)
--   The reference bodies are local JSON profiles on the owner's workstation; the
--   pipeline runs on Railway with no access to that filesystem. So the profiles
--   are flattened to a CSV by ``scripts/load_bond_reference_terms.py`` and
--   \copy'd into this table over ``railway ssh``. The build then reads the TABLE
--   and never a file, which keeps the worker deployable and the load auditable
--   (``loaded_at`` + ``batch_label``). The table is optional: a build with the
--   relation absent or empty simply enriches nothing and says so.
--
-- Idempotent DDL (CREATE ... IF NOT EXISTS) so install_schema may re-apply it.

CREATE TABLE IF NOT EXISTS bond_reference_terms (
    cusip9                text PRIMARY KEY CHECK (cusip9 ~ '^[0-9A-Z]{9}$'),
    isin                  text,
    coupon_rate           numeric,          -- percent of par per annum
    coupon_type           text,
    maturity_date         date,
    issue_date            date,
    seniority             text,
    secured               text CHECK (secured IS NULL OR secured IN ('secured','unsecured','not_reported')),
    day_count             text,
    payment_frequency     text,
    callable              boolean,
    amount_outstanding_mm numeric,          -- millions of the issue currency
    batch_label           text NOT NULL,
    loaded_at             timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE bond_reference_terms IS
    'Neutral CUSIP9-keyed reference terms; gap-fill input to bond_security_v1 (basis vendor_reference). Not a publication.';
