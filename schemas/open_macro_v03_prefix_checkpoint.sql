-- Checkpoint for the certified pre-cut input prefix of open_macro_v03.
--
-- WHY: every run re-read the ENTIRE pre-cut prefix -- all seed-series vintages in
-- ``macro_observation_vintage`` and all sleeve rows in ``eod_prices`` from 1998
-- through the pack cut -- shipped them to the worker, re-serialized them in the
-- certified canonical format and re-hashed them, only to compare the digest with
-- a pin that is a constant of the committed pack. That is a fixed window: on a
-- day where nothing pre-cut moved, the whole pass reproduces a known answer.
--
-- WHAT THIS IS NOT: it does not weaken the anti-tamper invariant. The checkpoint
-- is trusted only while a CHEAP database-side signature over the SAME window --
-- row count, max available_at/date and two differently-seeded hash sums over the
-- full row text -- still equals the signature recorded when the byte-exact digest
-- was last proven against the pack pin. Any pre-cut insert, delete or in-place
-- correction moves that signature and forces the full re-read + re-hash, which is
-- where the retroactive-mutation alarm lives (unchanged). A run may also be forced
-- back onto the full path at any time (OPEN_MACRO_V03_FULL_PREFIX_HASH=1), and the
-- checkpoint expires on its own after
-- OPEN_MACRO_V03_PREFIX_CHECKPOINT_MAX_AGE_HOURS (default 168h) so a byte-exact
-- re-proof happens at least weekly regardless of the signature.
--
-- The table is deliberately OUTSIDE the governed open_macro_v03 catalog
-- (EXPECTED_SCHEMA / verify_schema): it holds no product state, only a cache of a
-- verification result, and losing it costs one full verification.
--
-- APPLY: a migration, like every other schema file here. The worker never creates
-- it; when it is absent every run simply takes the full path.

CREATE TABLE IF NOT EXISTS open_macro_v03_prefix_checkpoint (
    -- One row per (certified pack, prefix table): a new pack cut invalidates by
    -- construction, because the pins it must reproduce belong to that pack.
    pack_sha256 char(64) NOT NULL CHECK (pack_sha256 ~ '^[0-9a-f]{64}$'),
    prefix_table text NOT NULL CHECK (prefix_table <> ''),
    -- The pin this checkpoint attests was reproduced byte-for-byte.
    prefix_sha256 char(64) NOT NULL CHECK (prefix_sha256 ~ '^[0-9a-f]{64}$'),
    -- Cheap change detector over the same window (canonical JSON, see
    -- src/workers/open_macro_v03.py::prefix_signature).
    signature text NOT NULL CHECK (signature <> ''),
    row_count bigint NOT NULL CHECK (row_count >= 0),
    verified_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pack_sha256, prefix_table)
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_runtime') THEN
        EXECUTE 'ALTER TABLE open_macro_v03_prefix_checkpoint OWNER TO app_runtime';
    END IF;
END
$$;
