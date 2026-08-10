-- Generic static mapping; no source-specific public vocabulary.
CREATE TABLE IF NOT EXISTS bond_rating_static (
    cusip9 char(9) PRIMARY KEY CHECK (cusip9 ~ '^[A-Z0-9]{9}$'),
    rating_bucket text NOT NULL CHECK (rating_bucket IN ('AAA','AA','A','BBB','BB','B','CCC','D','NR')),
    rating_as_of_month date NOT NULL CHECK (date_trunc('month', rating_as_of_month)::date = rating_as_of_month),
    rating_state text NOT NULL CHECK (rating_state IN ('rated','not_rated')),
    reason_code text NOT NULL,
    source_sha256 char(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_row_number bigint NOT NULL CHECK (source_row_number > 0),
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((rating_bucket = 'NR') = (rating_state = 'not_rated'))
);
ALTER TABLE bond_rating_static OWNER TO worker_writer;
REVOKE ALL ON bond_rating_static FROM PUBLIC;

CREATE OR REPLACE FUNCTION bond_rating_static_prevent_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bond_rating_static is immutable';
END;
$$;
ALTER FUNCTION bond_rating_static_prevent_mutation() OWNER TO worker_writer;
REVOKE ALL ON FUNCTION bond_rating_static_prevent_mutation() FROM PUBLIC;
DROP TRIGGER IF EXISTS bond_rating_static_immutable ON bond_rating_static;
CREATE TRIGGER bond_rating_static_immutable
BEFORE UPDATE OR DELETE ON bond_rating_static
FOR EACH ROW EXECUTE FUNCTION bond_rating_static_prevent_mutation();
