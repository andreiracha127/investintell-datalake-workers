-- Official Federal Reserve Summary of Economic Projections (SEP).
--
-- Each exact HTML byte stream is an immutable release observation. A corrected
-- Federal Reserve page therefore creates a new row with a new source_sha256;
-- neither the original release nor its normalized distribution is overwritten.
CREATE TABLE IF NOT EXISTS fomc_sep_releases (
    release_id       uuid PRIMARY KEY,
    release_date     date NOT NULL CHECK (release_date >= DATE '2012-01-01'),
    source_url       text NOT NULL CHECK (
        source_url ~ '^https://www[.]federalreserve[.]gov/monetarypolicy/fomcprojtabl[0-9]{8}[.]htm$'
    ),
    source_sha256    char(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    parser_version   text NOT NULL CHECK (parser_version <> ''),
    source_format    text NOT NULL CHECK (
        source_format IN ('quarter_point', 'eighth_point', 'range_bins')
    ),
    policy_source_url text NOT NULL CHECK (
        policy_source_url ~ '^https://www[.]federalreserve[.]gov/newsevents/(press/monetary/[0-9]{8}a[.]htm|pressreleases/monetary[0-9]{8}a[.]htm)$'
    ),
    policy_source_sha256 char(64) NOT NULL CHECK (policy_source_sha256 ~ '^[0-9a-f]{64}$'),
    policy_rate_lower_pct numeric(6,3) NOT NULL,
    policy_rate_upper_pct numeric(6,3) NOT NULL,
    policy_rate_midpoint_pct numeric(6,3) NOT NULL,
    observed_at      timestamptz NOT NULL,
    fetched_at       timestamptz NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (release_date, source_sha256, policy_source_sha256),
    CHECK (policy_rate_lower_pct <= policy_rate_upper_pct),
    CHECK (policy_rate_midpoint_pct = (policy_rate_lower_pct + policy_rate_upper_pct) / 2)
);

CREATE TABLE IF NOT EXISTS fomc_sep_rate_distributions (
    release_id        uuid NOT NULL REFERENCES fomc_sep_releases(release_id) ON DELETE RESTRICT,
    projection_horizon text NOT NULL CHECK (
        projection_horizon = 'longer_run'
        OR projection_horizon ~ '^[0-9]{4}$'
    ),
    rate_bin_low      numeric(6,3) NOT NULL,
    rate_bin_high     numeric(6,3) NOT NULL,
    bin_kind          text NOT NULL CHECK (bin_kind IN ('point', 'range')),
    participant_count smallint NOT NULL CHECK (participant_count > 0 AND participant_count <= 25),
    created_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (release_id, projection_horizon, rate_bin_low, rate_bin_high),
    CHECK (rate_bin_low <= rate_bin_high),
    CHECK (
        (bin_kind = 'point' AND rate_bin_low = rate_bin_high)
        OR (bin_kind = 'range' AND rate_bin_low < rate_bin_high)
    )
);

CREATE TABLE IF NOT EXISTS fomc_sep_current_pointer (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    release_id uuid NOT NULL UNIQUE REFERENCES fomc_sep_releases(release_id) ON DELETE RESTRICT,
    set_at     timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION fomc_sep_immutable_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

DROP TRIGGER IF EXISTS fomc_sep_releases_immutable ON fomc_sep_releases;
CREATE TRIGGER fomc_sep_releases_immutable
BEFORE UPDATE OR DELETE ON fomc_sep_releases
FOR EACH ROW EXECUTE FUNCTION fomc_sep_immutable_guard();

DROP TRIGGER IF EXISTS fomc_sep_rate_distributions_immutable ON fomc_sep_rate_distributions;
CREATE TRIGGER fomc_sep_rate_distributions_immutable
BEFORE UPDATE OR DELETE ON fomc_sep_rate_distributions
FOR EACH ROW EXECUTE FUNCTION fomc_sep_immutable_guard();

CREATE OR REPLACE FUNCTION fomc_sep_current_pointer_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_date date;
    prior_release_date date;
    horizon_count integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'fomc_sep_current_pointer cannot be deleted';
    END IF;
    SELECT release_date INTO target_date
    FROM fomc_sep_releases WHERE release_id = NEW.release_id;
    SELECT count(DISTINCT projection_horizon) INTO horizon_count
    FROM fomc_sep_rate_distributions WHERE release_id = NEW.release_id;
    IF target_date IS NULL OR horizon_count < 2 THEN
        RAISE EXCEPTION 'current SEP release requires a complete normalized distribution';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        SELECT release_date INTO prior_release_date
        FROM fomc_sep_releases WHERE release_id = OLD.release_id;
        IF target_date < prior_release_date THEN
            RAISE EXCEPTION 'current SEP release date cannot regress';
        END IF;
    END IF;
    NEW.set_at := now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS fomc_sep_current_pointer_guard ON fomc_sep_current_pointer;
CREATE TRIGGER fomc_sep_current_pointer_guard
BEFORE INSERT OR UPDATE OR DELETE ON fomc_sep_current_pointer
FOR EACH ROW EXECUTE FUNCTION fomc_sep_current_pointer_guard();

CREATE INDEX IF NOT EXISTS idx_fomc_sep_releases_date
    ON fomc_sep_releases (release_date DESC, fetched_at DESC);

CREATE OR REPLACE VIEW fomc_sep_current_release AS
SELECT r.*
FROM fomc_sep_current_pointer p
JOIN fomc_sep_releases r ON r.release_id = p.release_id;

CREATE OR REPLACE VIEW fomc_sep_current_distribution AS
SELECT d.*
FROM fomc_sep_current_pointer p
JOIN fomc_sep_rate_distributions d ON d.release_id = p.release_id;
