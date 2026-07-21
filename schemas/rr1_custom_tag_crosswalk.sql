-- Versioned custom-tag crosswalk: the ONLY governance surface through which a
-- filer-custom RR1 tag may ever be admitted to a canonical concept.  It is
-- deliberately born EMPTY of approved mappings -- real population is a future,
-- reviewed operation.  Nothing here auto-injects a custom fact into any canonical
-- metric: the derived snapshot builders read only RR-namespaced (``rr/%``) facts,
-- so an unresolved or low-confidence custom tag can NEVER populate a certified
-- canonical metric (Global Constraint 5).  This table only records the reviewed
-- mapping so a FUTURE, deliberate operation can consult it.
--
-- A custom tag is identified by (custom_tag, custom_version) where custom_version
-- is the accession that defined the tag -- by construction never ``rr/%``.  Each
-- reviewed mapping is versioned by crosswalk_version, carries a confidence in
-- [0,1], the review method (datatype / documentation / labels / calculations /
-- context) that produced it, and a review status.

CREATE TABLE IF NOT EXISTS rr1_custom_tag_crosswalk (
    custom_tag text NOT NULL CHECK (custom_tag <> ''),
    -- The version of a custom tag is the accession where it was defined; a standard
    -- RR taxonomy version (``rr/%``) is NOT custom and can never be registered here.
    custom_version text NOT NULL CHECK (custom_version <> '' AND custom_version NOT LIKE 'rr/%'),
    crosswalk_version text NOT NULL CHECK (crosswalk_version <> ''),
    canonical_concept text NOT NULL CHECK (canonical_concept <> ''),
    confidence numeric NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    method text NOT NULL CHECK (method IN ('datatype', 'documentation', 'labels', 'calculations', 'context')),
    review_status text NOT NULL CHECK (review_status IN ('proposed', 'approved', 'rejected')),
    rationale text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (custom_tag, custom_version, crosswalk_version)
);

-- Governance immutability: once a mapping is adjudicated (approved or rejected) it
-- is a frozen decision -- it can never be silently mutated or deleted.  A row that
-- is still 'proposed' may be adjudicated (updated) or withdrawn (deleted).
CREATE OR REPLACE FUNCTION rr1_custom_tag_crosswalk_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') AND OLD.review_status IN ('approved', 'rejected') THEN
        RAISE EXCEPTION 'RR1 custom-tag crosswalk adjudicated mapping is immutable';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS rr1_custom_tag_crosswalk_guard ON rr1_custom_tag_crosswalk;
CREATE TRIGGER rr1_custom_tag_crosswalk_guard
BEFORE UPDATE OR DELETE ON rr1_custom_tag_crosswalk
FOR EACH ROW EXECUTE FUNCTION rr1_custom_tag_crosswalk_guard();

-- Resolution gate: return the canonical concept for a custom tag ONLY when an
-- APPROVED mapping exists at or above the caller's confidence threshold, taking
-- the highest crosswalk_version.  A proposed/rejected mapping, or one below the
-- threshold, resolves to NULL -- so a low-confidence or unreviewed custom tag can
-- never flow into a canonical metric.
CREATE OR REPLACE FUNCTION rr1_crosswalk_resolve(
    p_custom_tag text,
    p_custom_version text,
    p_min_confidence numeric
) RETURNS text LANGUAGE sql STABLE AS $$
    SELECT canonical_concept
    FROM rr1_custom_tag_crosswalk
    WHERE custom_tag = p_custom_tag
      AND custom_version = p_custom_version
      AND review_status = 'approved'
      AND confidence >= p_min_confidence
    ORDER BY crosswalk_version DESC
    LIMIT 1
$$;
