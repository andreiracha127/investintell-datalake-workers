"""Focused tests for the public ``sec_regulatory_serving_v1`` materializer.

Uses synthetic ``sec_current_*`` snapshot stand-ins so the serving projection is
exercised independently of the family build machinery. Proves:
  * every present family projects into the public serving surface;
  * the RR1 4-state ``status`` passes through unchanged;
  * the crosswalk confidence gate (forward-note 12) and the fee fact's crosswalk
    evidence (forward-notes 12 & 15);
  * DATA leak absence (forward-notes 1 & 14): no internal filenames, raw_row_id,
    source_run_id, hashes, vendor names, or ``cik:`` / ``row:`` identifiers.

The N-CEN families and the five non-fee RR1 families were removed from the
product on 2026-07-30, together with the 3-state -> 4-state mapping and the
registrant->fund fan-out that were their exclusive machinery.

DSN-agnostic (Global Constraint 9): reads ``SEC_TEST_DATABASE_URL``.
"""
from __future__ import annotations

import json
import os
from datetime import date
from uuid import uuid4

import psycopg
import pytest

from src.sec_serving import contract, materializer

pytestmark = pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL ausente"
)

# Internal-identifier sentinels embedded in every synthetic payload/provenance.
SENT_RAW = "RAWROWLEAK123"
SENT_SRC = "SRCRUNLEAK456"
SENT_MD5 = "d41d8cd98f00b204e9800998ecf8427e"
SENT_CIK = "cik:LEAKCIK789"
SENT_ROW = "row:ROWKEYLEAK000"
SENT_FILE = "SUBMISSION_INTERNAL.tsv"
SENT_VENDOR = "InternalVendorNameX"
FORBIDDEN = [
    SENT_RAW, SENT_SRC, SENT_MD5, "LEAKCIK789", "ROWKEYLEAK000", SENT_FILE, SENT_VENDOR,
    "raw_row_id", "source_run_id", "ingestion_run_id", "text_block_md5", "provenance",
]

# A nested internal blob attached to every family payload; the scrub must remove
# every blocklisted KEY and neutralise the ``row:`` / ``cik:`` entity VALUES that
# survive under a public (non-blocklisted) key.  ``issuer_ref`` carries a ``cik:``
# value under a public key: its KEY is not blocked, so only the value rule can
# neutralise it (item 1a).
LEAK_BLOB = {
    "evidence": {
        "raw_row_id": SENT_RAW,
        "source_run_id": SENT_SRC,
        "text_block_md5": SENT_MD5,
        "registrant_cik": SENT_CIK,
        "fund_raw_row_id": SENT_RAW,
        "nested": {"submission_raw_row_id": SENT_RAW, "entity_key": SENT_ROW,
                   "issuer_ref": SENT_CIK},
    }
}
# A full internal provenance column (must never be projected at all).
PROV = json.dumps({
    "source_run_id": SENT_SRC, "raw_row_id": SENT_RAW, "text_block_md5": SENT_MD5,
    "fund_source_table": SENT_FILE, "vendor": SENT_VENDOR, "registrant_cik": SENT_CIK,
})


def _blob(**public: object) -> str:
    return json.dumps({**public, **LEAK_BLOB})


def _connect() -> psycopg.Connection:
    return psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"])


def _setup(cur) -> tuple[str, object, object]:
    schema = f"sec_serving_{uuid4().hex}"
    run_id, package_id = uuid4(), uuid4()
    cur.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
    cur.execute("CREATE TABLE sec_ingestion_runs(run_id uuid PRIMARY KEY, raw_validated_at timestamptz)")
    cur.execute("CREATE TABLE sec_source_packages(package_id uuid PRIMARY KEY, run_id uuid NOT NULL)")
    cur.execute(
        "CREATE VIEW sec_validated_raw_runs AS "
        "SELECT run_id, raw_validated_at FROM sec_ingestion_runs WHERE raw_validated_at IS NOT NULL"
    )
    materializer.install_schema(cur.connection)
    cur.execute("INSERT INTO sec_ingestion_runs VALUES(%s, now())", (run_id,))
    cur.execute("INSERT INTO sec_source_packages VALUES(%s, %s)", (package_id, run_id))
    _synthetic_snapshots(cur)
    return schema, run_id, package_id


def _synthetic_snapshots(cur) -> None:
    """Create ``sec_current_*`` stand-ins with public columns + embedded leaks."""

    # --- RR1 fact families (4-state status) ---------------------------------
    def rr1_fact(view: str, value_cols: str):
        cur.execute(f"""
            CREATE TABLE {view}(
                series_id text, class_id text, accession_number text,
                data_date date, effective_date date, filed_date date,
                document_id text, measure_id text, dimensions text, occurrence text,
                status text, reason_code text, provenance jsonb, coverage jsonb {value_cols})
        """)

    # original_tag/original_version mirror the real fee view (rr taxonomy tag + version);
    # these RR-namespaced sidecars never match a custom-tag crosswalk row (Constraint 5).
    rr1_fact("sec_current_rr1_fee_profiles",
             ", canonical_concept text, value_numeric numeric, original_tag text, original_version text")
    cur.execute(
        "INSERT INTO sec_current_rr1_fee_profiles VALUES "
        "('S1','C1','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'available',NULL,%s,'{}','net_expense',0.0075,'ExpensesOverAssets','rr/2023')",
        (PROV,),
    )
    cur.execute(
        "INSERT INTO sec_current_rr1_fee_profiles VALUES "
        "('S1','C2','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'unavailable','source_filing_unavailable',%s,'{}','net_expense',NULL,NULL,NULL)",
        (PROV,),
    )
    # A degraded RR1 row: the snapshot status passes through and carries the
    # QUANTITATIVE coverage reason (Task 1c).
    cur.execute(
        "INSERT INTO sec_current_rr1_fee_profiles VALUES "
        "('S1','C3','ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
        "'degraded','coverage_below_certified_threshold',%s,'{}','other_expense',0.001,"
        "'OtherExpensesOverAssets','rr/2023')",
        (PROV,),
    )
    # crosswalk: one approved+high-confidence (emitted), one proposed (excluded),
    # one approved but low-confidence (excluded).
    cur.execute("""
        CREATE TABLE rr1_custom_tag_crosswalk(
            custom_tag text, custom_version text, crosswalk_version text,
            canonical_concept text, confidence numeric, method text, review_status text,
            rationale text, created_at timestamptz DEFAULT now())
    """)
    cur.execute(
        "INSERT INTO rr1_custom_tag_crosswalk(custom_tag,custom_version,crosswalk_version,"
        "canonical_concept,confidence,method,review_status) VALUES "
        "('CustomTagLeakX','v1','cw1','management_fee',0.95,'labels','approved'),"
        "('CustomTagLeakY','v1','cw1','net_expense',0.50,'labels','approved'),"
        "('CustomTagLeakZ','v1','cw1','other_expense',0.99,'labels','proposed')",
    )


def _materialize(cur) -> dict:
    return materializer.materialize(cur.connection, as_of=date(2025, 12, 31), code_revision="test")


def test_materialize_projects_every_present_family_and_promotes_current() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        result = _materialize(cur)
        assert result["state"] == "current"
        # every declared family's source exists -> all written.
        assert set(result["families_written"]) == set(contract.family_names())
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()
        assert pointer is not None
        validated = cur.execute(
            "SELECT lifecycle_state FROM sec_derived_publications "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert validated == "validated"
        # publication_version is pinnable by the app.
        version = cur.execute(
            "SELECT publication_version FROM sec_derived_publications "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert version == 1
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_rr1_status_passes_through_with_typed_reasons() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)

        rows = {
            r[0]: {"state": r[1], "reason": r[2], "payload": r[3]}
            for r in cur.execute(
                "SELECT class_id, state, reason_code, payload::text "
                "FROM sec_current_regulatory_serving_facts WHERE family='rr1_fee'"
            ).fetchall()
        }
        # RR1 status passes through unchanged (available + degraded + unavailable).
        assert {v["state"] for v in rows.values()} == {"available", "degraded", "unavailable"}
        assert rows["C1"]["reason"] is None
        # Task 1c: an RR1 status pass-through degrade is a QUANTITATIVE coverage shortfall.
        assert rows["C3"]["reason"] == "coverage_below_certified_threshold"
        # An unavailable fact carries no payload at all.
        assert rows["C2"]["reason"] == "source_filing_unavailable"
        assert rows["C2"]["payload"] is None
        # forward-note 10 / Constraint 5: fee fraction declares its unit.
        assert '"declared_unit": "fraction"' in rows["C1"]["payload"]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_crosswalk_confidence_gate() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)
        rows = cur.execute(
            "SELECT payload::text FROM sec_current_regulatory_serving_facts "
            "WHERE family='rr1_custom_tag_crosswalk'"
        ).fetchall()
        # only the approved + confidence>=0.80 mapping is emitted; tag never exposed.
        assert len(rows) == 1
        assert "management_fee" in rows[0][0]
        assert "CustomTagLeak" not in rows[0][0]
        assert "net_expense" not in rows[0][0]  # approved-but-low-confidence excluded
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_serving_data_contains_no_internal_identifiers() -> None:
    """forward-notes 1 & 14: prove the serving DATA leaks nothing internal."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        _materialize(cur)
        dump = cur.execute(
            "SELECT string_agg(concat_ws('|', family, series_id, class_id, fund_id, fact_key, "
            "grain_origin, state, COALESCE(reason_code,''), COALESCE(snapshot_reason_code,''), "
            "COALESCE(accession_number,''), COALESCE(document_id,''), COALESCE(payload::text,'')), "
            "chr(10)) FROM sec_regulatory_serving_facts"
        ).fetchone()[0]
        assert dump  # non-empty
        for token in FORBIDDEN:
            assert token not in dump, f"leaked internal token {token!r}"
        # item 1a: a ``cik:`` value under a PUBLIC key (issuer_ref) is neutralised by
        # the scrubber's value rule (not merely stripped by a blocked key), so no raw
        # ``cik:`` identifier survives anywhere in the served data.
        assert "cik:" not in dump
        # the neutralised entity-key fallbacks (row:/cik:) surface the honest sentinel.
        assert "unavailable" in dump
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_fee_crosswalk_evidence_is_confidence_gated_and_tag_free() -> None:
    """forward-notes 12 & 15: a fee fact resolved from an APPROVED high-confidence custom
    mapping surfaces crosswalk evidence (concept + version + confidence, never the tag);
    approved-but-low-confidence and proposed mappings surface nothing."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        # class -> (custom original tag, canonical concept) matched against the setup's
        # synthetic crosswalk: X=approved 0.95, Y=approved 0.50, Z=proposed 0.99.
        for class_id, tag, concept in (
            ("CWH", "CustomTagLeakX", "management_fee"),
            ("CWL", "CustomTagLeakY", "net_expense"),
            ("CWP", "CustomTagLeakZ", "other_expense"),
        ):
            cur.execute(
                "INSERT INTO sec_current_rr1_fee_profiles VALUES "
                "('S1',%s,'ACC-9','2025-12-31','2025-12-31','2026-02-01','D1','M1','','0',"
                "'available',NULL,%s,'{}',%s,0.005,%s,'v1')",
                (class_id, PROV, concept, tag),
            )
        _materialize(cur)

        def payload_for(class_id: str) -> str:
            row = cur.execute(
                "SELECT payload::text FROM sec_current_regulatory_serving_facts "
                "WHERE family='rr1_fee' AND class_id=%s",
                (class_id,),
            ).fetchone()
            return row[0] if row else ""

        high = payload_for("CWH")
        assert '"crosswalk_evidence"' in high
        assert '"crosswalk_version": "cw1"' in high
        assert '"canonical_concept": "management_fee"' in high
        assert "CustomTagLeak" not in high  # the internal tag name never leaks
        # gated out -> no crosswalk_evidence key at all.
        assert "crosswalk_evidence" not in payload_for("CWL")
        assert "crosswalk_evidence" not in payload_for("CWP")
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_partial_family_surface_fails_and_promotes_nothing() -> None:
    """A missing family source view must fail closed: nothing validated, nothing
    promoted (the current pointer is never set)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        cur.execute("DROP TABLE sec_current_rr1_fee_profiles")
        with pytest.raises(materializer.ServingFamilyCoverageError):
            _materialize(cur)
        pointer = cur.execute(
            "SELECT publication_id FROM sec_derived_current_pointers "
            "WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()
        assert pointer is None
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_allow_missing_families_opts_out_of_the_coverage_gate() -> None:
    """The explicit opt-out lets a deliberately partial setup materialize + promote."""
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        cur.execute("DROP TABLE sec_current_rr1_fee_profiles")
        result = materializer.materialize(
            cur.connection, as_of=date(2025, 12, 31), code_revision="test",
            allow_missing_families=True,
        )
        assert result["state"] == "current"
        assert "rr1_fee" not in result["families_written"]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def test_materialize_is_idempotent() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        schema, _run, _pkg = _setup(cur)
        first = _materialize(cur)
        second = _materialize(cur)
        assert first["publication_id"] == second["publication_id"]
        count = cur.execute(
            "SELECT count(*) FROM sec_derived_publications WHERE product='sec_regulatory_serving_v1'"
        ).fetchone()[0]
        assert count == 1
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()
