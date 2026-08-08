"""Behavioural contract for the v2 SEC-API Render fallback worker."""

from __future__ import annotations

import contextlib
import hashlib

from src.nport import secapi_fixed_income as secapi
from src.workers import nport_fixed_income_secapi_fallback as worker


ACCESSION = "0000123456-26-000001"
PUBLICATION = "00000000-0000-0000-0000-000000000011"
RUN = "00000000-0000-0000-0000-000000000022"


def _filing() -> dict[str, object]:
    return {
        "accessionNo": ACCESSION,
        "formType": "NPORT-P",
        "fundInfo": {"totAssets": "100", "curMetrics": {"curMetric": []}},
    }


def _evidence() -> secapi.RenderFallbackEvidence:
    return secapi.RenderFallbackEvidence(
        filing=_filing(),
        form_response={"filings": []},
        query_response={"filings": [{"accessionNo": ACCESSION}]},
        render_raw=b"<edgarSubmission/>",
        document_url="https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/primary_doc.xml",
    )


class _Db:
    def __init__(self, accessions: list[str] | None = None, *, locked: bool = False):
        self.accessions = accessions if accessions is not None else [ACCESSION]
        self.locked = locked
        self.writes: list[tuple[secapi.FilingProjection, secapi.RenderFallbackEvidence]] = []

    def install_schema(self) -> None:
        self.installed = True

    def terminal_accessions(self, *_args: str) -> list[str]:
        return list(self.accessions)

    @contextlib.contextmanager
    def advisory_lock(self, *_args: str):
        yield not self.locked

    @contextlib.contextmanager
    def transaction(self):
        yield

    def existing_hashes(self, *_args: str):
        return None

    def write(self, projection, evidence) -> None:
        self.writes.append((projection, evidence))


class _Client:
    def __init__(self, evidence: secapi.RenderFallbackEvidence | None = None):
        self.evidence = evidence or _evidence()
        self.calls = 0

    def fetch_render_fallback_evidence(self, accession: str, *, on_provider_call):
        assert accession == ACCESSION
        for _ in range(3):
            on_provider_call()
            self.calls += 1
        return self.evidence


def _run(db: _Db, client: _Client, **kwargs):
    max_api_calls = kwargs.pop("max_api_calls", 3)
    return worker.run(
        publication_id=PUBLICATION,
        source_run_id=RUN,
        max_accessions=1,
        max_api_calls=max_api_calls,
        request_interval_seconds=0.01,
        db=db,
        client_factory=lambda: client,
        sleeper=lambda _seconds: None,
        **kwargs,
    )


def test_evidence_hashes_are_separate_and_compact_payload_never_contains_raw_xml():
    db, client = _Db(), _Client()

    result = _run(db, client)

    assert result["state"] == "complete"
    projection, evidence = db.writes[0]
    hashes = worker.evidence_hashes(evidence, projection)
    assert hashes["form_nport_response_sha256"] == secapi.response_sha256({"filings": []})
    assert hashes["render_raw_sha256"] == hashlib.sha256(b"<edgarSubmission/>").hexdigest()
    assert hashes["compact_payload_sha256"] == hashlib.sha256(projection.compact_json.encode()).hexdigest()
    assert "edgarSubmission" not in projection.compact_json


def test_no_terminal_accessions_makes_no_provider_calls():
    db, client = _Db([]), _Client()

    assert _run(db, client)["state"] == "complete"
    assert client.calls == 0
    assert db.writes == []


def test_happy_path_writes_manifest_then_fund_and_rates_in_one_transaction():
    db, client = _Db(), _Client()

    assert _run(db, client)["success"] == 1
    assert len(db.writes) == 1
    projection, _ = db.writes[0]
    assert projection.extractor_version == worker.PARSER_VERSION
    assert projection.fund["source_run_id"] == RUN


def test_postgres_writer_uses_database_document_id_before_compact_projections():
    projection = secapi.extract_filing(
        _filing(), publication_id=PUBLICATION, source_run_id=RUN,
        extractor_version=worker.PARSER_VERSION,
    )

    class Result:
        def fetchone(self):
            return ("00000000-0000-5000-8000-000000000033",)

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, sql, values):
            self.calls.append((sql, values))
            return Result()

    conn = Connection()
    worker.PostgresFallbackDb(conn).write(projection, _evidence())
    statements = [sql for sql, _values in conn.calls]
    assert "nport_fixed_income_secapi_fallback_document_id_v2" in statements[0]
    assert "fallback_manifest_v2" in statements[0]
    assert "fallback_fund_info_v2" in statements[1]
    assert all("raw_xml" not in statement for statement in statements)
    assert all(b"edgarSubmission" not in values for _statement, values in conn.calls)


def test_budget_below_a_complete_form_query_render_chain_makes_no_calls():
    db, client = _Db(), _Client()

    result = _run(db, client, max_api_calls=2)

    assert result["state"] == "partial"
    assert result["api_calls"] == 0
    assert client.calls == 0


def test_unsafe_resolver_fails_without_writing_overlay():
    class UnsafeClient(_Client):
        def fetch_render_fallback_evidence(self, accession: str, *, on_provider_call):
            raise secapi.AccessionMismatchError("resolver filing URL is not canonical")

    db = _Db()
    result = _run(db, UnsafeClient())

    assert result["state"] == "failed"
    assert db.writes == []


def test_existing_overlay_is_idempotent_and_scoped_lock_prevents_calls():
    class ExistingDb(_Db):
        def existing_hashes(self, *_args: str):
            return {**worker.evidence_hashes(_evidence(), secapi.extract_filing(
                _filing(), publication_id=PUBLICATION, source_run_id=RUN,
                extractor_version=worker.PARSER_VERSION,
            )), "parser_version": worker.PARSER_VERSION, "resolver_version": worker.RESOLVER_VERSION}

    db, client = ExistingDb(), _Client()
    result = _run(db, client)
    assert result["state"] == "complete"
    assert client.calls == 0

    locked, locked_client = _Db(locked=True), _Client()
    assert _run(locked, locked_client)["state"] == "locked"
    assert locked_client.calls == 0
