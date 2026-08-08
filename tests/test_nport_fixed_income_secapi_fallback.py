"""Behavioural contract for the v2 SEC-API Render fallback worker."""

from __future__ import annotations

import contextlib
import hashlib
import threading
import time

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

    def fetch_render_fallback_evidence(self, accession: str, *, on_provider_call=None, invoke_provider_call=None):
        assert accession == ACCESSION
        for _ in range(3):
            invoke_provider_call(lambda: None)
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


def test_complete_but_incompatible_overlay_evidence_fails_before_provider_calls():
    class IncompatibleDb(_Db):
        def incompatible_accessions(self, *_args: str) -> list[str]:
            return [ACCESSION]

    db, client = IncompatibleDb([]), _Client()
    result = _run(db, client)

    assert result["state"] == "conflict"
    assert result["accession_number"] == ACCESSION
    assert client.calls == 0


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
        def fetch_render_fallback_evidence(
            self, accession: str, *, on_provider_call=None, invoke_provider_call=None
        ):
            raise secapi.AccessionMismatchError("resolver filing URL is not canonical")

    db = _Db()
    result = _run(db, UnsafeClient())

    assert result["state"] == "failed"
    assert db.writes == []


def test_incomplete_existing_overlay_is_refetched_for_projection_repair():
    class ExistingDb(_Db):
        def existing_hashes(self, *_args: str):
            return {**worker.evidence_hashes(_evidence(), secapi.extract_filing(
                _filing(), publication_id=PUBLICATION, source_run_id=RUN,
                extractor_version=worker.PARSER_VERSION,
            )), "parser_version": worker.PARSER_VERSION, "resolver_version": worker.RESOLVER_VERSION}

    db, client = ExistingDb(), _Client()
    result = _run(db, client)
    assert result["state"] == "complete"
    assert client.calls == 3
    assert len(db.writes) == 1


def test_scoped_lock_prevents_calls():
    locked, locked_client = _Db(locked=True), _Client()
    assert _run(locked, locked_client)["state"] == "locked"
    assert locked_client.calls == 0



def test_concurrent_fetches_use_distinct_clients_and_keep_db_writes_on_main_thread():
    accession_two = "0000123456-26-000002"
    barrier = threading.Barrier(2)
    main_thread = threading.get_ident()
    created: list[object] = []

    def evidence_for(accession: str) -> secapi.RenderFallbackEvidence:
        return secapi.RenderFallbackEvidence(
            filing={**_filing(), "accessionNo": accession},
            form_response={"filings": []},
            query_response={"filings": [{"accessionNo": accession}]},
            render_raw=b"<edgarSubmission/>",
            document_url=(
                "https://www.sec.gov/Archives/edgar/data/123456/"
                f"{accession.replace('-', '')}/primary_doc.xml"
            ),
        )

    class Db(_Db):
        def __init__(self):
            super().__init__([ACCESSION, accession_two])
            self.write_threads: list[int] = []

        def write(self, projection, evidence) -> None:
            self.write_threads.append(threading.get_ident())
            super().write(projection, evidence)

    class Client:
        def fetch_render_fallback_evidence(
            self, accession: str, *, on_provider_call=None, invoke_provider_call=None
        ):
            invoke_provider_call(lambda: None)
            barrier.wait(timeout=2)
            invoke_provider_call(lambda: None)
            invoke_provider_call(lambda: None)
            return evidence_for(accession)

    db = Db()

    def factory():
        client = Client()
        created.append(client)
        return client

    result = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=6, request_interval_seconds=0.0001, concurrency=2, db=db,
        client_factory=factory, sleeper=lambda _seconds: None,
    )

    assert result["state"] == "complete"
    assert result["api_calls"] == 6
    assert len({id(client) for client in created}) == 2
    assert db.write_threads == [main_thread, main_thread]


def test_concurrent_scheduler_reserves_complete_chains_under_the_hard_call_budget():
    db = _Db([ACCESSION, "0000123456-26-000002"])
    client = _Client()

    result = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=5, request_interval_seconds=0.01, concurrency=4, db=db,
        client_factory=lambda: client, sleeper=lambda _seconds: None,
    )

    assert result["state"] == "partial"
    assert result["api_calls"] == 3
    assert client.calls == 3


def test_failure_keeps_prior_commits_and_does_not_write_the_failed_accession():
    accession_two = "0000123456-26-000002"

    class FailingClient:
        def fetch_render_fallback_evidence(
            self, _accession: str, *, on_provider_call=None, invoke_provider_call=None
        ):
            invoke_provider_call(lambda: None)
            raise secapi.PayloadError("unsafe response")

    db = _Db([ACCESSION, accession_two])
    clients = iter([_Client(), FailingClient()])
    result = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=6, request_interval_seconds=0.01, concurrency=1, db=db,
        client_factory=lambda: next(clients), sleeper=lambda _seconds: None,
    )

    assert result["state"] == "failed"
    assert result["accession_number"] == accession_two
    assert [projection.accession_number for projection, _evidence in db.writes] == [ACCESSION]


def test_concurrency_defaults_to_one_without_an_environment_override(monkeypatch):
    monkeypatch.delenv(worker.ENV_CONCURRENCY, raising=False)
    db, client = _Db(), _Client()

    result = _run(db, client)

    assert result["concurrency"] == 1


def test_later_success_is_committed_when_an_earlier_accession_fails_and_resume_skips_it():
    accession_two = "0000123456-26-000002"
    later_finished = threading.Event()
    fail_first = True
    provider_accessions: list[str] = []

    class StatefulDb(_Db):
        def __init__(self):
            super().__init__([ACCESSION, accession_two])
            self.persisted: set[str] = set()

        def existing_hashes(self, _publication: str, _run: str, accession: str):
            if accession not in self.persisted:
                return None
            return {
                "parser_version": worker.PARSER_VERSION,
                "resolver_version": worker.RESOLVER_VERSION,
            }

        def terminal_accessions(self, *_args: str) -> list[str]:
            return [accession for accession in self.accessions if accession not in self.persisted]

        def write(self, projection, evidence) -> None:
            self.persisted.add(projection.accession_number)
            super().write(projection, evidence)

    class Client:
        def fetch_render_fallback_evidence(
            self, accession: str, *, on_provider_call=None, invoke_provider_call=None
        ):
            nonlocal fail_first
            provider_accessions.append(accession)
            for _ in range(3):
                invoke_provider_call(lambda: None)
            if accession == ACCESSION and fail_first:
                later_finished.wait(timeout=2)
                raise secapi.PayloadError("first accession failed")
            if accession == accession_two:
                later_finished.set()
            return secapi.RenderFallbackEvidence(
                filing={**_filing(), "accessionNo": accession},
                form_response={"filings": []},
                query_response={"filings": [{"accessionNo": accession}]},
                render_raw=b"<edgarSubmission/>",
                document_url=(
                    "https://www.sec.gov/Archives/edgar/data/123456/"
                    f"{accession.replace('-', '')}/primary_doc.xml"
                ),
            )

    db = StatefulDb()
    first = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=6, request_interval_seconds=0.0001, concurrency=2, db=db,
        client_factory=Client, sleeper=lambda _seconds: None,
    )

    assert first["state"] == "failed"
    assert db.persisted == {accession_two}

    fail_first = False
    later_finished.clear()
    provider_accessions.clear()
    resumed = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=3, request_interval_seconds=0.0001, concurrency=2, db=db,
        client_factory=Client, sleeper=lambda _seconds: None,
    )

    assert resumed["state"] == "complete"
    assert provider_accessions == [ACCESSION]


def test_global_limiter_wraps_actual_sdk_request_starts():
    accession_two = "0000123456-26-000002"
    guard = threading.Lock()
    active_requests = 0
    max_active_requests = 0

    def tracked(operation):
        def run(*args, **kwargs):
            nonlocal active_requests, max_active_requests
            with guard:
                active_requests += 1
                max_active_requests = max(max_active_requests, active_requests)
            try:
                time.sleep(0.005)
                return operation(*args, **kwargs)
            finally:
                with guard:
                    active_requests -= 1

        return run

    class State:
        accession = ACCESSION

    class FormClient:
        def __init__(self, state):
            self.state = state

        @tracked
        def get_data(self, payload):
            self.state.accession = payload["query"].split('"')[1]
            return {"filings": []}

    class QueryClient:
        def __init__(self, state):
            self.state = state

        @tracked
        def get_filings(self, _payload):
            accession = self.state.accession
            return {"filings": [{
                "accessionNo": accession,
                "formType": "NPORT-P",
                "linkToFilingDetails": (
                    "https://www.sec.gov/Archives/edgar/data/123456/"
                    f"{accession.replace('-', '')}/xslFormNPORT-P_X01/primary_doc.xml"
                ),
            }]}

    class RenderClient:
        @tracked
        def get_file(self, _url):
            return """<edgarSubmission><submissionType>NPORT-P</submissionType>
            <fundInfo><totAssets>100</totAssets><curMetrics /></fundInfo>
            </edgarSubmission>"""

    def factory():
        state = State()
        return secapi.ExactNportClient(FormClient(state), QueryClient(state), RenderClient())

    result = worker.run(
        publication_id=PUBLICATION, source_run_id=RUN, max_accessions=2,
        max_api_calls=6, request_interval_seconds=0.0001, concurrency=2,
        db=_Db([ACCESSION, accession_two]), client_factory=factory,
        sleeper=lambda _seconds: None,
    )

    assert result["state"] == "complete"
    assert max_active_requests == 1
