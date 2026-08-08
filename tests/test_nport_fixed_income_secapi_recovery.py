"""Contract tests for the compact SEC API fixed-income recovery parser."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.nport import secapi_fixed_income as secapi
from src.workers import nport_fixed_income_secapi_recovery as worker


def _payload(**overrides):
    fund = {
        "totAssets": "100.25",
        "totLiabs": 20,
        "netAssets": "80.25",
        "amtPayOneYrBanksBorr": "0",
        "amtPayOneYrCtrldComp": None,
        "amtPayOneYrOthAffil": "2",
        "amtPayOneYrOther": "3",
        "amtPayAftOneYrBanksBorr": "4",
        "amtPayAftOneYrCtrldComp": "5",
        "amtPayAftOneYrOthAffil": "6",
        "amtPayAftOneYrOther": "7",
        "delayDeliv": "8",
        "standByCommit": "9",
        "cshNotRptdInCorD": "10",
        "creditSprdRiskInvstGrade": {
            "period3Mon": "1",
            "period1Yr": "2",
            "period5Yr": "3",
            "period10Yr": "4",
            "period30Yr": "5",
        },
        "creditSprdRiskNonInvstGrade": {
            "period3Mon": "-1",
            "period1Yr": "-2",
            "period5Yr": "-3",
            "period10Yr": "-4",
            "period30Yr": "-5",
        },
        "curMetrics": {
            "curMetric": [
                {
                    "curCd": "USD",
                    "intrstRtRiskdv01": {"period3Mon": "0", "period1Yr": "1"},
                    "intrstRtRiskdv100": {"period3Mon": "10", "period1Yr": "100"},
                }
            ]
        },
    }
    fund.update(overrides)
    return {
        "accessionNo": "0000123456-26-000001",
        "formType": "NPORT-P",
        "fundInfo": fund,
        "invstOrSecs": [{"cusip": "SECRET"}],
    }


def test_hash_is_stable_across_object_key_order():
    assert secapi.response_sha256({"b": [2], "a": 1}) == secapi.response_sha256(
        {"a": 1, "b": [2]}
    )


def test_extract_maps_all_fund_fields_rates_and_presence_without_positions():
    result = secapi.extract_filing(
        _payload(), publication_id="pub", source_run_id="run"
    )
    assert result.fund["tot_assets"] == Decimal("100.25")
    assert result.fund["amt_pay_one_yr_banks_borr"] == Decimal("0")
    assert result.fund["amt_pay_one_yr_ctrld_comp"] is None
    assert result.fund_presence["amt_pay_one_yr_banks_borr"] == "present"
    assert result.fund_presence["amt_pay_one_yr_ctrld_comp"] == "null"
    assert result.fund["credit_sprd_risk_invst_grade_30yr"] == Decimal("5")
    assert len(result.rates) == 1
    assert result.rates[0]["currency_code"] == "USD"
    assert result.rates[0]["dv01_3mon"] == Decimal("0")
    assert result.rates[0]["dv100_1yr"] == Decimal("100")
    assert "invstOrSecs" not in result.compact_json
    assert "SECRET" not in result.compact_json


@pytest.mark.parametrize("metrics", [None, {}, {"curMetric": []}])
def test_missing_null_and_empty_metrics_are_legitimate_zero_rate_states(metrics):
    result = secapi.extract_filing(
        _payload(curMetrics=metrics), publication_id="pub", source_run_id="run"
    )
    assert result.rates == []
    assert result.fund_presence["cur_metrics"] in {"missing", "null", "present"}


@pytest.mark.parametrize("bad", [{"curMetric": {}}, [], "USD"])
def test_malformed_metrics_container_is_rejected(bad):
    with pytest.raises(secapi.PayloadError, match="curMetrics"):
        secapi.extract_filing(
            _payload(curMetrics=bad), publication_id="pub", source_run_id="run"
        )


@pytest.mark.parametrize("bad", [True, "NaN", "Infinity", float("inf"), "oops"])
def test_nonfinite_and_invalid_numbers_are_rejected(bad):
    with pytest.raises(secapi.PayloadError, match="totAssets"):
        secapi.extract_filing(
            _payload(totAssets=bad), publication_id="pub", source_run_id="run"
        )


def test_fetch_exactly_one_uses_accession_query_and_rejects_wrong_response():
    class Client:
        def __init__(self):
            self.calls = []

        def get_data(self, payload):
            self.calls.append(payload)
            return {"total": {"value": 1, "relation": "eq"}, "filings": [_payload()]}

    client = Client()
    assert (
        secapi.fetch_exact_filing(client, "0000123456-26-000001")["accessionNo"]
        == "0000123456-26-000001"
    )
    assert client.calls == [{
        "query": 'accessionNo:"0000123456-26-000001"',
        "from": "0",
        "size": "1",
        "sort": [{"filedAt": {"order": "asc"}}],
    }]

    class Wrong:
        def get_data(self, *_a, **_k):
            return {"filings": [{"accessionNo": "wrong"}]}

    with pytest.raises(secapi.AccessionMismatchError):
        secapi.fetch_exact_filing(Wrong(), "0000123456-26-000001")

    class WrongForm:
        def get_data(self, *_a, **_k):
            return {"filings": [{**_payload(), "formType": "NPORT-P/A"}]}

    with pytest.raises(secapi.AccessionMismatchError, match="form type"):
        secapi.fetch_exact_filing(WrongForm(), "0000123456-26-000001")

    with pytest.raises(secapi.PayloadError, match="invalid SEC format"):
        secapi.fetch_exact_filing(client, 'bad" OR formType:"NPORT-P')


def test_exact_client_falls_back_to_verified_rendered_xml_without_positions():
    accession = "0000123456-26-000001"
    calls = []

    class FormClient:
        def get_data(self, payload):
            calls.append(("form", payload))
            return {"filings": []}

    class QueryClient:
        def get_filings(self, payload):
            calls.append(("query", payload))
            return {
                "filings": [
                    {
                        "accessionNo": accession,
                        "formType": "NPORT-P",
                        "linkToFilingDetails": (
                            "https://www.sec.gov/Archives/edgar/data/123456/"
                            "000012345626000001/xslFormNPORT-P_X01/primary_doc.xml"
                        ),
                    }
                ]
            }

    class RenderClient:
        def get_file(self, url):
            calls.append(("render", url))
            return """<?xml version="1.0"?>
            <edgarSubmission xmlns="http://www.sec.gov/edgar/nport">
              <headerData><submissionType>NPORT-P</submissionType></headerData>
              <formData>
                <fundInfo>
                  <totAssets>100.25</totAssets>
                  <curMetrics><curMetric><curCd>USD</curCd></curMetric></curMetrics>
                </fundInfo>
                <invstOrSecs><invstOrSec><cusip>SECRET</cusip></invstOrSec></invstOrSecs>
              </formData>
            </edgarSubmission>"""

    client = secapi.ExactNportClient(FormClient(), QueryClient(), RenderClient())
    provider_calls = []
    result = secapi.fetch_exact_filing(
        client, accession, on_provider_call=lambda: provider_calls.append(1)
    )

    assert result["accessionNo"] == accession
    assert result["formType"] == "NPORT-P"
    assert result["fundInfo"]["totAssets"] == "100.25"
    assert result["fundInfo"]["curMetrics"]["curMetric"] == [{"curCd": "USD"}]
    assert "invstOrSecs" not in result
    assert len(provider_calls) == 3
    assert calls[-1] == (
        "render",
        "https://www.sec.gov/Archives/edgar/data/123456/"
        "000012345626000001/primary_doc.xml",
    )


@pytest.mark.parametrize(
    "filing",
    [
        {
            "accessionNo": "0000123456-26-999999",
            "formType": "NPORT-P",
            "linkToFilingDetails": "https://www.sec.gov/Archives/edgar/data/1/2/primary_doc.xml",
        },
        {
            "accessionNo": "0000123456-26-000001",
            "formType": "13F-HR",
            "linkToFilingDetails": "https://www.sec.gov/Archives/edgar/data/1/2/primary_doc.xml",
        },
        {
            "accessionNo": "0000123456-26-000001",
            "formType": "NPORT-P",
            "linkToFilingDetails": "https://example.com/primary_doc.xml",
        },
    ],
)
def test_exact_client_rejects_unverified_render_fallback_identity(filing):
    class FormClient:
        def get_data(self, _payload):
            return {"filings": []}

    class QueryClient:
        def get_filings(self, _payload):
            return {"filings": [filing]}

    class RenderClient:
        def get_file(self, _url):
            raise AssertionError("unsafe render URL must not be fetched")

    client = secapi.ExactNportClient(FormClient(), QueryClient(), RenderClient())
    with pytest.raises(secapi.AccessionMismatchError):
        secapi.fetch_exact_filing(client, "0000123456-26-000001")


def test_exact_client_rejects_unsafe_or_oversized_xml():
    accession = "0000123456-26-000001"
    filing = {
        "accessionNo": accession,
        "formType": "NPORT-P",
        "linkToFilingDetails": (
            "https://www.sec.gov/Archives/edgar/data/123456/"
            "000012345626000001/xslFormNPORT-P_X01/primary_doc.xml"
        ),
    }

    class FormClient:
        def get_data(self, _payload):
            return {"filings": []}

    class QueryClient:
        def get_filings(self, _payload):
            return {"filings": [filing]}

    class RenderClient:
        def __init__(self, value):
            self.value = value

        def get_file(self, _url):
            return self.value

    for xml in ("<!DOCTYPE x><fundInfo/>", "x" * (secapi.MAX_RENDER_XML_BYTES + 1)):
        client = secapi.ExactNportClient(
            FormClient(), QueryClient(), RenderClient(xml)
        )
        with pytest.raises(secapi.PayloadError):
            secapi.fetch_exact_filing(client, accession)


def test_render_fallback_requires_three_calls_of_remaining_budget():
    accession = "0000123456-26-000001"

    class FormClient:
        def get_data(self, _payload):
            return {"filings": []}

    class QueryClient:
        def get_filings(self, _payload):
            return {
                "filings": [{
                    "accessionNo": accession,
                    "formType": "NPORT-P",
                    "linkToFilingDetails": (
                        "https://www.sec.gov/Archives/edgar/data/123456/"
                        "000012345626000001/xslFormNPORT-P_X01/primary_doc.xml"
                    ),
                }]
            }

    class RenderClient:
        def get_file(self, _url):
            raise AssertionError("render must not run after the budget is exhausted")

    client = secapi.ExactNportClient(FormClient(), QueryClient(), RenderClient())
    with pytest.raises(RuntimeError, match="budget exhausted"):
        worker._fetch_with_retry(
            client, accession, lambda _seconds: None, max_calls=2
        )


def test_retry_only_retries_transient_and_sanitizes_error():
    calls, pauses = [], []

    class Timeout(Exception):
        pass

    def operation():
        calls.append(1)
        if len(calls) < 3:
            raise Timeout("key=do-not-log")
        return "ok"

    assert (
        secapi.retry_transient(
            operation, sleeper=pauses.append, max_attempts=3, transient=(Timeout,)
        )
        == "ok"
    )
    assert len(calls) == 3 and pauses == [1.0, 2.0]
    with pytest.raises(Timeout):
        secapi.retry_transient(
            lambda: (_ for _ in ()).throw(Timeout("SEC_API_IO_KEY=x")),
            sleeper=lambda _x: None,
            max_attempts=1,
            transient=(Timeout,),
        )


def test_retry_respects_the_remaining_hard_api_call_budget():
    class Timeout(Exception):
        pass

    class Client:
        def __init__(self):
            self.calls = 0

        def get_data(self, _payload):
            self.calls += 1
            raise Timeout("temporary")

    client = Client()
    with pytest.raises(Timeout):
        worker._fetch_with_retry(
            client,
            "0000123456-26-000001",
            lambda _seconds: None,
            max_calls=1,
        )
    assert client.calls == 1


def test_source_document_ids_and_rows_are_deterministic():
    a = secapi.extract_filing(_payload(), publication_id="pub", source_run_id="run")
    b = secapi.extract_filing(_payload(), publication_id="pub", source_run_id="run")
    assert a.source_document_id == b.source_document_id
    assert a.fund["source_row_number"] == 0
    assert a.rates[0]["source_row_number"] == 1


def test_dry_run_is_db_manifest_only_and_makes_no_api_calls():
    class Db:
        def expected_accessions(self, publication_id):
            assert publication_id == "pub"
            return ["A", "B"]

        def successful_accessions(self, publication_id, source_run_id):
            return {"A"}

    result = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        dry_run=True,
        db=Db(),
        client_factory=lambda: pytest.fail("no API"),
    )
    assert result == {
        "state": "dry_run",
        "expected": 2,
        "success": 1,
        "pending": 1,
        "remaining": 1,
        "max_accessions": 1,
        "max_api_calls": 1,
        "request_interval_seconds": 1.0,
    }


def test_worker_enforces_budgets_and_persists_compact_projection_atomically():
    class Db:
        def expected_accessions(self, _publication_id):
            return ["0000123456-26-000001", "B"]

        def successful_accessions(self, *_args):
            return set()

        def advisory_lock(self, *_args):
            class Lock:
                def __enter__(self):
                    return True

                def __exit__(self, *_args):
                    return False

            return Lock()

        def transaction(self):
            class Tx:
                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

            return Tx()

        def existing_hash(self, *_args):
            return None

        def write(self, projection):
            self.projection = projection

    class Client:
        def get_data(self, *_args, **_kwargs):
            return {"filings": [_payload()]}

    db = Db()
    result = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=db,
        client_factory=Client,
        max_accessions=1,
        max_api_calls=1,
        sleeper=lambda _x: None,
    )
    assert (
        result["state"] == "partial"
        and result["processed"] == 1
        and result["remaining"] == 1
    )
    assert "invstOrSecs" not in db.projection.compact_json


def test_worker_stops_on_hash_conflict_without_writing():
    class Db:
        def expected_accessions(self, _p):
            return ["0000123456-26-000001"]

        def successful_accessions(self, *_args):
            return set()

        def advisory_lock(self, *_args):
            class Lock:
                def __enter__(self):
                    return True

                def __exit__(self, *_args):
                    return False

            return Lock()

        def transaction(self):
            class Tx:
                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

            return Tx()

        def existing_hash(self, *_args):
            return "different"

        def write(self, _projection):
            pytest.fail("conflict must not write")

    class Client:
        def get_data(self, *_args, **_kwargs):
            return {"filings": [_payload()]}

    result = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=Db(),
        client_factory=Client,
        sleeper=lambda _x: None,
    )
    assert (
        result["state"] == "conflict"
        and result["accession_number"] == "0000123456-26-000001"
    )


def test_terminal_payload_failure_is_persisted_and_not_retried() -> None:
    class Db:
        terminal = {}

        def expected_accessions(self, _p):
            return ["0000123456-26-000001"]

        def successful_accessions(self, *_args):
            return {}

        def terminal_accessions(self, *_args):
            return self.terminal

        def advisory_lock(self, *_args):
            class Lock:
                def __enter__(self):
                    return True

                def __exit__(self, *_args):
                    return False

            return Lock()

        def transaction(self):
            class Tx:
                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

            return Tx()

        def existing_hash(self, *_args):
            return None

        def record_failure(self, _p, _r, accession, **values):
            assert values["status"] == "terminal_error"
            assert values["attempt_increment"] == 1
            self.terminal[accession] = values["status"]

    class Client:
        def get_data(self, *_args, **_kwargs):
            return {"filings": [{"accessionNo": "wrong"}]}

    db = Db()
    first = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=db,
        client_factory=Client,
        sleeper=lambda _x: None,
    )
    assert first["state"] == "failed"
    second = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=db,
        client_factory=lambda: pytest.fail("terminal accession must not be retried"),
    )
    assert second["state"] == "blocked"
    assert second["terminal"] == 1


def test_existing_terminal_does_not_prevent_recovering_other_accessions() -> None:
    terminal_accession = "0000123456-26-999999"

    class Db:
        def expected_accessions(self, _p):
            return [terminal_accession, "0000123456-26-000001"]

        def successful_accessions(self, *_args):
            return set()

        def terminal_accessions(self, *_args):
            return {terminal_accession: "terminal_error"}

        def advisory_lock(self, *_args):
            class Lock:
                def __enter__(self):
                    return True

                def __exit__(self, *_args):
                    return False

            return Lock()

        def transaction(self):
            class Tx:
                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

            return Tx()

        def existing_hash(self, *_args):
            return None

        def write(self, projection):
            self.projection = projection

    class Client:
        def get_data(self, *_args, **_kwargs):
            return {"filings": [_payload()]}

    db = Db()
    result = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=db,
        client_factory=Client,
        max_accessions=2,
        max_api_calls=2,
        sleeper=lambda _x: None,
    )
    assert result["state"] == "blocked"
    assert result["terminal"] == 1
    assert result["processed"] == 1
    assert db.projection.accession_number == "0000123456-26-000001"


def test_worker_initializes_every_expected_manifest_before_constructing_client():
    events = []

    class Db:
        def expected_accessions(self, _p):
            return ["A", "B"]

        def successful_accessions(self, *_args):
            return {}

        def initialize_manifests(self, publication_id, source_run_id, accessions):
            events.append(
                ("manifest", publication_id, source_run_id, tuple(accessions))
            )

        def advisory_lock(self, *_args):
            class Lock:
                def __enter__(self):
                    return True

                def __exit__(self, *_args):
                    return False

            return Lock()

        def transaction(self):
            raise AssertionError("network must fail before row transaction")

    def client_factory():
        events.append(("client",))
        raise RuntimeError("stop")

    result = worker.run(
        "ignored",
        publication_id="pub",
        source_run_id="run",
        db=Db(),
        client_factory=client_factory,
        max_accessions=1,
        max_api_calls=1,
    )
    assert result["state"] == "failed"
    assert events == [("manifest", "pub", "run", ("A", "B")), ("client",)]


def test_worker_accepts_an_injected_clock_before_any_io():

    class Db:
        def expected_accessions(self, _p):
            return ["0000123456-26-000001", "0000123456-26-000001"]

        def successful_accessions(self, *_args):
            return {}

    # Duplicate identities fail before I/O: this test locks in that fail-closed property.
    with pytest.raises(RuntimeError, match="duplicates"):
        worker.run(
            "ignored",
            publication_id="pub",
            source_run_id="run",
            db=Db(),
            max_accessions=2,
            max_api_calls=2,
            clock=lambda: 0.0,
        )
