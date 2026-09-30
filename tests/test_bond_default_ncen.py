"""Form N-CEN fund-family independence evidence (plan v1_8 §4.1 item 6, amendment FE-1)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import importlib.util
import itertools
import json
import os
import pickle
import subprocess
import sys
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pytest

from src.bonds.default_events import ncen, nport
from src.bonds.default_events import sec_acquisition as sa
from src.bonds.default_events.contracts import date_only_public_available_at

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "bond_default_events" / "ncen"
UTC = dt.timezone.utc
RETRIEVED = dt.datetime(2026, 9, 25, 5, 29, tzinfo=UTC)
K = dt.datetime(2026, 9, 25, 23, 0, tzinfo=UTC)
R = dt.date(2026, 3, 31)
NS = ncen.NCEN_NAMESPACE


def acc(n: int) -> str:
    return f"0000000999-26-{n:06d}"


def cik(n: int) -> str:
    return f"{n:010d}"


# --- synthetic typed filings -----------------------------------------------------------
def adv(role: str = "adviser", fn: str | None = None, crd: str | None = None, lei: str | None = None
        ) -> ncen.AdviserRecord:
    return ncen.AdviserRecord(role=role, file_number=ncen.normalize_file_number(fn), crd=ncen.normalize_crd(crd),
                              lei=ncen.normalize_lei(lei), raw=(fn, crd, lei))


def uw(fn: str | None = None, crd: str | None = None) -> ncen.UnderwriterRecord:
    return ncen.UnderwriterRecord(file_number=ncen.normalize_file_number(fn), crd=ncen.normalize_crd(crd), lei=None,
                                  raw=(fn, crd, None))


def series_of(registrant: int) -> str:
    """Default series of synthetic registrant ``1nn``: ``S0000000nn``."""
    return f"S{registrant % 100:09d}"


def mk(n: int, registrant: int, *, period: dt.date = dt.date(2025, 12, 31), filed: dt.date = dt.date(2026, 2, 20),
       answer: str | None = "N", name: str | None = None, advisers: tuple = (), underwriters: tuple = (),
       form: str = "N-CEN", status: str = "parsed", funds: tuple | None = None, known: dt.datetime | None = None,
       retrieved: dt.datetime | None = None) -> ncen.NcenFiling:
    public = date_only_public_available_at(filed, "America/New_York")
    advisers = advisers or (adv(fn=f"801-{registrant}"),)
    return ncen.NcenFiling(
        accession_number=acc(n), registrant_cik=cik(registrant), form_type=form, form_type_source="dera",
        report_period_end=period, filing_date=filed, public_available_at=public,
        public_time_basis="date_only_conservative", data_known_at=known or public, source="dera",
        source_refs=(f"test:{n}",), family_answer=answer, family_name_raw=name,
        funds=funds if funds is not None else (ncen.NcenFund(series_of(registrant), tuple(advisers)),),
        underwriters=tuple(underwriters), status=status,
        reasons=() if status == "parsed" else ("synthetic",), retrieved_at=retrieved or known or public,
    )


def index(*filings: ncen.NcenFiling, **kwargs) -> ncen.NcenFilingIndex:
    return ncen.merge_filings(filings, **kwargs)


# --- normalization -------------------------------------------------------------------------
@pytest.mark.parametrize(("raw", "expected"), [
    ("801-00856", "801-856"), ("801-856", "801-856"), (" 8-35097 ", "8-35097"), ("N/A", None), ("", None),
    (None, None), ("801-0", None), ("801", None), ("801-12a", None), ("0801-1", None),
])
def test_file_number_normalization(raw, expected) -> None:
    assert ncen.normalize_file_number(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [
    ("000105496", "105496"), ("105496", "105496"), ("0", None), ("000000000", None), ("N/A", None),
    ("12-3", None), (None, None),
])
def test_crd_normalization(raw, expected) -> None:
    assert ncen.normalize_crd(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [
    ("7HTL8AEQSEDX602FBU63", "7HTL8AEQSEDX602FBU63"), ("7htl8aeqsedx602fbu63", "7HTL8AEQSEDX602FBU63"),
    ("N/A", None), ("00000000000000000000", None), ("SHORT", None), ("", None),
])
def test_lei_normalization(raw, expected) -> None:
    assert ncen.normalize_lei(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), [
    ("BlackRock-Advised Funds", "BLACKROCKADVISED"), ("BlackRock-advised Funds", "BLACKROCKADVISED"),
    ("EatonVance", "EATONVANCE"), ("EATON VANCE", "EATONVANCE"), ("PIMCO Funds", "PIMCO"), ("PIMCOFUNDS", "PIMCO"),
    ("Acme Fund Family Group", "ACME"), ("Acme Trust Funds", "ACME"), ("Fidelity Group of Funds", "FIDELITYGROUPOF"),
    ("Funds", None), ("Fund Trust", None), ("  ", None), (None, None), ("T. Rowe Price", "TROWEPRICE"),
])
def test_family_name_normalization(raw, expected) -> None:
    assert ncen.normalize_family_name(raw) == expected


@pytest.mark.parametrize(("day", "months", "expected"), [
    (dt.date(2021, 3, 31), 15, dt.date(2019, 12, 31)), (dt.date(2021, 5, 31), 15, dt.date(2020, 2, 29)),
    (dt.date(2026, 4, 30), 15, dt.date(2025, 1, 30)), (dt.date(2026, 1, 15), 15, dt.date(2024, 10, 15)),
])
def test_months_before(day, months, expected) -> None:
    assert ncen.months_before(day, months) == expected


# --- DERA packages -------------------------------------------------------------------------
def dera_rows(n: int = 1, registrant: int = 101, *, form: str = "N-CEN", filed: str = "20-FEB-2026",
              period: str = "31-DEC-2025", fam: str = "Y", name: str = "Acme Funds", series: str = "S000000001",
              advisers: tuple = (("Advisor", "801-00001", "000000011", "N/A"),),
              uws: tuple = (("8-00001", "000000021", ""),), reg_cik: str | None = None) -> dict[str, list[dict]]:
    fund_id = f"{acc(n)}_{cik(registrant)}_{series}"
    return {
        "SUBMISSION": [{"ACCESSION_NUMBER": acc(n), "SUBMISSION_TYPE": form, "CIK": cik(registrant),
                        "FILING_DATE": filed, "REPORT_ENDING_PERIOD": period}],
        "REGISTRANT": [{"ACCESSION_NUMBER": acc(n), "CIK": reg_cik or cik(registrant),
                        "IS_FAMILY_INVESTMENT_COMPANY": fam, "FAMILY_INVESTMENT_COMPANY_NAME": name}],
        "FUND_REPORTED_INFO": [{"FUND_ID": fund_id, "ACCESSION_NUMBER": acc(n), "SERIES_ID": series}],
        "ADVISER": [{"FUND_ID": fund_id, "ADVISER_TYPE": t, "FILE_NUM": f, "CRD_NUM": c, "ADVISER_LEI": le}
                    for t, f, c, le in advisers],
        "PRINCIPAL_UNDERWRITER": [{"ACCESSION_NUMBER": acc(n), "FILE_NUM": f, "CRD_NUM": c, "UNDERWRITER_LEI": le}
                                  for f, c, le in uws],
    }


def combine(*parts: dict[str, list[dict]]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {t: [] for t in ncen.PINNED_TABLES}
    for part in parts:
        for table, rows in part.items():
            out[table].extend(rows)
    return out


def dera_zip(path: Path, tables: dict[str, list[dict]], *, drop_table: str | None = None,
             drop_column: tuple[str, str] | None = None, raw_line: tuple[str, str] | None = None,
             member_prefix: str = "") -> str:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for table in ncen.PINNED_TABLES:
            if table == drop_table:
                continue
            columns = [c for c in (*ncen.REQUIRED_COLUMNS[table], "FUTURE_COLUMN") if (table, c) != drop_column]
            lines = ["\t".join(columns)] + ["\t".join(row.get(c, "") or "" for c in columns)
                                            for row in tables.get(table, [])]
            if raw_line is not None and raw_line[0] == table:
                lines.append(raw_line[1])
            archive.writestr(f"{member_prefix}{table}.tsv", "\n".join(lines) + "\n")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse(path: Path, sha: str, **kwargs) -> ncen.NcenPackageResult:
    return ncen.parse_dera_ncen_package(path, expected_sha256=sha, package_label="2026q1", retrieved_at=RETRIEVED,
                                        **kwargs)


def test_dera_package_parses_typed_filings(tmp_path: Path) -> None:
    path = tmp_path / "p.zip"
    sha = dera_zip(path, combine(dera_rows(1, 101, advisers=(
        ("Advisor", "801-00001", "000000011", "N/A"), ("Subadvisor", "801-2", "", "5493001Z012YSB2A0K51"),
        ("Terminated Subadvisor", "N/A", "33", "N/A"))), dera_rows(2, 102, fam="N", name="", uws=())))
    result = parse(path, sha)
    assert result.status == "parsed" and result.stats["accessions"] == 2
    first, second = result.filings
    assert first.registrant_cik == cik(101) and first.form_type == "N-CEN"
    assert first.report_period_end == dt.date(2025, 12, 31) and first.filing_date == dt.date(2026, 2, 20)
    assert first.public_available_at == dt.datetime(2026, 2, 21, 5, tzinfo=UTC)
    assert first.public_time_basis == "date_only_conservative"
    assert first.data_known_at == RETRIEVED and first.retrieved_at == RETRIEVED
    roles = [a.role for a in first.funds[0].advisers]
    assert roles == ["adviser", "sub_adviser", "terminated_sub_adviser"]
    profile = ncen.family_profile(first)
    assert profile.complete and profile.family_key == "ACME"
    assert profile.adviser_tokens == {"FN:801-1", "CRD:11", "FN:801-2", "LEI:5493001Z012YSB2A0K51", "CRD:33"}
    assert profile.underwriter_tokens == {"FN:8-1", "CRD:21"}
    assert second.family_answer == "N" and ncen.family_profile(second).complete


def test_dera_first_verified_public_bounds_data_known(tmp_path: Path) -> None:
    path = tmp_path / "p.zip"
    sha = dera_zip(path, dera_rows())
    posted = dt.datetime(2026, 4, 1, tzinfo=UTC)
    filing = parse(path, sha, first_verified_public_at=posted).filings[0]
    assert filing.data_known_at == posted and filing.public_available_at < posted


def test_dera_hash_mismatch_raises(tmp_path: Path) -> None:
    path = tmp_path / "p.zip"
    dera_zip(path, dera_rows())
    with pytest.raises(nport.PackageIntegrityError):
        parse(path, "0" * 64)


@pytest.mark.parametrize(("kwargs", "reason"), [
    ({"drop_table": "ADVISER"}, "pinned_table_missing:ADVISER"),
    ({"drop_column": ("REGISTRANT", "FAMILY_INVESTMENT_COMPANY_NAME")},
     "required_columns_missing:REGISTRANT:FAMILY_INVESTMENT_COMPANY_NAME"),
    ({"drop_column": ("ADVISER", "ADVISER_LEI")}, "required_columns_missing:ADVISER:ADVISER_LEI"),
    ({"raw_line": ("PRINCIPAL_UNDERWRITER", "x\ty")}, "row_width_mismatch:PRINCIPAL_UNDERWRITER:3"),
])
def test_dera_layout_defects_quarantine_package(tmp_path: Path, kwargs, reason: str) -> None:
    path = tmp_path / "p.zip"
    sha = dera_zip(path, dera_rows(), **kwargs)
    result = parse(path, sha)
    assert result.status == "quarantined" and reason in result.reasons and result.filings == ()


def test_dera_zip_traversal_refused(tmp_path: Path) -> None:
    path = tmp_path / "p.zip"
    sha = dera_zip(path, dera_rows(), member_prefix="../")
    with pytest.raises(nport.ZipSafetyError):
        parse(path, sha)


@pytest.mark.parametrize(("rows", "reason"), [
    (dera_rows(reg_cik=cik(999)), "registrant_cik_mismatch"),
    (dera_rows(advisers=(("Co-Advisor", "801-1", "1", ""),)), "adviser_type_unknown"),
    (dera_rows(period="2025-12-31"), "report_period_unparseable"),
    (dera_rows(fam="MAYBE"), "family_answer_unparseable"),
    (dera_rows(series="SERIES1"), "series_id_invalid"),
])
def test_dera_accession_defects_quarantine_accession(tmp_path: Path, rows, reason: str) -> None:
    path = tmp_path / "p.zip"
    sha = dera_zip(path, combine(rows, dera_rows(2, 102)))
    result = parse(path, sha)
    bad, good = result.filings
    assert result.status == "parsed" and bad.status == "quarantined" and reason in bad.reasons
    assert good.usable


def test_real_dera_slice_is_verbatim_and_parses(tmp_path: Path) -> None:
    slice_dir = FIXTURES / "dera_2026q2_slice"
    provenance = json.loads((slice_dir / "PROVENANCE.json").read_text(encoding="utf-8"))
    for member, meta in provenance["members"].items():
        body = (slice_dir / member).read_bytes()
        assert hashlib.sha256(body).hexdigest() == meta["fixture_sha256"]
        lines = body[:-1].split(b"\n")
        assert [hashlib.sha256(x).hexdigest() for x in lines] == meta["line_sha256"]
    path = tmp_path / "slice.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for table in ncen.PINNED_TABLES:
            archive.write(slice_dir / f"{table}.tsv", f"{table}.tsv")
    result = parse(path, hashlib.sha256(path.read_bytes()).hexdigest())
    assert result.status == "parsed"
    by_acc = {f.accession_number: f for f in result.filings}
    y, n = by_acc["0000035402-26-002197"], by_acc["0000894189-26-011966"]
    assert (y.registrant_cik, y.form_type, y.report_period_end) == ("0000225322", "N-CEN", dt.date(2026, 1, 31))
    assert y.filing_date == dt.date(2026, 4, 13) and y.public_available_at == dt.datetime(2026, 4, 14, 4, tzinfo=UTC)
    py = ncen.family_profile(y)
    assert py.complete and py.family_key == "FIDELITYGROUPOF"
    assert {"FN:801-7884", "CRD:108281", "LEI:5493001Z012YSB2A0K51"} <= py.adviser_tokens
    assert [a.role for a in y.funds[0].advisers].count("sub_adviser") == 3
    assert py.underwriter_tokens == {"FN:8-35097", "CRD:17507"}
    assert n.form_type == "N-CEN/A" and n.family_answer == "N"
    pn = ncen.family_profile(n)
    assert pn.complete and pn.family_key is None and not ncen.not_independent(py, pn)


# --- EDGAR primary_doc.xml -----------------------------------------------------------------
FUND = (
    "<managementInvestmentQuestion><mgmtInvFundName>F</mgmtInvFundName><mgmtInvSeriesId>S000000001</mgmtInvSeriesId>"
    "<investmentAdvisers><investmentAdviser><investmentAdviserName>A</investmentAdviserName>"
    "<investmentAdviserFileNo>801-00001</investmentAdviserFileNo><investmentAdviserCrdNo>000000011"
    "</investmentAdviserCrdNo><investmentAdviserLei>N/A</investmentAdviserLei></investmentAdviser>"
    "</investmentAdvisers>{extra}</managementInvestmentQuestion>"
)
UW = ("<principalUnderwriter><principalUnderwriterName>U</principalUnderwriterName><principalUnderwriterFileNumber>"
      "8-00001</principalUnderwriterFileNumber><principalUnderwriterCrdNumber>000000021"
      "</principalUnderwriterCrdNumber><principalUnderwriterLei></principalUnderwriterLei></principalUnderwriter>")
FAMILY_Y = '<registrantFamilyInvComp isRegistrantFamilyInvComp="Y" familyInvCompFullName="Acme Funds"/>'


def xml_doc(*, registrant: int = 101, form: str = "N-CEN", period: str = "2025-12-31", family: str = FAMILY_Y,
            fund_extra: str = "", funds: str | None = None, underwriters: str = UW, prolog: str = "",
            schema: str = "X0505", form_extra: str = "") -> bytes:
    body = funds if funds is not None else FUND.format(extra=fund_extra)
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>{prolog}\n'
        f'<edgarSubmission xmlns="{NS}" xmlns:com="http://www.sec.gov/edgar/common">'
        f"<schemaVersion>{schema}</schemaVersion><headerData><submissionType>{form}</submissionType><filerInfo>"
        f"<filer><issuerCredentials><cik>{cik(registrant)}</cik><ccc>XXXXXXXX</ccc></issuerCredentials></filer>"
        f'</filerInfo></headerData><formData><generalInfo reportEndingPeriod="{period}" isReportPeriodLt12="N"/>'
        f"<registrantInfo><registrantCik>{cik(registrant)}</registrantCik>{family}"
        f"<principalUnderwriters>{underwriters}</principalUnderwriters></registrantInfo>"
        f"<managementInvestmentQuestionSeriesInfo>{body}</managementInvestmentQuestionSeriesInfo>{form_extra}"
        f"</formData></edgarSubmission>"
    ).encode()


def parse_xml(doc: bytes, n: int = 1, retrieved: dt.datetime = RETRIEVED) -> ncen.NcenFiling:
    return ncen.parse_ncen_primary_doc(doc, accession_number=acc(n), source_url="https://www.sec.gov/x",
                                       retrieved_at=retrieved)


def test_xml_parses_family_advisers_and_underwriters() -> None:
    extra = ("<subAdvisers><subAdviser><subAdviserName>S</subAdviserName><subAdviserFileNo>801-2</subAdviserFileNo>"
             "<subAdviserCrdNo>N/A</subAdviserCrdNo><subAdviserLei>N/A</subAdviserLei></subAdviser></subAdvisers>"
             "<investmentAdvisersTerminated><investmentAdviserTerminated><investAdviserTerminatedFileNo>801-3"
             "</investAdviserTerminatedFileNo><investAdviserTerminatedCrdNo>N/A</investAdviserTerminatedCrdNo>"
             "<investAdviserTerminatedLei>N/A</investAdviserTerminatedLei></investmentAdviserTerminated>"
             "</investmentAdvisersTerminated>")
    filing = parse_xml(xml_doc(fund_extra=extra))
    assert filing.usable and filing.registrant_cik == cik(101) and filing.form_type == "N-CEN"
    assert filing.report_period_end == dt.date(2025, 12, 31) and filing.public_available_at is None
    assert filing.schema_version == "X0505" and filing.retrieved_at == RETRIEVED
    profile = ncen.family_profile(filing)
    assert profile.complete and profile.family_key == "ACME"
    assert profile.adviser_tokens == {"FN:801-1", "CRD:11", "FN:801-2", "FN:801-3"}
    assert profile.underwriter_tokens == {"FN:8-1", "CRD:21"}


def test_xml_bare_n_family_answer() -> None:
    filing = parse_xml(xml_doc(family="<isRegistrantFamilyInvComp>N</isRegistrantFamilyInvComp>"))
    assert filing.usable and filing.family_answer == "N" and ncen.family_profile(filing).complete


@pytest.mark.parametrize(("doc", "reason"), [
    (xml_doc(prolog='<!DOCTYPE a [<!ENTITY x "y">]>'), "xml_unsafe:"),
    (xml_doc().decode().encode("utf-16"), "xml_unsafe:"),
    (xml_doc(fund_extra='<x:investmentAdvisers xmlns:x="urn:evil"/>'), "xml_namespace_mismatch:investmentAdvisers"),
    (xml_doc(fund_extra="<investmentAdviserSbic>801-9</investmentAdviserSbic>"),
     "xml_unmapped_adviser_element:investmentAdviserSbic"),
    (xml_doc(fund_extra="<coUnderwriter/>"), "xml_unmapped_underwriter_element:coUnderwriter"),
    (xml_doc(family=FAMILY_Y + "<isRegistrantFamilyInvComp>N</isRegistrantFamilyInvComp>"),
     "xml_family_answer_repeated"),
    (xml_doc(family='<registrantFamilyInvComp isRegistrantFamilyInvComp="X"/>'), "xml_family_answer_unparseable"),
    (xml_doc(period="31-DEC-2025"), "report_period_unparseable"),
    (b'<?xml version="1.0"?><edgarSubmission xmlns="urn:other"/>', "xml_root_not_ncen"),
])
def test_xml_quarantine(doc: bytes, reason: str) -> None:
    filing = parse_xml(doc)
    assert filing.status == "quarantined" and any(r.startswith(reason) for r in filing.reasons), filing.reasons


def test_real_xml_fixtures_verbatim_and_parsed() -> None:
    provenance = json.loads((FIXTURES / "xml" / "PROVENANCE.json").read_text(encoding="utf-8"))
    parsed = {}
    for name, meta in provenance.items():
        data = (FIXTURES / "xml" / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == meta["sha256"] and len(data) == meta["bytes"]
        parsed[name] = ncen.parse_ncen_primary_doc(data, accession_number=name[:-4], source_url=meta["url"],
                                                   retrieved_at=dt.datetime.fromisoformat(meta["retrieved_at"]))
    y = parsed["0000910472-25-005685.xml"]
    assert y.usable and y.schema_version == "X0505" and y.registrant_cik == "0001722837"
    assert y.report_period_end == dt.date(2025, 9, 30) and y.funds[0].series_id is None
    py = ncen.family_profile(y)
    assert py.complete and py.family_key == "DESTRA" and len(py.adviser_tokens) == 5
    n = parsed["0001752724-20-182663.xml"]
    assert n.usable and n.schema_version == "X0201" and n.family_answer == "N"
    assert ncen.family_profile(n).complete
    empty = parsed["0001752724-25-058421.xml"]
    assert empty.usable and ncen.family_profile(empty).reasons == ("no_funds",)


def test_fetch_primary_doc_uses_sec_client() -> None:
    doc = xml_doc()
    requested: list[str] = []

    class StubClient:
        def get(self, url: str) -> sa.FetchResult:
            requested.append(url)
            return sa.FetchResult(url=url, status=200, sha256=hashlib.sha256(doc).hexdigest(), size=len(doc),
                                  content_type="text/xml", body=doc, fetched_at=RETRIEVED, from_cache=False)

    filing = ncen.fetch_primary_doc(StubClient(), cik(101), acc(7))  # type: ignore[arg-type]
    assert requested == [f"{sa.ARCHIVES_ROOT}/data/101/000000099926000007/primary_doc.xml"]
    assert filing.usable and filing.accession_number == acc(7)


# --- merge -------------------------------------------------------------------------------------
H = ncen.KNOWLEDGE_HISTORICAL
C = ncen.KNOWLEDGE_CURRENT_RUN
ACCEPTED = dt.datetime(2026, 2, 20, 21, 30, 1, tzinfo=UTC)


def entry(n: int, registrant: int = 101, form: str = "N-CEN", filed: dt.date = dt.date(2026, 2, 20)
          ) -> sa.FormIndexEntry:
    return sa.FormIndexEntry(form_type=form, company_name="X", cik=cik(registrant), date_filed=filed,
                             file_name=f"edgar/data/{registrant}/{acc(n)}.txt", accession_number=acc(n))


def header(n: int, registrant: int = 101, *, filed: dt.date = dt.date(2026, 2, 20), form: str = "N-CEN",
           accepted: dt.datetime = ACCEPTED, retrieved: dt.datetime = RETRIEVED) -> sa.AcceptanceHeader:
    return sa.AcceptanceHeader(
        accession_number=acc(n), url="u", document_sha256="d", header_text="h", header_sha256="s",
        acceptance_raw=accepted.strftime("%Y%m%d%H%M%S"), acceptance_at=accepted,
        submission_type=form, filing_date=filed, period=None, filer_ciks=(cik(registrant),), items=(),
        retrieved_at=retrieved)


def test_merge_matching_dera_and_xml_prefers_xml_time(tmp_path: Path) -> None:
    path = tmp_path / "p.zip"
    dera = parse(path, dera_zip(path, dera_rows())).filings[0]
    merged = index(dera, parse_xml(xml_doc()), index_entries=[entry(1)])
    filing = merged.by_cik[cik(101)][0]
    assert filing.usable and filing.source == "dera+edgar_xml" and filing.form_type_source == "edgar_index"
    assert filing.public_available_at == dt.datetime(2026, 2, 21, 5, tzinfo=UTC)
    assert filing.public_date_bound == filing.public_available_at and filing.acceptance_at is None
    assert filing.data_known_at == filing.public_available_at
    assert filing.retrieved_at == min(RETRIEVED, dera.retrieved_at)
    assert merged.stats["accessions_with_multiple_copies"] == 1


def _dera_copy(tmp_path: Path, label: str, **kwargs) -> ncen.NcenFiling:
    path = tmp_path / f"{label}.zip"
    result = ncen.parse_dera_ncen_package(path, expected_sha256=dera_zip(path, dera_rows(**kwargs)),
                                          package_label=label, retrieved_at=RETRIEVED)
    return result.filings[0]


def _all_orders(copies: list[ncen.NcenFiling], **kwargs) -> list[ncen.NcenFiling]:
    return [index(*order, **kwargs).by_cik[cik(101)][0] for order in itertools.permutations(copies)]


def test_merge_conflicting_dera_copies_quarantine_in_every_order(tmp_path: Path) -> None:
    a = _dera_copy(tmp_path, "2026q1")
    b = _dera_copy(tmp_path, "2026q2", advisers=(("Advisor", "801-00009", "", ""),))
    for merged in _all_orders([a, b], index_entries=[entry(1)]):
        assert merged.status == "quarantined" and "copy_projection_conflict" in merged.reasons


def test_merge_conflicting_xml_copies_quarantine_in_every_order() -> None:
    a = parse_xml(xml_doc())
    b = parse_xml(xml_doc(underwriters=UW.replace("8-00001", "8-00002")))
    for merged in _all_orders([a, b], index_entries=[entry(1)]):
        assert merged.status == "quarantined" and "copy_projection_conflict" in merged.reasons


def test_merge_disagreeing_dera_and_xml_quarantine_in_every_order(tmp_path: Path) -> None:
    dera = _dera_copy(tmp_path, "2026q1", advisers=(("Advisor", "801-00009", "", ""),))
    for merged in _all_orders([dera, parse_xml(xml_doc())], index_entries=[entry(1)]):
        assert merged.status == "quarantined" and "copy_projection_conflict" in merged.reasons


def test_merge_quarantined_copy_quarantines_in_every_order(tmp_path: Path) -> None:
    dera = _dera_copy(tmp_path, "2026q1")
    unsafe = parse_xml(xml_doc(prolog='<!DOCTYPE a [<!ENTITY x "y">]>'))
    other_dera = _dera_copy(tmp_path, "2026q2", series="SERIES1")  # accession-level quarantine
    for copies in ([dera, unsafe], [dera, other_dera]):
        for merged in _all_orders(copies, index_entries=[entry(1)]):
            assert merged.status == "quarantined" and "source_copy_quarantined" in merged.reasons


def test_merge_identity_conflict_quarantines(tmp_path: Path) -> None:
    dera = _dera_copy(tmp_path, "2026q1")
    other = dera.with_updates(registrant_cik=cik(555), source_refs=("other",))
    for order in itertools.permutations([dera, other]):
        merged = ncen.merge_filings(order)
        # The conflicting accession blocks every registrant it may belong to.
        assert set(merged.by_cik) == {cik(101), cik(555)}
        for items in merged.by_cik.values():
            (filing,) = items
            assert filing.status == "quarantined" and "copy_identity_conflict" in filing.reasons


def test_xml_form_differing_from_index_is_typed_conflict() -> None:
    amendment_xml = parse_xml(xml_doc(form="N-CEN/A"))
    filing = index(amendment_xml, index_entries=[entry(1, form="N-CEN")]).by_cik[cik(101)][0]
    assert filing.status == "quarantined" and "form_type_conflict" in filing.reasons
    header_conflict = index(parse_xml(xml_doc()), headers={acc(1): header(1, form="N-CEN/A")}).by_cik[cik(101)][0]
    assert "form_type_conflict" in header_conflict.reasons
    both = index(parse_xml(xml_doc()), index_entries=[entry(1)], headers={acc(1): header(1, form="N-CEN/A")})
    assert "form_type_conflict" in both.by_cik[cik(101)][0].reasons


def test_merge_excludes_nt_ncen_relabelled_by_dera_only() -> None:
    merged = index(mk(1, 101), index_entries=[entry(1, form="NT N-CEN")])
    assert merged.by_cik == {} and merged.excluded[acc(1)] == "form_not_ncen:NT N-CEN"
    assert merged.stats["nt_ncen_relabelled_by_dera"] == 1
    # An XML N-CEN copy under an NT index entry is not the relabel exception.
    conflict = index(parse_xml(xml_doc()), index_entries=[entry(1, form="NT N-CEN")])
    assert "form_type_conflict" in conflict.by_cik[cik(101)][0].reasons


def test_merge_header_sets_acceptance_time_and_checks_identity() -> None:
    merged = index(mk(1, 101), headers={acc(1): header(1)})
    filing = merged.by_cik[cik(101)][0]
    assert filing.public_time_basis == "edgar_acceptance" and filing.form_type_source == "edgar_header"
    assert filing.public_available_at == ACCEPTED == filing.acceptance_at
    assert filing.public_date_bound == dt.datetime(2026, 2, 21, 5, tzinfo=UTC)
    bad = index(mk(1, 101), headers={acc(1): header(1, registrant=555)}).by_cik[cik(101)][0]
    assert "header_cik_mismatch" in bad.reasons
    late = index(mk(1, 101), index_entries=[entry(1, filed=dt.date(2026, 3, 1))]).by_cik[cik(101)][0]
    assert "index_filing_date_mismatch" in late.reasons


def test_merge_index_placeholder_for_unacquired_filing() -> None:
    merged = index(mk(1, 101), index_entries=[entry(1), entry(2, filed=dt.date(2026, 5, 1))],
                   index_retrieved_at=RETRIEVED)
    placeholder = next(f for f in merged.by_cik[cik(101)] if f.accession_number == acc(2))
    assert placeholder.status == "quarantined" and placeholder.is_placeholder
    assert placeholder.report_period_end is None and placeholder.public_available_at is not None
    assert placeholder.retrieved_at == RETRIEVED


def test_xml_without_time_is_unplaceable() -> None:
    filing = index(parse_xml(xml_doc())).by_cik[cik(101)][0]
    assert filing.public_available_at is None and "public_time_unknown" in filing.reasons


# --- effective N-CEN -------------------------------------------------------------------------
def eff(idx: ncen.NcenFilingIndex, registrant: int = 101, report_date: dt.date = R, cutoff: dt.datetime = K,
        mode: str = H) -> ncen.EffectiveSelection:
    return ncen.effective_filing(idx, cik(registrant), report_date, cutoff, knowledge_mode=mode)


def test_effective_picks_latest_period() -> None:
    older = mk(1, 101, period=dt.date(2025, 6, 30), filed=dt.date(2025, 8, 20))
    original = mk(2, 101, period=dt.date(2025, 12, 31), filed=dt.date(2026, 2, 20))
    future = mk(4, 101, period=dt.date(2026, 6, 30), filed=dt.date(2026, 8, 20))
    idx = index(older, original, future)
    assert eff(idx).filing.accession_number == acc(2)
    assert eff(idx, report_date=dt.date(2026, 7, 31)).filing.accession_number == acc(4)
    with pytest.raises(ncen.NcenError):
        eff(idx, mode="historical")


def test_effective_window_is_fifteen_months() -> None:
    idx = index(mk(1, 101, period=dt.date(2024, 12, 31), filed=dt.date(2025, 2, 20)))
    assert eff(idx, report_date=dt.date(2026, 3, 31)).reason is None
    stale = eff(idx, report_date=dt.date(2026, 4, 30))
    assert stale.filing is None and stale.reason == "effective_filing_older_than_15_months"
    assert eff(idx, registrant=202).reason == "no_effective_filing"


def test_same_period_order_needs_exact_acceptance() -> None:
    first = mk(2, 101, filed=dt.date(2026, 2, 20))
    second = mk(3, 101, filed=dt.date(2026, 3, 10))
    # Date-only times only: order unresolved (never by accession or date-only bound).
    unresolved = eff(index(first, second))
    assert unresolved.reason == ncen.ORDER_UNRESOLVED and unresolved.filing is None
    # Exact acceptances decide, even against accession order.
    later_low = {acc(2): header(2, accepted=dt.datetime(2026, 3, 10, 20, tzinfo=UTC), filed=dt.date(2026, 3, 10)),
                 acc(3): header(3, accepted=ACCEPTED)}
    ordered = eff(index(first.with_updates(filing_date=dt.date(2026, 3, 10)),
                        second.with_updates(filing_date=dt.date(2026, 2, 20)), headers=later_low))
    assert ordered.reason is None and ordered.filing.accession_number == acc(2)
    assert {d.role for d in ordered.dependencies} == {"selected", "same_period_competitor"}
    tie = {acc(2): header(2), acc(3): header(3)}
    assert eff(index(first, second.with_updates(filing_date=dt.date(2026, 2, 20)), headers=tie)).reason == \
        ncen.ORDER_UNRESOLVED


def test_uncertain_admission_is_unresolved_not_ignored() -> None:
    original = mk(2, 101, filed=dt.date(2026, 2, 20))
    same_day = mk(3, 101, filed=dt.date(2026, 3, 10))
    k_same_day = dt.datetime(2026, 3, 10, 22, tzinfo=UTC)  # before the date-only bound 03-11 04:00Z
    selection = eff(index(original, same_day), cutoff=k_same_day)
    assert selection.reason == ncen.ORDER_UNRESOLVED
    assert ("admission_uncertain", acc(3)) in {(d.role, d.accession_number) for d in selection.dependencies}
    # With the exact acceptance after K the later filing is provably not public by K.
    accepted_late = {acc(3): header(3, filed=dt.date(2026, 3, 10), accepted=dt.datetime(2026, 3, 10, 23, tzinfo=UTC))}
    assert eff(index(original, same_day, headers=accepted_late), cutoff=k_same_day).filing.accession_number == acc(2)
    # A filing dated after K cannot have been public by K.
    assert eff(index(original, same_day), cutoff=dt.datetime(2026, 3, 9, tzinfo=UTC)).reason is None


def test_effective_never_falls_back_past_quarantined_or_unknown_data() -> None:
    older = mk(1, 101, period=dt.date(2025, 6, 30), filed=dt.date(2025, 8, 20))
    broken = mk(2, 101, status="quarantined")
    chosen = eff(index(older, broken))
    assert chosen.reason == "effective_filing_quarantined" and chosen.filing.accession_number == acc(2)
    late_data = mk(2, 101, known=dt.datetime(2026, 9, 30, tzinfo=UTC))
    assert eff(index(older, late_data)).reason == "effective_data_not_known_at_cutoff"


def test_effective_blocked_by_unplaceable_later_filing_only() -> None:
    known = mk(1, 101)
    later = index(known, index_entries=[entry(1), entry(2, filed=dt.date(2026, 5, 1))])
    assert eff(later).reason == ncen.UNACQUIRED_FILING
    period_less = mk(3, 101, filed=dt.date(2026, 5, 1), status="quarantined").with_updates(report_period_end=None)
    assert eff(index(known, period_less)).reason == "filing_period_unknown"
    assert eff(later, cutoff=dt.datetime(2026, 4, 1, tzinfo=UTC)).reason is None
    earlier = index(known, index_entries=[entry(1), entry(2, filed=dt.date(2025, 11, 1))])
    assert eff(earlier).reason is None
    untimed = index(known, parse_xml(xml_doc(period="2026-01-31"), n=5))
    assert eff(untimed).reason == "filing_public_time_unknown"


# --- dependencies (finding 6) --------------------------------------------------------------------
def test_blocking_dependency_recorded_with_time_in_digest_and_known_at() -> None:
    blocked = index(mk(1, 101), index_entries=[entry(1), entry(2, filed=dt.date(2026, 5, 1))])
    selection = eff(blocked)
    blocking = {d.accession_number: d for d in selection.dependencies}[acc(2)]
    assert blocking.role == "blocking_period_unknown"
    assert blocking.knowledge_time == dt.datetime(2026, 5, 2, 4, tzinfo=UTC)
    universe = {cik(101): {"S000000001"}, cik(104): {"S000000004"}}
    with_block = ncen.family_components(index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"),
                                              index_entries=[entry(1), entry(4, 104), entry(2, filed=dt.date(2026, 5, 1))]),
                                        universe, R, K, knowledge_mode=H)
    without = ncen.family_components(index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"),
                                           index_entries=[entry(1), entry(4, 104)]), universe, R, K, knowledge_mode=H)
    assert with_block.universe_digest != without.universe_digest
    assert with_block.knowledge_time == dt.datetime(2026, 5, 2, 4, tzinfo=UTC) > without.knowledge_time
    assert with_block.time_established


def test_unknown_dependency_time_voids_date_evidence() -> None:
    untimed = parse_xml(xml_doc(period="2026-01-31"), n=5)
    idx = index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"), untimed)
    result = ncen.family_components(idx, {cik(101): {"S000000001"}, cik(104): {"S000000004"}}, R, K,
                                    knowledge_mode=H)
    assert result.incomplete == {cik(101): ("filing_public_time_unknown",)}
    assert result.time_established is False


# --- knowledge mode (finding 7) ------------------------------------------------------------------
def test_knowledge_mode_separates_publicity_from_possession() -> None:
    xml = parse_xml(xml_doc(), retrieved=dt.datetime(2026, 9, 25, 5, tzinfo=UTC))
    idx = index(xml, index_entries=[entry(1, filed=dt.date(2026, 2, 20))])
    k_june = dt.datetime(2026, 6, 1, tzinfo=UTC)
    historical = eff(idx, cutoff=k_june, mode=H)
    assert historical.reason is None and historical.filing.retrieved_at > k_june
    current = eff(idx, cutoff=k_june, mode=C)
    assert current.reason == "no_effective_filing" and current.filing is None
    assert eff(idx, cutoff=K, mode=C).reason is None


def test_current_run_orders_only_with_headers_held_by_k() -> None:
    first, second = mk(2, 101), mk(3, 101, filed=dt.date(2026, 2, 20))
    k_june = dt.datetime(2026, 6, 1, tzinfo=UTC)
    headers = {acc(2): header(2, retrieved=dt.datetime(2026, 5, 1, tzinfo=UTC)),
               acc(3): header(3, accepted=ACCEPTED + dt.timedelta(minutes=5), retrieved=RETRIEVED)}
    filings = [f.with_updates(retrieved_at=dt.datetime(2026, 5, 1, tzinfo=UTC)) for f in (first, second)]
    idx = index(*filings, headers=headers)
    assert eff(idx, cutoff=k_june, mode=H).filing.accession_number == acc(3)
    assert eff(idx, cutoff=k_june, mode=C).reason == ncen.ORDER_UNRESOLVED


# --- amendment semantics (finding 5) ----------------------------------------------------------------
SUB_900 = ("<subAdvisers><subAdviser><subAdviserName>S</subAdviserName><subAdviserFileNo>801-900</subAdviserFileNo>"
           "<subAdviserCrdNo>N/A</subAdviserCrdNo><subAdviserLei>N/A</subAdviserLei></subAdviser></subAdvisers>")
AMENDED_AT = ACCEPTED + dt.timedelta(days=20)


def _amendment_index(*, schema: str = "X0505", with_xml: bool = True) -> ncen.NcenFilingIndex:
    original = parse_xml(xml_doc(fund_extra=SUB_900), n=1)
    amendment = parse_xml(xml_doc(form="N-CEN/A", schema=schema), n=3)  # omits sub-adviser 801-900
    peer = mk(2, 102, advisers=(adv(fn="801-2"), adv("sub_adviser", fn="801-900")))
    copies = [original, peer]
    if with_xml:
        copies.append(amendment)
    else:
        copies.append(mk(3, 101, form="N-CEN/A", filed=dt.date(2026, 3, 12), advisers=(adv(fn="801-00001"),)))
    headers = {acc(1): header(1), acc(3): header(3, form="N-CEN/A", accepted=AMENDED_AT, filed=dt.date(2026, 3, 12))}
    entries = [entry(1), entry(2, 102), entry(3, form="N-CEN/A", filed=dt.date(2026, 3, 12))]
    return index(*copies, index_entries=entries, headers=headers)


def test_complete_replacement_amendment_is_not_merged_with_original() -> None:
    idx = _amendment_index()
    selection = eff(idx)
    assert selection.filing.accession_number == acc(3) and selection.amendment_semantics == ncen.AMENDMENT_COMPLETE
    voters = {cik(101): {"S000000001"}, cik(102): {"S000000002"}}
    result = ncen.family_components(idx, voters, R, K, knowledge_mode=H)
    # The original's sub-adviser 801-900 (shared with 102) is gone: two families.
    assert len(result.components) == 2
    before_amendment = ncen.family_components(idx, voters, R, AMENDED_AT - dt.timedelta(seconds=1), knowledge_mode=H)
    assert len(before_amendment.components) == 1


@pytest.mark.parametrize(("kwargs", "semantics"), [
    ({"schema": "X0404"}, ncen.AMENDMENT_UNKNOWN), ({"with_xml": False}, ncen.AMENDMENT_UNKNOWN),
])
def test_unknown_amendment_semantics_block_instead_of_latest_wins(kwargs, semantics) -> None:
    idx = _amendment_index(**kwargs)
    selection = eff(idx)
    assert selection.amendment_semantics == semantics
    assert selection.reason == ncen.AMENDMENT_UNKNOWN_REASON and selection.filing.accession_number == acc(3)
    result = ncen.family_components(idx, {cik(101): {"S000000001"}, cik(102): {"S000000002"}}, R, K,
                                    knowledge_mode=H)
    assert result.incomplete == {cik(101): (ncen.AMENDMENT_UNKNOWN_REASON,)}


def test_amendment_semantics_evidence_is_versioned() -> None:
    assert ncen.amendment_semantics(mk(1, 101)) is None
    assert ncen.AMENDMENT_COVERED_SCHEMA_VERSIONS == frozenset({"X0505"})
    assert "General Instruction C.2" in (ncen.__doc__ or "")
    assert "§3.4" in (ncen.__doc__ or "") and ncen.AMENDMENT_SEMANTICS_VERSION.endswith("_v1")


# --- completeness and pairwise rule -----------------------------------------------------------
@pytest.mark.parametrize(("filing", "reason"), [
    (mk(1, 101, answer=None), "b5_unanswered"),
    (mk(1, 101, answer="Y", name=None), "b5_family_name_missing"),
    (mk(1, 101, answer="Y", name="Funds"), "b5_family_name_unparseable"),
    (mk(1, 101, funds=()), "no_funds"),
    (mk(1, 101, advisers=(adv("sub_adviser", fn="801-5"),)), "fund_without_current_adviser"),
    (mk(1, 101, advisers=(adv(fn="801-1"), adv("terminated_sub_adviser", fn="N/A", lei="N/A"))),
     "adviser_without_identifier"),
    (mk(1, 101, underwriters=(uw("N/A", "0"),)), "underwriter_without_identifier"),
    (mk(1, 101, status="quarantined"), "filing_quarantined"),
])
def test_incomplete_profiles(filing: ncen.NcenFiling, reason: str) -> None:
    profile = ncen.family_profile(filing)
    assert not profile.complete and reason in profile.reasons


@pytest.mark.parametrize("sentinel", ["N/A", "n/a", "NA", "None", "-", "--", "NOT APPLICABLE", "null", " n / a "])
def test_family_name_sentinels_are_missing_on_both_paths(tmp_path: Path, sentinel: str) -> None:
    assert ncen.is_missing_value_sentinel(sentinel)
    dera = _dera_copy(tmp_path, "2026q1", name=sentinel)
    xml = parse_xml(xml_doc(family=f'<registrantFamilyInvComp isRegistrantFamilyInvComp="Y" '
                                   f'familyInvCompFullName="{sentinel}"/>'))
    for filing in (dera, xml):
        profile = ncen.family_profile(filing)
        assert not profile.complete and profile.reasons == ("b5_family_name_sentinel",)
        assert filing.projection()["family_key"] is None
    assert ncen.FAMILY_SUFFIXES == ("FAMILY", "COMPLEX", "GROUP", "FUNDS", "FUND", "TRUST")
    assert not ncen.is_missing_value_sentinel("NA Capital Funds")


def profile(filing: ncen.NcenFiling) -> ncen.FamilyProfile:
    return ncen.family_profile(filing)


def test_pairwise_rule() -> None:
    a = profile(mk(1, 101, answer="Y", name="Acme Funds", advisers=(adv(fn="801-1"),)))
    same_name = profile(mk(2, 102, answer="Y", name="ACME FUND", advisers=(adv(fn="801-2"),)))
    other = profile(mk(3, 103, answer="Y", name="Beta Trust", advisers=(adv(fn="801-3"),)))
    solo = profile(mk(4, 104, answer="N", advisers=(adv(fn="801-4"),)))
    shared_lei = profile(mk(5, 105, advisers=(adv(fn="801-5"), adv("sub_adviser", lei="5493001Z012YSB2A0K51"))))
    lei_holder = profile(mk(6, 106, advisers=(adv(fn="801-6", lei="5493001z012ysb2a0k51"),)))
    crd_padded = profile(mk(7, 107, advisers=(adv(fn="801-7", crd="000000044"),)))
    crd_plain = profile(mk(8, 108, advisers=(adv("terminated_adviser", crd="44"), adv(fn="801-8"))))
    assert ncen.not_independent(a, same_name)
    assert not ncen.not_independent(a, other) and not ncen.not_independent(a, solo)
    assert ncen.not_independent(shared_lei, lei_holder) and ncen.not_independent(crd_padded, crd_plain)
    assert ncen.not_independent(a, a)
    incomplete = profile(mk(9, 109, answer=None))
    assert ncen.not_independent(incomplete, other)
    u1 = profile(mk(10, 110, underwriters=(uw("8-1", "21"),)))
    u2 = profile(mk(11, 111, underwriters=(uw("8-00001"),)))
    none = profile(mk(12, 112))
    assert ncen.not_independent(u1, u2) and not ncen.not_independent(u1, none)


# --- XML relationship-node consumption (finding 9) ----------------------------------------------
@pytest.mark.parametrize(("doc", "reason"), [
    (xml_doc(fund_extra=f"<extraWrapper>{SUB_900}</extraWrapper>"), "xml_relationship_node_unconsumed:subAdvisers"),
    (xml_doc(form_extra=f"<principalUnderwriters>{UW}</principalUnderwriters>"),
     "xml_relationship_node_unconsumed:principalUnderwriters"),
    (xml_doc(fund_extra="<note><investmentAdviserFileNo>801-77</investmentAdviserFileNo></note>"),
     "xml_relationship_node_unconsumed:investmentAdviserFileNo"),
])
def test_unconsumed_relationship_nodes_quarantine(doc: bytes, reason: str) -> None:
    filing = parse_xml(doc)
    assert filing.status == "quarantined" and reason in filing.reasons


# --- components ---------------------------------------------------------------------------------
def chain_index() -> ncen.NcenFilingIndex:
    return index(
        mk(1, 101, advisers=(adv(fn="801-1"), adv("sub_adviser", fn="801-900"))),
        mk(2, 102, advisers=(adv(fn="801-2"), adv("sub_adviser", fn="801-900")), underwriters=(uw("8-7"),)),
        mk(3, 103, advisers=(adv(fn="801-3"),), underwriters=(uw(crd="000007"), uw("8-7"))),
        mk(4, 104, answer="Y", name="Delta Funds", advisers=(adv(fn="801-4"),)),
        mk(5, 105, answer="Y", name="DELTA", advisers=(adv(fn="801-5"),)),
        mk(6, 106, answer=None),
    )


def voters(*registrants: int) -> dict[str, set[str]]:
    """Each synthetic registrant votes with its own listed series."""
    return {cik(n): {series_of(n)} for n in registrants}


def test_components_are_transitive_closure() -> None:
    result = ncen.family_components(chain_index(), voters(101, 102, 103, 104, 105, 106), R, K, knowledge_mode=H)
    members = sorted(c.members for c in result.components)
    assert members == [(cik(101), cik(102), cik(103)), (cik(104), cik(105))]
    assert result.incomplete == {cik(106): ("b5_unanswered",)}
    assert result.component_sizes == (3, 2)
    bridged = ncen.family_components(chain_index(), voters(101, 103), R, K, knowledge_mode=H)
    assert len(bridged.components) == 2
    with pytest.raises(ncen.NcenError):
        ncen.family_components(chain_index(), {"not-a-cik": {"S000000001"}}, R, K, knowledge_mode=H)


def test_component_ids_deterministic_and_scoped() -> None:
    universe = voters(101, 102, 103, 104, 105)
    first = ncen.family_components(chain_index(), universe, R, K, knowledge_mode=H)
    second = ncen.family_components(chain_index(), dict(reversed(list(universe.items()))), R, K, knowledge_mode=H)
    assert first.component_of == second.component_of and first.universe_digest == second.universe_digest
    assert first.component_of[cik(101)] == ncen.component_id_for(R, K, [cik(101), cik(102), cik(103)],
                                                                 knowledge_mode=H)
    other_k = ncen.family_components(chain_index(), universe, R, K + dt.timedelta(hours=1), knowledge_mode=H)
    other_mode = ncen.family_components(chain_index(), universe, R, K, knowledge_mode=C)
    assert set(first.component_of.values()).isdisjoint(other_k.component_of.values())
    assert set(first.component_of.values()).isdisjoint(other_mode.component_of.values())


def test_series_completeness_never_unions_filings() -> None:
    older = mk(1, 101, period=dt.date(2025, 9, 30), filed=dt.date(2025, 11, 20),
               funds=(ncen.NcenFund("S000000011", (adv(fn="801-1"),)),))
    newer = mk(2, 101, period=dt.date(2025, 12, 31), filed=dt.date(2026, 2, 20))
    result = ncen.family_components(index(older, newer), {cik(101): {"S000000001", "S000000011"}}, R, K,
                                    knowledge_mode=H)
    assert result.selections[cik(101)].filing.accession_number == acc(2)
    assert result.incomplete == {cik(101): (ncen.INCOMPLETE_SERIES_ABSENT,)}


# --- N-PORT vote inventory (finding 1) -----------------------------------------------------------
_NB_SPEC = importlib.util.spec_from_file_location(
    "ncen_test_dera_builder", ROOT / "tests" / "fixtures" / "bond_default_events" / "nport" / "dera_builder.py")
NB = importlib.util.module_from_spec(_NB_SPEC)
assert _NB_SPEC.loader is not None
_NB_SPEC.loader.exec_module(NB)
CUSIP_A = NB.make_cusip("03783310")
CUSIP_B = NB.make_cusip("59491810")
NPORT_RETRIEVED = dt.datetime(2026, 6, 1, tzinfo=UTC)
_NPORT_ACCESSIONS = itertools.count(1)


def nport_package(tmp_path: Path, holdings: list[tuple[int, str | None, str, str]], *,
                  retrieved: dt.datetime = NPORT_RETRIEVED, report_date: str = "31-MAR-2026",
                  label: str = "2026q2_nport.zip") -> nport.DeraPackageResult:
    """``holdings``: (registrant, series or None, cusip9, IS_DEFAULT)."""
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    filings: dict[tuple[int, str | None], Any] = {}
    for number, (registrant, series, cusip, flag) in enumerate(holdings, start=1):
        key = (registrant, series)
        if key not in filings:
            filings[key] = NB.Filing(f"{registrant:010d}-26-{next(_NPORT_ACCESSIONS):06d}", cik=cik(registrant),
                                     series=series, report_date=report_date, filing_date="20-MAY-2026")
        filings[key].hold(number, cusip, is_default=flag)
    tag = uuid.uuid4().hex[:8]
    path = tmp_path / f"{tag}_{label}"
    sha = NB.build_zip(path, NB.tables(filings.values()))
    return nport.parse_dera_package(path, expected_sha256=sha, package_label=label, output_dir=tmp_path / f"o_{tag}",
                                    work_dir=tmp_path / "work", retrieved_at=retrieved, duckdb_threads=2,
                                    duckdb_memory_limit="512MB")


CHAIN_HOLDINGS = [(101, "S000000001", CUSIP_A, "Y"), (103, "S000000003", CUSIP_A, "Y"),
                  (102, "S000000002", CUSIP_B, "N")]


def inventory_for(packages, cutoff: dt.datetime = K, mode: str = H) -> ncen.VoteInventory:
    return ncen.build_vote_inventory(packages, knowledge_cutoff=cutoff, knowledge_mode=mode)


def consensus(packages, idx, cusips=(CUSIP_A,), cutoff: dt.datetime = K, mode: str = H):
    inventory = inventory_for(packages, cutoff, mode)
    targets = inventory.target_votes(packages, set(cusips))
    return ncen.build_consensus_with_ncen(inventory, idx, target_votes=targets, knowledge_cutoff=cutoff,
                                          knowledge_mode=mode), inventory, targets


def state(result, cusip: str = CUSIP_A) -> nport.DateState:
    return next(s for s in result.date_states if s.cusip9 == cusip)


def test_inventory_universe_includes_votes_on_other_cusips(tmp_path: Path) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    (result, report), inventory, targets = consensus([package], chain_index())
    assert inventory.voters[R] == {cik(101): frozenset({"S000000001"}), cik(102): frozenset({"S000000002"}),
                                   cik(103): frozenset({"S000000003"})}
    assert {v.cusip9 for v in targets} == {CUSIP_A} and len(targets) == 2
    assert state(result).status == "candidate_y" and state(result).basis == "same_family_uncorroborated"
    assert report.components[R].component_sizes == (3,) and report.inventory_digest == inventory.digest
    assert inventory.sources[0].zip_sha256 == package.zip_sha256


@pytest.mark.parametrize("mode", [H, C])
def test_vote_preparation_occurs_once_per_inventory_and_target_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    original_status = nport.RevisionResolution.accession_status
    original_resolve = nport.resolve_accession_revisions
    calls = 0
    family_scans = 0

    class CountingFamilies(tuple):
        def __iter__(self):
            nonlocal family_scans
            family_scans += 1
            return super().__iter__()

    def counted(self: nport.RevisionResolution) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return original_status(self)

    def counted_resolution(*args, **kwargs) -> nport.RevisionResolution:
        resolution = original_resolve(*args, **kwargs)
        object.__setattr__(resolution, "families", CountingFamilies(resolution.families))
        return resolution

    monkeypatch.setattr(nport.RevisionResolution, "accession_status", counted)
    monkeypatch.setattr(nport, "resolve_accession_revisions", counted_resolution)
    inventory = inventory_for([package], K, mode)
    assert calls == 1
    assert family_scans == 2
    calls = 0
    family_scans = 0
    targets = inventory.target_votes([package], {CUSIP_A, CUSIP_B})
    assert calls == 1
    assert family_scans == 2
    assert len(targets) == 3


@pytest.mark.parametrize("mode", [H, C])
def test_prepared_inventory_and_targets_equal_per_chunk_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    prepared = nport._PreparedVoteBuilder
    reference_builds = 0

    class PerChunkReference:
        def __init__(self, resolution: nport.RevisionResolution) -> None:
            self.resolution = resolution
            self.disputed_families = tuple(f for f in resolution.families if f.status == "disputed")

        def build(self, rows) -> nport.VoteSet:
            nonlocal reference_builds
            reference_builds += 1
            return prepared(self.resolution).build(rows)

    monkeypatch.setattr(nport, "_PreparedVoteBuilder", PerChunkReference)
    reference = inventory_for([package], K, mode)
    reference_targets = reference.target_votes([package], {CUSIP_A, CUSIP_B})
    assert reference_builds > 2
    monkeypatch.setattr(nport, "_PreparedVoteBuilder", prepared)
    optimized = inventory_for([package], K, mode)
    optimized_targets = optimized.target_votes([package], {CUSIP_A, CUSIP_B})

    for name in (
        "knowledge_cutoff", "knowledge_mode", "sources", "voters", "known_at",
        "disputed_families", "stats", "digest", "_fingerprints", "_resolution",
    ):
        assert getattr(optimized, name) == getattr(reference, name)
    assert optimized_targets == reference_targets
    replay = pickle.loads(pickle.dumps(optimized))
    assert replay.digest == optimized.digest
    assert replay.target_votes([package], {CUSIP_A, CUSIP_B}) == optimized_targets


def test_caller_assembled_votes_are_not_a_universe(tmp_path: Path) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    inventory = inventory_for([package])
    targets = inventory.target_votes([package], {CUSIP_A})
    with pytest.raises(ncen.NcenError, match="vote_inventory_required"):
        ncen.build_consensus_with_ncen(list(targets), chain_index(), target_votes=targets, knowledge_cutoff=K,
                                       knowledge_mode=H)  # type: ignore[arg-type]
    with pytest.raises(ncen.NcenError, match="vote_inventory_sealed"):
        ncen.VoteInventory(knowledge_cutoff=K, knowledge_mode=H, sources=(), voters={}, known_at={},
                           disputed_families=(), stats={}, digest="x", _fingerprints=frozenset(),
                           _resolution=None, _seal=object())
    forged = dataclasses.replace(targets[0], value="N", y_lots=0, n_lots=1)
    with pytest.raises(ncen.NcenError, match="target_votes_not_in_inventory"):
        ncen.build_consensus_with_ncen(inventory, chain_index(), target_votes=[forged], knowledge_cutoff=K,
                                       knowledge_mode=H)
    with pytest.raises(ncen.NcenError, match="inventory_packages_mismatch"):
        inventory.target_votes([nport_package(tmp_path, CHAIN_HOLDINGS[:1])], {CUSIP_A})


def test_accepting_path_signature() -> None:
    import inspect

    names = set(inspect.signature(ncen.build_consensus_with_ncen).parameters)
    assert {"inventory", "target_votes", "knowledge_mode", "knowledge_cutoff"} <= names
    assert not names & {"votes", "universe", "universe_values", "family_evidence", "disputed_families"}
    assert "universe" not in set(inspect.signature(ncen.family_evidence_for).parameters)
    assert not hasattr(ncen, "voting_universe")


def test_consensus_unknown_family_cannot_support(tmp_path: Path) -> None:
    package = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (106, "S000000006", CUSIP_A, "Y")])
    (result, _), _, _ = consensus([package], chain_index())
    assert state(result).status == "candidate_y" and state(result).basis == "family_independence_unknown"
    package = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (104, "S000000004", CUSIP_A, "Y"),
                                       (106, "S000000006", CUSIP_A, "Y")])
    (result, _), _, _ = consensus([package], chain_index())
    assert state(result).status == "consensus_y" and state(result).basis == "independent_families"
    assert all(f.family_id.startswith("ncenfam:") for f in state(result).family_evidence)


def test_consensus_n_uses_identical_family_test(tmp_path: Path) -> None:
    package = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "N"), (104, "S000000004", CUSIP_A, "N")])
    (result, _), _, _ = consensus([package], chain_index())
    assert state(result).status == "consensus_n"
    package = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "N"), (102, "S000000002", CUSIP_A, "N")])
    (result, _), _, _ = consensus([package], chain_index())
    assert state(result).status == "candidate_n" and state(result).basis == "same_family"


def test_fe1b_absent_or_missing_series_via_inventory(tmp_path: Path) -> None:
    idx = index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"))
    absent = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (101, "S000000099", CUSIP_B, "N"),
                                      (104, "S000000004", CUSIP_A, "Y")])
    (result, report), _, _ = consensus([absent], idx)
    assert report.components[R].incomplete == {cik(101): (ncen.INCOMPLETE_SERIES_ABSENT,)}
    assert state(result).basis == "family_independence_unknown"
    no_series = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (101, None, CUSIP_B, "N"),
                                         (104, "S000000004", CUSIP_A, "Y")])
    (result, report), _, _ = consensus([no_series], idx)
    assert report.components[R].incomplete == {cik(101): (ncen.INCOMPLETE_NO_SERIES_ID,)}
    listed = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (104, "S000000004", CUSIP_A, "Y")])
    (result, report), _, _ = consensus([listed], idx)
    assert report.components[R].incomplete == {} and state(result).status == "consensus_y"


def test_absent_series_is_incomplete_for_that_date_only(tmp_path: Path) -> None:
    idx = index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"))
    march = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (101, "S000000099", CUSIP_B, "N"),
                                     (104, "S000000004", CUSIP_A, "Y")], label="a_nport.zip")
    feb = nport_package(tmp_path, [(101, "S000000001", CUSIP_A, "Y"), (104, "S000000004", CUSIP_A, "Y")],
                        report_date="28-FEB-2026", label="b_nport.zip")
    (result, _), _, _ = consensus([march, feb], idx)
    by_date = {s.report_date: s for s in result.date_states if s.cusip9 == CUSIP_A}
    assert by_date[R].basis == "family_independence_unknown"
    assert by_date[dt.date(2026, 2, 28)].status == "consensus_y"


def test_dependency_time_unknown_emits_no_evidence(tmp_path: Path) -> None:
    untimed = parse_xml(xml_doc(period="2026-01-31"), n=5)
    idx = index(mk(1, 101), mk(4, 104, answer="Y", name="Delta Funds"), mk(7, 107), untimed)
    package = nport_package(tmp_path, [(104, "S000000004", CUSIP_A, "Y"), (107, "S000000007", CUSIP_A, "Y"),
                                       (101, "S000000001", CUSIP_B, "N")])
    (result, report), _, _ = consensus([package], idx)
    assert report.date_status[R] == ncen.DEPENDENCY_TIME_UNKNOWN
    assert state(result).status == "candidate_y" and state(result).basis == "family_independence_unknown"


# --- K1/K2 bridge regression (finding 2) ---------------------------------------------------------
def test_evidence_at_k1_cannot_produce_consensus_at_k2(tmp_path: Path) -> None:
    early = nport_package(tmp_path, CHAIN_HOLDINGS[:2], retrieved=dt.datetime(2026, 6, 1, tzinfo=UTC),
                          label="a_nport.zip")
    bridge = nport_package(tmp_path, CHAIN_HOLDINGS[2:], retrieved=dt.datetime(2026, 7, 1, tzinfo=UTC),
                           label="b_nport.zip")
    k1, k2 = dt.datetime(2026, 6, 15, tzinfo=UTC), dt.datetime(2026, 7, 15, tzinfo=UTC)
    (at_k1, report_k1), inv_k1, targets_k1 = consensus([early, bridge], chain_index(), cutoff=k1)
    assert state(at_k1).status == "consensus_y"  # the bridge vote is not public at K1
    (at_k2, _), _, targets_k2 = consensus([early, bridge], chain_index(), cutoff=k2)
    assert state(at_k2).status == "candidate_y"
    with pytest.raises(ncen.NcenError, match="inventory_knowledge_cutoff_mismatch"):
        ncen.build_consensus_with_ncen(inv_k1, chain_index(), target_votes=targets_k1, knowledge_cutoff=k2,
                                       knowledge_mode=H)
    diagnostic = ncen.family_evidence_for(chain_index(), inv_k1, knowledge_cutoff=k1, knowledge_mode=H)
    assert not any(isinstance(node, nport.FamilyEvidence) for node in _walk(diagnostic))
    with pytest.raises((AttributeError, TypeError, nport.NportError)):
        nport.build_consensus_states(targets_k2, knowledge_cutoff=k2,
                                     family_evidence=dict(diagnostic.components))  # type: ignore[arg-type]
    assert report_k1.knowledge_cutoff == k1 and report_k1.inventory_digest == inv_k1.digest


def test_inventory_current_run_mode_is_bound(tmp_path: Path) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    inventory = inventory_for([package], K, C)
    with pytest.raises(ncen.NcenError, match="inventory_knowledge_mode_mismatch"):
        ncen.family_evidence_for(chain_index(), inventory, knowledge_cutoff=K, knowledge_mode=H)
    (result, _), _, _ = consensus([package], chain_index(), mode=C)
    assert state(result).status == "candidate_y"


# --- non-accepting per-state diagnostic ---------------------------------------------------------
def _walk(value: object, seen: set[int] | None = None):
    seen = set() if seen is None else seen
    if id(value) in seen:
        return
    seen.add(id(value))
    yield value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        for item in dataclasses.fields(value):
            yield from _walk(getattr(value, item.name), seen)
    elif isinstance(value, dict | list | tuple | set | frozenset):
        items = list(value.items()) if isinstance(value, dict) else list(value)
        for item in items:
            yield from _walk(item, seen)


def test_per_state_diagnostic_is_less_strict_and_non_accepting(tmp_path: Path) -> None:
    package = nport_package(tmp_path, CHAIN_HOLDINGS)
    (result, _), inventory, targets = consensus([package], chain_index())
    assert state(result).basis == "same_family_uncorroborated"
    diagnostic = ncen.diagnostic_per_state_components(chain_index(), inventory, targets, knowledge_cutoff=K,
                                                      knowledge_mode=H)
    assert diagnostic.accepting is False and diagnostic.rule_version == ncen.RULE_VERSION
    (only,) = diagnostic.states
    assert (only.cusip9, only.report_date, only.y_registrants, only.component_count) == (CUSIP_A, R, 2, 2)
    nodes = list(_walk(diagnostic))
    assert not any(isinstance(node, nport.FamilyEvidence) for node in nodes)
    assert not any(isinstance(node, str) and node.startswith("ncenfam:") for node in nodes)
    with pytest.raises((AttributeError, TypeError, nport.NportError)):
        nport.build_consensus_states(targets, knowledge_cutoff=K,
                                     family_evidence={cik(101): diagnostic.states})  # type: ignore[dict-item]


def test_component_ids_deterministic_in_fresh_processes(tmp_path: Path) -> None:
    package = nport_package(tmp_path / "shared", CHAIN_HOLDINGS)
    assert package.projection_path is not None
    package_dir = package.projection_path.parent
    script = (
        "import sys, json\n"
        f"sys.path.insert(0, {str(ROOT)!r}); sys.path.insert(0, {str(Path(__file__).parent)!r})\n"
        "from pathlib import Path\n"
        "import test_bond_default_ncen as t\n"
        "from src.bonds.default_events import ncen, nport\n"
        "r = ncen.family_components(t.chain_index(), t.voters(105, 101, 104, 102, 103, 106), t.R, t.K,"
        " knowledge_mode=t.H)\n"
        f"p = nport.DeraPackageResult.load(Path({str(package_dir)!r}))\n"
        "inv = t.inventory_for([p])\n"
        "targets = inv.target_votes([p], {t.CUSIP_A, t.CUSIP_B})\n"
        "rep = ncen.family_evidence_for(t.chain_index(), inv, knowledge_cutoff=t.K, knowledge_mode=t.H)\n"
        "print(json.dumps([r.component_of, r.universe_digest, inv.digest, dict(inv.stats),"
        " [repr(v) for v in targets],"
        " {str(d): [c.component_of, c.universe_digest] for d, c in rep.components.items()}], sort_keys=True))\n"
    )
    outputs = []
    for seed in ("1", "2", "977", "31337"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        done = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                              timeout=300, check=False, env=env, cwd=str(ROOT))
        assert done.returncode == 0, done.stderr[-2000:]
        outputs.append(done.stdout.strip().splitlines()[-1])
    assert len(set(outputs)) == 1
    local = ncen.family_components(chain_index(), voters(101, 102, 103, 104, 105, 106), R, K, knowledge_mode=H)
    assert json.loads(outputs[0])[0] == dict(local.component_of)


def test_real_fixtures_have_lf_only_bytes() -> None:
    """The fixture tree is ``text eol=lf``; verbatim SEC bytes must already be LF-only."""
    for path in [*(FIXTURES / "dera_2026q2_slice").glob("*.tsv"), *(FIXTURES / "xml").glob("*.xml")]:
        assert b"\r" not in path.read_bytes()
