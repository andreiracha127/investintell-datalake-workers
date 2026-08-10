"""Contracts for the direct, database-backed Finnhub terms backfill."""
from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Any

import pytest

from scripts import backfill_bond_reference_terms as backfill
from src.workers import _finnhub


PROFILE = {
    "amountOutstanding": 525,
    "asset": "Corporate",
    "assetType": "Debt",
    "bondType": "Senior Note",
    "callable": True,
    "coupon": 8.375,
    "couponType": "Fixed",
    "cusip": "00033GAA3",
    "datedDate": "2024-01-22",
    "debtType": "Senior Unsecured Note",
    "dayCount": "30/360",
    "figi": "BBG01ABCDE12",
    "firstCouponDate": "2024-07-15",
    "industryGroup": "Technology Hardware",
    "industrySubGroup": "Electronic Equipment",
    "isin": "US00033GAA31",
    "issueDate": "2024-01-22",
    "maturityDate": "2029-01-15",
    "offeringPrice": 99.875,
    "originalOffering": 600_000_000,
    "paymentFrequency": "Semi-Annual",
    "securityLevel": "Senior",
}


class _Result:
    def __init__(self, *, one: Any = None, many: list[Any] | None = None) -> None:
        self._one, self._many = one, many or []

    def fetchone(self) -> Any:
        return self._one

    def fetchall(self) -> list[Any]:
        return self._many


class _Connection:
    def __init__(self, candidates: list[tuple[str] | tuple[str, bool]], *, cursor: str | None = None) -> None:
        self.candidates = candidates
        self.cursor = cursor
        self.queries: list[tuple[str, Any]] = []
        self.commits = 0

    def execute(self, sql: str, params: Any = None) -> _Result:
        self.queries.append((sql, params))
        if "SELECT resume_cursor" in sql:
            return _Result(one=(self.cursor,) if self.cursor else None)
        if "FROM bond_curated_universe" in sql:
            return _Result(many=self.candidates)
        if "UPDATE bond_reference_terms_finnhub_run" in sql:
            self.cursor = params[1]
        return _Result()

    def commit(self) -> None:
        self.commits += 1


class _Client:
    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def profile_by_cusip(self, cusip9: str) -> dict[str, Any]:
        self.calls.append(cusip9)
        value = self.responses[cusip9]
        if isinstance(value, BaseException):
            raise value
        return value


def test_profile_mapping_covers_every_requested_vendor_attribute() -> None:
    """Deleting any mapped term must fail this literal, vendor-shaped contract."""
    row, identity_basis = backfill.profile_to_terms("00033GAA3", PROFILE)

    assert identity_basis == "returned_cusip"
    assert row == {
        "cusip9": "00033GAA3", "isin": "US00033GAA31", "coupon_rate": 8.375,
        "coupon_type": "Fixed", "maturity_date": "2029-01-15", "issue_date": "2024-01-22",
        "seniority": "Senior", "secured": "unsecured", "day_count": "30/360",
        "payment_frequency": "Semi-Annual", "callable": True, "amount_outstanding_mm": 525,
        "amount_outstanding_vendor": 525, "asset": "Corporate", "asset_type": "Debt",
        "bond_type": "Senior Note", "dated_date": "2024-01-22", "debt_type": "Senior Unsecured Note",
        "figi": "BBG01ABCDE12", "first_coupon_date": "2024-07-15",
        "industry_group": "Technology Hardware", "industry_sub_group": "Electronic Equipment",
        "offering_price_vendor": 99.875, "original_offering_vendor": 600_000_000,
    }


def test_missing_cusip_is_accepted_only_when_us_isin_embeds_the_requested_cusip() -> None:
    """Removing this fallback would reject the observed live Finnhub shape."""
    missing_cusip = {key: value for key, value in PROFILE.items() if key != "cusip"}

    assert backfill.profile_identity_basis("00033GAA3", missing_cusip) == "isin_embedded_cusip9"
    row, identity_basis = backfill.profile_to_terms("00033GAA3", missing_cusip)
    assert row["isin"] == "US00033GAA31"
    assert identity_basis == "isin_embedded_cusip9"


def test_mismatched_returned_cusip_is_refused_without_terms_write() -> None:
    """Removing the identity check would incorrectly persist somebody else's terms."""
    conn = _Connection([("00033GAA3",)])
    wrong = {**PROFILE, "cusip": "037833100"}
    summary = backfill.run_batch(
        conn, _Client({"00033GAA3": wrong}), batch_label="2026-08-08", limit=1
    )

    assert summary["mismatch"] == 1
    assert summary["loaded"] == 0
    assert not any("INSERT INTO bond_reference_terms (" in sql for sql, _ in conn.queries)
    attempt = next(
        params
        for sql, params in conn.queries
        if "INSERT INTO bond_reference_terms_finnhub_attempt" in sql
    )
    assert attempt[3:5] == ("refused", "cusip_mismatch")


@pytest.mark.parametrize(
    "profile, reason",
    [
        ({**PROFILE, "cusip": None, "isin": "US0378331005"}, "isin_cusip_mismatch"),
        ({**PROFILE, "cusip": None, "isin": None}, "missing_identity_evidence"),
    ],
)
def test_missing_returned_cusip_without_matching_embedded_isin_is_refused(profile, reason) -> None:
    """An ISIN is identity evidence only when its CUSIP9 segment agrees exactly."""
    with pytest.raises(ValueError, match=reason):
        backfill.profile_identity_basis("00033GAA3", profile)


def test_empty_200_is_counted_as_empty_failure_not_loaded() -> None:
    """A caller must not turn an empty Finnhub profile into successful no-data."""
    conn = _Connection([("00033GAA3",)])
    summary = backfill.run_batch(
        conn,
        _Client({"00033GAA3": _finnhub.FinnhubProfileError("empty_profile")}),
        batch_label="2026-08-08", limit=1,
    )

    assert summary["empty"] == 1
    assert summary["loaded"] == 0
    assert summary["reason_counts"] == {"empty_profile": 1}


def test_provider_profile_error_is_retryable_and_does_not_advance_cursor() -> None:
    conn = _Connection([("00033GAA3",)], cursor="000000000")

    summary = backfill.run_batch(
        conn,
        _Client({"00033GAA3": _finnhub.FinnhubProfileError("provider_error")}),
        batch_label="2026-08-08", limit=1,
    )

    assert summary["transient"] == 1
    assert summary["empty"] == 0
    assert summary["reason_counts"] == {"provider_error": 1}
    assert summary["cursor_after"] == "000000000"
    assert not any("UPDATE bond_reference_terms_finnhub_run" in sql for sql, _ in conn.queries)


def test_attempt_ledger_ddl_allows_provider_error_only_for_transient_state() -> None:
    """The persisted Python retry reason must satisfy the ledger CHECK on new and existing tables."""
    ddl = backfill.SCHEMA_PATH.read_text(encoding="utf-8")

    assert "bond_reference_terms_finnhub_attempt_reason_state_ck" in ddl
    assert "(profile_state = 'transient' AND reason_code IN ('transient_error','unexpected_error','provider_error'))" in ddl
    assert ddl.count("'provider_error'") == 2
    assert "(profile_state = 'empty' AND reason_code = 'provider_error')" not in ddl
    assert "(profile_state = 'refused' AND reason_code IN ('provider_error'))" not in ddl


def test_unexpected_client_failure_is_typed_without_skipping_its_retry_cursor() -> None:
    """An unclassified client exception must be recorded yet remain retryable."""
    conn = _Connection([("00033GAA3",)])
    summary = backfill.run_batch(
        conn, _Client({"00033GAA3": RuntimeError("socket adapter broke")}),
        batch_label="2026-08-08", limit=1,
    )

    assert summary["transient"] == 1
    assert summary["reason_counts"] == {"unexpected_error": 1}
    assert summary["resume_cursor"] is None


def test_resume_cursor_and_refresh_upsert_replace_vendor_reference_terms() -> None:
    """A successful stale refresh must replace values, including explicit vendor nulls."""
    conn = _Connection([("00033GAA3",), ("037833100",)], cursor="000000000")
    client = _Client({"00033GAA3": PROFILE, "037833100": {**PROFILE, "cusip": "037833100"}})
    summary = backfill.run_batch(conn, client, batch_label="2026-08-08", limit=2)

    assert client.calls == ["00033GAA3", "037833100"]
    assert summary["cursor_before"] == "000000000"
    assert summary["cursor_after"] == "037833100"
    assert summary["loaded"] == 2
    assert summary["already_complete"] == 0
    selection = next((sql, params) for sql, params in conn.queries if "FROM bond_curated_universe" in sql)
    assert selection[1][0] == "000000000"
    upsert_sql = next(sql for sql, _ in conn.queries if "INSERT INTO bond_reference_terms (" in sql)
    for column in backfill._TERM_COLUMNS[1:]:
        assert f"{column} = EXCLUDED.{column}" in upsert_sql
        assert f"COALESCE(bond_reference_terms.{column}, EXCLUDED.{column})" not in upsert_sql
    # CUSIP identity and governed loader metadata stay outside the refresh surface.
    assert "cusip9 = EXCLUDED.cusip9" not in upsert_sql
    assert "batch_label = EXCLUDED.batch_label" not in upsert_sql
    assert "loaded_at = EXCLUDED.loaded_at" not in upsert_sql


def test_transient_failure_stops_before_cursor_advance_and_retries_same_cusip() -> None:
    """Advancing past a transient failure would make that CUSIP permanently invisible."""
    conn = _Connection([("00033GAA3",), ("037833100",)], cursor="000000000")
    first = backfill.run_batch(
        conn, _Client({"00033GAA3": _finnhub.FinnhubTransientError("down")}),
        batch_label="2026-08-08", limit=2,
    )

    assert first["transient"] == 1
    assert first["cursor_after"] == "000000000"
    assert not any("UPDATE bond_reference_terms_finnhub_run" in sql for sql, _ in conn.queries)

    retry = _Client({"00033GAA3": PROFILE, "037833100": {**PROFILE, "cusip": "037833100"}})
    second = backfill.run_batch(conn, retry, batch_label="2026-08-08", limit=2)
    assert retry.calls == ["00033GAA3", "037833100"]
    assert second["loaded"] == 2


@pytest.mark.parametrize(
    "failure, counter",
    [
        (_finnhub.FinnhubConfigError("bad key"), "config_error"),
        (RuntimeError("adapter broke"), "transient"),
    ],
)
def test_nonterminal_failures_never_advance_the_current_cusip_cursor(failure, counter) -> None:
    """A config or adapter failure must leave its same CUSIP available to retry."""
    conn = _Connection([("00033GAA3",)], cursor="000000000")
    summary = backfill.run_batch(
        conn, _Client({"00033GAA3": failure}), batch_label="2026-08-08", limit=1
    )

    assert summary[counter] == 1
    assert summary["cursor_after"] == "000000000"
    assert not any("UPDATE bond_reference_terms_finnhub_run" in sql for sql, _ in conn.queries)


def test_requested_cusip_must_be_a_cusip9_before_any_profile_terms_are_accepted() -> None:
    """Dropping the CUSIP9 check would let malformed identity keys into the ledger."""
    with pytest.raises(ValueError, match="invalid_requested_cusip"):
        backfill.profile_identity_basis("00033GAA", PROFILE)


def test_window_excludes_fresh_successes_before_applying_the_limit() -> None:
    """A completed prefix must not consume a bounded run ahead of stale/missing CUSIPs."""
    conn = _Connection([("037833100", False)], cursor="000000000")
    summary = backfill.run_batch(
        conn, _Client({"037833100": {**PROFILE, "cusip": "037833100"}}),
        batch_label="2026-08-08", limit=1,
    )

    assert summary["already_complete"] == 0
    assert summary["attempted"] == 1
    assert summary["cursor_after"] == "037833100"
    selection_sql = next(sql for sql, _ in conn.queries if "FROM bond_curated_universe" in sql)
    eligibility = "r.finnhub_profile_state IS DISTINCT FROM 'success'"
    assert eligibility in selection_sql
    assert "NOT EXISTS" in selection_sql
    assert "FROM bond_reference_terms_finnhub_attempt a" in selection_sql
    assert "a.profile_state IN ('empty', 'refused')" in selection_sql
    assert "a.fetched_at >= now() - %s::interval" in selection_sql
    assert selection_sql.index(eligibility) < selection_sql.index("ORDER BY c.cusip9 LIMIT %s")
    selection_params = next(params for sql, params in conn.queries if sql == selection_sql)
    assert selection_params == ("000000000", "30 days", "30 days", 1)


def test_schema_declares_worker_writer_ownership_and_legacy_constraints() -> None:
    """Dropping ownership/constraint migration would break the runtime deployment contract."""
    ddl = backfill.SCHEMA_PATH.read_text(encoding="utf-8")

    for relation in (
        "bond_reference_terms",
        "bond_reference_terms_finnhub_run",
        "bond_reference_terms_finnhub_attempt",
    ):
        assert f"ALTER TABLE {relation} OWNER TO worker_writer" in ddl
    assert "bond_reference_terms_finnhub_profile_state_ck" in ddl
    assert "bond_reference_terms_finnhub_lineage_ck" in ddl


def test_retry_attempts_are_append_only_and_keep_transient_then_success_truth() -> None:
    """A one-row-per-run ledger would erase the recovery that operators need to see."""
    conn = _Connection([("00033GAA3",)])
    first = backfill.run_batch(
        conn, _Client({"00033GAA3": _finnhub.FinnhubTransientError("down")}),
        batch_label="2026-08-08", limit=1,
    )
    second = backfill.run_batch(
        conn, _Client({"00033GAA3": PROFILE}), batch_label="2026-08-08", limit=1,
    )

    attempts = [
        params
        for sql, params in conn.queries
        if "INSERT INTO bond_reference_terms_finnhub_attempt" in sql
    ]
    assert first["transient"] == 1 and second["loaded"] == 1
    assert [(params[3], params[4]) for params in attempts] == [
        ("transient", "transient_error"), ("success", "returned_cusip"),
    ]
    ddl = backfill.SCHEMA_PATH.read_text(encoding="utf-8")
    assert "PRIMARY KEY (run_id, cusip9, fetched_at)" in ddl
    assert any(
        "ON CONFLICT (run_id,cusip9,fetched_at) DO NOTHING" in constant
        for constant in backfill._record_attempt.__code__.co_consts
        if isinstance(constant, str)
    )


@pytest.mark.parametrize(
    "summary, expected_exit",
    [
        ({"loaded": 1, "empty": 0, "mismatch": 0, "transient": 0, "config_error": 0}, 0),
        ({"loaded": 1, "empty": 1, "mismatch": 0, "transient": 0, "config_error": 0}, 2),
        ({"loaded": 0, "empty": 0, "mismatch": 1, "transient": 0, "config_error": 0}, 2),
        ({"loaded": 0, "empty": 0, "mismatch": 0, "transient": 1, "config_error": 0}, 2),
    ],
)
def test_main_prints_summary_but_returns_red_for_any_profile_failure(
    monkeypatch, capsys, summary, expected_exit
) -> None:
    """A green CLI exit on partial or empty enrichment would hide operational failure."""
    @contextmanager
    def fake_client(_env_file):
        yield object()

    monkeypatch.setattr(backfill, "_client_from_optional_env_file", fake_client)
    monkeypatch.setattr(backfill, "run", lambda *_args, **_kwargs: summary)

    assert backfill.main(["--limit", "1"]) == expected_exit
    assert json.loads(capsys.readouterr().out) == summary


def test_direct_batch_never_requires_a_local_profile_cache(tmp_path, monkeypatch) -> None:
    """The runnable batch uses only its DB/client seams, even in an empty directory."""
    monkeypatch.chdir(tmp_path)
    conn = _Connection([])
    summary = backfill.run_batch(conn, _Client({}), batch_label="2026-08-08", limit=10)

    assert summary["attempted"] == 0
    assert not list(tmp_path.iterdir())
