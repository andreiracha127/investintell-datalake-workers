"""Offline v2 -> v3 replay harness, exercised on a SYNTHETIC v2 bundle only.

The production replay runs on the custodian's retained private bundle; these
tests never read it. The synthetic bundle reproduces, with synthetic
identifiers, the exact shapes the v2 generator and v2 auditor wrote (policy,
source snapshot, canonical capture with cohort and SEC blocks, dossier with its
mirrored capture/config pins) and the production shape of the G4 v2 delta
(ACTIVE funds whose SEC row is only stale, or absent). The committed
expected-counts fixture carries only production aggregates (no identifiers).
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import re
import stat
import uuid
from collections import Counter

import pytest

from scripts import generate_fund_nav_policy_v1 as generator
from src.workers._nav_policy import (
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
    uuid_set_digest,
)
from tests import nav_policy_v3_replay as replay
from tests._nav_identity_fixtures import catalog, entity, only, sec_row

TAU = dt.datetime(2026, 9, 23, 10, 0, tzinfo=dt.timezone.utc)
CAPTURED = TAU + dt.timedelta(minutes=5)
FRESH = TAU - dt.timedelta(days=1)
STALE = TAU - dt.timedelta(days=30)
V2_REFERENCE = (
    "nav-current-catalog-snapshot-v2:public.instruments_universe+public.funds_v+"
    "public.instrument_identity:w1-tiingo-adjusted-daily-v1:current_only_not_pit:"
    "identity=registry-ticker-series-claims-v2"
)
V2_ROUND5 = "nav-identity-audit-contract-v2-round5"
STALE_IDS = (3, 4, 5)
MISSING_IDS = (6, 7)
LABEL = "Large Blend"
CONFIGS = replay.ROOT / "configs"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _compact(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _doc(value) -> bytes:
    return (_compact(value) + "\n").encode("ascii")


def _entities():
    return [
        *[entity(n, fund_type="etf" if n % 2 else "mutual_fund") for n in range(1, 21)],
        entity(21, fund_type="mmf"),  # ACTIVE, not daily
        entity(22, active=None),  # UNKNOWN activity (non-SEC)
        only(entity(23, active=False), fund=False, registry=False),  # INACTIVE
    ]


def _config(mutate=None) -> bytes:
    """The replay (v3) config: the repository file with synthetic quotas."""
    config = json.loads((CONFIGS / "nav_identity_audit_v3.json").read_bytes())
    sleeve = config["builder"]["label_to_sleeve"][LABEL]
    config["builder"]["stage1_quotas"] = {sleeve: 10}
    if mutate is not None:
        mutate(config)
    return json.dumps(config).encode()


def _v2_cohort(rows, builder) -> dict:
    """The v2 auditor's canonical cohort block (independent re-statement)."""
    rows = sorted(rows, key=_compact)
    return {
        "query_sha256": _sha(builder["cohort_query"].encode("utf-8")),
        "parameters_sha256": _sha(
            _compact(builder.get("cohort_parameters") or {}).encode("utf-8")
        ),
        "state": "captured",
        "code": None,
        "rows": rows,
        "row_count": len(rows),
        "rows_sha256": _sha(_doc(rows)),
    }


def _v2_sec_block(sec) -> dict:
    """The v2 auditor's canonical SEC block (v2 isoformat text, no ``code``)."""
    rows = sorted(
        (dict(row, synced_at=row["synced_at"].isoformat()) for row in sec),
        key=_compact,
    )
    instants = [row["synced_at"] for row in sec]
    return {
        "source_contract": "sec-company-tickers-mf-class-map-v1",
        "relation": "public.sec_company_tickers_mf",
        "timestamp_column": "updated_at",
        "query_sha256": replay.SEC_PINS["query_sha256"],
        "lineage_query_sha256": replay.SEC_PINS["lineage_query_sha256"],
        "query_contract_sha256": replay.SEC_PINS["query_contract_sha256"],
        "state": "captured",
        "rows": rows,
        "row_count": len(rows),
        "rows_sha256": _sha(_doc(rows)),
        "lineage": {
            "row_count": len(sec),
            "min_synced_at": min(instants).isoformat() if sec else None,
            "max_synced_at": max(instants).isoformat() if sec else None,
        },
    }


def _v2_docs(*, sec_override=None, drift=False) -> dict:
    """Synthetic retained v2 documents (v2 shapes, v2 versions, v2 digests)."""
    entities = _entities()
    instruments, funds, identity, all_fresh = catalog(*entities, sec_at=FRESH)
    calendar = generator.build_calendar(dt.date(2024, 1, 1), dt.date(2027, 12, 31))
    # Internal gates only (every candidate corroborated) == v2 semantics.
    policy = generator.build_policy(
        calendar,
        instruments,
        funds,
        identity,
        all_fresh,
        TAU,
        "current-daily-nav-xnys-usd-adjusted",
        "2026-09-24.2",
    )
    generation = policy["generation"]
    policy["generator_version"] = generation["generator_version"] = replay.V2_GENERATOR
    generation["source_query_version"] = replay.V2_QUERY_VERSION
    generation["source_query_sha256"] = replay.V2_SOURCE_QUERY_SHA256
    generation["generated_at"] = TAU.isoformat()
    for key in (
        "sec_company_tickers_mf",
        "structural_sec_failures",
        "structural_daily_claim_failures",
        "structural_daily_sec_failures",
    ):
        generation["counts"].pop(key)
    for row in policy["instrument_evidence"]:
        row["evidence_reference"] = V2_REFERENCE
        row["known_at"] = row["effective_at"] = TAU.isoformat()
    sources = {
        name: sorted(generator.canonical_source_rows(rows, name), key=_compact)
        for name, rows in (
            ("funds", funds),
            ("identity", identity),
            ("instruments", instruments),
        )
    }
    digest = _sha(_doc(sources))
    generation["source_snapshot_sha256"] = digest
    generation["instrument_evidence_digest"] = instrument_evidence_digest(
        policy["instrument_evidence"]
    )
    generation["policy_hash"] = policy_content_digest(policy)
    generation["generation_sha256"] = generation_metadata_digest(generation)
    snapshot = {
        "kind": replay.V2_SNAPSHOT_KIND,
        "generator_version": replay.V2_GENERATOR,
        "decision_at": generation["generated_at"],
        "source_query_version": replay.V2_QUERY_VERSION,
        "source_query_sha256": replay.V2_SOURCE_QUERY_SHA256,
        "source_snapshot_sha256": digest,
        "row_counts": {name: len(rows) for name, rows in sources.items()},
        "sources": sources,
    }
    sec = [
        sec_row(n, synced=STALE if n in STALE_IDS else FRESH)
        for n in range(1, 22)
        if n not in MISSING_IDS
    ]
    sec.append(sec_row(90, class_id="S000000090:T90", synced=FRESH))  # unrelated poison
    if sec_override is not None:
        sec = sec_override
    captured_sources = copy.deepcopy(sources)
    if drift:
        captured_sources["identity"][0]["figi"] = None
        captured_sources["identity"][0]["cusip_9"] = "ZZ0000000"
    v2_config = json.loads((CONFIGS / "nav_identity_audit_v2.json").read_bytes())
    cohort_rows = [
        {"instrument_id": str(uuid.UUID(int=n)), "strategy_label": LABEL}
        for n in range(1, 23)
    ]
    capture = {
        "kind": replay.V2_CAPTURE_KIND,
        "audit_contract_version": V2_ROUND5,
        "captured_at": CAPTURED.isoformat(),
        "source_query_sha256": replay.V2_SOURCE_QUERY_SHA256,
        "sources": captured_sources,
        "source_row_counts": {k: len(v) for k, v in captured_sources.items()},
        "source_snapshot_sha256": _sha(_doc(captured_sources)),
        "cohort": _v2_cohort(cohort_rows, v2_config["builder"]),
        "sec": _v2_sec_block(sec),
    }
    audit = {
        "audit_version": replay.V2_AUDIT_VERSION,
        "inputs": {
            "audit_contract_version": V2_ROUND5,
            "audit_contract_sha256": replay.V2_AUDIT_CONTRACTS[V2_ROUND5],
            "source_snapshot_sha256": digest,
            "live_source_snapshot_sha256": capture["source_snapshot_sha256"],
            "source_query_sha256": replay.V2_SOURCE_QUERY_SHA256,
            "sec_source_contract": "sec-company-tickers-mf-class-map-v1",
            "sec_query_contract_sha256": replay.SEC_PINS["query_contract_sha256"],
        },
        "details": {},
        "differences": {
            "sec": {
                "per_active_outcome": {
                    str(uuid.UUID(int=n)): "missing" for n in MISSING_IDS
                },
                "stale_matched": [str(uuid.UUID(int=n)) for n in STALE_IDS],
                "future_matched": [],
            }
        },
    }
    return {
        "policy": policy,
        "snapshot": snapshot,
        "capture": capture,
        "audit": audit,
        "v2_config": v2_config,
    }


def _raw(docs, *, mutate_audit=None, capture_bytes=_doc) -> tuple:
    """Serialize and hash-link the documents exactly as v2 would have.

    The dossier's mirrored capture/config values are derived from the
    (possibly tampered) capture; ``mutate_audit`` then tampers the dossier.
    """
    docs = copy.deepcopy(docs)
    policy_raw = generator.canonical_json(docs["policy"])
    snapshot = docs["snapshot"]
    snapshot["policy_artifact_sha256"] = _sha(policy_raw)
    snapshot_raw = generator.canonical_json(snapshot)
    capture = docs["capture"]
    capture_raw = capture_bytes(capture)
    config_raw = json.dumps(docs["v2_config"], indent=2).encode()
    audit = docs["audit"]
    cohort, sec = capture["cohort"], capture["sec"]
    uids = [row["instrument_id"] for row in cohort["rows"]]
    audit["inputs"].update(
        policy_artifact_sha256=_sha(policy_raw),
        source_snapshot_file_sha256=_sha(snapshot_raw),
        audit_config_sha256=_sha(config_raw),
        capture={
            "capture_bundle_sha256": _sha(capture_raw),
            "captured_at": capture["captured_at"],
            "source_snapshot_sha256": capture["source_snapshot_sha256"],
            "source_row_counts": capture["source_row_counts"],
            "cohort": {
                k: cohort[k]
                for k in (
                    "state",
                    "code",
                    "row_count",
                    "rows_sha256",
                    "query_sha256",
                    "parameters_sha256",
                )
            },
            "sec": {
                k: sec.get(k)
                for k in (
                    "state",
                    "code",
                    "source_contract",
                    "relation",
                    "timestamp_column",
                    "query_sha256",
                    "lineage_query_sha256",
                    "query_contract_sha256",
                    "row_count",
                    "rows_sha256",
                    "lineage",
                )
            },
        },
    )
    audit["details"]["A7"] = {
        "light_revision": docs["v2_config"]["builder"]["light_revision"],
        "cohort_query_sha256": cohort["query_sha256"],
        "cohort_parameters_sha256": cohort["parameters_sha256"],
        "cohort_rows_sha256": cohort["rows_sha256"],
        "cohort_rows": len(uids),
        "cohort_distinct": len(set(uids)),
        "duplicate_uuids": sum(1 for c in Counter(uids).values() if c > 1),
    }
    if mutate_audit is not None:
        mutate_audit(audit)
    audit_raw = generator.canonical_json(audit)
    return policy_raw, snapshot_raw, capture_raw, audit_raw, config_raw


EXPECTED = {
    "active_daily_delta": 5,
    "excluded": 5,
    "sec_first_failure": {"sec.missing": 2, "sec.stale": 3},
    "v2": {
        "active": 21,
        "inactive": 1,
        "structural_pre_claims_daily": 20,
        "unknown": 1,
    },
    "v3": {
        "active": 16,
        "inactive": 1,
        "structural_pre_claims_daily": 20,
        "unknown": 6,
    },
}


def _run(raw=None, expected=EXPECTED, config=None):
    return replay.replay(
        *(raw or _raw(_v2_docs())),
        config or _config(),
        json.dumps(expected).encode(),
    )


def _failed(summary) -> list[str]:
    return [name for name, ok in summary["checks"].items() if not ok]


# ── privacy oracle (independent of the tool's own sanitizer) ─────────────────
_UUID_TEXT = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_TICKER_TEXT = re.compile(r"(?<![0-9A-Za-z])T[0-9]+(?![0-9A-Za-z])")
_SERIES_OR_CLASS = re.compile(r"(?<![0-9A-Za-z])[CS][0-9]{9}(?![0-9A-Za-z])")
_ROW_KEYS = {"instrument_id", "class_id", "series_id", "ticker", "synced_at", "rows"}


def _assert_shareable(value, path="$"):
    """No identifier, ticker, series/class, source-row key or list anywhere."""
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _ROW_KEYS and key != "private", path
            _assert_shareable(key, f"{path}.<key>")
            _assert_shareable(item, f"{path}.{key}")
    elif isinstance(value, list):
        raise AssertionError(f"list at {path}")
    elif isinstance(value, str):
        for pattern in (_UUID_TEXT, _TICKER_TEXT, _SERIES_OR_CLASS):
            assert not pattern.search(value), (path, pattern.pattern)


def _assert_text_shareable(text: str):
    for pattern in (_UUID_TEXT, _TICKER_TEXT, _SERIES_OR_CLASS):
        assert not pattern.search(text), pattern.pattern
    assert "Traceback" not in text
    for line in filter(None, text.splitlines()):
        _assert_shareable(json.loads(line))


# ── reproduction ─────────────────────────────────────────────────────────────
def test_synthetic_replay_reproduces_the_exact_sec_delta():
    result = _run()
    assert len(result) == 2  # no private per-identifier return value at all
    passed, summary = result
    assert passed, _failed(summary)
    aggregates = summary["aggregates"]
    assert aggregates["excluded"] == aggregates["excluded_daily"] == 5
    assert aggregates["excluded_by_reason"] == {"sec.missing": 2, "sec.stale": 3}
    assert aggregates["active_daily_delta"] == 5
    assert (
        aggregates["v2"]["active_daily"] == 20
        and aggregates["v3"]["active_daily"] == 15
    )
    assert aggregates["semantic_mismatches"] == dict.fromkeys(replay.MISMATCH_FIELDS, 0)
    assert aggregates["a7"]["status"] == "PASS"
    excluded = {str(uuid.UUID(int=n)) for n in (*STALE_IDS, *MISSING_IDS)}
    assert summary["digests"]["excluded_set_sha256"] == uuid_set_digest(excluded)
    assert summary["provenance"]["cohort_rows"] == 22
    assert summary["provenance"]["v2_audit_contract_version"] == V2_ROUND5
    _assert_shareable(summary)


def test_committed_expected_counts_are_aggregates_only_and_conserved():
    expected = json.loads(replay.DEFAULT_EXPECTED.read_bytes())
    assert set(expected) == replay.EXPECTED_KEYS
    _assert_shareable(expected)
    v2, v3 = expected["v2"], expected["v3"]
    excluded = expected["excluded"]
    assert excluded == sum(expected["sec_first_failure"].values()) == 23
    assert expected["sec_first_failure"] == {"sec.missing": 10, "sec.stale": 13}
    assert expected["active_daily_delta"] == 23 <= excluded
    assert v3["active"] == v2["active"] - excluded == 2901
    assert v3["unknown"] == v2["unknown"] + excluded == 8197
    assert v3["inactive"] == v2["inactive"] == 8979
    assert (
        v3["structural_pre_claims_daily"] == v2["structural_pre_claims_daily"] == 5102
    )
    assert sum(v2[k] for k in ("active", "unknown", "inactive")) == sum(
        v3[k] for k in ("active", "unknown", "inactive")
    )


def test_replay_stops_when_the_expected_aggregates_differ():
    for field, value in (
        ("sec_first_failure", {"sec.missing": 3, "sec.stale": 2}),
        ("active_daily_delta", 4),
    ):
        wrong = copy.deepcopy(EXPECTED)
        wrong[field] = value
        passed, summary = _run(expected=wrong)
        assert not passed
        assert _failed(summary) == ["aggregates_equal_expected"]
        _assert_shareable(summary)


def test_replay_stops_when_the_excluded_set_differs_from_v2_differences():
    def mutate(audit):
        audit["differences"]["sec"]["stale_matched"].pop()
        audit["differences"]["sec"]["per_active_outcome"][str(uuid.UUID(int=5))] = (
            "partial"
        )

    passed, summary = _run(_raw(_v2_docs(), mutate_audit=mutate))
    assert not passed
    assert _failed(summary) == ["excluded_equals_v2_private_differences"]
    _assert_shareable(summary)


def test_replay_stops_on_a_systemic_sec_source_defect_at_tau():
    future = [sec_row(n, synced=TAU + dt.timedelta(seconds=1)) for n in range(1, 22)]
    passed, summary = _run(_raw(_v2_docs(sec_override=future)))
    assert not passed
    assert summary["codes"] == {
        "generator": "sec_source_future",
        "auditor": "sec_source_future",
    }
    _assert_shareable(summary)


# ── P2: generator vs independent auditor, per identifier ─────────────────────
def _mutate_generator_output(monkeypatch, mutate):
    """Mutate the v3 generator AFTER the synthetic v2 bundle is built."""
    original = generator.classify_catalog

    def wrapped(*args):
        evidence, counts, digests = original(*args)
        mutate(evidence, counts, digests)
        return evidence, counts, digests

    monkeypatch.setattr(generator, "classify_catalog", wrapped)


def _row(evidence, n):
    uid = str(uuid.UUID(int=n))
    return next(row for row in evidence if row["instrument_id"] == uid)


def test_status_kept_but_frequency_unknown_is_caught(monkeypatch):
    """ACTIVE stays ACTIVE (status/reasons/P unchanged) but loses daily."""

    def mutate(evidence, counts, digests):
        row = _row(evidence, 1)
        row["valuation_frequency"] = "unknown"
        row["return_basis_verified"] = False
        counts["active_daily"] -= 1
        counts["valuation_frequency"]["daily"] -= 1
        counts["valuation_frequency"]["unknown"] += 1
        digests["active_daily_set_sha256"] = uuid_set_digest(
            r["instrument_id"]
            for r in evidence
            if r["fund_status"] == "ACTIVE" and r["valuation_frequency"] == "daily"
        )

    raw = _raw(_v2_docs())
    _mutate_generator_output(monkeypatch, mutate)
    passed, summary = _run(raw)
    assert not passed
    assert set(_failed(summary)) == {
        "generator_equals_auditor",
        "active_daily_set_equal",
        "daily_loss_is_excluded_daily",
        "aggregates_equal_expected",
    }
    mismatches = summary["aggregates"]["semantic_mismatches"]
    assert mismatches == {
        "universe": 0,
        "status": 0,
        "valuation_frequency": 1,
        "verification_flags": 1,
        "first_failure": 0,
    }
    assert summary["aggregates"]["active_daily_delta"] == 6
    _assert_shareable(summary)


@pytest.mark.parametrize(
    "flag", ["identity_verified", "currency_verified", "return_basis_verified"]
)
def test_a_single_false_verification_flag_is_caught(monkeypatch, flag):
    raw = _raw(_v2_docs())
    _mutate_generator_output(
        monkeypatch,
        lambda evidence, counts, digests: _row(evidence, 2).update({flag: False}),
    )
    passed, summary = _run(raw)
    assert not passed
    assert _failed(summary) == ["generator_equals_auditor"]
    assert summary["aggregates"]["semantic_mismatches"]["verification_flags"] == 1


def test_histogram_preserving_reason_swap_is_caught(monkeypatch):
    """Two generator reasons swapped: statuses and histogram are unchanged."""
    original = generator._SecIndex.first_failure
    swap = {"T3": "sec.missing", "T6": "sec.stale"}

    def mutated(self, ticker, series, declared):
        code = original(self, ticker, series, declared)
        return swap.get(ticker, code)

    raw = _raw(_v2_docs())
    monkeypatch.setattr(generator._SecIndex, "first_failure", mutated)
    passed, summary = _run(raw)
    assert not passed
    assert _failed(summary) == ["generator_equals_auditor"]
    assert summary["aggregates"]["semantic_mismatches"]["first_failure"] == 2
    assert summary["aggregates"]["sec_first_failure"] == {
        "sec.missing": 2,
        "sec.stale": 3,
    }


def test_generator_digest_not_matching_its_evidence_is_caught(monkeypatch):
    raw = _raw(_v2_docs())
    _mutate_generator_output(
        monkeypatch,
        lambda evidence, counts, digests: digests.update(
            active_daily_set_sha256="0" * 64
        ),
    )
    passed, summary = _run(raw)
    assert _failed(summary) == ["generator_digests_match_evidence"]


def test_status_swap_between_generator_and_auditor_is_caught(monkeypatch):
    raw = _raw(_v2_docs())
    _mutate_generator_output(
        monkeypatch,
        lambda evidence, counts, digests: _row(evidence, 22).update(
            fund_status="ACTIVE"
        ),
    )
    passed, summary = _run(raw)
    assert not passed
    assert "generator_equals_auditor" in _failed(summary)
    assert "generator_reconstruction_consistent" in _failed(summary)
    assert summary["aggregates"]["semantic_mismatches"]["status"] == 1


# ── P3: provenance of the retained capture and the v2 -> v3 config mapping ───
def _tamper_capture(mutate):
    docs = _v2_docs()
    mutate(docs["capture"])
    return _raw(docs)


@pytest.mark.parametrize(
    "mutate,code",
    [
        (
            lambda c: c.update(kind="nav-identity-audit-capture-v2"),
            "v2_capture_contract_invalid",
        ),
        (
            lambda c: c.update(
                audit_contract_version="nav-identity-audit-contract-v2-round4"
            ),
            "v2_capture_contract_invalid",
        ),
        (
            lambda c: c.update(source_query_sha256="0" * 64),
            "v2_capture_contract_invalid",
        ),
        (lambda c: c.update(extra=None), "v2_capture_contract_invalid"),
        (
            lambda c: c["source_row_counts"].update(funds=1),
            "v2_capture_sources_invalid",
        ),
        # Cohort query / parameter pins derived from the v2 audit config.
        (
            lambda c: c["cohort"].update(query_sha256="0" * 64),
            "v2_capture_cohort_pin_mismatch",
        ),
        (
            lambda c: c["cohort"].update(parameters_sha256="0" * 64),
            "v2_capture_cohort_pin_mismatch",
        ),
        (lambda c: c["cohort"].update(state="invalid"), "v2_capture_cohort_invalid"),
        (lambda c: c["cohort"].update(code="x"), "v2_capture_cohort_invalid"),
        (lambda c: c["cohort"]["rows"].reverse(), "v2_capture_cohort_rows_invalid"),
        (
            lambda c: c["cohort"]["rows"][0].update(extra=1),
            "v2_capture_cohort_rows_invalid",
        ),
        (
            lambda c: c["cohort"].update(row_count=21),
            "v2_capture_cohort_digest_invalid",
        ),
        (
            lambda c: c["cohort"]["rows"][0].update(strategy_label="Other"),
            "v2_capture_cohort_digest_invalid",
        ),
        # SEC source pins: relation, timestamp column, queries, contract.
        (
            lambda c: c["sec"].update(relation="public.sec_fund_classes"),
            "v2_capture_sec_pin_invalid",
        ),
        (
            lambda c: c["sec"].update(timestamp_column="fetched_at"),
            "v2_capture_sec_pin_invalid",
        ),
        (
            lambda c: c["sec"].update(query_sha256="0" * 64),
            "v2_capture_sec_pin_invalid",
        ),
        (
            lambda c: c["sec"].update(lineage_query_sha256="0" * 64),
            "v2_capture_sec_pin_invalid",
        ),
        (
            lambda c: c["sec"].update(query_contract_sha256="0" * 64),
            "v2_capture_sec_pin_invalid",
        ),
        (
            lambda c: c["sec"].update(source_contract="other"),
            "v2_capture_sec_pin_invalid",
        ),
        (lambda c: c["sec"].update(code=None), "v2_capture_sec_invalid"),
        (lambda c: c["sec"].update(state="unavailable"), "v2_capture_sec_invalid"),
        (lambda c: c["sec"]["rows"].reverse(), "v2_capture_sec_row_invalid"),
        (lambda c: c["sec"].update(row_count=1), "v2_capture_sec_digest_invalid"),
        (
            lambda c: c["sec"]["rows"][0].update(ticker="ZZZ"),
            "v2_capture_sec_digest_invalid",
        ),
        (
            lambda c: c["sec"]["lineage"].update(row_count=1),
            "v2_capture_sec_lineage_invalid",
        ),
        (
            lambda c: c["sec"]["lineage"].update(max_synced_at=STALE.isoformat()),
            "v2_capture_sec_lineage_invalid",
        ),
        (
            lambda c: c.update(captured_at=(TAU - dt.timedelta(1)).isoformat()),
            "v2_capture_precedes_generation",
        ),
    ],
)
def test_retained_capture_provenance_is_validated(mutate, code):
    with pytest.raises(replay.ReplayBlocked, match=f"^{code}$"):
        _run(_tamper_capture(mutate))


def test_capture_bytes_must_be_the_v2_canonical_document():
    raw = _raw(_v2_docs(), capture_bytes=lambda c: json.dumps(c, indent=1).encode())
    with pytest.raises(replay.ReplayBlocked, match="^v2_capture_not_canonical$"):
        _run(raw)


def test_drifted_v2_capture_is_blocked():
    with pytest.raises(replay.ReplayBlocked, match="^v2_capture_drifted$"):
        _run(_raw(_v2_docs(drift=True)))


@pytest.mark.parametrize(
    "mutate,code",
    [
        (
            lambda a: a["inputs"].update(audit_config_sha256="0" * 64),
            "v2_audit_config_mismatch",
        ),
        (
            lambda a: a["inputs"].update(audit_contract_sha256="0" * 64),
            "v2_audit_contract_invalid",
        ),
        (
            lambda a: a["inputs"].update(
                audit_contract_version="nav-identity-audit-contract-v2"
            ),
            "v2_audit_contract_invalid",
        ),
        (
            lambda a: a.update(audit_version="nav-identity-audit-v3"),
            "v2_audit_contract_invalid",
        ),
        (
            lambda a: a["inputs"].update(policy_artifact_sha256="0" * 64),
            "v2_audit_link_invalid",
        ),
        (
            lambda a: a["inputs"].update(live_source_snapshot_sha256="0" * 64),
            "v2_audit_link_invalid",
        ),
        (
            lambda a: a["inputs"].update(sec_query_contract_sha256="0" * 64),
            "v2_audit_link_invalid",
        ),
        (
            lambda a: a["inputs"]["capture"].update(capture_bundle_sha256="0" * 64),
            "v2_audit_capture_mismatch",
        ),
        (
            lambda a: a["inputs"]["capture"]["cohort"].update(
                parameters_sha256="0" * 64
            ),
            "v2_audit_capture_mismatch",
        ),
        (
            lambda a: a["inputs"]["capture"]["sec"].update(relation="public.other"),
            "v2_audit_capture_mismatch",
        ),
        (
            lambda a: a["inputs"]["capture"]["sec"]["lineage"].update(row_count=0),
            "v2_audit_capture_mismatch",
        ),
        (
            lambda a: a["details"]["A7"].update(light_revision="0" * 40),
            "v2_audit_cohort_mismatch",
        ),
        (
            lambda a: a["details"]["A7"].update(cohort_distinct=21),
            "v2_audit_cohort_mismatch",
        ),
        (
            lambda a: a["details"]["A7"].update(duplicate_uuids=1),
            "v2_audit_cohort_mismatch",
        ),
        (lambda a: a["details"].pop("A7"), "v2_audit_cohort_mismatch"),
    ],
)
def test_dossier_mirror_of_capture_and_config_is_validated(mutate, code):
    with pytest.raises(replay.ReplayBlocked, match=f"^{code}$"):
        _run(_raw(_v2_docs(), mutate_audit=mutate))


def test_v2_config_is_the_one_the_dossier_embeds_and_pins_the_cohort():
    docs = _v2_docs()
    docs["v2_config"]["builder"]["cohort_parameters"] = {"limit": 1}
    # Consistently rehashed config: the retained cohort was not selected by it.
    with pytest.raises(replay.ReplayBlocked, match="^v2_capture_cohort_pin_mismatch$"):
        _run(_raw(docs))
    docs = _v2_docs()
    docs["v2_config"]["audit_config_version"] = "nav-identity-audit-config-v3"
    with pytest.raises(replay.ReplayBlocked, match="^v2_audit_config_invalid$"):
        _run(_raw(docs))
    docs = _v2_docs()
    docs["v2_config"]["sec"]["relation"] = "public.sec_fund_classes"
    with pytest.raises(replay.ReplayBlocked, match="^v2_audit_config_sec_invalid$"):
        _run(_raw(docs))


def test_cohort_multiplicity_is_bound_to_the_dossier_and_judged_by_a7():
    docs = _v2_docs()
    cohort = docs["capture"]["cohort"]
    rows = sorted([*cohort["rows"], dict(cohort["rows"][0])], key=_compact)
    cohort.update(rows=rows, row_count=len(rows), rows_sha256=_sha(_doc(rows)))
    # Dossier that says "no duplicates" for the duplicated cohort: blocked.
    with pytest.raises(replay.ReplayBlocked, match="^v2_audit_cohort_mismatch$"):
        _run(
            _raw(
                docs,
                mutate_audit=lambda a: a["details"]["A7"].update(duplicate_uuids=0),
            )
        )
    # Consistent provenance: A7 judges the duplicate (cohort_unique) -> STOP.
    passed, summary = _run(_raw(docs))
    assert not passed
    assert _failed(summary) == ["a7_stage1_minimums_preserved"]
    assert summary["provenance"]["cohort_rows"] == 23
    assert summary["provenance"]["cohort_distinct"] == 22


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["builder"].update(
            cohort_parameters={**c["builder"].get("cohort_parameters", {}), "x": 1}
        ),
        lambda c: c["builder"].update(cohort_query=c["builder"]["cohort_query"] + " "),
        lambda c: c["builder"].update(light_revision="0" * 40),
    ],
    ids=["parameters", "query", "light_revision"],
)
def test_replay_config_must_select_the_retained_cohort(mutate):
    with pytest.raises(replay.ReplayBlocked, match="^replay_config_builder_mismatch$"):
        _run(config=_config(mutate))


def test_replay_config_sec_contract_and_generator_query_must_match(monkeypatch):
    with pytest.raises(replay.ReplayBlocked, match="^replay_config_invalid:"):
        _run(config=_config(lambda c: c["sec"].update(relation="public.other")))
    monkeypatch.setattr(
        replay.nav_policy, "SEC_QUERY", replay.nav_policy.SEC_QUERY + " "
    )
    with pytest.raises(replay.ReplayBlocked, match="^replay_config_sec_mismatch$"):
        _run()


@pytest.mark.parametrize(
    "target,mutate,code",
    [
        ("policy", lambda d: d.update(policy_id="x"), "v2_policy_digest_invalid"),
        (
            "policy",
            lambda d: d["generation"].update(active_daily_set_sha256="0" * 64),
            "v2_policy_digest_invalid",
        ),
        (
            "policy",
            lambda d: d.update(generator_version="fund-nav-policy-generator-v3"),
            "v2_policy_contract_invalid",
        ),
        (
            "snapshot",
            lambda d: d["sources"]["identity"][0].update(ticker="ZZZ"),
            "v2_snapshot_link_invalid",
        ),
    ],
)
def test_replay_blocks_broken_v2_policy_links(target, mutate, code):
    docs = _v2_docs()
    mutate(docs[target])
    with pytest.raises(replay.ReplayBlocked, match=f"^{code}$"):
        _run(_raw(docs))


# ── P1: closed, sanitized output on pass, stop and block ─────────────────────
def test_public_schema_rejects_any_identifier_or_row(monkeypatch):
    _passed, summary = _run()
    document = {
        "kind": replay.EVIDENCE_KIND,
        "generator_version": "g",
        "audit_contract_version": "a",
        "audit_contract_sha256": "0" * 64,
        "inputs": {
            **{key: "0" * 64 for key in replay.INPUT_KEYS if key != "code_sha256"},
            "code_sha256": dict.fromkeys(replay.CODE_FILES, "0" * 64),
        },
        **summary,
    }
    replay.validate_public(document)
    for inject in (
        lambda d: d["aggregates"]["excluded_by_reason"].update(
            {str(uuid.UUID(int=3)): 1}
        ),
        lambda d: d["codes"].update(generator=str(uuid.UUID(int=3))),
        lambda d: d["provenance"].update(light_revision="S000000003"),
        lambda d: d.update(private={}),
        lambda d: d["aggregates"].update(rows=[]),
        lambda d: d["digests"].update(excluded_set_sha256=["x"]),
        lambda d: d["checks"].update(extra=True),
    ):
        tampered = copy.deepcopy(document)
        inject(tampered)
        with pytest.raises(replay.ReplayBlocked, match="replay_output_not_sanitized"):
            replay.validate_public(tampered)


def _cli(tmp_path, raw, *, expected=EXPECTED, config=None):
    source = tmp_path / "retained-v2"
    source.mkdir(mode=0o700, exist_ok=True)
    names = ("policy", "snapshot", "capture", "audit")
    paths = {}
    for name, content in zip(names, raw[:4]):
        paths[name] = source / f"{name}.json"
        paths[name].write_bytes(content)
    (tmp_path / "v2-config.json").write_bytes(raw[4])
    (tmp_path / "config.json").write_bytes(config or _config())
    (tmp_path / "expected.json").write_text(json.dumps(expected))
    custody = tmp_path / "replay-v3"
    custody.mkdir(mode=0o700, exist_ok=True)
    output = custody / "replay-v3.json"

    def argv(policy_pin=None):
        args = []
        for flag, name in zip(
            ("v2-policy", "v2-source-snapshot", "v2-capture", "v2-audit"), names
        ):
            pin = _sha(paths[name].read_bytes())
            if name == "policy" and policy_pin is not None:
                pin = policy_pin
            args += [f"--{flag}-file", str(paths[name]), f"--{flag}-sha256", pin]
        return [
            *args,
            "--v2-audit-config",
            str(tmp_path / "v2-config.json"),
            "--audit-config",
            str(tmp_path / "config.json"),
            "--expected",
            str(tmp_path / "expected.json"),
            "--custody-root",
            str(custody),
            "--output",
            str(output),
        ]

    return paths, output, argv


@pytest.mark.skipif(os.name != "posix", reason="private custody is POSIX-only")
def test_replay_cli_pass_writes_only_sanitized_evidence_once(tmp_path, capsys):
    raw = _raw(_v2_docs())
    paths, output, argv = _cli(tmp_path, raw)
    before = {name: path.read_bytes() for name, path in paths.items()}
    assert replay.main(argv(policy_pin="0" * 64)) == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "status": "blocked",
        "code": "v2_policy_sha256_mismatch",
    }
    assert captured.err == "" and not output.exists()
    assert replay.main(argv()) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    _assert_text_shareable(captured.out)
    report = json.loads(captured.out)
    assert report["status"] == "pass"
    assert report["evidence_sha256"] == _sha(output.read_bytes())
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    evidence = json.loads(output.read_bytes())
    _assert_shareable(evidence)
    assert set(evidence) == replay.PUBLIC_KEYS
    assert evidence["kind"] == replay.EVIDENCE_KIND
    assert evidence["inputs"]["v2_audit_config_sha256"] == _sha(raw[4])
    assert set(evidence["inputs"]["code_sha256"]) == set(replay.CODE_FILES)
    assert {k: v for k, v in report.items() if k != "evidence_sha256"} == evidence
    # No overwrite; the retained v2 inputs are never modified.
    assert replay.main(argv()) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "artifact_already_exists"
    assert {name: path.read_bytes() for name, path in paths.items()} == before


@pytest.mark.skipif(os.name != "posix", reason="private custody is POSIX-only")
@pytest.mark.parametrize("case", ["wrong_counts", "wrong_exclusions"])
def test_replay_cli_stop_is_exit_3_and_equally_sanitized(tmp_path, capsys, case):
    if case == "wrong_counts":
        raw = _raw(_v2_docs())
        expected = {**EXPECTED, "excluded": 6}
    else:
        raw = _raw(
            _v2_docs(),
            mutate_audit=lambda a: a["differences"]["sec"]["per_active_outcome"].update(
                {str(uuid.UUID(int=8)): "missing"}
            ),
        )
        expected = EXPECTED
    _paths, output, argv = _cli(tmp_path, raw, expected=expected)
    assert replay.main(argv()) == 3
    captured = capsys.readouterr()
    assert captured.err == ""
    _assert_text_shareable(captured.out)
    evidence = json.loads(output.read_bytes())
    _assert_shareable(evidence)
    assert evidence["status"] == "stop"
    assert not all(evidence["checks"].values())


@pytest.mark.skipif(os.name != "posix", reason="private custody is POSIX-only")
def test_replay_cli_block_prints_a_static_code_only(tmp_path, capsys):
    docs = _v2_docs()
    docs["capture"]["sec"]["relation"] = "public.sec_fund_classes"
    _paths, output, argv = _cli(tmp_path, _raw(docs))
    assert replay.main(argv()) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    _assert_text_shareable(captured.out)
    assert json.loads(captured.out) == {
        "status": "blocked",
        "code": "v2_capture_sec_pin_invalid",
    }
    assert not output.exists()


@pytest.mark.skipif(os.name != "posix", reason="private custody is POSIX-only")
def test_replay_cli_unexpected_error_has_no_traceback(tmp_path, capsys, monkeypatch):
    def explode(*args, **kwargs):
        raise RuntimeError(str(uuid.UUID(int=3)))

    monkeypatch.setattr(replay, "replay", explode)
    _paths, output, argv = _cli(tmp_path, _raw(_v2_docs()))
    assert replay.main(argv()) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    _assert_text_shareable(captured.out)
    assert json.loads(captured.out) == {
        "status": "blocked",
        "code": "replay_input_invalid",
        "reason": "RuntimeError",
    }
    assert not output.exists()
