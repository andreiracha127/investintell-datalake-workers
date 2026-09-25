"""Offline XNYS policy construction, identity v2 lifecycle and artifact custody."""

from __future__ import annotations

import copy
import datetime as dt
import errno
import hashlib
import importlib.metadata
import json
import os
import random
import stat
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from scripts import verify_fund_nav_identity_v2 as verifier
from src.workers._nav_policy import (
    CATALOG_EVIDENCE_REFERENCE,
    IDENTITY_FAILURE_CODES,
    SOURCE_QUERY_SHA256,
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
    uuid_set_digest,
)
from tests._nav_identity_fixtures import (
    catalog,
    entity,
    only,
    oracle_cusip_valid,
    oracle_figi_valid,
    oracle_isin_valid,
    synthetic_cusip,
    synthetic_figi,
    synthetic_isin,
    wrong_check,
)

START = dt.date(2024, 1, 1)
END = dt.date(2027, 12, 31)
OBSERVED = dt.datetime(2026, 9, 23, 10, 0, tzinfo=dt.timezone.utc)
V1_REFERENCE = (
    "nav-current-catalog-snapshot-v1:public.instruments_universe+public.funds_v:"
    "w1-tiingo-adjusted-daily-v1:current_only"
)


def _classify(*entities, **extra):
    instruments, funds, identity = catalog(*entities, **extra)
    evidence, counts, digests = generator.classify_catalog(
        instruments, funds, identity, OBSERVED
    )
    return (
        {uuid.UUID(row["instrument_id"]).int: row for row in evidence},
        counts,
        digests,
    )


def _reason(subject, *others, **extra) -> str | None:
    """First-failure code of the subject (control ACTIVE entities must stay ACTIVE)."""
    rows, counts, _ = _classify(subject, *others, **extra)
    number = uuid.UUID(
        str(subject[0]["instrument_id"] if subject[0] else subject[1]["instrument_id"])
    ).int
    status = rows[number]["fund_status"]
    if status == "ACTIVE":
        assert counts["identity_first_failure"] == {}
        return None
    assert status == "UNKNOWN"
    (code,) = counts["identity_first_failure"]
    return code


def _policy(
    *entities, policy_version="2026-09-24.2", observed=OBSERVED, calendar=None, **extra
):
    instruments, funds, identity = catalog(*entities, **extra)
    calendar = calendar or generator.build_calendar(START, END)
    return generator.build_policy(
        calendar, instruments, funds, identity, observed, "policy-demo", policy_version
    ), (instruments, funds, identity)


def _rehash(policy: dict) -> dict:
    """Recompute every digest so only the semantic tamper remains."""
    generation = policy["generation"]
    generation["instrument_evidence_digest"] = instrument_evidence_digest(
        policy["instrument_evidence"]
    )
    generation["policy_hash"] = policy_content_digest(policy)
    generation["generation_sha256"] = generation_metadata_digest(generation)
    return policy


@pytest.fixture(scope="module")
def calendar() -> dict:
    return generator.build_calendar(START, END)


def test_xnys_pin_holiday_early_close_dst_and_final_deadline(calendar):
    import exchange_calendars as xcals

    assert xcals.__version__ == "4.13.2"
    assert importlib.metadata.version("exchange_calendars") == "4.13.2"
    assert "exchange_calendars==4.13.2" in (
        generator.ROOT / "requirements.txt"
    ).read_text(encoding="utf-8")
    assert calendar["calendar_source"] == "exchange_calendars/XNYS"
    assert len(calendar["sessions"]) >= 401
    assert calendar["coverage_start"] == "2024-01-02"  # Jan 1 exchange holiday
    assert calendar["coverage_end"] == "2027-12-31"
    lookup = {row["session_date"]: row for row in calendar["sessions"]}
    assert "2024-07-04" not in lookup
    early = lookup["2024-11-29"]
    assert (
        dt.datetime.fromisoformat(early["valuation_close_at"])
        .astimezone(generator.NY)
        .hour
        == 13
    )
    assert (
        dt.datetime.fromisoformat(early["nav_due_at"])
        .astimezone(generator.NY)
        .strftime("%H:%M")
        == "18:05"
    )
    for day, expected_utc in (
        ("2024-03-08", "23:05"),
        ("2024-03-11", "22:05"),
        ("2024-11-01", "22:05"),
        ("2024-11-04", "23:05"),
    ):
        due = dt.datetime.fromisoformat(lookup[day]["nav_due_at"])
        assert due.strftime("%H:%M") == expected_utc
        assert due.astimezone(generator.NY).date() == dt.date.fromisoformat(day)
    assert calendar["valid_through"] == calendar["sessions"][-1]["nav_due_at"]
    assert generator.verify_artifact(calendar)["mode"] == "calendar"


def test_calendar_short_window_and_package_mismatch_fail(monkeypatch):
    with pytest.raises(
        generator.PolicyGenerationError, match="calendar_window_too_short"
    ):
        generator.build_calendar(dt.date(2026, 1, 1), dt.date(2026, 3, 31))
    monkeypatch.setattr(generator.importlib.metadata, "version", lambda name: "4.14.0")
    with pytest.raises(
        generator.PolicyGenerationError, match="calendar_package_version_mismatch"
    ):
        generator.build_calendar(START, END)


@pytest.mark.parametrize(
    "mutation",
    [
        "package",
        "version",
        "digest",
        "session_drop",
        "duplicate",
        "deadline",
        "early_close",
        "expiration",
    ],
)
def test_calendar_tamper_is_rejected(calendar, mutation):
    policy = copy.deepcopy(calendar)
    if mutation == "package":
        policy["source_reference"] = policy["source_reference"].replace(
            "4.13.2", "4.14.0"
        )
    elif mutation == "version":
        policy["calendar_version"] = "floating-latest"
    elif mutation == "digest":
        policy["calendar_digest"] = "0" * 64
    elif mutation == "session_drop":
        policy["sessions"].pop(200)
    elif mutation == "duplicate":
        policy["sessions"].insert(200, policy["sessions"][199])
    elif mutation == "deadline":
        policy["sessions"][10]["nav_due_at"] = policy["sessions"][10][
            "valuation_close_at"
        ]
    elif mutation == "early_close":
        next(row for row in policy["sessions"] if row["session_date"] == "2024-11-29")[
            "valuation_close_at"
        ] = "2024-11-29T21:00:00+00:00"
    elif mutation == "expiration":
        policy["valid_through"] = "2027-12-31T23:59:00+00:00"
    with pytest.raises((generator.PolicyGenerationError, ValueError)):
        generator.verify_artifact(policy)


# ── checksums: generator, independent verifier and oracle agree ─────────────
def test_synthetic_claim_vectors_pass_and_single_digit_mutations_fail():
    zero_check = next(n for n in range(1, 500) if synthetic_cusip(n).endswith("0"))
    lettered = synthetic_cusip(0, body="ZZ*@#A1B")
    for cusip in (synthetic_cusip(1), synthetic_cusip(zero_check), lettered):
        assert generator.cusip_problem(cusip) is None
        assert verifier.cusip_status(cusip) is None
        assert generator.cusip_problem(wrong_check(cusip)) == "checksum"
        assert verifier.cusip_status(wrong_check(cusip)) == "checksum"
    isin_zero = next(n for n in range(1, 500) if synthetic_isin(n).endswith("0"))
    for isin in (synthetic_isin(1), synthetic_isin(isin_zero), synthetic_isin(7)):
        assert (
            generator.isin_problem(isin) is None and verifier.isin_status(isin) is None
        )
        assert generator.isin_problem(wrong_check(isin)) == "checksum"
        assert verifier.isin_status(wrong_check(isin)) == "checksum"
    for figi in (synthetic_figi(1), synthetic_figi(42)):
        assert (
            generator.figi_problem(figi) is None and verifier.figi_status(figi) is None
        )
        assert generator.figi_problem(wrong_check(figi)) == "checksum"


def test_isin_embedded_cusip_checksum_is_required_even_when_luhn_passes():
    bad_cusip = wrong_check(synthetic_cusip(3))
    isin = synthetic_isin(3, cusip=bad_cusip)  # Luhn computed over the bad body
    assert generator.isin_problem(isin) == "checksum"
    assert verifier.isin_status(isin) == "checksum"
    # An ISIN never embeds CUSIP specials (alphanumeric only).
    special = "US" + synthetic_cusip(0, body="ZZ*@#A1B")
    assert generator.isin_problem(special + "0") == "invalid_format"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("GB00ZZ0000017", "unsupported_prefix"),
        ("CA" + synthetic_cusip(1) + "0", "unsupported_prefix"),
        ("US123", "invalid_format"),
        ("US ZZ000001X9", "invalid_format"),
        ("US" + synthetic_cusip(1)[:8] + "\uff11" + "0", "invalid_format"),
    ],
)
def test_isin_format_prefix_unicode_and_whitespace(value, expected):
    assert generator.isin_problem(value) == expected
    assert verifier.isin_status(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("ZAG00000001" + "0", "invalid_format"),  # vowel
        ("ZZX00000001" + "0", "invalid_format"),  # third char must be G
        ("ZZG0000000A" + "0", "invalid_format"),  # vowel in body
        ("ZZG00000001", "invalid_format"),  # length
    ],
)
def test_figi_format(value, expected):
    assert generator.figi_problem(value) == expected
    assert verifier.figi_status(value) == expected


@pytest.mark.parametrize("prefix", ["BS", "BM", "GG", "GB", "GH", "KY", "VG"])
def test_figi_reserved_prefixes_rejected(prefix):
    from tests._nav_identity_fixtures import oracle_figi_check

    body = f"{prefix}G00000001"
    figi = body + oracle_figi_check(body)
    assert generator.figi_problem(figi) == "reserved_prefix"
    assert verifier.figi_status(figi) == "reserved_prefix"


def test_random_candidates_agree_with_independent_oracle():
    rng = random.Random(20260924)
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ*@#"
    for _ in range(4000):
        cusip = "".join(rng.choice(alphabet) for _ in range(8)) + rng.choice(
            "0123456789"
        )
        expected = oracle_cusip_valid(cusip)
        assert (generator.cusip_problem(cusip) is None) == expected
        assert (verifier.cusip_status(cusip) is None) == expected
        isin = (
            "US"
            + "".join(rng.choice(alphabet[:36]) for _ in range(9))
            + rng.choice("0123456789")
        )
        expected = oracle_isin_valid(isin)
        assert (generator.isin_problem(isin) is None) == expected
        assert (verifier.isin_status(isin) is None) == expected
        figi = "".join(rng.choice("BCDFGHJKLMNPQRSTVWXYZ") for _ in range(2)) + "G"
        figi += "".join(rng.choice("BCDFGHJKLMNPQRSTVWXYZ0123456789") for _ in range(8))
        figi += rng.choice("0123456789")
        expected = oracle_figi_valid(figi)
        assert (generator.figi_problem(figi) is None) == expected
        assert (verifier.figi_status(figi) is None) == expected
    # Valid synthetic values generated by the oracle are accepted by both.
    for n in range(300):
        assert generator.isin_problem(synthetic_isin(n)) is None
        assert verifier.isin_status(synthetic_isin(n)) is None
        assert generator.figi_problem(synthetic_figi(n)) is None


# ── lifecycle matrix (synthetic identities only) ─────────────────────────────
CONTROL = entity(900)


@pytest.mark.parametrize(
    "subject",
    [
        entity(1, iu_isin=None, reg_isin=None, cusip=None),  # claims absent
        entity(1, reg_isin=None, cusip=None),  # IU-only ISIN
        entity(1, iu_isin=None),  # registry-only with coherent funds_v
        entity(1),  # both equal
        entity(1, figi=synthetic_figi(1)),
        entity(1, iu_isin=None, reg_isin=None),  # explicit CUSIP only
        entity(
            1, iu_isin=None, reg_isin=None, cusip=synthetic_cusip(0, body="ZZ*@#A1B")
        ),
        entity(1, iu_isin=synthetic_isin(1).lower(), ticker="t1 "),  # normalization
    ],
)
def test_valid_or_absent_claims_are_active(subject):
    assert _reason(subject, CONTROL) is None


@pytest.mark.parametrize(
    "subject,code",
    [
        (entity(1, iu_isin="GB00ZZ0000017"), "isin.unsupported_prefix"),
        (entity(1, iu_isin="US123", reg_isin=None, cusip=None), "isin.invalid_format"),
        (entity(1, iu_isin=wrong_check(synthetic_isin(1))), "isin.checksum"),
        (entity(1, iu_isin=synthetic_isin(5)), "isin.mismatch"),
        (
            entity(1, iu_isin=None, reg_isin=None, cusip="ZZ00001"),
            "cusip.invalid_format",
        ),
        (
            entity(
                1, iu_isin=None, reg_isin=None, cusip=wrong_check(synthetic_cusip(1))
            ),
            "cusip.checksum",
        ),
        (
            entity(1, cusip=synthetic_cusip(7)),
            "cusip.mismatch",
        ),  # ISIN→CUSIP contradiction
        (
            entity(
                1, iu_isin=synthetic_isin(8), reg_isin=None, cusip=synthetic_cusip(1)
            ),
            "cusip.mismatch",
        ),
        (entity(1, figi="ZAG000000010"), "figi.invalid_format"),
        (entity(1, figi=wrong_check(synthetic_figi(1))), "figi.checksum"),
        (entity(1, status="candidate"), "registry.status_not_canonical"),
        (entity(1, status="unresolved"), "registry.status_not_canonical"),
        (entity(1, status=None), "registry.status_not_canonical"),
        (entity(1, status="Canonical"), "registry.status_not_canonical"),
        (entity(1, conflict=None), "registry.conflict_state_not_empty"),
        (entity(1, conflict=[]), "registry.conflict_state_not_empty"),
        (entity(1, conflict="{}"), "registry.conflict_state_not_empty"),
        (entity(1, conflict={"ticker": "other"}), "registry.conflict_state_not_empty"),
        (entity(1, instrument_type="equity"), "instrument_type.not_fund"),
        (entity(1, iu_currency="EUR"), "currency.iu_not_usd"),
        (entity(1, iu_currency="usd"), "currency.iu_not_usd"),
        (entity(1, fv_currency="EUR"), "currency.funds_v_not_usd"),
        (entity(1, fund_type="closed_end"), "fund_type.unsupported"),
        (entity(1, fund_type="ETF"), "fund_type.unsupported"),
        (entity(1, active=None), "activity.unknown"),
        (entity(1, active=False), "activity.not_active"),
        (entity(1, series=None), "series.missing"),
    ],
)
def test_invalid_claims_and_gates_first_failure(subject, code):
    assert _reason(subject, CONTROL) == code


def test_ticker_missing_and_mismatch():
    subject = entity(1)
    subject[2]["ticker"] = None
    subject[1]["ticker"] = None
    assert _reason(subject, CONTROL) == "ticker.missing"
    subject = entity(1)
    subject[0]["ticker"] = "OTHER"
    assert _reason(subject, CONTROL) == "ticker.mismatch"


def test_isin_family_precedence_is_rank_not_source_order():
    # IU ISIN has a bad checksum, registry ISIN has an unsupported prefix.
    subject = entity(
        1, iu_isin=wrong_check(synthetic_isin(1)), reg_isin="GB00ZZ0000017", cusip=None
    )
    assert _reason(subject, CONTROL) == "isin.unsupported_prefix"


def test_mmf_active_not_daily_and_unknown_never_verified():
    rows, counts, digests = _classify(
        entity(1, fund_type="mmf"), entity(2, active=None), CONTROL
    )
    assert (
        rows[1]["fund_status"] == "ACTIVE"
        and rows[1]["valuation_frequency"] == "unknown"
    )
    assert (
        rows[1]["return_basis_verified"] is False
        and rows[1]["identity_verified"] is True
    )
    assert rows[2]["fund_status"] == "UNKNOWN"
    assert not any(
        rows[2][f]
        for f in ("identity_verified", "return_basis_verified", "currency_verified")
    )
    assert counts["active"] == 2 and counts["active_daily"] == 1
    assert digests["active_daily_set_sha256"] == uuid_set_digest(
        [str(uuid.UUID(int=900))]
    )


def test_inactive_rule_unchanged_and_missing_registry_neither_promotes_nor_redefines():
    inactive = only(entity(3, active=False), fund=False, registry=False)
    duplicate_inactive = only(entity(4, active=False), fund=False, registry=False)
    rows, counts, _ = _classify(
        inactive, duplicate_inactive, CONTROL, iu=[duplicate_inactive[0]]
    )
    assert rows[3]["fund_status"] == "INACTIVE"
    assert rows[4]["fund_status"] == "UNKNOWN"
    assert counts["inactive_reason"] == {"inactive_without_funds_v": 1}
    assert counts["identity_first_failure"] == {"cardinality.iu_duplicate": 1}
    # INACTIVE does not require a registry row; ACTIVE does.
    assert (
        _reason(only(entity(5), registry=False), CONTROL)
        == "cardinality.registry_missing"
    )


@pytest.mark.parametrize(
    "source,code",
    [
        ("iu", "cardinality.iu_duplicate"),
        ("funds", "cardinality.funds_v_duplicate"),
        ("registry", "cardinality.registry_duplicate"),
    ],
)
def test_identical_duplicate_rows_fail_cardinality(source, code):
    subject = entity(1)
    index = {"iu": 0, "funds": 1, "registry": 2}[source]
    assert _reason(subject, CONTROL, **{source: [subject[index]]}) == code


def test_missing_sources_are_cardinality_not_projection_abort():
    assert _reason(only(entity(1), iu=False), CONTROL) == "cardinality.iu_missing"
    assert (
        _reason(only(entity(1), registry=False), CONTROL)
        == "cardinality.registry_missing"
    )
    subject = entity(1)
    diverging = dict(subject[2], ticker="ELSEWHERE")
    # Two registry rows: no single comparable projection, so cardinality, no abort.
    assert (
        _reason(subject, CONTROL, registry=[diverging])
        == "cardinality.registry_duplicate"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("series_id", "S-OTHER"),
        ("ticker", "T-OTHER"),
        ("isin", synthetic_isin(77)),
        ("cusip", synthetic_cusip(77)),
        ("isin", None),  # NULL versus value also diverges
        ("cusip", None),
    ],
)
def test_funds_v_projection_divergence_aborts_entire_artifact(field, value):
    subject = entity(1)
    subject[1][field] = value
    with pytest.raises(
        generator.PolicyGenerationError, match="catalog_identity_projection_mismatch"
    ):
        _classify(subject, CONTROL)


def test_projection_abort_covers_duplicate_funds_rows():
    subject = entity(1)
    assert (
        _reason(subject, CONTROL, funds=[subject[1]]) == "cardinality.funds_v_duplicate"
    )
    with pytest.raises(
        generator.PolicyGenerationError, match="catalog_identity_projection_mismatch"
    ):
        _classify(subject, CONTROL, funds=[dict(subject[1], ticker="T-OTHER")])


def test_global_collisions_block_every_owner_and_all_asset_types():
    # Two candidate funds claiming one ticker: both UNKNOWN.
    a, b = entity(1, ticker="DUP"), entity(2, ticker="DUP")
    _, counts, _ = _classify(a, b, CONTROL)
    assert counts["identity_first_failure"] == {"ticker.global_conflict": 2}
    # A non-fund IU row, a registry-only row and a candidate registry row count.
    equity = dict(entity(50)[0], instrument_type="equity", ticker="T1")
    assert _reason(entity(1), CONTROL, iu=[equity]) == "ticker.global_conflict"
    registry_only = dict(entity(60)[2], ticker="T1")
    assert (
        _reason(entity(1), CONTROL, registry=[registry_only])
        == "ticker.global_conflict"
    )
    candidate = dict(entity(61)[2], isin=synthetic_isin(1), cusip_9=None, status=None)
    assert _reason(entity(1), CONTROL, registry=[candidate]) == "isin.global_conflict"
    inactive = dict(entity(62, active=False)[0], isin=synthetic_isin(1))
    rows, _, _ = _classify(entity(1), CONTROL, iu=[inactive])
    assert rows[1]["fund_status"] == "UNKNOWN" and rows[62]["fund_status"] == "INACTIVE"
    # A derived CUSIP from an ISIN with a failing checksum still owns the CUSIP.
    bad_isin = dict(
        entity(63)[0], instrument_type="equity", isin=wrong_check(synthetic_isin(1))
    )
    assert _reason(entity(1), CONTROL, iu=[bad_isin]) == "cusip.global_conflict"
    figi_owner = dict(
        entity(64)[2], ticker="T64", figi=synthetic_figi(1), isin=None, cusip_9=None
    )
    assert (
        _reason(entity(1, figi=synthetic_figi(1)), CONTROL, registry=[figi_owner])
        == "figi.global_conflict"
    )


def test_same_uuid_claims_across_sources_are_not_duplicates():
    rows, counts, _ = _classify(entity(1, figi=synthetic_figi(1)), CONTROL)
    assert rows[1]["fund_status"] == "ACTIVE" and counts["identity_first_failure"] == {}


@pytest.mark.parametrize(
    "mutate,code",
    [
        (lambda rows: rows[0].__setitem__("ticker", 5), "catalog_source_type_invalid"),
        (
            lambda rows: rows[0].__setitem__("is_active", 1),
            "catalog_source_type_invalid",
        ),
        (
            lambda rows: rows[2].__setitem__("conflict_state", {1, 2}),
            "catalog_source_type_invalid",
        ),
        (
            lambda rows: rows[0].__setitem__("instrument_id", "not-a-uuid"),
            "catalog_source_uuid_invalid",
        ),
        (
            lambda rows: rows[0].__setitem__("instrument_id", None),
            "catalog_source_uuid_invalid",
        ),
        (
            lambda rows: rows[0].__setitem__(
                "instrument_id", "urn:uuid:" + str(uuid.UUID(int=1))
            ),
            "catalog_source_uuid_invalid",
        ),
        (lambda rows: rows[1].pop("cusip"), "catalog_source_schema_invalid"),
    ],
)
def test_source_types_are_strict_and_abort(mutate, code):
    subject = [dict(row) for row in entity(1)]
    mutate(subject)
    with pytest.raises(generator.PolicyGenerationError, match=code):
        _classify(tuple(subject), CONTROL)


def test_uppercase_uuid_text_is_canonicalized():
    subject = [dict(row) for row in entity(1)]
    for row in subject:
        row["instrument_id"] = str(uuid.UUID(int=1)).upper()
    rows, _, _ = _classify(tuple(subject), CONTROL)
    assert rows[1]["instrument_id"] == str(uuid.UUID(int=1))


@pytest.mark.parametrize("source", ["iu", "funds", "registry"])
def test_source_row_limit_aborts_before_classifying(source):
    padding = [dict(entity(1)[{"iu": 0, "funds": 1, "registry": 2}[source]])] * 100_001
    instruments, funds, identity = catalog(CONTROL)
    lists = {"iu": instruments, "funds": funds, "registry": identity}
    lists[source] = padding
    with pytest.raises(
        generator.PolicyGenerationError, match="catalog_row_limit_exceeded"
    ):
        generator.classify_catalog(
            lists["iu"], lists["funds"], lists["registry"], OBSERVED
        )


def test_every_emitted_reason_is_registered_and_counts_close():
    rows, counts, _ = _classify(
        entity(1, active=None),
        entity(2, figi="ZAG000000010"),
        entity(3, active=False),
        only(entity(4, active=False), fund=False, registry=False),
        CONTROL,
    )
    assert set(counts["identity_first_failure"]) <= set(IDENTITY_FAILURE_CODES)
    assert (
        sum(counts["identity_first_failure"].values())
        == counts["fund_status"]["UNKNOWN"]
    )
    assert (
        counts["structural_pre_claims"] - counts["active"]
        == counts["structural_claim_failures"]
    )
    assert counts["structural_claim_failures"] == 1  # the FIGI failure only


# ── seeded properties ────────────────────────────────────────────────────────
def _random_catalog(rng: random.Random, size: int = 40):
    entities = []
    for n in range(1, size + 1):
        choice = rng.random()
        kwargs = {}
        if choice < 0.15:
            kwargs.update(iu_isin=None, reg_isin=None, cusip=None)
        elif choice < 0.25:
            kwargs.update(reg_isin=None, cusip=None)
        elif choice < 0.35:
            kwargs.update(iu_isin=None)
        if rng.random() < 0.1:
            kwargs["active"] = rng.choice([False, None])
        if rng.random() < 0.1:
            kwargs["fund_type"] = rng.choice(["mmf", "closed_end"])
        if rng.random() < 0.1:
            kwargs["ticker"] = f"T{rng.randint(1, size)}"
        if rng.random() < 0.1:
            kwargs["figi"] = synthetic_figi(rng.randint(1, 5))
        if rng.random() < 0.05:
            kwargs["status"] = "candidate"
        subject = entity(n, **kwargs)
        roll = rng.random()
        if roll < 0.05:
            subject = only(subject, fund=False, registry=False)
        elif roll < 0.08:
            subject = only(subject, registry=False)
        entities.append(subject)
    return catalog(*entities)


def _active(instruments, funds, identity) -> set[str]:
    evidence, _, _ = generator.classify_catalog(instruments, funds, identity, OBSERVED)
    return {row["instrument_id"] for row in evidence if row["fund_status"] == "ACTIVE"}


@pytest.mark.parametrize("seed", range(20))
def test_properties_permutation_duplication_contradiction_and_owner(seed):
    rng = random.Random(seed)
    instruments, funds, identity = _random_catalog(rng)
    base_evidence, base_counts, base_digests = generator.classify_catalog(
        instruments, funds, identity, OBSERVED
    )
    base_active = {
        r["instrument_id"] for r in base_evidence if r["fund_status"] == "ACTIVE"
    }
    shuffled = [list(rows) for rows in (instruments, funds, identity)]
    for rows in shuffled:
        rng.shuffle(rows)
    assert generator.classify_catalog(*shuffled, OBSERVED) == (
        base_evidence,
        base_counts,
        base_digests,
    )
    assert generator.source_snapshot_sha256(
        *shuffled
    ) == generator.source_snapshot_sha256(instruments, funds, identity)
    # Duplicating any row never promotes.
    for rows_index in range(3):
        lists = [list(rows) for rows in (instruments, funds, identity)]
        lists[rows_index].append(dict(rng.choice(lists[rows_index])))
        assert _active(*lists) <= base_active
    if base_active:
        target = uuid.UUID(sorted(base_active)[rng.randrange(len(base_active))])
        target_ticker = next(
            r["ticker"] for r in instruments if r["instrument_id"] == target
        )
        # A contradiction (foreign owner of the ticker) demotes it, promotes nobody.
        owner = dict(entity(5000)[0], instrument_type="equity", ticker=target_ticker)
        after = _active(instruments + [owner], funds, identity)
        assert after <= base_active and str(target) not in after
        # A new unique, valid FIGI on the target never demotes anyone.
        registry = [
            dict(r, figi=synthetic_figi(9999))
            if r["instrument_id"] == target and r["figi"] is None
            else r
            for r in identity
        ]
        assert _active(instruments, funds, registry) >= base_active - {str(target)}
        if all(r["figi"] is None for r in identity if r["instrument_id"] == target):
            assert _active(instruments, funds, registry) == base_active
    # Any extra claimant (non-fund owner of an existing claim) never adds ACTIVE.
    donor = rng.choice(identity)
    claimant = dict(
        entity(6000)[0],
        instrument_type="bond",
        ticker=donor["ticker"],
        isin=donor["isin"],
    )
    assert _active(instruments + [claimant], funds, identity) <= base_active


# ── policy document, hashes and operator strictness ─────────────────────────
def test_policy_content_hash_is_stable_across_generation_timestamps(calendar):
    subject = entity(1)
    subject[0]["name"] = "private-holder-name"
    subject[1]["owner_email"] = "private@example.invalid"
    first, _ = _policy(subject, calendar=calendar)
    second, _ = _policy(
        subject, calendar=calendar, observed=OBSERVED + dt.timedelta(seconds=1)
    )
    assert first["generation"]["policy_hash"] == second["generation"]["policy_hash"]
    assert (
        first["generation"]["instrument_evidence_digest"]
        == second["generation"]["instrument_evidence_digest"]
    )
    assert generator.canonical_json(first) != generator.canonical_json(second)
    assert first["generation"]["policy_hash"] == policy_content_digest(first)
    assert operator._policy(first)[0] == first
    assert generator.verify_artifact(first)["mode"] == "build"
    assert first["generator_version"] == "fund-nav-policy-generator-v2"
    assert first["generation"]["source_query_sha256"] == SOURCE_QUERY_SHA256
    assert {r["evidence_reference"] for r in first["instrument_evidence"]} == {
        CATALOG_EVIDENCE_REFERENCE
    }
    assert "identity=registry-ticker-series-claims-v2" in CATALOG_EVIDENCE_REFERENCE
    assert (
        "current_only" in CATALOG_EVIDENCE_REFERENCE
        and "not_pit" in CATALOG_EVIDENCE_REFERENCE
    )
    text = generator.canonical_json(first).decode()
    assert "postgresql://" not in text and "@" not in json.dumps(
        first["instrument_evidence"]
    )
    assert '"T1"' not in text and synthetic_isin(1) not in text
    assert "private-holder-name" not in text and "private@example.invalid" not in text


def test_policy_hash_differs_from_v1_contract_but_calendar_identity_is_shared(calendar):
    policy, _ = _policy(entity(1), calendar=calendar)
    v1_shape = copy.deepcopy(policy)
    v1_shape["generator_version"] = "fund-nav-policy-generator-v1"
    assert policy_content_digest(v1_shape) != policy["generation"]["policy_hash"]
    for field in (
        "calendar_id",
        "calendar_version",
        "calendar_digest",
        "calendar_session_count",
    ):
        assert policy[field] == calendar[field]
    assert policy["calendar_version"].endswith(
        "-v1"
    )  # calendar reused, not re-versioned


@pytest.mark.parametrize(
    "field,value",
    [
        ("policy_hash", "0" * 64),
        ("instrument_evidence_digest", "0" * 64),
        ("source_query_sha256", "0" * 64),
        ("source_snapshot_sha256", "0" * 64),
        ("calendar_digest", "0" * 64),
        ("generation_sha256", "0" * 64),
        ("active_set_sha256", "0" * 64),
        ("active_daily_set_sha256", "0" * 64),
    ],
)
def test_generator_metadata_digest_tamper_fails_operator(calendar, field, value):
    policy, _ = _policy(entity(1), calendar=calendar)
    policy["generation"][field] = value
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)


def test_source_snapshot_digest_hashes_v1_verbatim_semantics_are_not_reused(calendar):
    policy, sources = _policy(entity(1), calendar=calendar)
    assert policy["generation"][
        "source_snapshot_sha256"
    ] == generator.source_snapshot_sha256(*sources)
    changed = copy.deepcopy(sources)
    changed[2][0]["sec_class_id"] = "C000000001"  # any registry column changes the SHA
    assert (
        generator.source_snapshot_sha256(*changed)
        != policy["generation"]["source_snapshot_sha256"]
    )


def _tampered_counts(policy, mutate):
    mutate(policy["generation"]["counts"])
    policy["generation"]["generation_sha256"] = generation_metadata_digest(
        policy["generation"]
    )
    return policy


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c["identity_first_failure"].__setitem__("activity.unknown", 2),
        lambda c: c["identity_first_failure"].__setitem__("unregistered.code", 1),
        lambda c: c.__setitem__("active", True),
        lambda c: c.__setitem__("active_daily", c["active_daily"] + 1),
        lambda c: c.__setitem__(
            "structural_pre_claims", c["structural_pre_claims"] + 1
        ),
        lambda c: c.__setitem__("structural_claim_failures", -1),
        lambda c: c["isin_presence_active"].__setitem__("both", 0),
        lambda c: c["inactive_reason"].__setitem__("inactive_without_funds_v", 5),
        lambda c: c["fund_status"].__setitem__("ACTIVE", 3),
        lambda c: c.pop("instrument_identity"),
    ],
)
def test_canonical_but_semantically_tampered_counts_are_rejected(calendar, mutate):
    policy, _ = _policy(
        entity(1),
        entity(2, active=None),
        only(entity(3, active=False), fund=False, registry=False),
        calendar=calendar,
    )
    _tampered_counts(policy, mutate)
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)


def test_active_digest_tamper_with_consistent_generation_hash_is_rejected(calendar):
    policy, _ = _policy(entity(1), entity(2), calendar=calendar)
    policy["generation"]["active_set_sha256"] = uuid_set_digest([str(uuid.UUID(int=1))])
    policy["generation"]["generation_sha256"] = generation_metadata_digest(
        policy["generation"]
    )
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)


def test_unknown_with_verified_flag_is_rejected_even_when_rehashed(calendar):
    policy, _ = _policy(entity(1), entity(2, active=None), calendar=calendar)
    row = next(
        r for r in policy["instrument_evidence"] if r["fund_status"] == "UNKNOWN"
    )
    row["identity_verified"] = True
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(_rehash(policy))


@pytest.mark.parametrize(
    "tamper",
    [
        "generator_v1",
        "query_v1",
        "reference_v1",
        "provider",
    ],
)
def test_retired_or_unsupported_contract_is_rejected_even_when_rehashed(
    calendar, tamper
):
    policy, _ = _policy(entity(1), calendar=calendar)
    if tamper == "generator_v1":
        policy["generator_version"] = policy["generation"]["generator_version"] = (
            "fund-nav-policy-generator-v1"
        )
    elif tamper == "query_v1":
        policy["generation"]["source_query_version"] = "nav-current-catalog-snapshot-v1"
    elif tamper == "reference_v1":
        for row in policy["instrument_evidence"]:
            row["evidence_reference"] = V1_REFERENCE
    else:
        policy["provider_contract"] = policy["generation"]["provider_contract"] = (
            "unsupported"
        )
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(_rehash(policy))
    with pytest.raises((generator.PolicyGenerationError, ValueError)):
        generator.verify_artifact(policy)


@pytest.mark.parametrize("reference", [V1_REFERENCE, CATALOG_EVIDENCE_REFERENCE])
def test_hand_authored_policy_cannot_claim_generator_catalog_references(
    calendar, reference
):
    policy, _ = _policy(entity(1), calendar=calendar)
    policy.pop("generation")
    policy.pop("generator_version")
    policy.pop("provider_contract")
    for row in policy["instrument_evidence"]:
        row["evidence_reference"] = reference
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)
    for row in policy["instrument_evidence"]:
        row["evidence_reference"] = "fixture-identity-verified"
    assert operator._policy(policy)[0] == policy  # existing hand-authored fixture path


def test_source_snapshot_export_is_hash_linked_and_tamper_evident(calendar):
    policy, sources = _policy(entity(1), entity(2, iu_isin=None), calendar=calendar)
    snapshot = generator.build_source_snapshot(policy, *sources)
    raw = generator.canonical_json(snapshot)
    assert (
        generator.verify_source_snapshot(snapshot, policy, raw=raw)[
            "source_snapshot_sha256"
        ]
        == policy["generation"]["source_snapshot_sha256"]
    )
    assert snapshot["row_counts"] == {"funds": 2, "identity": 2, "instruments": 2}
    with pytest.raises(
        generator.PolicyGenerationError, match="source_snapshot_not_canonical"
    ):
        generator.verify_source_snapshot(snapshot, policy, raw=raw + b" ")
    tampered = copy.deepcopy(snapshot)
    tampered["sources"]["identity"][0]["figi"] = synthetic_figi(3)
    with pytest.raises(
        generator.PolicyGenerationError, match="source_snapshot_link_invalid"
    ):
        generator.verify_source_snapshot(tampered, policy)
    other, _ = _policy(
        entity(1),
        entity(2, iu_isin=None),
        calendar=calendar,
        observed=OBSERVED + dt.timedelta(seconds=5),
    )
    with pytest.raises(
        generator.PolicyGenerationError, match="source_snapshot_link_invalid"
    ):
        generator.verify_source_snapshot(snapshot, other)


def test_output_creation_overwrite_and_offline_verify(tmp_path, calendar, capsys):
    target = tmp_path / "reference.json"
    content = generator.canonical_json(calendar)
    generator.write_artifact(target, content, force=False, build=False)
    assert target.read_bytes() == content
    if os.name == "posix":
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_already_exists"
    ):
        generator.write_artifact(target, content, force=False, build=False)
    generator.write_artifact(target, content, force=True, build=False)
    assert generator.main(["verify", "--policy-file", str(target)]) == 0
    printed = capsys.readouterr().out
    assert "T1" not in printed and "postgresql://" not in printed
    assert (
        generator.verify_artifact(
            json.loads(target.read_bytes()), raw=target.read_bytes()
        )["mode"]
        == "calendar"
    )


def test_calendar_cli_emits_unpublished_reference_without_dsn(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.delenv("NAV_READINESS_DATABASE_URL", raising=False)
    target = tmp_path / "calendar.json"
    assert (
        generator.main(
            [
                "calendar",
                "--coverage-start",
                START.isoformat(),
                "--coverage-end",
                END.isoformat(),
                "--output",
                str(target),
            ]
        )
        == 0
    )
    artifact = json.loads(target.read_bytes())
    assert artifact["publication_state"] == "unpublished"
    assert "instrument_evidence" not in artifact and "policy_hash" not in artifact
    assert generator.main(["verify", "--policy-file", str(target)]) == 0
    assert "private" not in capsys.readouterr().out


def _private_custody(tmp_path):
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    return root


def test_build_requires_posix_and_private_external_custody(tmp_path, monkeypatch):
    root = _private_custody(tmp_path)
    with pytest.raises(generator.PolicyGenerationError, match="custody_root_required"):
        generator.write_artifact(
            root / "policy.json", b"private", force=False, build=True
        )
    monkeypatch.setattr(generator, "_platform_name", lambda: "nt")
    with pytest.raises(
        generator.PolicyGenerationError, match="productive_artifact_requires_posix"
    ):
        generator.write_artifact(
            root / "policy.json", b"private", force=False, build=True, custody_root=root
        )
    generator.write_artifact(
        root / "calendar.json", b"unpublished", force=False, build=False
    )
    assert (root / "calendar.json").read_bytes() == b"unpublished"


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_build_rejects_git_directory_file_and_symlink_into_checkout(tmp_path):
    root = _private_custody(tmp_path)
    worktree = root / "worktree"
    worktree.mkdir()
    (worktree / ".git").touch()  # Worktree marker file; contents never read.
    checkout = root / "checkout"
    checkout.mkdir()
    (checkout / ".git").mkdir()
    for destination in (worktree / "artifact.json", checkout / "artifact.json"):
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_inside_git_checkout"
        ):
            generator.write_artifact(
                destination, b"secret", force=False, build=True, custody_root=root
            )
        assert not destination.exists()
    link = root / "link"
    link.symlink_to(checkout, target_is_directory=True)
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_inside_git_checkout"
    ):
        generator.write_artifact(
            link / "artifact.json",
            b"secret",
            force=False,
            build=True,
            custody_root=root,
        )
    root.chmod(0o755)
    with pytest.raises(
        generator.PolicyGenerationError, match="custody_root_not_private"
    ):
        generator.write_artifact(
            root / "artifact.json",
            b"secret",
            force=False,
            build=True,
            custody_root=root,
        )


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_disk_failure_leaves_no_partial_destination_and_preserves_existing(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    if force:
        destination.write_bytes(b"old-complete-artifact")
    real_fsync = os.fsync

    def fail_file_sync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(errno.ENOSPC, "synthetic disk failure")
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", fail_file_sync)
        with pytest.raises(OSError):
            generator.write_artifact(
                destination,
                b"new-complete-artifact",
                force=force,
                build=True,
                custody_root=root,
            )
    assert (
        destination.read_bytes() == b"old-complete-artifact"
        if force
        else not destination.exists()
    )
    assert not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_midwrite_failure_cleans_private_temp_without_publishing(tmp_path, monkeypatch):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    real_fdopen = os.fdopen

    class BrokenStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, data):
            self.stream.write(data[: len(data) // 2])
            raise OSError(errno.ENOSPC, "synthetic midwrite failure")

    with monkeypatch.context() as ctx:
        ctx.setattr(
            generator.os, "fdopen", lambda fd, mode: BrokenStream(real_fdopen(fd, mode))
        )
        with pytest.raises(OSError):
            generator.write_artifact(
                destination,
                b"complete-artifact",
                force=False,
                build=True,
                custody_root=root,
            )
    assert not destination.exists() and not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_atomic_publish_syscall_failure_keeps_prior_artifact(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    if force:
        destination.write_bytes(b"old-complete")

    def fail_publish(*_args, **_kwargs):
        raise OSError(errno.EIO, "synthetic publish failure")

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "replace" if force else "link", fail_publish)
        with pytest.raises(OSError):
            generator.write_artifact(
                destination, b"new-complete", force=force, build=True, custody_root=root
            )
    if force:
        assert destination.read_bytes() == b"old-complete"
    else:
        assert not destination.exists()
    assert not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_symlink_inserted_during_private_temp_write_fails_closed(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside-unchanged")
    real_fsync = os.fsync

    def race_before_publish(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            destination.symlink_to(outside)
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", race_before_publish)
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_target_not_regular"
        ):
            generator.write_artifact(
                destination, b"private", force=force, build=True, custody_root=root
            )
    assert outside.read_bytes() == b"outside-unchanged"
    assert destination.is_symlink() and not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_parent_replaced_by_git_symlink_during_write_fails_closed(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    parent = root / "nested"
    parent.mkdir(mode=0o700)
    moved = root / "moved"
    checkout = tmp_path / "another-checkout"
    checkout.mkdir()
    (checkout / ".git").touch()
    destination = parent / "policy.json"
    real_fsync = os.fsync

    def swap_parent(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            parent.rename(moved)
            parent.symlink_to(checkout, target_is_directory=True)
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", swap_parent)
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_parent_changed"
        ):
            generator.write_artifact(
                destination, b"private", force=False, build=True, custody_root=root
            )
    assert not (checkout / "policy.json").exists()
    assert not list(moved.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_intermediate_parent_symlink_race_is_rejected_before_private_write(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    middle = root / "middle"
    middle.mkdir(mode=0o700)
    inner = middle / "inner"
    inner.mkdir(mode=0o700)
    checkout = tmp_path / "other-checkout"
    checkout.mkdir()
    (checkout / ".git").mkdir()
    (checkout / "inner").mkdir()
    destination = inner / "policy.json"
    original = generator._pinned_parent

    def swap_before_open(path):
        middle.rename(root / "moved")
        middle.symlink_to(checkout, target_is_directory=True)
        return original(path)

    monkeypatch.setattr(generator, "_pinned_parent", swap_before_open)
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_parent_changed"
    ):
        generator.write_artifact(
            destination, b"private", force=False, build=True, custody_root=root
        )
    assert not (checkout / "inner" / "policy.json").exists()
    assert not list((root / "moved" / "inner").glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_concurrent_no_replace_creators_publish_one_complete_file(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    barrier = Barrier(2)
    check = generator._check_parent_and_target

    def concurrent_check(*args, **kwargs):
        check(*args, **kwargs)
        barrier.wait(timeout=5)

    monkeypatch.setattr(generator, "_check_parent_and_target", concurrent_check)

    def create(content):
        try:
            generator.write_artifact(
                destination, content, force=False, build=True, custody_root=root
            )
            return "published"
        except generator.PolicyGenerationError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, (b"first-complete", b"second-complete")))
    assert sorted(results) == ["artifact_already_exists", "published"]
    assert destination.read_bytes() in (b"first-complete", b"second-complete")
    assert not list(root.glob(".nav-policy-*.tmp"))


def _fake_snapshot(monkeypatch, *entities):
    rows = catalog(*entities)
    monkeypatch.setenv("NAV_FAKE_DSN", "postgresql://fake.invalid/never-used")
    monkeypatch.setattr(
        generator, "read_catalog_snapshot", lambda dsn: (OBSERVED, *rows)
    )
    return rows


def _build_args(root, output, *extra):
    return [
        "build",
        "--dsn-env",
        "NAV_FAKE_DSN",
        "--custody-root",
        str(root),
        "--output",
        str(output),
        "--coverage-start",
        START.isoformat(),
        "--coverage-end",
        END.isoformat(),
        "--policy-id",
        "current-daily-nav-xnys-usd-adjusted",
        "--policy-version",
        "2026-09-24.2",
        *extra,
    ]


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_build_exports_private_hash_linked_source_snapshot(
    tmp_path, monkeypatch, capsys
):
    root = _private_custody(tmp_path)
    _fake_snapshot(monkeypatch, entity(1), entity(2, reg_isin=None, cusip=None))
    policy_path, snapshot_path = root / "policy-v2.json", root / "source-v2.json"
    assert (
        generator.main(
            _build_args(
                root, policy_path, "--source-snapshot-output", str(snapshot_path)
            )
        )
        == 0
    )
    printed = capsys.readouterr().out
    report = json.loads(printed)
    assert report["status"] == "ok" and report["counts"]["fund_status"] == {"ACTIVE": 2}
    for path in (policy_path, snapshot_path):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert str(uuid.UUID(int=1)) not in printed and "T1" not in printed
    assert synthetic_isin(1) not in printed
    snapshot = json.loads(snapshot_path.read_bytes())
    assert (
        snapshot["policy_artifact_sha256"]
        == hashlib.sha256(policy_path.read_bytes()).hexdigest()
    )
    assert (
        generator.main(
            [
                "verify",
                "--policy-file",
                str(policy_path),
                "--source-snapshot-file",
                str(snapshot_path),
            ]
        )
        == 0
    )
    assert (
        json.loads(capsys.readouterr().out)["source_snapshot_sha256"]
        == snapshot["source_snapshot_sha256"]
    )
    # No automatic overwrite of either file.
    assert (
        generator.main(
            _build_args(
                root, policy_path, "--source-snapshot-output", str(snapshot_path)
            )
        )
        == 2
    )
    assert json.loads(capsys.readouterr().out)["code"] == "artifact_already_exists"


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_snapshot_export_failure_keeps_policy_as_incomplete_bundle(
    tmp_path, monkeypatch, capsys
):
    root = _private_custody(tmp_path)
    _fake_snapshot(monkeypatch, entity(1))
    policy_path, snapshot_path = root / "policy-v2.json", root / "source-v2.json"
    real_write = generator.write_artifact
    calls = []

    def fail_second(path, content, **kwargs):
        calls.append(path)
        if len(calls) == 2:
            raise OSError(errno.ENOSPC, "synthetic export failure")
        return real_write(path, content, **kwargs)

    monkeypatch.setattr(generator, "write_artifact", fail_second)
    assert (
        generator.main(
            _build_args(
                root, policy_path, "--source-snapshot-output", str(snapshot_path)
            )
        )
        == 2
    )
    blocked = json.loads(capsys.readouterr().out)
    assert (blocked["stage"], blocked["bundle"]) == (
        "source_snapshot_export",
        "incomplete",
    )
    assert (
        blocked["artifact_sha256"]
        == hashlib.sha256(policy_path.read_bytes()).hexdigest()
    )
    assert policy_path.exists() and not snapshot_path.exists()
    assert (
        generator.main(
            _build_args(
                root, root / "x.json", "--source-snapshot-output", str(root / "x.json")
            )
        )
        == 2
    )
    assert (
        json.loads(capsys.readouterr().out)["code"] == "source_snapshot_output_collides"
    )


def test_projection_mismatch_blocks_build_without_writing(
    tmp_path, monkeypatch, capsys
):
    subject = entity(1)
    subject[1]["ticker"] = "T-OTHER"
    _fake_snapshot(monkeypatch, subject, CONTROL)
    output = tmp_path / "policy.json"
    assert generator.main(_build_args(tmp_path, output)) == 2
    assert (
        json.loads(capsys.readouterr().out)["code"]
        == "catalog_identity_projection_mismatch"
    )
    assert not output.exists()
