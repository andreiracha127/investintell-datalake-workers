"""Guard pins for the Stage A (direct-activation) evidence root.

Stage A validates the signed candidate live (``live_validation``), then measures its
reproducibility + SLO conformance under the Phase 1 machinery. This suite pins the
committed Stage A artifacts and the three measured records the round writes, reusing
the Phase 1 guard semantics (strict JSON loader with duplicate-key + non-finite
rejection, recursive governance walk with string-truthy semantics, regeneration pins,
hash pins). It NEVER asserts any activation flag is on: Stage A flips nothing.

CRLF note: ``core.autocrlf=true`` on Windows can re-smudge committed JSON to CRLF, but
the records pin sha256 of the LF bytes the harness wrote; every hash helper here
normalizes CRLF->LF before hashing so the pins hold on either checkout.

Records written by the N=8 host + N=8 container measurement round (present in CI, may
lag a local run): ``reproducibility_record.json``, ``slo_threshold_amendment_record.json``,
``slo_conformance_record.json``. The tests that pin them open them directly (no silent
skip) so a missing/deleted record is a hard failure in CI.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module", autouse=True)
def _real_repo_harness():
    """The cloud-runner suite purges and rebinds ``sys.modules['harness']`` to its
    bundle copy (which has no ``direct_activation``), so the in-test package imports
    here would resolve the WRONG harness when the full CI suite runs (see the same
    note in tests/test_dark_launch_readiness.py). Swap the real repo harness in for
    this module's tests and restore whatever was there afterwards."""
    saved = {k: v for k, v in sys.modules.items()
             if k == "harness" or k.startswith("harness.")}
    for key in saved:
        del sys.modules[key]
    sys.path.insert(0, str(ROOT))
    try:
        yield
    finally:
        for key in [k for k in sys.modules
                    if k == "harness" or k.startswith("harness.")]:
            del sys.modules[key]
        sys.modules.update(saved)
        sys.path.remove(str(ROOT))
STAGE_A_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_a_001"
SNAPSHOT = STAGE_A_ROOT / "input_snapshot"
DARK_ROOT = ROOT / "artifacts" / "a5" / "open_macro_v03_dark_launch_001"
THRESHOLDS = DARK_ROOT / "monitoring_thresholds_record.json"

LIVE_VALIDATION_RECORD = STAGE_A_ROOT / "live_validation_record.json"
REPRODUCIBILITY_RECORD = STAGE_A_ROOT / "reproducibility_record.json"
AMENDMENT_RECORD = STAGE_A_ROOT / "slo_threshold_amendment_record.json"
CONFORMANCE_RECORD = STAGE_A_ROOT / "slo_conformance_record.json"

DIRECT_ACTIVATION_ID = "open_macro_v03_direct_activation_001"

REQUIRED_OWNER_ROLES = {
    "technical_owner",
    "quant_owner",
    "risk_owner",
    "operations_owner",
    "product_portfolio_owner",
    "final_approver",
}

# Activation flags that must never be truthy anywhere in a Stage A artifact (the
# Phase 1 FORBIDDEN set, adapted). A string "true" counts as truthy and must fail.
FORBIDDEN_ACTIVATION_KEYS = {
    "runtime_activation",
    "runtime_activation_allowed",
    "runtime_activation_attempt",
    "activation_allowed",
    "activation_allowed_in_this_pr",
    "activation_requested",
    "freeze_ready",
    "official_result",
    "official_result_published",
    "allocator_publish",
    "allocator_publish_attempt",
    "allocator_received_output",
    "allow_allocator_publish",
    "allow_db_write",
    "db_write_official",
    "official_db_write_attempt",
    "productive_db_received_official_result",
    "approve_controlled_activation",
    "A5_unblocked",
    "production_endpoint_activated",
    "production_endpoint_activation_attempt",
    "backend_executes_engine",
    "backend_executes_docker",
    "backend_executes_subprocess",
    "docker_execution_from_backend",
    "feature_flag_default",
    "approved",
}

# Wall-clock generation markers that would make the snapshot manifest non-deterministic.
# (The pinned query bounds ``vintage_available_at_*`` are deterministic parameters, not
# clock reads, so they are intentionally NOT in this set.)
FORBIDDEN_CLOCK_KEYS = {
    "generated_at",
    "created_at",
    "exported_at",
    "written_at",
    "run_at",
    "timestamp",
    "date_generated",
    "now",
}


# --- strict JSON loader (Phase 1 pattern, local copy) ----------------------------

def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError(f"duplicate JSON key {key!r}")
        seen[key] = value
    return seen


def _reject_non_finite_constant(constant: str) -> None:
    raise ValueError(f"non-finite JSON constant {constant!r}")


def _reject_non_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number {value!r}")
    return parsed


def _loads_strict(text: str) -> Any:
    return json.loads(
        text,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_non_finite_constant,
        parse_float=_reject_non_finite_float,
    )


def _load_strict(path: Path) -> Any:
    return _loads_strict(path.read_text(encoding="utf-8"))


def _sha256_lf(path: Path) -> str:
    """sha256 of the file bytes with CRLF normalized to LF (matches the harness pins)."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _p95(values: list[float]) -> float:
    """Nearest-rank p95, byte-for-byte the producer's ``measure_observability._p95``."""
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return ordered[index]


def _recompute_slo_from_runs(repro: dict[str, Any]) -> dict[str, Any]:
    """Independently derive the SLO metrics from the raw 16-run samples in the
    reproducibility record, replicating ``measure_stage_a.measured_metrics`` exactly:
    worst-leg p95 latency (max of per-leg p95s), peak memory over all runs, error rate
    over all runs, retry rate 0 (no retry path). This is the ground truth the signed
    conformance/amendment records must match — recomputing here means a hand-edited or
    stale conformance record cannot certify SLO values from a different round."""
    legs = repro["legs"]
    per_leg_p95 = {leg: round(_p95([r["wall_ms"] for r in legs[leg]["runs"]]), 3)
                   for leg in legs}
    all_runs = [r for leg in legs.values() for r in leg["runs"]]
    exits = [r["exit_code"] for r in all_runs]
    return {
        "latency_p95_ms": max(per_leg_p95.values()),
        "latency_p95_ms_per_leg": per_leg_p95,
        "memory_peak_bytes": max(r["memory_peak_bytes"] for r in all_runs),
        "error_rate": sum(1 for e in exits if e != 0) / len(all_runs),
        "retry_rate": 0.0,
    }


def _stage_a_json_files() -> list[Path]:
    return sorted(p for p in STAGE_A_ROOT.rglob("*.json") if p.is_file())


def _walk(node: Any):
    """Yield every (key, value) pair, recursively, through dicts and lists."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _is_truthy_flag(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def _assert_governance_blocked(payload: Any, *, where: str) -> None:
    for key, value in _walk(payload):
        if key in FORBIDDEN_ACTIVATION_KEYS:
            assert not _is_truthy_flag(value), f"{where}: {key}={value!r} is truthy"
        if key in {"A5", "a5_status"}:
            assert value == "blocked", f"{where}: {key}={value!r}; expected blocked"
        if key == "db_write_mode":
            assert value == "none", f"{where}: db_write_mode={value!r}; expected none"


# --- 1. strict loader over every Stage A JSON ------------------------------------

def test_all_stage_a_json_loads_strict() -> None:
    files = _stage_a_json_files()
    assert LIVE_VALIDATION_RECORD in files  # sanity: the committed record is present
    for path in files:
        _load_strict(path)  # raises on duplicate keys or NaN/Infinity/overflow floats


def test_strict_loader_rejects_duplicate_keys() -> None:
    with pytest.raises(ValueError, match="duplicate JSON key 'runtime_activation'"):
        _loads_strict('{"runtime_activation": false, "runtime_activation": true}')


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_strict_loader_rejects_non_finite_constants(constant: str) -> None:
    with pytest.raises(ValueError, match="non-finite JSON constant"):
        _loads_strict(f'{{"value": {constant}}}')


@pytest.mark.parametrize("number", ["1e9999", "-1e9999"])
def test_strict_loader_rejects_float_overflow(number: str) -> None:
    with pytest.raises(ValueError, match="non-finite JSON number"):
        _loads_strict(f'{{"value": {number}}}')


# --- 2. recursive governance walk + string-truthy semantics ----------------------

def test_stage_a_artifacts_keep_activation_blocked() -> None:
    for path in _stage_a_json_files():
        _assert_governance_blocked(_load_strict(path), where=path.name)


def test_governance_walk_treats_string_true_as_truthy() -> None:
    """Belt-and-suspenders: a stringified 'true' on a forbidden flag must fail, even
    nested inside a list (the string-truthy escape the Phase 1 guards close)."""
    payload = {"nested": [{"runtime_activation": "true"}]}
    with pytest.raises(AssertionError, match="runtime_activation='true' is truthy"):
        _assert_governance_blocked(payload, where="synthetic")


# --- 3. live_validation_record equals a fresh regeneration -----------------------

def test_live_validation_record_equals_regeneration() -> None:
    """The committed record must be exactly what the committed executor derives today
    from pinned pack v2 + the pinned delta snapshot; a hand-edited record cannot
    survive. Reconstitutes the full 148-month latched chain (~84 s) -- acceptable."""
    from harness.direct_activation import live_validation as lv

    committed = _load_strict(LIVE_VALIDATION_RECORD)
    regenerated = lv.compute(worker_commit_override=committed["provenance"]["worker_commit"])
    assert regenerated == committed


# --- 3b. staleness gate rejects future (PIT-impossible) macro data ---------------

def test_staleness_gate_rejects_future_macro_vintage() -> None:
    """A macro vintage available AFTER VALIDATION_AS_OF is future data: PIT-impossible
    for honest inputs (the exporter's upper bound rejects it at export). Its negative
    age would satisfy ``age <= bound`` and be read as ``fresh``; the consume-path gate
    must fail loud instead, closing the tampered-snapshot / exporter-regression case
    that already passed the manifest byte gate. A same-day vintage (age 0) is valid."""
    from harness.direct_activation import live_validation as lv

    def _basket(overrides: dict[str, date] | None = None) -> list[dict]:
        # a full SEED basket (so the seed-present gate is satisfied); ``overrides``
        # dates a specific series, everything else sits fresh at the as-of. One shared
        # observation period so the freshness anchor is that (latest) period.
        overrides = overrides or {}
        return [{"series_id": sid, "observation_period": "2026-05-01",
                 "available_at": f"{overrides.get(sid, lv.VALIDATION_AS_OF).isoformat()}"
                                 "T00:00:00+00:00"}
                for sid in lv.EXPECTED_SEED_SERIES]

    # future macro vintage fails loud in the vintage loop (before touching prices)
    with pytest.raises(lv.LiveValidationError, match="future vintage"):
        lv.staleness_gate(
            _basket({"PAYEMS": lv.VALIDATION_AS_OF + timedelta(days=1)}), [])

    # same-day (age 0) is within bounds -> passes the future-vintage guard
    same_day_prices = [{"ticker": t, "date": lv.VALIDATION_AS_OF.isoformat()}
                       for t in lv.sleeve_mod.SLEEVE_TICKERS]
    result = lv.staleness_gate(_basket(), same_day_prices)
    assert result["series"]["PAYEMS"]["age_days"] == 0


def test_staleness_gate_requires_every_seed_series() -> None:
    """A SEED series entirely absent from the composed vintages is never age-checked
    by the per-series loop, yet the remaining basket would otherwise return
    ``staleness: pass`` — and the decision coverage gate still consumes it at 0.80.
    The gate must fail loud when any seed series is missing (here MICH, the 0.20
    inflation-expectations source) before certifying freshness."""
    from harness.direct_activation import live_validation as lv

    without_mich = [{"series_id": sid,
                     "available_at": f"{lv.VALIDATION_AS_OF.isoformat()}T00:00:00+00:00"}
                    for sid in lv.EXPECTED_SEED_SERIES if sid != "MICH"]
    with pytest.raises(lv.LiveValidationError, match="SEED series absent.*MICH"):
        lv.staleness_gate(without_mich, [])


def test_staleness_gate_rejects_future_priced_bar() -> None:
    """A price bar dated AFTER the as-of yields a negative day span -> empty range ->
    business_age 0 -> would read as 'fresh', and compute() would price at that future
    date. The gate must fail loud (symmetric with the macro future-vintage guard)."""
    from harness.direct_activation import live_validation as lv

    fresh_vintages = [{"series_id": sid, "observation_period": "2026-05-01",
                       "available_at": f"{lv.VALIDATION_AS_OF.isoformat()}T00:00:00+00:00"}
                      for sid in lv.EXPECTED_SEED_SERIES]
    future = (lv.VALIDATION_AS_OF + timedelta(days=1)).isoformat()
    prices = [{"ticker": t, "date": lv.VALIDATION_AS_OF.isoformat()}
              for t in lv.sleeve_mod.SLEEVE_TICKERS]
    prices.append({"ticker": lv.sleeve_mod.SLEEVE_TICKERS[0], "date": future})
    with pytest.raises(lv.LiveValidationError, match="future price bar"):
        lv.staleness_gate(fresh_vintages, prices)


def test_staleness_freshness_anchors_on_latest_observation() -> None:
    """A recent revision to an OLD observation period must NOT mask a stale latest
    observation. Freshness is the age of the newest observation period's PIT vintage,
    not max(available_at) over all vintages."""
    from harness.direct_activation import live_validation as lv

    def _row(sid, obs, avail):
        return {"series_id": sid, "observation_period": obs,
                "available_at": f"{avail}T00:00:00+00:00"}

    # 7 series fresh; PAYEMS: newest observation (2026-01) is stale (available 2026-02-15,
    # ~138d old) but carries a RECENT revision of an ancient 2020-01 observation.
    rows = [_row(sid, "2026-05-01", "2026-06-15")
            for sid in lv.EXPECTED_SEED_SERIES if sid != "PAYEMS"]
    rows += [_row("PAYEMS", "2026-01-01", "2026-02-15"),
             _row("PAYEMS", "2020-01-01", "2026-07-01")]  # recent revision of old obs

    # max(available_at) over PAYEMS is 2026-07-01 (age 2d) -> would pass the OLD metric;
    # anchored on the latest observation (2026-01) it is ~138d old -> fails the 45d bound
    with pytest.raises(lv.LiveValidationError, match="PAYEMS latest observation"):
        lv.staleness_gate(rows, [])


def test_vintage_delta_window_gate_rejects_out_of_window_rows() -> None:
    """A delta vintage dated on/before PACK_CUT or after VALIDATION_AS_OF must be
    rejected before composing (it could restate certified pack PIT history)."""
    from harness.direct_activation import live_validation as lv

    lv.vintage_delta_window_gate([])  # empty (the committed case) passes

    in_window = [{"series_id": "INDPRO", "observation_period": "2026-06-01",
                  "available_at": "2026-07-01T12:00:00+00:00"}]
    lv.vintage_delta_window_gate(in_window)  # strictly after PACK_CUT, <= as-of

    pre_cut = [{"series_id": "INDPRO", "observation_period": "2026-05-01",
                "available_at": "2026-06-15T00:00:00+00:00"}]  # before pack cut
    with pytest.raises(lv.LiveValidationError, match="vintage delta window"):
        lv.vintage_delta_window_gate(pre_cut)

    post_asof = [{"series_id": "INDPRO", "observation_period": "2026-06-01",
                  "available_at": "2026-07-04T00:00:00+00:00"}]  # after as-of
    with pytest.raises(lv.LiveValidationError, match="vintage delta window"):
        lv.vintage_delta_window_gate(post_asof)


# --- 4. snapshot manifest hash + row pins ----------------------------------------

def test_snapshot_manifest_equals_full_reconstruction() -> None:
    """The committed manifest must equal, dict == dict, the manifest the committed
    exporter derives from its pinned constants (VALIDATION_AS_OF, PRICE_OVERLAP_START,
    PACK_CUT, series/tickers, pack pin, fixed provenance) plus hashes/row counts
    recomputed from the committed delta files — so NO field escapes the guard: a
    hand-edited bound, provenance, row count, or an added key all fail, not just the
    two file pins."""
    from harness.direct_activation import export_snapshot as es
    from harness.direct_activation import live_validation as lv
    from scripts.p1_export import export_p1_sources as p1

    manifest = _load_strict(SNAPSHOT / "snapshot_manifest.json")

    def _lf_bytes(path: Path) -> bytes:
        return path.read_bytes().replace(b"\r\n", b"\n")

    price_path = SNAPSHOT / "delta_eod_prices.json"
    vintage_path = SNAPSHOT / "delta_macro_vintages.json"
    price_rows = _load_strict(price_path)
    vintage_rows = _load_strict(vintage_path)
    assert isinstance(price_rows, list) and isinstance(vintage_rows, list)

    expected = es.build_manifest(
        _lf_bytes(price_path), price_rows, _lf_bytes(vintage_path), vintage_rows)
    assert manifest == expected

    assert manifest["pack_v2_sha256"] == lv.PACK_SHA256_PIN
    assert manifest["provenance"] == {
        "source": "production datalake (read-only)",
        "db_service_id": p1.DB_SOURCE,
        "db_name": "market",
        "db_schema": "public",
        "tables": ["eod_prices", "macro_observation_vintage"],
    }

    # deterministic manifest: no wall-clock generation timestamp anywhere
    manifest_keys = {key for key, _ in _walk(manifest)}
    assert manifest_keys.isdisjoint(FORBIDDEN_CLOCK_KEYS), (
        f"manifest carries clock keys {manifest_keys & FORBIDDEN_CLOCK_KEYS}")


def test_verify_snapshot_manifest_rejects_tampered_deterministic_fields() -> None:
    """The consume-path manifest gate must reject a manifest regenerated with the SAME
    delta hashes/row counts but a different as-of, SEED basket, sleeve, or query bound —
    the metadata that says WHAT was validated. The committed manifest passes; each
    single-field tamper (with the byte pins left intact) fails loud."""
    import copy

    from harness.direct_activation import live_validation as lv

    manifest = _load_strict(SNAPSHOT / "snapshot_manifest.json")
    delta_prices = _load_strict(SNAPSHOT / "delta_eod_prices.json")
    delta_vintages = _load_strict(SNAPSHOT / "delta_macro_vintages.json")

    # baseline: the committed, consistent manifest passes
    lv.verify_snapshot_manifest(manifest, delta_vintages, delta_prices)

    def _tampered(mutate) -> dict[str, Any]:
        clone = copy.deepcopy(manifest)
        mutate(clone)
        return clone

    def _drop_mich(m):
        m["query_parameters"]["series_ids"] = [
            s for s in m["query_parameters"]["series_ids"] if s != "MICH"]

    cases = [
        ("validation_as_of",
         lambda m: m.__setitem__("validation_as_of", "2026-07-04")),
        ("query validation_as_of",
         lambda m: m["query_parameters"].__setitem__("validation_as_of", "2026-07-04")),
        ("pack_cut",
         lambda m: m["query_parameters"].__setitem__("pack_cut", "2026-06-29")),
        ("price_overlap_start",
         lambda m: m["query_parameters"].__setitem__("price_overlap_start", "2026-06-29")),
        ("series_ids diverged from the SEED basket", _drop_mich),
        ("tickers diverged from the sleeve",
         lambda m: m["query_parameters"].__setitem__(
             "tickers", m["query_parameters"]["tickers"] + ["QQQ"])),
        ("vintage upper bound",
         lambda m: m["query_parameters"].__setitem__(
             "vintage_available_at_upper_inclusive", "2026-07-04T23:59:59.999999+00:00")),
        ("vintage lower bound",
         lambda m: m["query_parameters"].__setitem__(
             "vintage_available_at_lower_exclusive", "2026-06-29T23:59:59.999999+00:00")),
        # full-manifest binding: identity/provenance/extra-key tampers over the SAME
        # delta bytes must be rejected by the executor too, not only the CI test
        ("full manifest diverged",
         lambda m: m["provenance"].__setitem__("source", "staging datalake (read-only)")),
        ("full manifest diverged",
         lambda m: m.__setitem__("direct_activation_id", "some_other_activation")),
        ("full manifest diverged", lambda m: m.__setitem__("stage", "B")),
        ("full manifest diverged",
         lambda m: m.__setitem__("staleness_gate_at_export", "fail")),
        ("full manifest diverged", lambda m: m.__setitem__("unexpected_key", True)),
    ]
    for match, mutate in cases:
        with pytest.raises(lv.LiveValidationError, match=match):
            lv.verify_snapshot_manifest(_tampered(mutate), delta_vintages, delta_prices)


# --- 5. delta composition: ticker set + date bounds ------------------------------

def test_delta_prices_cover_exactly_the_sleeve_within_bounds() -> None:
    from harness.direct_activation import export_snapshot as es

    delta_prices = _load_strict(SNAPSHOT / "delta_eod_prices.json")
    assert delta_prices  # non-empty

    tickers = {row["ticker"] for row in delta_prices}
    assert tickers == set(es.SLEEVE_TICKERS)

    lower, upper = es.PRICE_OVERLAP_START, es.VALIDATION_AS_OF
    for row in delta_prices:
        d = date.fromisoformat(row["date"])
        assert lower <= d <= upper, f"{row['ticker']} {row['date']} outside export bounds"


def test_snapshot_table_queries_are_schema_qualified() -> None:
    """The live snapshot SELECTs must schema-qualify the tables (public.*) so a DSN or
    role-default search_path override cannot resolve them to a same-host scratch schema
    while the manifest still stamps production provenance."""
    from harness.direct_activation import export_snapshot as es

    assert es.PINNED_DB_SCHEMA == "public"
    assert "FROM public.eod_prices\n" in es._EOD_PRICES_SQL
    assert "FROM public.macro_observation_vintage\n" in es._MACRO_VINTAGE_SQL
    # no bare (unqualified) reference to either table survives
    assert "FROM eod_prices" not in es._EOD_PRICES_SQL
    assert "FROM macro_observation_vintage" not in es._MACRO_VINTAGE_SQL


def test_overlap_completeness_gate_rejects_a_delta_missing_an_overlap_bar() -> None:
    """The consume path must re-run the overlap-completeness gate: a hash-pinned delta
    that drops a pack bar in PRICE_OVERLAP_START..PACK_CUT would otherwise be silently
    backfilled from the pack tail without re-validation. The committed delta passes."""
    from harness.direct_activation import live_validation as lv

    pack_prices = lv._load_json(lv.PACK / "data" / "canonical" / "eod_prices.json")
    delta_prices = _load_strict(SNAPSHOT / "delta_eod_prices.json")

    # committed delta covers the whole overlap tail -> passes
    lv.overlap_completeness_gate(pack_prices, delta_prices)

    # drop one delta bar whose key is a PACK overlap key -> that pack key becomes
    # uncovered and the gate fails loud
    lo, hi = lv.PRICE_OVERLAP_START.isoformat(), lv.PACK_CUT.isoformat()
    sleeve = set(lv.sleeve_mod.SLEEVE_TICKERS)
    pack_overlap_keys = {(r["ticker"], r["date"]) for r in pack_prices
                         if r["ticker"] in sleeve and lo <= r["date"] <= hi}
    assert pack_overlap_keys, "pack has no overlap-tail bars to test coverage against"
    holed = [r for r in delta_prices
             if (r["ticker"], r["date"]) != next(iter(pack_overlap_keys))]
    assert len(holed) < len(delta_prices), "expected to drop a covered overlap bar"
    with pytest.raises(lv.LiveValidationError, match="overlap-completeness gate"):
        lv.overlap_completeness_gate(pack_prices, holed)


# --- 5b. DSN pin structurally binds the current GCloud production DB ---------------

def test_dsn_pin_accepts_only_gcloud_market_and_rejects_lookalikes() -> None:
    from harness.direct_activation import export_snapshot as es
    from scripts.p1_export import export_p1_sources as p1

    host = p1.PINNED_GCLOUD_NLB_HOST
    # The post-cutover production shape used by the P1 _002 exporter.
    es._assert_pinned_db_source(
        f"postgresql://u:p@{host}:5432/market")
    # Keyword-form DSNs are parsed through libpq too; they are not URL-only.
    es._assert_pinned_db_source(
        f"host={host} dbname=market user=u")

    # The GCloud pin anywhere except the effective single host must not pass. The
    # legacy Tiger source is rejected after the production cutover.
    for bad in (
        "postgresql://u:p@t83f4np6x4.proj1.tsdb.cloud.timescale.com:32648/tsdb",
        f"postgresql://{host}:p@staging.example.com/market",      # pin as username
        f"postgresql://u:{host}@staging.example.com/market",      # pin as password
        f"postgresql://u:p@staging.example.com/{host}",           # pin as dbname
        f"postgresql://u:p@{host}.example.com/market",            # suffix lookalike
        "postgresql://u:p@localhost/market",                      # foreign host
        f"dbname={host}",                                        # hostless keyword DSN
        # comma-separated host LIST: libpq connects to the FIRST reachable host, so a
        # staging host smuggled alongside the pin must be refused.
        f"host=staging.example.com,{host} dbname=market",
        # hostaddr override: libpq connects to that address regardless of the pinned
        # host, so any override is refused even when one side carries the pin.
        f"host={host} hostaddr=203.0.113.7 dbname=market",
        f"host=staging.example.com hostaddr={host} dbname=market",
        # Right host, wrong database: GCloud's app and scratch DBs must not pass.
        f"postgresql://u:p@{host}:5432/app",
        f"host={host} dbname=investintell_alloc user=u",
        # ...and an OMITTED dbname (libpq would default it to the username) is refused
        f"host={host} user=u",
    ):
        with pytest.raises(es.SnapshotExportError):
            es._assert_pinned_db_source(bad)


# --- 6. reproducibility_record (measured round) ----------------------------------

def test_reproducibility_record_pins_a_clean_16_run_reproduction() -> None:
    repro = _load_strict(REPRODUCIBILITY_RECORD)
    live = _load_strict(LIVE_VALIDATION_RECORD)

    assert repro["artifact_type"] == "direct_activation_stage_a_reproducibility_record"
    assert repro["runs_per_leg"] == 8
    assert repro["runs_total"] == 16
    assert repro["mismatch_count"] == 0
    assert repro["verdict"] == "reproduced"

    legs = repro["legs"]
    assert set(legs) == {"host", "container"}
    hashes: set[str] = set()
    for leg_name in ("host", "container"):
        leg = legs[leg_name]
        assert len(leg["runs"]) == 8, leg_name
        for run in leg["runs"]:
            assert run["exit_code"] == 0, leg_name
            hashes.add(run["logical_output_hash"])
        assert leg["logical_output_hash"] is not None, leg_name
        hashes.add(leg["logical_output_hash"])
    # identical logical output across every run and both legs
    hashes.add(repro["logical_output_hash"])
    assert len(hashes) == 1, f"non-identical logical output: {sorted(hashes)}"

    job = repro["job_identity"]
    assert job["clean_tree"] is True  # bool, not the string "true"
    assert re.fullmatch(r"[0-9a-f]{40}", job["worker_commit"])

    # HARD evidence-to-branch binding: every exact Stage A compute-file hash must equal
    # the git blob at CURRENT HEAD (the tree being merged). An unrelated worker file is
    # outside this manifest; a post-measurement decision/measurement change is inside it
    # and therefore fails CI until Stage A is recertified.
    from harness.direct_activation.compute_manifest import STAGE_A_COMPUTE_PATHS

    expected_surfaces = set(STAGE_A_COMPUTE_PATHS)
    assert set(job["tree_hashes"]) == expected_surfaces
    worker_commit = job["worker_commit"]
    # The AUTHORITATIVE, shallow-safe binding is tree_hashes == HEAD:<surface> (the
    # merged tree, always present in any checkout including a PR merge commit).
    #
    # worker_commit is, by construction, the clean CODE commit measured BEFORE the
    # evidence child commit — the measured logical_output_hash embeds worker_commit, so
    # it can never be the evidence commit itself; it is that commit's parent (a genuine
    # ancestor of HEAD). A shallow clone of the merge commit need not fetch that object,
    # so resolving <worker_commit>:<surface> unconditionally would fail there. Bind it
    # only when its object is present: then it MUST be an ancestor of HEAD and its
    # surface trees must match the pins.
    wc_present = subprocess.run(
        ["git", "cat-file", "-e", f"{worker_commit}^{{commit}}"],
        cwd=ROOT, capture_output=True,
    ).returncode == 0
    if wc_present:
        is_ancestor = subprocess.run(
            ["git", "merge-base", "--is-ancestor", worker_commit, "HEAD"],
            cwd=ROOT, capture_output=True,
        ).returncode
        assert is_ancestor == 0, (
            f"job_identity.worker_commit {worker_commit} is present but is NOT an "
            f"ancestor of HEAD — the evidence cites a commit off the merged history")
    for surface, pinned in job["tree_hashes"].items():
        head_object = subprocess.run(
            ["git", "rev-parse", f"HEAD:{surface}"],
            cwd=ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip()
        assert pinned == head_object, (
            f"tree_hashes[{surface!r}] pins {pinned} but HEAD:{surface} is "
            f"{head_object} — a compute surface changed after the measured round; "
            f"re-run Stage A so the evidence binds to the merged tree")
        if wc_present:
            commit_object = subprocess.run(
                ["git", "rev-parse", f"{worker_commit}:{surface}"],
                cwd=ROOT, capture_output=True, text=True, check=True,
            ).stdout.strip()
            assert pinned == commit_object, (
                f"tree_hashes[{surface!r}] pins {pinned} but {worker_commit}:{surface} "
                f"is {commit_object} — the cited worker_commit did not contain the "
                f"measured compute surface")

    assert repro["live_validation_record_sha256"] == _sha256_lf(LIVE_VALIDATION_RECORD)
    assert repro["governance"] == live["governance"]
    _assert_governance_blocked(repro, where=REPRODUCIBILITY_RECORD.name)


# --- 7. slo_threshold_amendment_record -------------------------------------------

def test_slo_threshold_amendment_record_derives_and_inherits_correctly() -> None:
    # The amendment record is CONDITIONAL evidence: the runner writes it only when the
    # round breaches the Phase 1 latency threshold, and its no-breach branch deletes any
    # leftover amendment. Source-of-truth for "was there an amendment?" is the
    # conformance record's latency status. If a future official rerun comes in under the
    # Phase 1 threshold, it publishes a plain `pass` conformance with NO amendment, and
    # this guard must not fail on the (correctly) absent record — it asserts the absence
    # instead. When latency is `pass_amended` (this round), the full derive+inherit chain
    # below runs and additionally binds the conformance -> amendment sha256.
    conformance = _load_strict(CONFORMANCE_RECORD)
    latency = conformance["conformance"]["latency_slo"]
    if latency["status"] != "pass_amended":
        assert not AMENDMENT_RECORD.exists(), (
            "conformance latency is plain `pass` but an amendment record is present; "
            "the runner should have removed it on the no-breach branch")
        assert "amendment_sha256" not in latency
        return

    amendment = _load_strict(AMENDMENT_RECORD)
    live = _load_strict(LIVE_VALIDATION_RECORD)
    assert latency["amendment_sha256"] == _sha256_lf(AMENDMENT_RECORD), (
        "conformance record references a different amendment than the committed one")
    thresholds_doc = _load_strict(THRESHOLDS)
    phase1 = {slo["id"]: slo for slo in thresholds_doc["slos"]}

    amended = amendment["amended_slo"]
    assert amended["id"] == "latency_slo"

    measured = amendment["measured_round"]
    assert amended["threshold"] == math.ceil(1.5 * measured["p95_ms"])
    assert measured["reproducibility_record_sha256"] == _sha256_lf(REPRODUCIBILITY_RECORD)

    inherited = amendment["inherited_unchanged"]
    assert inherited["source"]["path"].endswith("monitoring_thresholds_record.json")
    assert inherited["source"]["sha256"] == _sha256_lf(THRESHOLDS)
    inherited_slos = {slo["id"]: slo for slo in inherited["slos"]}
    assert set(inherited_slos) == {"memory_slo", "error_rate_slo", "retry_rate_slo"}
    for slo_id, slo in inherited_slos.items():
        assert slo["threshold"] == phase1[slo_id]["threshold"], slo_id

    approvals = amendment["approval_matrix"]["approvals"]
    roles = [entry["role"] for entry in approvals]
    assert set(roles) == REQUIRED_OWNER_ROLES
    assert len(roles) == len(REQUIRED_OWNER_ROLES)  # no duplicates
    assert "engineering" not in roles
    for entry in approvals:
        assert isinstance(entry["owner"], str) and entry["owner"].strip(), entry["role"]
    assert amendment["approval_matrix"]["approval_matrix_complete"] is True

    assert amendment["governance"] == live["governance"]
    _assert_governance_blocked(amendment, where=AMENDMENT_RECORD.name)


# --- 8. slo_conformance_record ---------------------------------------------------

def test_slo_conformance_record_pins_measured_vs_signed_thresholds() -> None:
    conformance = _load_strict(CONFORMANCE_RECORD)
    live = _load_strict(LIVE_VALIDATION_RECORD)
    thresholds_doc = _load_strict(THRESHOLDS)
    phase1 = {slo["id"]: slo["threshold"] for slo in thresholds_doc["slos"]}

    sources = conformance["thresholds_source"]
    assert isinstance(sources, list) and sources
    for entry in sources:
        assert set(entry) >= {"path", "sha256"}
        real = ROOT / entry["path"]
        assert real.is_file(), entry["path"]
        assert entry["sha256"] == _sha256_lf(real), entry["path"]

    # Ground truth: recompute every SLO metric from the 16 recorded runs so a stale or
    # hand-edited conformance record cannot certify measured values from a different
    # round (a run whose actual p95 or peak memory breached) while CI stays green.
    recomputed = _recompute_slo_from_runs(_load_strict(REPRODUCIBILITY_RECORD))
    measured = conformance["measured"]
    assert measured["memory_peak_bytes"] == recomputed["memory_peak_bytes"]
    assert measured["error_rate"] == recomputed["error_rate"]
    assert measured["retry_rate"] == recomputed["retry_rate"]
    assert measured["latency_p95_ms"] == recomputed["latency_p95_ms"]
    assert measured["latency_p95_ms_per_leg"] == recomputed["latency_p95_ms_per_leg"]

    conf = conformance["conformance"]
    # the per-SLO self-reported `measured` must equal the run-derived ground truth too
    assert conf["memory_slo"]["measured"] == recomputed["memory_peak_bytes"]
    assert conf["error_rate_slo"]["measured"] == recomputed["error_rate"]
    assert conf["retry_rate_slo"]["measured"] == recomputed["retry_rate"]
    for slo_id in ("memory_slo", "error_rate_slo", "retry_rate_slo"):
        row = conf[slo_id]
        assert row["status"] == "pass", slo_id
        assert row["measured"] <= row["threshold"], slo_id
        assert row["threshold"] == phase1[slo_id], slo_id

    latency = conf["latency_slo"]
    assert latency["status"] in {"pass", "pass_amended"}
    if latency["status"] == "pass_amended":
        assert latency["phase1_threshold"] == 37826
        # the certified p95 is the one actually measured over the 16 runs, and it must
        # still sit under the amended ceiling — recompute, don't trust the field alone
        assert latency["measured_p95_ms"] == recomputed["latency_p95_ms"]
        assert latency["measured_p95_ms"] <= latency["amended_threshold"]
        assert latency["amendment_sha256"] == _sha256_lf(AMENDMENT_RECORD)
        # the amendment derives its ceiling from the SAME measured p95
        amendment = _load_strict(AMENDMENT_RECORD)
        assert amendment["measured_round"]["p95_ms"] == recomputed["latency_p95_ms"]
    else:
        assert recomputed["latency_p95_ms"] <= phase1["latency_slo"]

    assert conformance["verdict"] == "conform"
    assert conformance["governance"] == live["governance"]
    _assert_governance_blocked(conformance, where=CONFORMANCE_RECORD.name)


# --- 8b. measurement machinery: reproducible fingerprint + image pin -------------

def test_canonical_hash_excludes_worker_commit() -> None:
    """The logical fingerprint must be reproducible from ANY commit: provenance.
    worker_commit is excluded so re-running the official path from the evidence commit
    (which injects that commit as worker_commit) reproduces the cited hash instead of
    tripping the cited-record gate. Every other field still affects the hash."""
    from harness.direct_activation.measure_stage_a_child import canonical_hash

    base = {"decision_today": {"quadrant": "expansion"},
            "provenance": {"worker_commit": "a" * 40, "modules": ["m"]}}
    other_commit = {"decision_today": {"quadrant": "expansion"},
                    "provenance": {"worker_commit": "b" * 40, "modules": ["m"]}}
    assert canonical_hash(base) == canonical_hash(other_commit)  # worker_commit ignored

    changed_modules = {"decision_today": {"quadrant": "expansion"},
                       "provenance": {"worker_commit": "a" * 40, "modules": ["m2"]}}
    changed_decision = {"decision_today": {"quadrant": "contraction"},
                        "provenance": {"worker_commit": "a" * 40, "modules": ["m"]}}
    assert canonical_hash(base) != canonical_hash(changed_modules)
    assert canonical_hash(base) != canonical_hash(changed_decision)


def test_official_measure_run_rejects_non_default_image() -> None:
    """An official run writing to the committed Stage A dir must measure on the Phase 1
    image so the SLO evidence binds to the activation runtime; a non-default --image is
    refused before any measurement (mirrors the --skip-container / --worker-commit /
    non-8 run-count guards on the committed path)."""
    from harness.direct_activation import measure_stage_a as ms

    with pytest.raises(SystemExit):
        ms.main(["--image", "some-other-image:latest"])


# --- 9. cross-record identity pins -----------------------------------------------

def test_stage_a_records_share_identity_and_worker_commit() -> None:
    live = _load_strict(LIVE_VALIDATION_RECORD)
    repro = _load_strict(REPRODUCIBILITY_RECORD)
    conformance = _load_strict(CONFORMANCE_RECORD)

    records = [live, repro, conformance]
    # The amendment record exists only when this round breached the Phase 1 latency
    # threshold (`pass_amended`); a future no-breach official rerun deletes it. Load it
    # into the shared-identity checks only when the conformance record says it exists,
    # so a no-amendment rerun does not fail here with FileNotFoundError before checking
    # the remaining records (mirrors the amendment-aware gate in the amendment test).
    if conformance["conformance"]["latency_slo"]["status"] == "pass_amended":
        records.append(_load_strict(AMENDMENT_RECORD))

    for record in records:
        assert record["direct_activation_id"] == DIRECT_ACTIVATION_ID
        assert record["stage"] == "A"

    assert repro["job_identity"]["worker_commit"] == live["provenance"]["worker_commit"]
