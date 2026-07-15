"""Stage A executor: pre-activation live validation of the signed candidate.

Usage (from the repo root):

    python -m harness.direct_activation.live_validation

Computes TODAY's consumable decision + compressed_50 allocation for the signed
candidate over **committed pack v2 + the pinned live delta snapshot** — the entire
latched global chain is reconstituted from pinned inputs, so a carried position's
seed is fully provenance-pinned (a run whose carry seed cannot be reconstructed
FAILS). Fail-loud gates: staleness (Phase 1 criteria), composition conflicts,
non-NaN prices for all six sleeve tickers (the PriceFrame silent-zero risk),
well-formed decision, weight constraints. Writes ``live_validation_record.json``.

Governance: Stage A changes NOTHING — read-only over committed/pinned inputs;
A5 stays blocked; no DB write of any kind.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from harness.phase0q import decision as decision_mod
from harness.phase0q import sleeve as sleeve_mod
from scripts.p1_export.export_p1_sources import DB_SOURCE as P1_DB_SOURCE
from src.macro_sources import SEED_SOURCES

ROOT = Path(__file__).resolve().parents[2]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_003"
STAGE_A = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_a_001"
SNAPSHOT = STAGE_A / "input_snapshot"

VALIDATION_AS_OF = _dt.date(2026, 7, 3)
PACK_CUT = _dt.date(2026, 6, 30)       # pack v2 manifest as_of (delta lower bound)
PRICE_OVERLAP_START = _dt.date(2026, 6, 26)  # delta re-exports the pack price tail from here
CHAIN_START = _dt.date(2014, 3, 1)
PACK_SHA256_PIN = "caa78716aa641823cdb04482b23e2251c34c69d2ec7368eb78f411b570434bad"
CANDIDATE = sleeve_mod.SleeveParams(candidate_id="open_macro_v03_compressed_50")

# The SEED macro basket the decision consumes (imported from SEED_SOURCES, the one
# authoritative definition). Every one of these MUST be present in the composed
# vintages and declared by the snapshot manifest; a silently-dropped low-weight
# series (e.g. MICH, 0.20 on the inflation axis) would leave the decision at 0.80
# coverage while dodging the freshness gate, so absence is a fail-loud error.
EXPECTED_SEED_SERIES = tuple(sorted(spec.series_id for spec in SEED_SOURCES))

# Deterministic snapshot-manifest identity/provenance the consume path binds in full
# (so false metadata over the same delta bytes is rejected here, not only by the CI
# reconstruction test). These mirror the exporter's build_manifest constants.
DIRECT_ACTIVATION_ID = "open_macro_v03_direct_activation_001"
SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_ARTIFACT_TYPE = "direct_activation_input_snapshot_manifest"
DB_SERVICE_ID = P1_DB_SOURCE
DB_SOURCE = "production datalake (read-only)"
DB_NAME = "market"
DB_SCHEMA = "public"
DB_TABLES = ["eod_prices", "macro_observation_vintage"]

# staleness criteria (Phase 1 staleness_verification_record semantics)
MONTHLY_MAX_AGE_DAYS = 45
MICH_MAX_AGE_DAYS = 90          # documented publication-lag policy
PRICE_MAX_AGE_BUSINESS_DAYS = 3

_VINTAGE_KEY = ("series_id", "observation_period", "revision_number", "available_at")


class LiveValidationError(AssertionError):
    """A live-validation gate did not hold; no record may be written."""


def _require(condition: bool, note: str) -> None:
    if not condition:
        raise LiveValidationError(note)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    # CRLF-normalized so the pin is checkout-independent (the canonical bytes are
    # the LF git blob; a Windows autocrlf checkout must hash identically).
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _as_of_end_utc(day: _dt.date) -> str:
    """End-of-day UTC upper bound the exporter stamps for a given as-of date."""
    return f"{day.isoformat()}T23:59:59.999999+00:00"


def verify_snapshot_manifest(manifest: dict[str, Any], delta_vintages: list[dict],
                             delta_prices: list[dict], *,
                             snapshot_dir: Path = SNAPSHOT) -> None:
    """Bind the whole signed manifest BEFORE deriving anything.

    The measurement clean-tree gate only covers compute surfaces (harness/, src/, ...)
    and ignores the artifact snapshot dir, and no Stage A record pins the manifest
    hash, so a manifest regenerated with the SAME delta hashes/row counts but a
    different as-of, query bound, SEED basket, sleeve, or pack cut would otherwise be
    certified alongside false metadata. Fail loud if any consumed byte OR any
    deterministic query field diverges from what this executor actually computes over.
    """
    _require(manifest["pack_v2_sha256"] == PACK_SHA256_PIN,
             "snapshot manifest: pack_v2_sha256 diverged from the signed pin")
    for name, rows in (("delta_macro_vintages.json", delta_vintages),
                       ("delta_eod_prices.json", delta_prices)):
        entry = manifest["files"][name]
        _require(_sha256_file(snapshot_dir / name) == entry["sha256"],
                 f"snapshot manifest: {name} sha256 diverged from the manifest pin")
        _require(len(rows) == entry["rows"],
                 f"snapshot manifest: {name} row count diverged from the manifest pin")

    # Deterministic query fields — the metadata an auditor reads to know WHAT was
    # validated must match the executor's own pinned parameters, not just the bytes.
    _require(manifest["validation_as_of"] == VALIDATION_AS_OF.isoformat(),
             "snapshot manifest: validation_as_of diverged from the pinned as-of")
    qp = manifest["query_parameters"]
    _require(qp["validation_as_of"] == VALIDATION_AS_OF.isoformat(),
             "snapshot manifest: query validation_as_of diverged from the pinned as-of")
    _require(qp["pack_cut"] == PACK_CUT.isoformat(),
             "snapshot manifest: pack_cut diverged from the pinned pack cut")
    _require(qp["price_overlap_start"] == PRICE_OVERLAP_START.isoformat(),
             "snapshot manifest: price_overlap_start diverged from the pinned "
             "overlap window used to revalidate recent price bars")
    _require(frozenset(qp["series_ids"]) == frozenset(EXPECTED_SEED_SERIES),
             "snapshot manifest: series_ids diverged from the SEED basket")
    _require(frozenset(qp["tickers"]) == frozenset(sleeve_mod.SLEEVE_TICKERS),
             "snapshot manifest: tickers diverged from the sleeve")
    _require(qp["vintage_available_at_lower_exclusive"] == _as_of_end_utc(PACK_CUT),
             "snapshot manifest: vintage lower bound diverged from the pack cut")
    _require(qp["vintage_available_at_upper_inclusive"] == _as_of_end_utc(VALIDATION_AS_OF),
             "snapshot manifest: vintage upper bound diverged from the as-of ceiling")

    # Full-manifest binding: reconstruct the ENTIRE expected manifest deterministically
    # and require exact equality, so identity/provenance metadata (artifact_type, stage,
    # direct_activation_id, schema_version, provenance.source/db_service_id/tables,
    # staleness_gate_at_export) and any EXTRA keys are bound too — the executor itself is
    # the fail-loud manifest binding for the measurement path, not only the CI test.
    expected = {
        "artifact_type": SNAPSHOT_ARTIFACT_TYPE,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "stage": "A",
        "direct_activation_id": DIRECT_ACTIVATION_ID,
        "validation_as_of": VALIDATION_AS_OF.isoformat(),
        "pack_v2_sha256": PACK_SHA256_PIN,
        "query_parameters": {
            "validation_as_of": VALIDATION_AS_OF.isoformat(),
            "price_overlap_start": PRICE_OVERLAP_START.isoformat(),
            "pack_cut": PACK_CUT.isoformat(),
            "vintage_available_at_lower_exclusive": _as_of_end_utc(PACK_CUT),
            "vintage_available_at_upper_inclusive": _as_of_end_utc(VALIDATION_AS_OF),
            "series_ids": list(EXPECTED_SEED_SERIES),
            "tickers": list(sleeve_mod.SLEEVE_TICKERS),
        },
        "files": {
            "delta_eod_prices.json": {
                "sha256": _sha256_file(snapshot_dir / "delta_eod_prices.json"),
                "rows": len(delta_prices),
            },
            "delta_macro_vintages.json": {
                "sha256": _sha256_file(snapshot_dir / "delta_macro_vintages.json"),
                "rows": len(delta_vintages),
            },
        },
        "staleness_gate_at_export": "pass",
        "provenance": {
            "source": DB_SOURCE,
            "db_service_id": DB_SERVICE_ID,
            "db_name": DB_NAME,
            "db_schema": DB_SCHEMA,
            "tables": DB_TABLES,
        },
    }
    _require(manifest == expected,
             "snapshot manifest: full manifest diverged from the deterministic "
             "reconstruction (identity/provenance/extra-key tamper)")


def compose_rows(base: list[dict], delta: list[dict], key_fields: tuple[str, ...],
                 *, what: str) -> list[dict]:
    """Pack rows + delta rows with deterministic dedup; a same-key VALUE conflict is
    a fail-loud error (an overlapping export must agree byte-for-byte)."""
    merged: dict[tuple, dict] = {}
    for row in base:
        merged[tuple(row[f] for f in key_fields)] = row
    for row in delta:
        key = tuple(row[f] for f in key_fields)
        if key in merged:
            _require(merged[key] == row,
                     f"{what}: overlapping key {key} disagrees between pack and delta")
        merged[key] = row
    return list(merged.values())


def vintage_delta_window_gate(delta_vintages: list[dict]) -> None:
    """Every delta vintage row's ``available_at`` MUST fall in the exporter's delta
    window (PACK_CUT, VALIDATION_AS_OF] — strictly after the pack cut, no later than the
    as-of ceiling.

    ``compose_rows`` only cross-checks keys present in BOTH pack and delta, so a
    regenerated/hash-pinned delta carrying a PRE-cut vintage revision (or a post-as-of
    one) would be merged silently and could alter the certified pack-v2 PIT history while
    the manifest still claims the delta starts after the pack cut. Enforce the row-level
    bounds before composing."""
    lower_exclusive = _dt.datetime.fromisoformat(_as_of_end_utc(PACK_CUT))
    upper_inclusive = _dt.datetime.fromisoformat(_as_of_end_utc(VALIDATION_AS_OF))
    for row in delta_vintages:
        raw = row["available_at"]
        parsed = _dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:                 # normalize a naive stamp to UTC
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        _require(lower_exclusive < parsed <= upper_inclusive,
                 f"vintage delta window: {row.get('series_id')} available_at {raw} is "
                 f"outside (PACK_CUT {PACK_CUT}, VALIDATION_AS_OF {VALIDATION_AS_OF}]; a "
                 f"pre-cut/post-as-of delta vintage cannot silently restate pack history")


def overlap_completeness_gate(pack_prices: list[dict], delta_prices: list[dict]) -> None:
    """Every pack sleeve price key in ``PRICE_OVERLAP_START..PACK_CUT`` MUST be present
    in the delta.

    The overlap tail exists so composition re-validates that prod has not restated
    recent bars — but ``compose_rows`` only fails on a same-key VALUE conflict, i.e. it
    cross-checks bars present in BOTH pack and delta. A bar the delta OMITS in the
    overlap is silently kept from the pack with no re-validation. The exporter runs this
    check before writing, but the consume path must run it too: a hash-pinned delta that
    drops overlap bars would otherwise be composed (and certified) without the pack tail
    ever being re-checked against the pinned snapshot."""
    sleeve = set(sleeve_mod.SLEEVE_TICKERS)
    lo, hi = PRICE_OVERLAP_START.isoformat(), PACK_CUT.isoformat()
    delta_keys = {(r["ticker"], r["date"]) for r in delta_prices}
    missing = sorted((row["ticker"], row["date"]) for row in pack_prices
                     if row["ticker"] in sleeve and lo <= row["date"] <= hi
                     and (row["ticker"], row["date"]) not in delta_keys)
    _require(not missing,
             f"overlap-completeness gate: the delta omits {len(missing)} pack overlap "
             f"bar(s) {missing[:5]}; the {lo}..{hi} tail was not re-validated "
             f"byte-for-byte against the pinned delta")


def staleness_report(vintage_rows: list[dict], price_rows: list[dict],
                     as_of: _dt.date = VALIDATION_AS_OF) -> dict[str, Any]:
    """Non-raising freshness report as of ``as_of`` with the FULL hardened Stage A
    semantics, breach-listed instead of first-failure-raised.

    The Stage B worker consumes this to write a durable staleness-block ledger row
    listing EVERY violated condition (a POSITIVE, replayable record) rather than
    crashing on the first one. Every hardened check of the raising gate is preserved
    verbatim as a reported breach:

    * SEED basket completeness FIRST — a SEED series absent from the composed
      vintages is a reported breach (worker semantics: input unavailability is a
      staleness/availability block, recorded in the ledger and cross-checked by the
      monitor/Stage C verifier — never a silent pass at 0.80 coverage). The raising
      ``staleness_gate`` wrapper still fails loud for Stage A.
    * Freshness anchored on the LATEST OBSERVATION PERIOD's PIT-selected vintage
      (newest ``available_at`` WITHIN the newest ``observation_period``), NEVER on
      ``max(available_at)`` over all vintages — a recent revision to an old
      observation cannot mask a stale latest print.
    * Future macro vintages (age < 0) and future price bars are breaches — PIT-
      impossible for honest inputs; ``age <= bound`` alone would read them as fresh.

    Each breach carries the EXACT message the raising gate would use, so the
    wrapper reproduces Stage A behaviour byte-for-byte."""
    breaches: list[dict[str, Any]] = []
    # The freshness loop below only visits series PRESENT in the composed vintages;
    # a SEED series absent altogether would never be age-checked yet the basket would
    # still return `staleness: pass`. Require the full SEED basket to be present FIRST
    # so a dropped low-weight critical source (e.g. MICH) cannot slip past the 90-day
    # gate and be treated as consumable at 0.80 coverage.
    present = {r["series_id"] for r in vintage_rows}
    missing = [sid for sid in EXPECTED_SEED_SERIES if sid not in present]
    if missing:
        # Short-circuit exactly like the original raising gate did: nothing else is
        # derived past an incomplete basket (the basket breach IS the blocking
        # reason; the worker records it verbatim in the ledger stale_detail).
        return {"series": {}, "prices": {}, "breaches": [{
            "kind": "series_basket", "missing_series": missing,
            "message": f"staleness: SEED series absent from composed vintages: {missing}",
        }], "criteria": {"monthly_max_age_days": MONTHLY_MAX_AGE_DAYS,
                         "mich_max_age_days": MICH_MAX_AGE_DAYS,
                         "price_max_age_business_days": PRICE_MAX_AGE_BUSINESS_DAYS}}
    per_series = {}
    for sid in sorted({r["series_id"] for r in vintage_rows}):
        series_rows = [r for r in vintage_rows if r["series_id"] == sid]
        # Freshness is the age of the LATEST OBSERVATION PERIOD's PIT-selected vintage,
        # NOT max(available_at) over all vintages. A legitimate ALFRED/FRED revision to
        # an OLD observation carries a recent available_at; anchoring on max(available_at)
        # would let that recent revision mask a latest observation period that has gone
        # stale (no new print in 45/90d). Anchor on the newest observation period and use
        # the newest available_at WITHIN it (the PIT-selected revision the decision uses).
        latest_obs = max(r["observation_period"] for r in series_rows)
        last = max(_dt.date.fromisoformat(r["available_at"][:10])
                   for r in series_rows if r["observation_period"] == latest_obs)
        age = (as_of - last).days
        bound = MICH_MAX_AGE_DAYS if sid == "MICH" else MONTHLY_MAX_AGE_DAYS
        per_series[sid] = {"latest_observation_period": latest_obs,
                           "last_available_at": last.isoformat(), "age_days": age,
                           "bound_days": bound}
        # A vintage available AFTER the as-of date is future macro data: PIT-impossible
        # for honest inputs (the exporter's upper bound rejects it at export time).
        # `age <= bound` alone would read that as "fresh"; report it as a breach.
        if age < 0:
            breaches.append({
                "kind": "series", "series_id": sid, "reason": "future_vintage",
                "latest_observation_period": latest_obs,
                "last_available_at": last.isoformat(), "age_days": age,
                "message": (f"staleness: {sid} latest observation {latest_obs} available "
                            f"{last} is after as-of {as_of} (future vintage, "
                            f"age {age}d)"),
            })
        elif age > bound:
            breaches.append({
                "kind": "series", "series_id": sid, "reason": "over_max_age",
                "latest_observation_period": latest_obs,
                "last_available_at": last.isoformat(), "age_days": age,
                "bound_days": bound,
                "message": (f"staleness: {sid} latest observation {latest_obs} last "
                            f"available {last} ({age}d > {bound}d)"),
            })
    per_ticker = {}
    for ticker in sleeve_mod.SLEEVE_TICKERS:
        dates = [_dt.date.fromisoformat(r["date"]) for r in price_rows
                 if r["ticker"] == ticker]
        if not dates:
            per_ticker[ticker] = {"last_price_date": None, "business_age_days": None}
            breaches.append({
                "kind": "price", "ticker": ticker, "reason": "no_prices",
                "message": f"staleness: no prices at all for {ticker}",
            })
            continue
        last = max(dates)
        business_age = len([1 for i in range(1, (as_of - last).days + 1)
                            if (last + _dt.timedelta(days=i)).weekday() < 5])
        per_ticker[ticker] = {"last_price_date": last.isoformat(),
                              "business_age_days": business_age}
        # A price bar dated AFTER the as-of is future data (symmetric with the macro
        # future-vintage guard above); a negative day span makes the business_age
        # range() empty -> business_age 0 -> would read as "fresh". Report it.
        if last > as_of:
            breaches.append({
                "kind": "price", "ticker": ticker, "reason": "future_price_bar",
                "last_price_date": last.isoformat(),
                "message": (f"staleness: {ticker} last price {last} is after as-of "
                            f"{as_of} (future price bar)"),
            })
        elif business_age > PRICE_MAX_AGE_BUSINESS_DAYS:
            breaches.append({
                "kind": "price", "ticker": ticker, "reason": "over_max_business_age",
                "last_price_date": last.isoformat(),
                "business_age_days": business_age,
                "bound_business_days": PRICE_MAX_AGE_BUSINESS_DAYS,
                "message": (f"staleness: {ticker} last price {last} "
                            f"({business_age} business days old)"),
            })
    return {"series": per_series, "prices": per_ticker, "breaches": breaches,
            "criteria": {"monthly_max_age_days": MONTHLY_MAX_AGE_DAYS,
                         "mich_max_age_days": MICH_MAX_AGE_DAYS,
                         "price_max_age_business_days": PRICE_MAX_AGE_BUSINESS_DAYS}}


def staleness_gate(vintage_rows: list[dict], price_rows: list[dict],
                   as_of: _dt.date = VALIDATION_AS_OF) -> dict[str, Any]:
    """Fail-loud freshness gate (Stage A semantics): a thin raising wrapper over
    :func:`staleness_report`. The report carries the exact message of every hardened
    check (SEED basket completeness, PIT-anchored latest-observation freshness,
    future-vintage/future-price guards, business-day price age); the FIRST breach in
    check order raises with that same message, so the behaviour is identical to the
    original inline gate. Returns the (breach-free) series/prices/criteria detail."""
    report = staleness_report(vintage_rows, price_rows, as_of)
    for breach in report["breaches"]:
        _require(False, breach["message"])
    return {"series": report["series"], "prices": report["prices"],
            "criteria": report["criteria"]}


def consumable_today(chain: list, as_of: _dt.date = VALIDATION_AS_OF) -> tuple[Any, str, Any]:
    """Today's consumable decision: the LAST valid decision in the latched chain
    (fresh if decided on ``as_of``, carried otherwise) — the ratified carry semantics."""
    valid = [row for row in chain if row.has_valid_quadrant()]
    _require(bool(valid), "no valid decision anywhere in the latched chain (no carry seed)")
    last = valid[-1]
    validity = "fresh" if last.as_of == as_of else "carried"
    return last, validity, last.as_of


def compute(worker_commit_override: str | None = None) -> dict[str, Any]:
    """Compute today's consumable decision + allocation record.

    ``worker_commit_override`` pins ``provenance.worker_commit`` to a caller-supplied
    value INSTEAD of shelling out to ``git rev-parse HEAD``. This exists so a
    measurement child can run inside the hardened quant-engine container (which has
    no git and no ``.git``) with the commit injected by the host runner, keeping the
    logical output byte-identical across host and container legs. When ``None`` the
    behaviour is unchanged: HEAD is read from the ambient clean worktree.
    """
    pack_manifest = _load_json(PACK / "manifest.json")
    _require(pack_manifest["input_pack_sha256"] == PACK_SHA256_PIN,
             "pack v2 sha diverged from the signed pin")

    pack_vintages = _load_json(PACK / "data" / "canonical" / "macro_observation_vintage.json")
    pack_prices = _load_json(PACK / "data" / "canonical" / "eod_prices.json")
    delta_vintages = _load_json(SNAPSHOT / "delta_macro_vintages.json")
    delta_prices = _load_json(SNAPSHOT / "delta_eod_prices.json")

    # Bind the whole signed manifest (consumed bytes + deterministic query fields)
    # BEFORE deriving anything — see verify_snapshot_manifest for why the artifact
    # dir is otherwise unguarded by the measurement clean-tree gate.
    manifest = _load_json(SNAPSHOT / "snapshot_manifest.json")
    verify_snapshot_manifest(manifest, delta_vintages, delta_prices)

    # re-run the exporter's row-level gates on the pinned delta before composing:
    # every delta vintage must fall in the (PACK_CUT, VALIDATION_AS_OF] window, and the
    # price overlap tail must be complete — so a hash-pinned delta cannot restate pack
    # history or drop overlap bars that compose_rows would silently backfill.
    vintage_delta_window_gate(delta_vintages)
    overlap_completeness_gate(pack_prices, delta_prices)

    vintage_rows = compose_rows(pack_vintages, delta_vintages, _VINTAGE_KEY,
                                what="vintages")
    price_rows = compose_rows(pack_prices, delta_prices, ("ticker", "date"),
                              what="prices")

    staleness = staleness_gate(vintage_rows, price_rows)

    chain = decision_mod.run_decision_series(vintage_rows, CHAIN_START, VALIDATION_AS_OF)
    last, validity, seed_as_of = consumable_today(chain)

    # allocation: today's consumable compressed_50 target — the product
    prices = sleeve_mod.PriceFrame(price_rows)
    last_price_date = max(prices.dates)
    available = []
    for ticker in sleeve_mod.SLEEVE_TICKERS:
        price = prices.price(ticker, last_price_date)
        # the PriceFrame silent-zero risk: a NaN price MUST fail here, never
        # propagate into a plausible-but-wrong flat allocation
        _require(price == price and price is not None and price > 0,
                 f"price gate: {ticker} has no usable price at {last_price_date}")
        available.append(ticker)
    weights = sleeve_mod.target_weights(last.quadrant, CANDIDATE, available,
                                        compressed=True)
    total = sum(weights.values())
    _require(abs(total - 1.0) < 1e-9, f"weights do not sum to 1: {total}")
    risk = weights.get("SPY", 0.0) + weights.get("DBC", 0.0)
    defensive = weights.get("TLT", 0.0) + weights.get("SHY", 0.0) + weights.get("TIP", 0.0)
    _require(risk <= sleeve_mod.RISK_CAP_BASELINE + 1e-9, f"risk cap breached: {risk}")
    _require(defensive >= sleeve_mod.DEFENSIVE_FLOOR_BASELINE - 1e-9,
             f"defensive floor breached: {defensive}")

    if worker_commit_override is not None:
        worker_commit = worker_commit_override
    else:
        worker_commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                       capture_output=True, text=True, check=True).stdout.strip()
    valid_chain = [row for row in chain if row.has_valid_quadrant()]
    return {
        "artifact_type": "direct_activation_live_validation_record",
        "schema_version": 1,
        "stage": "A",
        "direct_activation_id": "open_macro_v03_direct_activation_001",
        "validation_as_of": VALIDATION_AS_OF.isoformat(),
        "candidate": {"sleeve": "compressed_50",
                      "candidate_id": CANDIDATE.candidate_id,
                      "risk_cap": sleeve_mod.RISK_CAP_BASELINE,
                      "defensive_floor": sleeve_mod.DEFENSIVE_FLOOR_BASELINE},
        "inputs": {
            "pack_v2_sha256": PACK_SHA256_PIN,
            "delta_vintage_rows": len(delta_vintages),
            "delta_vintages_note": ("legitimately EMPTY: the monthly basket's max "
                                    "available_at (2026-06-15, INDPRO) predates the pack "
                                    "cut (2026-06-30); July releases are not out yet and "
                                    "MICH sits inside its documented publication lag"),
            "delta_price_rows": len(delta_prices),
            "delta_files_sha256": {
                "delta_macro_vintages.json": _sha256_file(SNAPSHOT / "delta_macro_vintages.json"),
                "delta_eod_prices.json": _sha256_file(SNAPSHOT / "delta_eod_prices.json"),
            },
            "composed_vintage_rows": len(vintage_rows),
            "composed_price_rows": len(price_rows),
        },
        "staleness_gate": staleness,
        "decision_today": {
            "as_of": VALIDATION_AS_OF.isoformat(),
            "decision_validity": validity,
            "carry_seed_as_of": seed_as_of.isoformat(),
            "quadrant": last.quadrant,
            "candidate_confidence": round(last.candidate_confidence, 12),
            "coverage_quality": round(last.coverage_quality, 12),
            "status": last.status,
            "chain_length": len(chain),
            "valid_decisions_in_chain": len(valid_chain),
        },
        "allocation_today": {
            "book": "compressed_50",
            "weights": {t: round(weights.get(t, 0.0), 12)
                        for t in sleeve_mod.SLEEVE_TICKERS},
            "priced_at": last_price_date.isoformat(),
            "risk_assets_weight": round(risk, 12),
            "defensive_assets_weight": round(defensive, 12),
        },
        "gates": {
            "pack_pin": "pass", "composition": "pass", "staleness": "pass",
            "carry_seed_reconstructed": "pass", "prices_non_nan_all_six": "pass",
            "weights_constraints": "pass",
        },
        "provenance": {"worker_commit": worker_commit,
                       "modules": ["harness.phase0q.decision", "harness.phase0q.sleeve"]},
        "governance": {"A5": "blocked", "activation_allowed": False,
                       "allocator_publish": False, "db_write_mode": "none",
                       "official_result": False, "runtime_activation": False,
                       "note": "Stage A validates only; every flip happens in Stage B"},
    }


def main() -> int:
    record = compute()
    out = STAGE_A / "live_validation_record.json"
    out.write_text(json.dumps(record, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
                   encoding="utf-8", newline="\n")
    d = record["decision_today"]
    a = record["allocation_today"]
    print(f"decision: {d['quadrant']} ({d['decision_validity']}, seed {d['carry_seed_as_of']}, "
          f"confidence {d['candidate_confidence']})")
    print(f"allocation ({a['book']}): " + " ".join(
        f"{t}={a['weights'][t]:.4f}" for t in sleeve_mod.SLEEVE_TICKERS))
    print("all gates pass; record written (A5 blocked; Stage A validates only)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
