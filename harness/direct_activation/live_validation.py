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
from src.macro_sources import SEED_SOURCES

ROOT = Path(__file__).resolve().parents[2]
PACK = ROOT / "fixtures" / "p1_packs" / "open_macro_v03_certified_input_pack_002"
STAGE_A = ROOT / "artifacts" / "a5" / "open_macro_v03_direct_activation_stage_a_001"
SNAPSHOT = STAGE_A / "input_snapshot"

VALIDATION_AS_OF = _dt.date(2026, 7, 3)
PACK_CUT = _dt.date(2026, 6, 30)       # pack v2 manifest as_of (delta lower bound)
CHAIN_START = _dt.date(2014, 3, 1)
PACK_SHA256_PIN = "23a639781853bd53e37eb44359c30a613bc3c82a9dfc5a65c9b5b81f1d04d337"
CANDIDATE = sleeve_mod.SleeveParams(candidate_id="open_macro_v03_compressed_50")

# The SEED macro basket the decision consumes (imported from SEED_SOURCES, the one
# authoritative definition). Every one of these MUST be present in the composed
# vintages and declared by the snapshot manifest; a silently-dropped low-weight
# series (e.g. MICH, 0.20 on the inflation axis) would leave the decision at 0.80
# coverage while dodging the freshness gate, so absence is a fail-loud error.
EXPECTED_SEED_SERIES = tuple(sorted(spec.series_id for spec in SEED_SOURCES))

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
    _require(frozenset(qp["series_ids"]) == frozenset(EXPECTED_SEED_SERIES),
             "snapshot manifest: series_ids diverged from the SEED basket")
    _require(frozenset(qp["tickers"]) == frozenset(sleeve_mod.SLEEVE_TICKERS),
             "snapshot manifest: tickers diverged from the sleeve")
    _require(qp["vintage_available_at_lower_exclusive"] == _as_of_end_utc(PACK_CUT),
             "snapshot manifest: vintage lower bound diverged from the pack cut")
    _require(qp["vintage_available_at_upper_inclusive"] == _as_of_end_utc(VALIDATION_AS_OF),
             "snapshot manifest: vintage upper bound diverged from the as-of ceiling")


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


def staleness_gate(vintage_rows: list[dict], price_rows: list[dict]) -> dict[str, Any]:
    # The freshness loop below only visits series PRESENT in the composed vintages;
    # a SEED series absent altogether would never be age-checked yet the basket would
    # still return `staleness: pass`. Require the full SEED basket to be present FIRST
    # so a dropped low-weight critical source (e.g. MICH) cannot slip past the 90-day
    # gate and be treated as consumable at 0.80 coverage.
    present = {r["series_id"] for r in vintage_rows}
    missing = [sid for sid in EXPECTED_SEED_SERIES if sid not in present]
    _require(not missing, f"staleness: SEED series absent from composed vintages: {missing}")
    per_series = {}
    for sid in sorted({r["series_id"] for r in vintage_rows}):
        last = max(_dt.date.fromisoformat(r["available_at"][:10])
                   for r in vintage_rows if r["series_id"] == sid)
        age = (VALIDATION_AS_OF - last).days
        # A vintage available AFTER the as-of date is future macro data: PIT-impossible
        # for honest inputs (the exporter's upper bound rejects it at export time), so
        # a negative age can only come from a tampered snapshot that passed the manifest
        # gate or a future exporter regression. `age <= bound` alone would read that as
        # "fresh"; fail loud here in the consume path before deriving any decision.
        _require(age >= 0, f"staleness: {sid} last available {last} is after as-of "
                           f"{VALIDATION_AS_OF} (future macro vintage, age {age}d)")
        bound = MICH_MAX_AGE_DAYS if sid == "MICH" else MONTHLY_MAX_AGE_DAYS
        _require(age <= bound, f"staleness: {sid} last available {last} ({age}d > {bound}d)")
        per_series[sid] = {"last_available_at": last.isoformat(), "age_days": age,
                           "bound_days": bound}
    per_ticker = {}
    for ticker in sleeve_mod.SLEEVE_TICKERS:
        dates = [_dt.date.fromisoformat(r["date"]) for r in price_rows
                 if r["ticker"] == ticker]
        _require(bool(dates), f"staleness: no prices at all for {ticker}")
        last = max(dates)
        business_age = len([1 for i in range(1, (VALIDATION_AS_OF - last).days + 1)
                            if (last + _dt.timedelta(days=i)).weekday() < 5])
        _require(business_age <= PRICE_MAX_AGE_BUSINESS_DAYS,
                 f"staleness: {ticker} last price {last} ({business_age} business days old)")
        per_ticker[ticker] = {"last_price_date": last.isoformat(),
                              "business_age_days": business_age}
    return {"series": per_series, "prices": per_ticker,
            "criteria": {"monthly_max_age_days": MONTHLY_MAX_AGE_DAYS,
                         "mich_max_age_days": MICH_MAX_AGE_DAYS,
                         "price_max_age_business_days": PRICE_MAX_AGE_BUSINESS_DAYS}}


def consumable_today(chain: list) -> tuple[Any, str, Any]:
    """Today's consumable decision: the LAST valid decision in the latched chain
    (fresh if decided today, carried otherwise) — the ratified carry semantics."""
    valid = [row for row in chain if row.has_valid_quadrant()]
    _require(bool(valid), "no valid decision anywhere in the latched chain (no carry seed)")
    last = valid[-1]
    validity = "fresh" if last.as_of == VALIDATION_AS_OF else "carried"
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
