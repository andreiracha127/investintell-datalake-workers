"""Regression coverage for one contract shared by seed and transactional loading."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from decimal import Decimal

import pytest

from tools.nport_dera.nport_bulk_parse import CSV_COLS
from tools.nport_secapi import contract, convert, validate


def _seed(directory: Path, *, pct: str = "100", market_value: str = "100", rows: int = 1) -> Path:
    path = directory / "2026-05-31.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_COLS)
        for i in range(rows):
            writer.writerow([
                "2026-05-31", "0000000001", f"KEY{i}", "US1234567890", "Issuer", "EC", "CORP",
                market_value, "1", "USD", pct, "false", "1", "S1",
            ])
    return path


@pytest.mark.parametrize("pct", ["1", ""])
def test_small_new_series_must_pass_percentage_bands(tmp_path, pct):
    profile = validate.profile_csv(str(_seed(tmp_path, pct=pct)))
    assert any("sum to 100" in p for p in validate.verdict(profile))


def test_missing_market_value_blocks_an_otherwise_clean_series(tmp_path):
    profile = validate.profile_csv(str(_seed(tmp_path, market_value="")))
    assert any("market value" in p for p in validate.verdict(profile))


def _container(directory: Path, holdings: list) -> Path:
    path = directory / "2026-07.jsonl"
    path.write_text(json.dumps({
        "submissionType": "NPORT-P", "accessionNo": "0000000001-26-000001",
        "filedAt": "2026-07-20T00:00:00Z",
        "genInfo": {"repPdDate": "2026-05-31", "regCik": "1", "seriesId": "S1"},
        "invstOrSecs": holdings,
    }) + "\n", encoding="utf-8")
    return path


def _holding(**values) -> dict:
    return {"cusip": "123456789", "identifiers": {"isin": {"value": "US1234567890"}},
            "valUSD": 100, "pctVal": 100, "balance": 1, "curCd": "USD", **values}


def test_manifest_accounts_for_malformed_loss_without_counting_policy_duplicates(tmp_path):
    source = _container(tmp_path, [_holding(), None, "broken", _holding()])
    manifest = convert.convert([str(source)], str(tmp_path / "seed"))
    date = manifest["report_dates"]["2026-05-31"]
    assert date["source_holdings"] == 4
    assert date["malformed_holdings"] == 2
    assert date["malformed_share"] == 0.5
    assert date["conflict_key_dupes"] == 1
    assert date["filing_quality"] == [{
        "accession": "0000000001-26-000001", "series_id": "S1", "source_holdings": 4,
        "malformed_holdings": 2, "rows": 1, "conflict_key_dupes": 1,
        "source_pct_sum": "200", "source_pct_present": 2,
    }]


def _aggregate(*, series="S1", rows=100, isin=100, missing=0, pct="100") -> dict:
    return {"series_id": series, "rows": rows, "isin": isin, "mv_missing": missing,
            "pct_sum": Decimal(pct), "pct_missing": 0, "market_value_usd_total": 100, "usd_rows": rows}


def test_quality_verdict_compares_market_value_counts_at_one_percent_boundary():
    at = contract.profile_series("2026-05-31", [_aggregate(rows=100000, isin=100000, missing=1000)])
    above = contract.profile_series("2026-05-31", [_aggregate(rows=100000, isin=100000, missing=1001)])
    assert contract.quality_verdict(at) == []
    assert any("missing market value" in p for p in contract.quality_verdict(above))


def test_isin_floor_uses_exact_counts_even_when_display_rounds_to_floor():
    readings, bad = contract.judge_isin_fill({"2026-05-31": (100000, 89996)}, ["2026-05-31"])
    assert readings[0]["isin_fill"] == 0.9
    assert bad == readings
    assert contract.judge_isin_fill({"2026-05-31": (100000, 90000)}, ["2026-05-31"])[1] == []


def test_isin_present_matches_sql_for_null_empty_and_whitespace():
    assert not contract.isin_present(None)
    assert not contract.isin_present("")
    assert contract.isin_present(" ")


def _quality(series: str, *, total: int, bad: int = 0) -> dict:
    return {"accession": f"filing-{series}", "series_id": series, "source_holdings": total,
            "malformed_holdings": bad, "rows": total - bad, "conflict_key_dupes": 0,
            "source_pct_sum": "100", "source_pct_present": total - bad}


def test_per_filing_malformed_gate_cannot_be_diluted_by_clean_date():
    date = {"filing_quality": [_quality("GOOD", total=10000), _quality("BAD", total=2, bad=1)]}
    problems = contract.malformed_verdict(date)
    assert len(problems) == 1 and "filing-BAD" in problems[0]
    assert contract.malformed_verdict(date, only_series={"GOOD"}) == []


def test_malformed_accounting_is_exact_at_one_percent_and_requires_selected_series():
    assert contract.malformed_verdict({"filing_quality": [_quality("S1", total=100, bad=1)]}) == []
    above = contract.malformed_verdict({"filing_quality": [_quality("S1", total=99, bad=1)]})
    assert len(above) == 2
    absent = contract.malformed_verdict({"filing_quality": [_quality("S1", total=100)]}, {"S2"})
    assert any("lacks selected series" in p for p in absent)


def test_csv_and_sql_aggregates_produce_identical_contract_profile(tmp_path):
    csv_profile = validate.profile_csv(str(_seed(tmp_path)))
    sql_profile = contract.profile_series("2026-05-31", [_aggregate(rows=1, isin=1)])
    assert {k: v for k, v in csv_profile.items() if k not in {"file", "keys"}} == {
        k: v for k, v in sql_profile.items() if k != "keys"
    }


def test_missing_percentage_is_in_the_series_denominator_without_a_redundant_unit_gate():
    profiles = [_aggregate(series="S1"), _aggregate(series="S2", pct="0")]
    profile = contract.profile_series("2026-05-31", profiles)
    assert profile["series"] == 2 and profile["pct_within_5_count"] == 1
    assert len(contract.quality_verdict(profile)) == 2
    # Bands are proportions: a single extreme observation is reported instead
    # of adding a conflicting all-series hard gate to a clean cohort.
    profiles = [_aggregate(series=f"S{i}") for i in range(9)] + [_aggregate(series="EXTREME", pct="1001")]
    profile = contract.profile_series("2026-05-31", profiles)
    assert profile["pct_sum_over_1000"] == 1
    assert contract.quality_verdict(profile) == []


def test_old_monthly_manifest_requires_reconversion(tmp_path):
    path = _seed(tmp_path)
    (tmp_path / "manifest.json").write_text(json.dumps({
        "generated_by": "tools.nport_secapi.convert", "report_dates": {"2026-05-31": {"rows": 1}},
    }), encoding="utf-8")
    assert any("reconvert" in p for p in validate.verdict(validate.profile_csv(str(path))))


def test_source_aware_bands_accept_legitimate_leverage_and_cash():
    profile = contract.profile_series("2026-05-31", [_aggregate(pct="160.9")], reference_sums={"S1": "160.85"})
    assert profile["pct_sum_within_5"] == 0.0  # descriptive 100-centered reading
    assert contract.quality_verdict(profile) == []
    profile = contract.profile_series("2026-05-31", [_aggregate(pct="22.3")], reference_sums={"S1": "22.2"})
    assert contract.quality_verdict(profile) == []


def test_source_aware_bands_refuse_units_and_dropped_derivative_weights():
    scaled = contract.profile_series("2026-05-31", [_aggregate(pct="1")], reference_sums={"S1": "100"})
    lost = contract.profile_series("2026-05-31", [_aggregate(pct="414.75")], reference_sums={"S1": "-2.13"})
    assert len(contract.quality_verdict(scaled)) == 2
    assert len(contract.quality_verdict(lost)) == 2


def test_missing_percentages_block_even_when_source_reference_is_zero():
    aggregate = _aggregate(pct="0")
    aggregate["pct_missing"] = aggregate["rows"]
    profile = contract.profile_series("2026-05-31", [aggregate], reference_sums={"S1": "0"})
    assert any("no percentage" in p for p in contract.quality_verdict(profile))


def test_percentage_band_preserves_decimal_precision_at_boundary():
    outside = "94.9999999999999999999999999999999999999999999"
    profile = contract.profile_series("2026-05-31", [_aggregate(pct=outside)])
    assert profile["pct_within_5_count"] == 0


def test_source_csv_uses_independent_raw_percentage_sum(tmp_path):
    source = _container(tmp_path, [_holding(pctVal="160.85")])
    out = tmp_path / "seed"
    manifest = convert.convert([str(source)], str(out))
    quality = manifest["report_dates"]["2026-05-31"]["filing_quality"][0]
    assert quality["source_pct_sum"] == "160.85" and quality["source_pct_present"] == 1
    assert validate.verdict(validate.profile_csv(str(out / "2026-05-31.csv"))) == []


@pytest.mark.parametrize('field', ['balance', 'valUSD', 'pctVal'])
def test_invalid_non_null_numeric_holding_is_recorded_and_rejected(tmp_path, field):
    holdings = [_holding(cusip=f'{i:09d}', pctVal=25) for i in range(4)]
    holdings[0][field] = 'broken'
    source = _container(tmp_path, holdings)
    out = tmp_path / 'seed'
    manifest = convert.convert([str(source)], str(out))
    quality = manifest['report_dates']['2026-05-31']['filing_quality'][0]
    assert quality['malformed_holdings'] == 1
    assert quality['rows'] == 3
    assert any('malformed' in p for p in validate.verdict(validate.profile_csv(str(out / '2026-05-31.csv'))))


def test_dropping_a_bad_balance_preserves_its_raw_percentage_reference(tmp_path):
    source = _container(tmp_path, [_holding(balance='broken', pctVal=90),
                                   _holding(cusip='987654321', pctVal=10)])
    out = tmp_path / 'seed'
    quality = convert.convert([str(source)], str(out))['report_dates']['2026-05-31']['filing_quality'][0]
    assert quality['source_pct_sum'] == '100'
    assert quality['source_pct_present'] == 2 and quality['malformed_holdings'] == 1
    profile = validate.profile_csv(str(out / '2026-05-31.csv'))
    assert profile['pct_reference_within_5'] == 0
