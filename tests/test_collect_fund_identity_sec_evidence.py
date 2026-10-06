"""Pure parts of scripts/collect_fund_identity_sec_evidence (no network)."""

from __future__ import annotations

import hashlib
import json

from scripts import collect_fund_identity_sec_evidence as collect
from scripts import repair_fund_identity_sec_v1 as repair


def test_targets_cover_every_input_a_plan_rests_on():
    plan = {
        "changes": [
            {"evidence": {repair.RULES[1]: {"class_id": "C1", "old_ticker": "OLD", "new_ticker": "NEW"}}},
            {"evidence": {repair.RULES[2]: {"terminated_class_id": "C2", "old_ticker": "DEADX",
                                            "new_ticker": "LIVEX"}}},
            {"evidence": {repair.RULES[4]: {"ticker": "VTI"}}},
            {"evidence": {repair.RULES[5]: {"series_ids": ["S1"]}}},
            {"evidence": {repair.RULES[7]: {"class_id": "C3", "old_class_id": "C4", "ticker": "VVPLX"}}},
        ],
        "review": {
            "orphan_no_current_tiingo_nav": [{"ticker": "FEOTX"}],
            "class_repoint_candidate": [{"terminated_class_id": "C5", "old_ticker": "PINUX",
                                         "new_ticker": "PINZX"}],
            "series_moved_unproven": [{"ticker": "STNC", "sec_class": "C6", "registry_class": "C7"}],
        },
    }
    targets = collect.targets_from_plan(plan)
    assert targets["tiingo"] == ["FEOTX", "LIVEX", "NEW", "PINZX", "VTI"]
    assert ("C1", "OLD") in targets["pairs"] and ("C1", "NEW") in targets["pairs"]
    assert {("C2", "DEADX"), ("C3", "VVPLX"), ("C4", "VVPLX"), ("C5", "PINUX"),
            ("C6", "STNC"), ("C7", "STNC")} <= set(targets["pairs"])
    assert targets["classes"] == ["C2", "C4", "C5"] and targets["series"] == ["S1"]


def test_assemble_is_deterministic_and_loads_as_evidence(tmp_path):
    for name, _sha in repair.SERIES_CLASS_FILES.values():
        (tmp_path / name).write_bytes(name.encode())
    (tmp_path / collect.TICKERS_JSON_NAME).write_bytes(b"{}")
    filing = {"accessionNo": "0001-26-1", "formType": "497", "filedAt": "2026-09-29T00:00:00-04:00",
              "cik": "1", "classes": [{"series": "S1", "class": "C1", "ticker": "NEW", "name": "x"}]}
    sec = {"C1|NEW|first": {"filings": [filing]}, "C1|latest": {"filings": [filing]},
           "S1|series_latest": {"filings": [filing]}}
    tiingo = {"NEW": {"status": 200, "endDate": "2026-10-05", "observed_at": "2026-10-06T06:00:00+00:00"},
              "UNSTAMPED": {"status": 200}}
    raw = collect.assemble(sec, tiingo, tmp_path, "2026-10-06")
    assert raw == collect.assemble(dict(reversed(sec.items())), tiingo, tmp_path, "2026-10-06")
    evidence = repair.parse_evidence(raw, hashlib.sha256(raw).hexdigest())
    assert set(evidence.tiingo) == {"NEW"}
    assert evidence.class_ticker_filings[("C1", "NEW")][0]["role"] == "first_with_ticker"
    assert evidence.class_last_filings["C1"]["accession_no"] == "0001-26-1"
    assert json.loads(raw)["sec_series_class_datasets"]["2026"]["sha256"] == hashlib.sha256(
        repair.SERIES_CLASS_FILES[2026][0].encode()).hexdigest()
