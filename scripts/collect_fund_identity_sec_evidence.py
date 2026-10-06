"""Collect and assemble the pinned evidence bundle of ``repair_fund_identity_sec_v1``.

The repair only decides from pinned inputs; this tool refreshes them. It reads
a plan written by ``python -m scripts.repair_fund_identity_sec_v1 --plan-file``
(changes + review lists), derives what that plan rests on, fetches it and
writes a NEW bundle:

* sec-api.io Query API (``SEC_API_IO_KEY``): for every (class, ticker) pair the
  first and last filing listing the class under that ticker, the newest filing
  listing each terminated class, and the newest filing of each series R6
  deactivates. Forms: prospectus (485BPOS/485APOS/497/497K/497J), N-CEN,
  N-CSR(S), NPORT-P, N-14, N-8F.
* Tiingo daily meta (``TIINGO_API_KEY``): start/end date of every ticker R2, R3
  or R5 depends on, with the observation instant. R2/R3/R5 ignore an
  observation older than 30 days, so re-collect before applying a plan whose
  bundle has aged.
* The sha256 of the SEC series/class datasets and of ``company_tickers_mf.json``
  in ``--dataset-dir``.

Every response is cached (JSON) in ``--cache-dir`` so a re-run only fetches
what is missing (``--refresh-tiingo`` re-observes Tiingo). The database is
never touched. Keys never reach stdout. The output must not exist; after a
review, the operator re-pins ``EVIDENCE_SHA256`` in the repair script.

    python -m scripts.collect_fund_identity_sec_evidence --plan-file plan.json \\
        --dataset-dir E:/tmp-deploy/sec-cache --cache-dir E:/tmp-deploy/sec-cache \\
        --collected-on 2026-10-06 --out contracts/fund-identity-sec/evidence_v2.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import repair_fund_identity_sec_v1 as repair  # noqa: E402

SEC_CACHE_NAME = "secapi_evidence_20261006.json"
TIINGO_CACHE_NAME = "tiingo_meta_20261006.json"
TICKERS_JSON_NAME = "company_tickers_mf_20261006.json"
DATASET_URLS = {
    2026: ("investment-company-series-class-information/investment-company-series-class-2026.csv", "2026-06-01"),
    2025: ("investment-company-series-class-information/investment-company-series-class-2025.csv", "2025-06-02"),
    2024: ("investment-company-series-and-class-information/investment-company-series-class-2024.csv", "2024-06-05"),
    2023: ("investment-company-series-class-information/investment_company_series_class_2023.csv", "2023-06-08"),
}
DATASET_BASE = "https://www.sec.gov/files/investment/data/other/"
FORMS = ("497", "497K", "497J", "485BPOS", "485APOS", "N-CEN", "N-CSR", "N-CSRS", "NPORT-P", "N-14", "N-8F")
FORM_QUERY = "(" + " OR ".join(f'formType:"{form}"' for form in FORMS) + ")"
TIINGO_URL = "https://api.tiingo.com/tiingo/daily/{}"
_TOKEN = re.compile(r"(token=)[^&\s\"']+")


def targets_from_plan(plan: dict) -> dict:
    """What a plan rests on: Tiingo tickers, (class, ticker) pairs, classes, series."""
    tiingo, pairs, classes, series = set(), set(), set(), set()
    for change in plan.get("changes", []):
        for rule, facts in change.get("evidence", {}).items():
            if not isinstance(facts, dict):
                continue
            if rule == repair.RULES[1]:
                pairs |= {(facts["class_id"], facts["old_ticker"]), (facts["class_id"], facts["new_ticker"])}
                tiingo.add(facts["new_ticker"])
            elif rule == repair.RULES[2]:
                pairs.add((facts["terminated_class_id"], facts["old_ticker"]))
                classes.add(facts["terminated_class_id"])
                tiingo.add(facts["new_ticker"])
            elif rule == repair.RULES[4]:
                tiingo.add(facts["ticker"])
            elif rule == repair.RULES[5]:
                series |= set(facts["series_ids"])
            elif rule == repair.RULES[7]:
                pairs.add((facts["class_id"], facts["ticker"]))
                if facts.get("old_class_id"):
                    pairs.add((facts["old_class_id"], facts["ticker"]))
                    classes.add(facts["old_class_id"])
    review = plan.get("review", {})
    for bucket, key in (
        ("orphan_no_current_tiingo_nav", "ticker"),
        ("ticker_renamed_no_current_tiingo", "new_ticker"),
        ("class_repoint_no_current_tiingo", "new_ticker"),
        ("class_repoint_candidate", "new_ticker"),
    ):
        tiingo |= {item[key] for item in review.get(bucket, []) if item.get(key)}
    for item in review.get("class_repoint_candidate", []):
        pairs.add((item["terminated_class_id"], item["old_ticker"]))
        classes.add(item["terminated_class_id"])
    for item in review.get("series_moved_unproven", []):
        for cls in (item.get("sec_class"), item.get("registry_class")):
            if cls:
                pairs.add((cls, item["ticker"]))
    return {"tiingo": sorted(tiingo), "pairs": sorted(pairs), "classes": sorted(classes),
            "series": sorted(series)}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assemble(sec_cache: dict, tiingo_cache: dict, dataset_dir: Path, collected_on: str) -> bytes:
    """The bundle bytes (canonical: sorted keys, indent 1, trailing newline)."""
    pairs, latest, series = [], [], []
    for key, value in sorted(sec_cache.items()):
        parts = key.split("|")
        for filing in value.get("filings", []):
            base = {"accession_no": filing["accessionNo"], "form_type": filing["formType"],
                    "filed_at": filing["filedAt"], "filer_cik": filing["cik"]}
            if len(parts) == 3:
                cls, ticker, role = parts
                for item in filing["classes"]:
                    if item["class"] == cls and item["ticker"] == ticker:
                        pairs.append({"class_id": cls, "ticker": ticker, "series_id": item["series"],
                                      "role": f"{role}_with_ticker", **base})
            elif parts[1] == "latest":
                for item in filing["classes"][:1]:
                    latest.append({"class_id": parts[0], "series_id": item["series"],
                                   "ticker": item["ticker"], "role": "latest_listing", **base})
                break  # only the newest listing
            elif parts[1] == "series_latest":
                series.append({"series_id": parts[0], "role": "latest_filing", **base})
    tiingo = {
        ticker: {k: meta.get(k) for k in ("status", "startDate", "endDate", "exchangeCode", "observed_at")
                 if meta.get(k) is not None}
        for ticker, meta in sorted(tiingo_cache.items()) if "observed_at" in meta
    }
    datasets = {}
    for year, (path, updated) in sorted(DATASET_URLS.items(), reverse=True):
        name = repair.SERIES_CLASS_FILES[year][0]
        datasets[str(year)] = {"file": name, "url": DATASET_BASE + path, "sec_updated": updated,
                               "sha256": _sha(dataset_dir / name)}
    doc = {
        "kind": repair.EVIDENCE_KIND,
        "collected_on": collected_on,
        "notes": "Pinned inputs of scripts/repair_fund_identity_sec_v1.py. 'current SEC' is "
                 "public.sec_company_tickers_mf read in the plan's own snapshot; these are the "
                 "history/corroboration sources.",
        "sec_series_class_datasets": datasets,
        "sec_company_tickers_mf_json": {
            "file": TICKERS_JSON_NAME, "url": "https://www.sec.gov/files/company_tickers_mf.json",
            "sha256": _sha(dataset_dir / TICKERS_JSON_NAME),
            "role": "cross-check of the DB sync; not a decision input",
        },
        "sec_api": {"endpoint": "sec-api.io Query API", "forms": list(FORMS)},
        "class_ticker_filings": sorted(pairs, key=lambda r: (r["class_id"], r["ticker"], r["role"])),
        "class_last_filings": sorted(latest, key=lambda r: r["class_id"]),
        "series_last_filings": sorted(series, key=lambda r: r["series_id"]),
        "tiingo_meta": tiingo,
    }
    return (json.dumps(doc, sort_keys=True, indent=1) + "\n").encode("ascii")


def _slim(filing: dict, classes: set[str]) -> dict:
    out = {"accessionNo": filing.get("accessionNo"), "formType": filing.get("formType"),
           "filedAt": filing.get("filedAt"), "cik": filing.get("cik"), "classes": []}
    for series in filing.get("seriesAndClassesContractsInformation") or []:
        for item in series.get("classesContracts") or []:
            if item.get("classContract") in classes:
                out["classes"].append({"series": series.get("series"), "class": item.get("classContract"),
                                       "ticker": item.get("ticker"), "name": item.get("name")})
    return out


def collect_sec(targets: dict, cache: dict, save, *, api_key: str) -> None:
    from sec_api import QueryApi

    api = QueryApi(api_key=api_key)

    def search(tag: str, query: str, classes: set[str], order: str, size: int) -> None:
        if tag in cache:
            return
        for attempt in range(4):
            try:
                result = api.get_filings({
                    "query": f"{query} AND {FORM_QUERY}", "from": "0", "size": str(size),
                    "sort": [{"filedAt": {"order": order}}],
                })
                break
            except Exception as exc:  # noqa: BLE001 - the SDK raises bare Exception
                print(json.dumps({"retry": tag, "error": _TOKEN.sub(r"\1REDACTED", str(exc))[:160]}))
                time.sleep(2 + 3 * attempt)
        else:
            return
        cache[tag] = {"total": (result.get("total") or {}).get("value"),
                      "filings": [_slim(f, classes) for f in result.get("filings", [])]}
        save()
        time.sleep(0.3)

    base = 'seriesAndClassesContractsInformation.classesContracts.classContract:"{}"'
    for cls in sorted({c for c, _t in targets["pairs"]} | set(targets["classes"])):
        search(f"{cls}|latest", base.format(cls), {cls}, "desc", 2)
    for cls, ticker in targets["pairs"]:
        query = f'{base.format(cls)} AND seriesAndClassesContractsInformation.classesContracts.ticker:"{ticker}"'
        search(f"{cls}|{ticker}|first", query, {cls}, "asc", 1)
        search(f"{cls}|{ticker}|last", query, {cls}, "desc", 1)
    for series in targets["series"]:
        search(f"{series}|series_latest", f'seriesAndClassesContractsInformation.series:"{series}"',
               set(), "desc", 1)


def collect_tiingo(tickers: list[str], cache: dict, save, *, api_key: str, pace: float,
                   refresh: bool) -> None:
    for ticker in tickers:
        if not refresh and "observed_at" in cache.get(ticker, {}):
            continue
        while True:
            request = urllib.request.Request(TIINGO_URL.format(urllib.parse.quote(ticker)),
                                             headers={"Authorization": f"Token {api_key}"})
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    meta = json.loads(response.read().decode())
                    result = {"status": 200, "startDate": meta.get("startDate"),
                              "endDate": meta.get("endDate"), "name": meta.get("name"),
                              "exchangeCode": meta.get("exchangeCode")}
            except urllib.error.HTTPError as exc:
                result = {"status": exc.code}
            except (urllib.error.URLError, TimeoutError, ValueError):
                result = {"status": "error"}
            if result["status"] in (429, "error"):
                time.sleep(120)
                continue
            break
        result["observed_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        cache[ticker] = result
        save()
        time.sleep(pace)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--plan-file", type=Path, required=True)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--collected-on", default=dt.date.today().isoformat())
    parser.add_argument("--tiingo-pace", type=float, default=2.0, help="seconds between Tiingo requests")
    parser.add_argument("--refresh-tiingo", action="store_true")
    parser.add_argument("--offline", action="store_true", help="assemble from the caches only")
    args = parser.parse_args(argv)
    if args.out.exists():
        print(json.dumps({"status": "refused", "reason": "out_exists"}))
        return 2
    plan = json.loads(args.plan_file.read_text(encoding="utf-8"))
    targets = targets_from_plan(plan)
    sec_path, tiingo_path = args.cache_dir / SEC_CACHE_NAME, args.cache_dir / TIINGO_CACHE_NAME
    sec_cache, tiingo_cache = _load(sec_path), _load(tiingo_path)
    if not args.offline:
        sec_key, tiingo_key = os.environ.get("SEC_API_IO_KEY"), os.environ.get("TIINGO_API_KEY")
        if not sec_key or not tiingo_key:
            print(json.dumps({"status": "refused", "reason": "api_keys_missing"}))
            return 3
        collect_sec(targets, sec_cache, lambda: sec_path.write_text(json.dumps(sec_cache, indent=0)),
                    api_key=sec_key)
        collect_tiingo(targets["tiingo"], tiingo_cache,
                       lambda: tiingo_path.write_text(json.dumps(tiingo_cache, indent=0)),
                       api_key=tiingo_key, pace=args.tiingo_pace, refresh=args.refresh_tiingo)
    raw = assemble(sec_cache, tiingo_cache, args.dataset_dir, args.collected_on)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("xb") as handle:
        handle.write(raw)
    print(json.dumps({"status": "written", "out": str(args.out), "sha256": hashlib.sha256(raw).hexdigest(),
                      "targets": {k: len(v) for k, v in targets.items()}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
