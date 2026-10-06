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
* sec-api.io Form N-CEN API: the newest N-CEN listing each series R5 judges
  (``fundTypes`` of Item C.3, where "Underlying fund" marks an underlying fund
  of an insurance separate account), first by registrant, then by series.
* sec-api.io full-text search + EDGAR (``--sec-user-agent``): for each such
  registrant, the 485BPOS filings mentioning insurance separate accounts,
  newest first across all search phrases, until every targeted series is
  covered by a filing header: the sentence restricting the shares (if any,
  searched in every HTML document of the filing) and the series each filing
  covers. The repair decides from the newest
  filing with a restriction sentence covering a series.
* The sha256 of the SEC series/class datasets and of ``company_tickers_mf.json``
  in ``--dataset-dir``.

Every response is cached (JSON) in ``--cache-dir`` so a re-run only fetches
what is missing. Before writing, every answer the plan's targets need must be
in the caches (``--offline`` included); otherwise nothing is written
(``cache_incomplete``). A required answer that cannot be fetched (retries
exhausted, Tiingo included) aborts the run the same way.
``--refresh-tiingo`` re-observes Tiingo; ``--refresh-sec`` drops the
time-varying SEC answers (newest/last filings, N-CEN, prospectus
searches) and keeps only the immutable first-filing ones: use both when
collecting a later bundle. The database is
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
import html
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
NCEN_CACHE_NAME = "secapi_ncen_20261006.json"
PROSPECTUS_CACHE_NAME = "secapi_insurance_prospectus_20261006.json"
EDGAR = "https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/{name}"
PROSPECTUS_PHRASES = (
    '"insurance company separate accounts"',
    '"separate accounts of insurance companies"',
    '"separate accounts of participating insurance companies"',
    '"variable annuity contracts and variable life insurance policies"',
)
_SENTENCE_TARGET = (r"(separate accounts?|variable annuit\w*|variable life|insurance compan\w*|"
                    r"insurance contracts?|insurance products?)")
_SENTENCE_PATTERNS = (
    re.compile(r"[^.]{0,160}\b(offer|offers|offered|sold|sells|available|issued|issues)\b[^.]{0,40}"
               r"\b(only|exclusively|solely)\b\s+(to|through|as|for|by)\b[^.]{0,120}"
               + _SENTENCE_TARGET + r"[^.]{0,160}\.", re.I),
    re.compile(r"[^.]{0,160}\b(only|exclusively|solely)\s+(available|offered|sold)\b[^.]{0,120}"
               + _SENTENCE_TARGET + r"[^.]{0,160}\.", re.I),
    # Tried last, so it never displaces a sentence the patterns above find.
    re.compile(r"[^.]{0,160}\b(purchased|purchase|held|owned|acquired|bought)\b[^.]{0,40}"
               r"\b(only|exclusively|solely)\b\s+(by|through|to)\b[^.]{0,120}"
               + _SENTENCE_TARGET + r"[^.]{0,160}\.", re.I),
)
# Full-text search returns at most this many hits per page (ordered by relevance).
FTS_PAGE_SIZE = 100
FTS_MAX_PAGES = 20
_SERIES_HEADER = re.compile(r"(?:&lt;|<)SERIES-ID(?:&gt;|>)\s*(S\d{9})")
# Answers that change as registrants file: dropped by --refresh-sec.
class CollectionError(RuntimeError):
    """A required answer could not be fetched: never write a partial bundle."""


_TIME_VARYING_SEC = re.compile(r"\|(last|latest|series_latest)$")
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
TIINGO_ATTEMPTS = 5
TIINGO_BACKOFF_SECONDS = 120
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
    # R5 rests on the insurance status of each candidate series.
    insurance: set[tuple[str, str]] = set()
    for change in plan.get("changes", []):
        facts = change.get("evidence", {}).get(repair.RULES[4])
        if isinstance(facts, dict) and facts.get("registrant_cik"):
            insurance.add((facts["series_id"], facts["registrant_cik"]))
    for bucket in ("orphan_insurance_only_class", "orphan_insurance_status_unverified"):
        for item in review.get(bucket, []):
            if item.get("registrant_cik"):
                insurance.add((item["series_id"], item["registrant_cik"]))
    return {"tiingo": sorted(tiingo), "pairs": sorted(pairs), "classes": sorted(classes),
            "series": sorted(series), "insurance": sorted(insurance)}


def extract_restriction(text: str) -> str | None:
    """The first prospectus sentence restricting the shares to insurance channels."""
    flat = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", text))).replace("\ufffd", "'")
    for pattern in _SENTENCE_PATTERNS:
        match = pattern.search(flat)
        if match:
            parts = re.split(r"(?<=[a-z0-9)])\s+(?=[A-Z])", match.group(0).strip())
            sentence = next(
                (part for part in reversed(parts)
                 if re.search(_SENTENCE_TARGET, part, re.I)
                 and re.search(r"\b(only|exclusively|solely)\b", part, re.I)),
                match.group(0).strip(),
            )
            return sentence[:300]
    return None


def header_series(header_html: str) -> list[str]:
    """Series covered by a filing, from its EDGAR ``-index-headers.html``."""
    return sorted(set(_SERIES_HEADER.findall(header_html)))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assemble(sec_cache: dict, tiingo_cache: dict, dataset_dir: Path, collected_on: str,
             ncen_cache: dict | None = None, prospectus_cache: dict | None = None) -> bytes:
    """The bundle bytes (canonical: sorted keys, indent 1, trailing newline)."""
    ncen_series: dict[str, dict] = {}
    for filing in (ncen_cache or {}).values():
        if not filing:
            continue
        for item in filing.get("series", []):
            series_id = item.get("series_id")
            entry = {"accession_no": filing["accession_no"], "filed_at": filing["filed_at"],
                     "period_of_report": filing.get("period_of_report"),
                     "registrant_cik": filing.get("registrant_cik"),
                     "fund_name": item.get("name"), "fund_types": sorted(item.get("fund_types") or [])}
            if series_id and (series_id not in ncen_series
                              or entry["filed_at"] > ncen_series[series_id]["filed_at"]):
                ncen_series[series_id] = entry
    prospectus = []
    for value in (prospectus_cache or {}).values():
        matches = value.get("matches")
        if matches is None:  # single-filing cache entries of the first collector
            matches = [value["match"]] if value.get("match") else []
        for match in matches:
            if not match.get("quote"):
                continue  # no restriction sentence found: says nothing either way
            prospectus.append({
                "registrant_cik": value["registrant_cik"], "accession_no": match["accession_no"],
                "form_type": match.get("form_type"), "filed_at": match.get("filed_at"),
                "document": match.get("document"), "quote": match["quote"],
                "series_ids": match.get("series_ids_header") or match.get("series_ids") or [],
            })
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
        "ncen_series": dict(sorted(ncen_series.items())),
        "insurance_prospectus": sorted(prospectus, key=lambda r: (r["registrant_cik"], r["accession_no"])),
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
            raise CollectionError(f"sec_query_exhausted:{tag}")
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


def collect_ncen(insurance: list, cache: dict, save, *, api_key: str) -> None:
    from sec_api import FormNcenApi

    api = FormNcenApi(api_key=api_key)

    def fetch(tag: str, query: str) -> dict:
        if tag in cache:
            return cache[tag]
        for attempt in range(4):
            try:
                result = api.get_data({"query": query, "from": "0", "size": "1",
                                       "sort": [{"filedAt": {"order": "desc"}}]})
                break
            except Exception as exc:  # noqa: BLE001 - the SDK raises bare Exception
                print(json.dumps({"retry": tag, "error": _TOKEN.sub(r"\1REDACTED", str(exc))[:160]}))
                time.sleep(2 + 3 * attempt)
        else:
            raise CollectionError(f"ncen_query_exhausted:{tag}")
        data = result.get("data", [])
        cache[tag] = {} if not data else {
            "accession_no": data[0].get("accessionNo"), "filed_at": data[0].get("filedAt"),
            "form_type": data[0].get("formType"), "period_of_report": data[0].get("periodOfReport"),
            "registrant_cik": str((data[0].get("registrantInfo") or {}).get("registrantCik") or ""),
            "series": [{"series_id": m.get("mgmtInvSeriesId"), "name": m.get("mgmtInvFundName"),
                        "fund_types": m.get("fundTypes") or []}
                       for m in data[0].get("managementInvestmentQuestionSeriesInfo") or []],
        }
        save()
        time.sleep(0.3)
        return cache[tag]

    by_cik: dict[str, set[str]] = {}
    for series, cik in insurance:
        by_cik.setdefault(str(int(cik)), set()).add(series)
    for cik, wanted in sorted(by_cik.items()):
        filing = fetch(f"cik|{cik}", f'registrantInfo.registrantCik:"{cik}"')
        listed = {item["series_id"] for item in filing.get("series", [])}
        for series in sorted(wanted - listed):
            fetch(f"series|{series}", f'managementInvestmentQuestionSeriesInfo.mgmtInvSeriesId:"{series}"')


def collect_prospectus(insurance: list, cache: dict, save, *, api_key: str, user_agent: str,
                       doc_dir: Path) -> None:
    from sec_api import FullTextSearchApi

    api = FullTextSearchApi(api_key=api_key)
    doc_dir.mkdir(parents=True, exist_ok=True)

    def edgar(cik: int, folder: str, name: str) -> bytes:
        path = doc_dir / f"{folder}_{name}"
        if not path.is_file():
            request = urllib.request.Request(EDGAR.format(cik=cik, folder=folder, name=name),
                                             headers={"User-Agent": user_agent})
            with urllib.request.urlopen(request, timeout=60) as response:
                path.write_bytes(response.read())
            time.sleep(0.2)  # SEC fair access: well under 10 requests per second
        return path.read_bytes()

    wanted_by_cik: dict[str, set[str]] = {}
    for series, cik in insurance:
        wanted_by_cik.setdefault(str(int(cik)), set()).add(series)
    for cik, wanted in sorted(wanted_by_cik.items()):
        tag = f"cik|{cik}"
        if set(cache.get(tag, {}).get("wanted", ())) >= wanted:
            continue
        hits: dict[str, dict] = {}
        for phrase in PROSPECTUS_PHRASES:  # every phrase and page, then the newest filings first
            page = 1
            while True:
                for attempt in range(4):
                    try:
                        result = api.get_filings({"query": phrase, "formTypes": ["485BPOS"],
                                                  "ciks": [cik.zfill(10)], "startDate": "2024-01-01",
                                                  "endDate": dt.date.today().isoformat(),
                                                  "page": str(page)})
                        break
                    except Exception as exc:  # noqa: BLE001 - the SDK raises bare Exception
                        print(json.dumps({"retry": tag,
                                          "error": _TOKEN.sub(r"\1REDACTED", str(exc))[:160]}))
                        time.sleep(2 + 3 * attempt)
                else:
                    raise CollectionError(f"prospectus_search_exhausted:{tag}")
                time.sleep(0.3)
                filings = result.get("filings", [])
                for filing in filings:
                    hits.setdefault(filing["accessionNo"], filing)
                total = int((result.get("total") or {}).get("value") or 0)
                if not filings or page * FTS_PAGE_SIZE >= total:
                    break
                page += 1
                if page > FTS_MAX_PAGES:
                    raise CollectionError(f"prospectus_search_too_many_hits:{tag}")
        matches, covered = [], set()
        # Every hit, newest first, until every targeted series is covered: only an
        # exhaustive scan may record the whole ``wanted`` set as searched.
        for hit in sorted(hits.values(), key=lambda f: f.get("filedAt") or "", reverse=True):
            if wanted <= covered:
                break
            accession = hit["accessionNo"]
            folder = accession.replace("-", "")
            series = header_series(
                edgar(int(cik), folder, f"{accession}-index-headers.html").decode("utf-8", "replace"))
            if not set(series) & (wanted - covered):
                continue  # covers no targeted series that a newer filing left uncovered
            index = json.loads(edgar(int(cik), folder, "index.json"))
            documents = sorted(
                (item for item in index["directory"]["item"]
                 if item["name"].lower().endswith((".htm", ".html")) and "index" not in item["name"].lower()),
                key=lambda item: -int(item.get("size") or 0),
            )
            quote = document = None
            for item in documents:  # every document: a small one may carry the sentence
                quote = extract_restriction(edgar(int(cik), folder, item["name"]).decode("utf-8", "replace"))
                if quote:
                    document = item["name"]
                    break
            matches.append({"accession_no": accession, "form_type": hit.get("formType"),
                            "filed_at": hit.get("filedAt"), "document": document, "quote": quote,
                            "series_ids_header": series})
            covered |= set(series)
        cache[tag] = {"registrant_cik": cik, "wanted": sorted(wanted), "matches": matches}
        save()


def collect_tiingo(tickers: list[str], cache: dict, save, *, api_key: str, pace: float,
                   refresh: bool) -> None:
    for ticker in tickers:
        if not refresh and "observed_at" in cache.get(ticker, {}):
            continue
        for _attempt in range(TIINGO_ATTEMPTS):
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
                time.sleep(TIINGO_BACKOFF_SECONDS)
                continue
            break
        else:
            raise CollectionError(f"tiingo_exhausted:{ticker}")
        result["observed_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        cache[ticker] = result
        save()
        time.sleep(pace)


def missing_cache_entries(targets: dict, sec_cache: dict, tiingo_cache: dict, ncen_cache: dict,
                          prospectus_cache: dict) -> list[str]:
    """Every answer the targets need that the caches do not hold (the collectors' own keys)."""
    missing = []
    for cls in sorted({c for c, _t in targets["pairs"]} | set(targets["classes"])):
        missing += [tag for tag in (f"{cls}|latest",) if tag not in sec_cache]
    for cls, ticker in targets["pairs"]:
        missing += [tag for tag in (f"{cls}|{ticker}|first", f"{cls}|{ticker}|last") if tag not in sec_cache]
    missing += [f"{s}|series_latest" for s in targets["series"] if f"{s}|series_latest" not in sec_cache]
    missing += [f"tiingo|{t}" for t in targets["tiingo"] if "observed_at" not in tiingo_cache.get(t, {})]
    wanted_by_cik: dict[str, set[str]] = {}
    for series, cik in targets["insurance"]:
        wanted_by_cik.setdefault(str(int(cik)), set()).add(series)
    for cik, wanted in sorted(wanted_by_cik.items()):
        filing = ncen_cache.get(f"cik|{cik}")
        if filing is None:
            missing.append(f"ncen|cik|{cik}")
        else:
            listed = {item["series_id"] for item in filing.get("series", [])}
            missing += [f"ncen|series|{s}" for s in sorted(wanted - listed) if f"series|{s}" not in ncen_cache]
        if not set(prospectus_cache.get(f"cik|{cik}", {}).get("wanted", ())) >= wanted:
            missing.append(f"prospectus|cik|{cik}")
    return missing


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
    parser.add_argument("--refresh-sec", action="store_true",
                        help="re-fetch time-varying SEC answers (newest/last filings, N-CEN, prospectus)")
    parser.add_argument("--offline", action="store_true", help="assemble from the caches only")
    parser.add_argument("--sec-user-agent", default=os.environ.get("SEC_USER_AGENT"),
                        help="descriptive User-Agent with a contact e-mail for sec.gov (or SEC_USER_AGENT)")
    args = parser.parse_args(argv)
    if args.out.exists():
        print(json.dumps({"status": "refused", "reason": "out_exists"}))
        return 2
    plan = json.loads(args.plan_file.read_text(encoding="utf-8"))
    targets = targets_from_plan(plan)
    sec_path, tiingo_path = args.cache_dir / SEC_CACHE_NAME, args.cache_dir / TIINGO_CACHE_NAME
    ncen_path, prospectus_path = args.cache_dir / NCEN_CACHE_NAME, args.cache_dir / PROSPECTUS_CACHE_NAME
    sec_cache, tiingo_cache = _load(sec_path), _load(tiingo_path)
    ncen_cache, prospectus_cache = _load(ncen_path), _load(prospectus_path)
    if args.refresh_sec:
        sec_cache = {k: v for k, v in sec_cache.items() if not _TIME_VARYING_SEC.search(k)}
        ncen_cache, prospectus_cache = {}, {}
    if not args.offline:
        try:
            _collect_online(args, targets, sec_cache, tiingo_cache, ncen_cache, prospectus_cache,
                            sec_path, tiingo_path, ncen_path, prospectus_path)
        except CollectionError as exc:
            print(json.dumps({"status": "refused", "reason": str(exc)}))
            return 2
        except _KeysMissing:
            print(json.dumps({"status": "refused", "reason": "api_keys_or_user_agent_missing"}))
            return 3
    missing = missing_cache_entries(targets, sec_cache, tiingo_cache, ncen_cache, prospectus_cache)
    if missing:
        print(json.dumps({"status": "refused", "reason": "cache_incomplete", "missing": len(missing),
                          "first": missing[:10]}))
        return 2
    raw = assemble(sec_cache, tiingo_cache, args.dataset_dir, args.collected_on,
                   ncen_cache, prospectus_cache)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("xb") as handle:
        handle.write(raw)
    print(json.dumps({"status": "written", "out": str(args.out), "sha256": hashlib.sha256(raw).hexdigest(),
                      "targets": {k: len(v) for k, v in targets.items()}}))
    return 0


class _KeysMissing(Exception):
    pass


def _collect_online(args, targets, sec_cache, tiingo_cache, ncen_cache, prospectus_cache,
                    sec_path, tiingo_path, ncen_path, prospectus_path) -> None:
    """Fetch every missing answer; an exhausted required query aborts the whole run."""
    sec_key, tiingo_key = os.environ.get("SEC_API_IO_KEY"), os.environ.get("TIINGO_API_KEY")
    if not sec_key or not tiingo_key or not args.sec_user_agent:
        raise _KeysMissing
    collect_sec(targets, sec_cache, lambda: sec_path.write_text(json.dumps(sec_cache, indent=0)),
                api_key=sec_key)
    collect_ncen(targets["insurance"], ncen_cache,
                 lambda: ncen_path.write_text(json.dumps(ncen_cache, indent=0)), api_key=sec_key)
    collect_prospectus(targets["insurance"], prospectus_cache,
                       lambda: prospectus_path.write_text(json.dumps(prospectus_cache, indent=0)),
                       api_key=sec_key, user_agent=args.sec_user_agent,
                       doc_dir=args.cache_dir / "edgar")
    collect_tiingo(targets["tiingo"], tiingo_cache,
                   lambda: tiingo_path.write_text(json.dumps(tiingo_cache, indent=0)),
                   api_key=tiingo_key, pace=args.tiingo_pace, refresh=args.refresh_tiingo)


if __name__ == "__main__":
    raise SystemExit(main())
