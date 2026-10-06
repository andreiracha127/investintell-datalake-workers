r"""Convert sec-api.io monthly ``form-nport`` containers into loader CSVs.

Input: one or more monthly containers (``2026-07.jsonl.gz``; plain ``.jsonl`` is
accepted too), one parsed NPORT-P filing per line, partitioned by FILING month.
Output: one CSV per ``report_date`` with exactly the header
``tools.nport_dera.nport_parallel_load`` COPYs (``CSV_COLS``), plus a
``manifest.json`` describing what went in and what was left out.

FIELD MAPPING
-------------
=================  =========================================================
report_date        ``genInfo.repPdDate`` (ISO already)
cik                ``genInfo.regCik`` (10 digits; zero-padded if not)
series_id          ``genInfo.seriesId`` -> ``filerInfo.seriesClassInfo.seriesId``
                   -> ``CIK:<cik>`` (funds without a series, as DERA does)
cusip              the conflict key, see below
isin               ``identifiers.isin.value``
issuer_name        ``name`` (not ``title``); ``N/A`` -> empty
asset_class        ``assetCat`` or ``assetConditional.assetCat``
sector             ``issuerCat`` or ``issuerConditional.issuerCat``
market_value       ``valUSD`` truncated toward zero to an integer (USD)
quantity           ``balance``, source digits
currency           ``curCd`` or ``currencyConditional.curCd``
pct_of_nav         ``pctVal``, source digits. Already a percentage: a portfolio
                   sums to ~100, not ~1. Never multiply.
is_restricted      ``isRestrictedSec == 'Y'``
fair_value_level   ``fairValLevel``; ``N/A`` -> empty
=================  =========================================================

JSON is parsed with ``parse_float=Decimal`` so ``quantity`` and ``pct_of_nav``
carry the filing's digits, not a float repr. The DERA files print ``.136`` where
this prints ``0.136``; both COPY to the same ``numeric``.

PARITY WITH WHAT PRODUCTION HOLDS
---------------------------------
Run over the 2026-04/05/06 containers, this converter reproduces the rows the
2026-08-06 load left in ``sec_nport_holdings`` exactly: per series ``n_holdings``,
``total_market_value``, ``coverage_pct`` and ``n_synthetic`` of
``cagg_nport_series_profile`` are equal for all 2,483 series of 2026-02-28, all
6,819 of 2026-03-31 and the 4,051 shared series of 2026-04-30 (the other 81 were
filed on 2026-07-02, in the July container). One mapping differs on purpose: the
``filerInfo`` fallback for ``series_id``. Three ETFs of CIK 0002043390 carry their
series only there; the 2026-08-06 load filed all three under ``CIK:0002043390``,
where they collide on the conflict key.

THE CONFLICT KEY
----------------
``sec_nport_holdings`` is keyed ``(report_date, series_id, cusip)`` and the
loader inserts ``ON CONFLICT DO NOTHING``, so ``cusip`` must be unique per
holding or holdings silently vanish. As in ``nport_bulk_parse._synthetic_cusip``
it is, in priority order: the real CUSIP, ``IS:<isin>``, ``LE:<lei>``, then a
per-holding key. sec-api has no DERA ``HOLDING_ID``, so the last resort is
``H:<accessionNo>:<position>`` (1-based position in ``invstOrSecs``) - unique
inside a filing and stable across re-downloads.

The default ``dera`` key policy is the DERA parser's rule byte for byte, which
is what keeps the parity above. It has a known cost, measured and left as an
explicit decision (``--key-policy strict``): ``999999999`` counts as a real
CUSIP and ``N/A`` as a real LEI/ISIN, so every holding a filer reports that way
folds into one row per series - on 2026-03-31, ~274k holdings (12%), mostly
derivatives with no identifier at all, and one 217-holding filing loaded as 3
rows. ``strict`` keeps them, which also moves the ISIN fill from ~0.98 to ~0.86,
under the 0.90 floor that ``nport_parallel_load``'s verify and
``src/workers/nport_identifier_coverage`` both gate on. Switching needs that
floor re-based on identifiable rows first; it is not a converter-only change.

ONE FILING PER (report_date, series_id)
---------------------------------------
A series reports a month once, but the dataset carries the original NPORT-P,
any NPORT-P/A that restates it, and late filings, possibly in different monthly
containers. Mixing them by key leaves holdings from a superseded filing alive
next to the amendment. So the newest filing with holdings wins wholesale
(``filedAt``, then ``accessionNo``), and only its holdings are emitted. Inside
that filing, a repeated key keeps its first holding - the same row the loader
would keep - so every emitted CSV is already free of conflict-key duplicates and
its ISIN reading predicts the table, not the file.

Usage:
  python -m tools.nport_secapi.convert --out E:\tmp-deploy\nport-q3-seed \
      --min-report-date 2026-05-01 \
      <cache>\form-nport\2026\2026-06.jsonl.gz <cache>\form-nport\2026\2026-07.jsonl.gz ...
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator

from tools.nport_dera.nport_bulk_parse import CSV_COLS

@dataclass(frozen=True)
class KeyPolicy:
    """How a holding's conflict key (``cusip`` column) and ``isin`` are chosen."""

    name: str
    cusip_placeholders: frozenset[str]
    #: Tokens that mean "no identifier" for an ISIN or an LEI. Empty for the
    #: DERA policy, which keeps any non-empty text - ``N/A`` included.
    id_placeholders: frozenset[str]


#: Byte-for-byte the rule ``tools.nport_dera.nport_bulk_parse`` applied to every
#: row already in ``sec_nport_holdings``. Default, so the months loaded from
#: sec-api read like the months loaded from DERA - and like what the 0.90 ISIN
#: floor of the loader's verify and of ``nport_identifier_coverage`` was
#: calibrated on.
DERA_POLICY = KeyPolicy(
    name="dera",
    cusip_placeholders=frozenset({"", "N/A", "NA", "NONE", "000000000", "0", "XXXXXXXXX"}),
    id_placeholders=frozenset({""}),
)
#: Stops placeholder identifiers from folding distinct holdings onto one key:
#: ``999999999`` (and the fillers ``src/bonds/identifiers.py`` rejects) is not a
#: CUSIP, ``N/A`` is not an LEI or an ISIN. Measured on report_date 2026-03-31
#: it keeps ~274k holdings (12%) the DERA rule discards - mostly derivatives
#: without any identifier - and therefore drops the ISIN fill from ~0.98 to
#: ~0.86, under the 0.90 floor both gates use. Opt-in until that floor is
#: re-based on identifiable rows.
STRICT_POLICY = KeyPolicy(
    name="strict",
    cusip_placeholders=frozenset({
        "", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "0",
        "000000000", "999999999", "XXXXXXXXX", "NNNNNNNNN",
    }),
    id_placeholders=frozenset({"", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "0"}),
)
POLICIES = {policy.name: policy for policy in (DERA_POLICY, STRICT_POLICY)}

NPORT_FORMS = frozenset({"NPORT-P", "NPORT-P/A"})

_NULLISH = frozenset({"N/A", "NA", "NONE"})
_ACCESSION_RE = re.compile(r'"accessionNo"\s*:\s*"([^"]+)"')
_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")


def _open_text(path: str):
    return gzip.open(path, "rt", encoding="utf-8") if path.endswith(".gz") else open(path, encoding="utf-8")


def container_month(path: str) -> str:
    """``.../2026/2026-07.jsonl.gz`` -> ``2026-07``. Anchored on the file name."""
    match = _MONTH_RE.match(os.path.basename(path))
    if not match:
        raise ValueError(f"cannot read a YYYY-MM month from container name {path!r}")
    return f"{match.group(1)}-{match.group(2)}"


def _month_add(month: str, n: int) -> str:
    y, m = int(month[:4]), int(month[5:7])
    idx = y * 12 + (m - 1) + n
    return f"{idx // 12:04d}-{idx % 12 + 1:02d}"


def _clean(raw: Any) -> str:
    """Text of a scalar, trimmed, NUL-free, with the DERA null tokens emptied."""
    if raw is None:
        return ""
    text = str(raw).replace("\x00", "").strip()
    return "" if text.upper() in _NULLISH else text


def _num(raw: Any) -> str:
    """A JSON number (int or Decimal) as plain positional digits, '' if absent."""
    if raw is None or isinstance(raw, bool):
        return ""
    if isinstance(raw, int):
        return str(raw)
    try:
        value = raw if isinstance(raw, Decimal) else Decimal(str(raw).strip())
    except InvalidOperation:
        return ""
    if not value.is_finite():
        return ""
    return format(value, "f")


def _bigint(raw: Any) -> str:
    """``valUSD`` -> integer dollars, truncated toward zero like the DERA parser."""
    digits = _num(raw)
    return str(int(Decimal(digits))) if digits else ""


def _identifier(raw: Any, policy: KeyPolicy) -> str:
    text = "" if raw is None else str(raw).replace("\x00", "").strip()
    return "" if text.upper() in policy.id_placeholders else text


def _isin(holding: dict, policy: KeyPolicy) -> str:
    ids = holding.get("identifiers") or {}
    node = ids.get("isin") if isinstance(ids, dict) else None
    if isinstance(node, list):  # defensive: never observed, but cheap
        node = next((n for n in node if isinstance(n, dict) and n.get("value")), None)
    value = node.get("value") if isinstance(node, dict) else node
    return _identifier(value, policy)


def conflict_key(
    holding: dict, isin: str, accession: str, position: int, policy: KeyPolicy = DERA_POLICY,
) -> tuple[str, str]:
    """(cusip_value, kind) with kind in real | IS | LE | H.

    Same priority as ``nport_bulk_parse._synthetic_cusip``; only the last-resort
    key differs, because sec-api carries no DERA ``HOLDING_ID``.
    """
    cusip = "" if holding.get("cusip") is None else str(holding["cusip"]).replace("\x00", "").strip()
    if cusip.upper() not in policy.cusip_placeholders:
        return cusip, "real"
    if isin:
        return f"IS:{isin}", "IS"
    lei = _identifier(holding.get("lei"), policy)
    if lei:
        return f"LE:{lei}", "LE"
    return f"H:{accession}:{position}", "H"


def _first(holding: dict, key: str, conditional: str) -> str:
    value = holding.get(key)
    if value is None:
        cond = holding.get(conditional)
        value = cond.get(key) if isinstance(cond, dict) else None
    return _clean(value)


def holding_row(report_date: str, cik: str, series_id: str, cusip: str, isin: str, h: dict) -> list[str]:
    restricted = "true" if _clean(h.get("isRestrictedSec")).upper() == "Y" else "false"
    return [
        report_date,
        cik,
        cusip,
        isin,
        _clean(h.get("name")),
        _first(h, "assetCat", "assetConditional"),
        _first(h, "issuerCat", "issuerConditional"),
        _bigint(h.get("valUSD")),
        _num(h.get("balance")),
        _first(h, "curCd", "currencyConditional"),
        _num(h.get("pctVal")),
        restricted,
        _clean(h.get("fairValLevel")),
        series_id,
    ]


@dataclass(frozen=True)
class FilingMeta:
    accession: str
    filed_at: str
    form: str
    report_date: str
    series_id: str
    cik: str
    n_holdings: int
    container: str

    @property
    def rank(self) -> tuple[str, str]:
        return (self.filed_at, self.accession)


def _series_and_cik(filing: dict) -> tuple[str, str]:
    gen = filing.get("genInfo") or {}
    filer = filing.get("filerInfo") or {}
    cik = _clean(gen.get("regCik")) or _clean(((filer.get("filer") or {}).get("issuerCredentials") or {}).get("cik"))
    if cik.isdigit():
        cik = cik.zfill(10)
    series = _clean(gen.get("seriesId")) or _clean((filer.get("seriesClassInfo") or {}).get("seriesId"))
    if not series and cik:
        series = f"CIK:{cik}"
    return series, cik


def _iso(raw: Any) -> str | None:
    text = _clean(raw)
    try:
        return dt.date.fromisoformat(text[:10]).isoformat() if text else None
    except ValueError:
        return None


def _iter_filings(path: str) -> Iterator[dict]:
    with _open_text(path) as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


@dataclass
class ScanResult:
    winners: dict[tuple[str, str], FilingMeta] = field(default_factory=dict)
    superseded: collections.Counter = field(default_factory=collections.Counter)
    skipped: collections.Counter = field(default_factory=collections.Counter)
    filings_by_container: dict[str, int] = field(default_factory=dict)


def scan(paths: list[str]) -> ScanResult:
    """Pass 1: pick the winning filing per (report_date, series_id)."""
    result = ScanResult()
    seen_accessions: set[str] = set()
    candidates: dict[tuple[str, str], FilingMeta] = {}
    for path in paths:
        month = container_month(path)
        n = 0
        for filing in _iter_filings(path):
            n += 1
            form = _clean(filing.get("submissionType")).upper()
            if form not in NPORT_FORMS:
                result.skipped[f"form:{form or 'missing'}"] += 1
                continue
            accession = _clean(filing.get("accessionNo"))
            if not accession:
                result.skipped["no_accession"] += 1
                continue
            if accession in seen_accessions:
                result.skipped["duplicate_accession"] += 1
                continue
            seen_accessions.add(accession)
            report_date = _iso((filing.get("genInfo") or {}).get("repPdDate"))
            if report_date is None:
                result.skipped["no_report_date"] += 1
                continue
            series_id, cik = _series_and_cik(filing)
            if not series_id:
                result.skipped["no_series_no_cik"] += 1
                continue
            holdings = filing.get("invstOrSecs") or []
            if not holdings:
                result.skipped["no_holdings"] += 1
                continue
            meta = FilingMeta(
                accession=accession, filed_at=_clean(filing.get("filedAt")), form=form,
                report_date=report_date, series_id=series_id, cik=cik,
                n_holdings=len(holdings), container=month,
            )
            key = (report_date, series_id)
            current = candidates.get(key)
            if current is None:
                candidates[key] = meta
            else:
                result.superseded[report_date] += 1
                if meta.rank > current.rank:
                    candidates[key] = meta
        result.filings_by_container[month] = n
    result.winners = candidates
    return result


def _new_counter() -> collections.Counter:
    return collections.Counter()


def convert(
    paths: list[str],
    out_dir: str,
    *,
    min_report_date: str | None = None,
    report_dates: set[str] | None = None,
    partial_months: set[str] | None = None,
    key_policy: KeyPolicy = DERA_POLICY,
) -> dict:
    """Write ``<out_dir>/<report_date>.csv`` for every in-scope report_date.

    ``min_report_date`` / ``report_dates`` bound what is WRITTEN; everything
    else is still counted in the manifest under ``excluded_report_dates`` so a
    late filing for an already-loaded month is visible instead of silent.
    ``partial_months`` names containers that are still filling (the current
    month): a report_date whose main publication month is partial is flagged.
    """
    paths = sorted(paths, key=container_month)
    months = [container_month(p) for p in paths]
    if len(set(months)) != len(months):
        raise ValueError(f"the same container month was given twice: {months}")
    partial_months = set(partial_months or ())
    scan_result = scan(paths)

    def in_scope(rd: str) -> bool:
        if report_dates is not None and rd not in report_dates:
            return False
        return min_report_date is None or rd >= min_report_date

    winners_by_acc = {m.accession: m for m in scan_result.winners.values()}
    per_date: dict[str, collections.Counter] = collections.defaultdict(_new_counter)
    series_by_date: dict[str, set[str]] = collections.defaultdict(set)
    containers_by_date: dict[str, collections.Counter] = collections.defaultdict(_new_counter)
    forms_by_date: dict[str, collections.Counter] = collections.defaultdict(_new_counter)
    excluded: dict[str, collections.Counter] = collections.defaultdict(_new_counter)
    excluded_series: dict[str, set[str]] = collections.defaultdict(set)

    os.makedirs(out_dir, exist_ok=True)
    emitted: set[str] = set()
    writers: dict[str, Any] = {}
    handles: dict[str, Any] = {}
    try:
        for path in paths:
            for meta, filing in _iter_winning_filings(path, winners_by_acc, emitted):
                rd = meta.report_date
                holdings = filing.get("invstOrSecs") or []
                if not in_scope(rd):
                    excluded[rd]["rows"] += len(holdings)
                    excluded[rd]["filings"] += 1
                    excluded_series[rd].add(meta.series_id)
                    continue
                if rd not in writers:
                    fh = open(os.path.join(out_dir, f"{rd}.csv"), "w", encoding="utf-8", newline="")
                    handles[rd] = fh
                    writers[rd] = csv.writer(fh, lineterminator="\n")
                    writers[rd].writerow(CSV_COLS)
                counter = per_date[rd]
                counter["filings"] += 1
                series_by_date[rd].add(meta.series_id)
                containers_by_date[rd][meta.container] += 1
                forms_by_date[rd][meta.form] += 1
                seen: set[str] = set()
                for position, holding in enumerate(holdings, start=1):
                    if not isinstance(holding, dict):
                        counter["malformed_holding"] += 1
                        continue
                    isin = _isin(holding, key_policy)
                    cusip, kind = conflict_key(holding, isin, meta.accession, position, key_policy)
                    if cusip in seen:
                        counter["conflict_key_dupes"] += 1
                        continue
                    seen.add(cusip)
                    writers[rd].writerow(holding_row(rd, meta.cik, meta.series_id, cusip, isin, holding))
                    counter["rows"] += 1
                    counter[f"key_{kind}"] += 1
                    if isin:
                        counter["isin"] += 1
    finally:
        for fh in handles.values():
            fh.close()

    manifest = {
        "generated_by": "tools.nport_secapi.convert",
        "inputs": [_describe_input(p) for p in paths],
        "partial_months": sorted(partial_months),
        "key_policy": key_policy.name,
        "min_report_date": min_report_date,
        "skipped_filings": dict(scan_result.skipped),
        "superseded_filings_by_report_date": dict(sorted(scan_result.superseded.items())),
        "report_dates": {},
        "excluded_report_dates": {
            rd: {"rows": c["rows"], "filings": c["filings"], "series": len(excluded_series[rd])}
            for rd, c in sorted(excluded.items())
        },
    }
    available = set(months)
    for rd in sorted(per_date):
        c = per_date[rd]
        pub = _month_add(rd[:7], 2)
        stragglers = _month_add(rd[:7], 3)
        manifest["report_dates"][rd] = {
            "file": f"{rd}.csv",
            "rows": c["rows"],
            "series": len(series_by_date[rd]),
            "filings": c["filings"],
            "amendments": forms_by_date[rd].get("NPORT-P/A", 0),
            "isin": c["isin"],
            "isin_fill": round(c["isin"] / c["rows"], 4) if c["rows"] else 0.0,
            "keys": {k: c[f"key_{k}"] for k in ("real", "IS", "LE", "H")},
            "conflict_key_dupes": c["conflict_key_dupes"],
            "filings_by_container": dict(sorted(containers_by_date[rd].items())),
            # N-PORT becomes public ~60 days after the period: a month-end's
            # filings land mostly two containers later, stragglers one after.
            "publication_month": pub,
            "publication_month_in_inputs": pub in available,
            "publication_month_complete": pub in available and pub not in partial_months,
            "straggler_month_complete": stragglers in available and stragglers not in partial_months,
            "partial": not (pub in available and pub not in partial_months),
        }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=1, sort_keys=False)
        fh.write("\n")
    return manifest


def _iter_winning_filings(
    path: str, winners_by_acc: dict[str, FilingMeta], emitted: set[str],
) -> Iterator[tuple[FilingMeta, dict]]:
    """Pass 2: fully parse only the winning filings of this container.

    A regex over the raw line picks the candidates first, so superseded and
    out-of-container lines cost no JSON parse. Every accession on the line is
    considered and the parsed top-level ``accessionNo`` decides, so a nested
    field of the same name can neither hide nor fake a winner. ``emitted``
    yields each accession once: a line repeated in a container is the same
    filing, and emitting it twice would double the series.
    """
    month = container_month(path)
    with _open_text(path) as fh:
        for line in fh:
            hits = {a.strip() for a in _ACCESSION_RE.findall(line)}
            if not any((m := winners_by_acc.get(a)) is not None and m.container == month for a in hits):
                continue
            filing = json.loads(line, parse_float=Decimal)
            meta = winners_by_acc.get(_clean(filing.get("accessionNo")))
            if meta is not None and meta.container == month and meta.accession not in emitted:
                emitted.add(meta.accession)
                yield meta, filing


def _describe_input(path: str) -> dict:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return {"container": container_month(path), "path": str(Path(path)), "bytes": os.path.getsize(path),
            "sha256": digest.hexdigest()}


def _print_summary(manifest: dict) -> None:
    w = sys.stderr.write
    w(f"{'report_date':<12} {'rows':>10} {'series':>7} {'isin_fill':>9} {'real':>9} {'IS:':>8} "
      f"{'LE:':>6} {'H:':>6} {'/A':>5}  status\n")
    for rd, r in manifest["report_dates"].items():
        k = r["keys"]
        if not r["publication_month_in_inputs"]:
            status = f"main month {r['publication_month']} not in inputs"
        elif r["partial"]:
            status = f"PARTIAL ({r['publication_month']} still filling)"
        else:
            status = "stragglers pending" if not r["straggler_month_complete"] else "complete"
        w(f"{rd:<12} {r['rows']:>10,} {r['series']:>7,} {r['isin_fill']:>9.4f} {k['real']:>9,} {k['IS']:>8,} "
          f"{k['LE']:>6,} {k['H']:>6,} {r['amendments']:>5}  {status}\n")
    if manifest["excluded_report_dates"]:
        w("excluded (out of scope, NOT written):\n")
        for rd, r in manifest["excluded_report_dates"].items():
            w(f"  {rd}  rows={r['rows']:,} series={r['series']}\n")
    w(f"skipped filings: {manifest['skipped_filings']}\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("containers", nargs="+", help="monthly form-nport containers (.jsonl.gz or .jsonl)")
    ap.add_argument("--out", required=True, help="seed directory: one <report_date>.csv each + manifest.json")
    ap.add_argument("--min-report-date", default=None, help="write only report_dates >= this ISO date")
    ap.add_argument("--report-dates", default="", help="comma-separated ISO dates; write only these")
    ap.add_argument(
        "--partial-months", default=None,
        help="comma-separated YYYY-MM containers still filling; default: any container at or after "
             "the current UTC month",
    )
    ap.add_argument(
        "--key-policy", choices=sorted(POLICIES), default=DERA_POLICY.name,
        help="dera (default): the rule every existing row was keyed with; strict: placeholder "
             "identifiers do not fold distinct holdings together (see module docstring)",
    )
    args = ap.parse_args(argv)

    months = [container_month(p) for p in args.containers]
    if args.partial_months is None:
        now = dt.datetime.now(dt.UTC).strftime("%Y-%m")
        partial = {m for m in months if m >= now}
    else:
        partial = {m.strip() for m in args.partial_months.split(",") if m.strip()}
    report_dates = {d.strip() for d in args.report_dates.split(",") if d.strip()} or None
    manifest = convert(
        args.containers, args.out, min_report_date=args.min_report_date,
        report_dates=report_dates, partial_months=partial, key_policy=POLICIES[args.key_policy],
    )
    _print_summary(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
