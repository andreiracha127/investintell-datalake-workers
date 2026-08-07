"""Flatten local bond reference profiles into a \\copy-ready CSV.

WHY A CSV AND NOT A DIRECT WRITE
    The reference bodies are JSON files on an operator workstation; the workers
    run on Railway with no access to that filesystem, and the datalake has no
    public TCP path. So the transport is: flatten here, ship the CSV over
    ``railway ssh``, ``\\copy`` it into ``bond_reference_terms``, and let the
    build read the TABLE. The worker stays deployable, and the load leaves an
    auditable ``batch_label`` / ``loaded_at`` behind.

WHAT IT REFUSES TO DO
    * No name of any kind is emitted -- the reference carries terms only. Issuer
      names come from the reported filings and their consensus, never from here.
    * A blank/absent field stays absent (empty CSV cell -> SQL NULL); nothing is
      defaulted. ``dayCount`` is present on roughly one profile in ten, so most
      rows honestly carry no day count.
    * ``secured`` is emitted ONLY when the reference states it in so many words
      (a debt-type token containing "SECURED"/"UNSECURED"); the seniority label
      alone is not evidence about collateral.

USAGE
    python scripts/load_bond_reference_terms.py \\
        --profiles <dir of CUSIP9.json> \\
        --out bond_reference_terms.csv \\
        [--only <file with one CUSIP9 per line>] \\
        [--batch-label 2026-08-07]

    Then, from the same workstation:
      railway ssh --project <id> --environment production \\
          --service market-clean-serial -- \\
          psql -U postgres -d market -c "\\copy bond_reference_terms \\
          (cusip9,isin,coupon_rate,coupon_type,maturity_date,issue_date,seniority,\\
           secured,day_count,payment_frequency,callable,amount_outstanding_mm,batch_label) \\
          FROM STDIN WITH (FORMAT csv, HEADER true)" < bond_reference_terms.csv

    The file MUST have LF line endings (``\\copy`` breaks on CRLF); this script
    writes LF explicitly via ``newline=""`` + an LF-only writer.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

CUSIP9_RE = re.compile(r"^[0-9A-Z]{9}$")

COLUMNS = (
    "cusip9", "isin", "coupon_rate", "coupon_type", "maturity_date", "issue_date",
    "seniority", "secured", "day_count", "payment_frequency", "callable",
    "amount_outstanding_mm", "batch_label",
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _number(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):
        return None  # NaN/infinity is absence, never a stored value
    return repr(number)


def _iso_date(value: Any) -> str | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _secured(debt_type: Any) -> str | None:
    """Only an explicit collateral statement counts; seniority is not evidence."""
    text = _text(debt_type)
    if text is None:
        return None
    upper = text.upper()
    if "UNSECURED" in upper:
        return "unsecured"
    if "SECURED" in upper:
        return "secured"
    return None


def _boolean(value: Any) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    text = _text(value)
    if text is None:
        return None
    if text.lower() in ("true", "false"):
        return text.lower()
    return None


def row_from_profile(cusip9: str, body: dict[str, Any], batch_label: str) -> dict[str, Any] | None:
    profile = body.get("profile") if isinstance(body.get("profile"), dict) else body
    if not isinstance(profile, dict):
        return None
    coupon = _number(profile.get("coupon"))
    amount = _number(profile.get("amountOutstanding"))
    return {
        "cusip9": cusip9,
        "isin": _text(profile.get("isin")) or _text(body.get("isin")),
        "coupon_rate": coupon,
        "coupon_type": _text(profile.get("couponType")),
        "maturity_date": _iso_date(profile.get("maturityDate")),
        "issue_date": _iso_date(profile.get("issueDate")),
        "seniority": _text(profile.get("securityLevel")),
        "secured": _secured(profile.get("debtType")),
        "day_count": _text(profile.get("dayCount")),
        "payment_frequency": _text(profile.get("paymentFrequency")),
        "callable": _boolean(profile.get("callable")),
        "amount_outstanding_mm": amount,
        "batch_label": batch_label,
    }


def build_rows(
    profiles_dir: Path, *, batch_label: str, only: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    skipped: list[str] = []
    for path in sorted(profiles_dir.glob("*.json")):
        cusip9 = path.stem.strip().upper()
        if not CUSIP9_RE.match(cusip9) or (only is not None and cusip9 not in only):
            continue
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            skipped.append(cusip9)
            continue
        if not isinstance(body, dict):
            skipped.append(cusip9)
            continue
        row = row_from_profile(cusip9, body, batch_label)
        if row is None:
            skipped.append(cusip9)
            continue
        rows.append(row)
    return rows, skipped


def write_csv(rows: list[dict[str, Any]], out: Path) -> None:
    # newline="" + lineterminator="\n" keeps the file LF-only: \copy rejects CRLF.
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(COLUMNS), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: ("" if row.get(key) is None else row[key]) for key in COLUMNS})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--only", type=Path, default=None,
                        help="optional file of CUSIP9s (one per line) to restrict the batch")
    parser.add_argument("--batch-label", default=date.today().isoformat())
    args = parser.parse_args(argv)

    only: set[str] | None = None
    if args.only is not None:
        only = {
            line.strip().upper()
            for line in args.only.read_text(encoding="utf-8").splitlines()
            if CUSIP9_RE.match(line.strip().upper())
        }

    rows, skipped = build_rows(args.profiles, batch_label=args.batch_label, only=only)
    write_csv(rows, args.out)
    print(f"rows={len(rows)} skipped={len(skipped)} out={args.out}")
    for name in ("seniority", "callable", "amount_outstanding_mm", "secured", "day_count"):
        filled = sum(1 for row in rows if row.get(name) not in (None, ""))
        print(f"  {name:22s} {filled}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
