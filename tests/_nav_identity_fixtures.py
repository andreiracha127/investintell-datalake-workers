"""Synthetic identity fixtures and an independent checksum oracle (tests only).

Every identifier is synthetic: CUSIP bodies start with ``ZZ``, ISINs are built
from those CUSIPs and FIGIs use the non-reserved ``ZZG`` prefix; SEC classes
are ``C`` + 9 digits of the entity number. The oracle is a third formulation
(``int(c, 36)``, ``divmod`` digit sums, brute-force Luhn), distinct from both
the generator and the independent verifier.

``catalog`` returns the FOUR v3 sources; unless told otherwise every entity
with a registry row gets exactly one complete SEC row consistent with its
registry ticker/series (and declared class), synced at ``SEC_AT`` (fresh for
the fixed decision instants used by the offline suites).
"""

from __future__ import annotations

import datetime as dt
import uuid

_SPECIALS = {"*": 36, "@": 37, "#": 38}
_ALNUM = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_FIGI_CONSONANTS = set("BCDFGHJKLMNPQRSTVWXYZ")
AUTO = object()
EMPTY_CONFLICT = object()
SEC_AT = dt.datetime(2026, 9, 23, 0, 0, tzinfo=dt.timezone.utc)


def _value(ch: str) -> int:
    return _SPECIALS[ch] if ch in _SPECIALS else int(ch, 36)


def oracle_cusip_check(body8: str) -> str:
    total = sum(sum(divmod(_value(ch) * (1 + i % 2), 10)) for i, ch in enumerate(body8))
    return str((10 - total % 10) % 10)


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        x = int(ch)
        if i % 2:
            x *= 2
            if x > 9:
                x -= 9
        total += x
    return total % 10 == 0


def oracle_isin_check(prefix11: str) -> str:
    expanded = "".join(str(int(ch, 36)) for ch in prefix11)
    return next(str(d) for d in range(10) if _luhn_ok(expanded + str(d)))


def oracle_figi_check(prefix11: str) -> str:
    total = sum(
        sum(divmod(int(ch, 36) * (1 + i % 2), 10)) for i, ch in enumerate(prefix11)
    )
    return str((10 - total % 10) % 10)


def oracle_cusip_valid(value: str) -> bool:
    return (
        len(value) == 9
        and all(ch in _ALNUM or ch in _SPECIALS for ch in value[:8])
        and value[8] in "0123456789"
        and oracle_cusip_check(value[:8]) == value[8]
    )


def oracle_isin_valid(value: str) -> bool:
    return (
        len(value) == 12
        and value[:2] == "US"
        and all(ch in _ALNUM for ch in value[:11])
        and value[11] in "0123456789"
        and oracle_cusip_valid(value[2:11])
        and oracle_isin_check(value[:11]) == value[11]
    )


def oracle_figi_valid(value: str) -> bool:
    return (
        len(value) == 12
        and value[0] in _FIGI_CONSONANTS
        and value[1] in _FIGI_CONSONANTS
        and value[2] == "G"
        and all(ch in _FIGI_CONSONANTS or ch in "0123456789" for ch in value[3:11])
        and value[11] in "0123456789"
        and value[:2] not in {"BS", "BM", "GG", "GB", "GH", "KY", "VG"}
        and oracle_figi_check(value[:11]) == value[11]
    )


def synthetic_cusip(n: int, *, body: str | None = None) -> str:
    body = body or f"ZZ{n:06d}"
    return body + oracle_cusip_check(body)


def synthetic_isin(n: int, *, cusip: str | None = None) -> str:
    prefix = "US" + (cusip or synthetic_cusip(n))
    return prefix + oracle_isin_check(prefix)


def synthetic_figi(n: int) -> str:
    prefix = f"ZZG{n:08d}"
    return prefix + oracle_figi_check(prefix)


def wrong_check(value: str) -> str:
    return value[:-1] + str((int(value[-1]) + 1) % 10)


def entity(
    n: int,
    *,
    ticker: str | None | object = AUTO,
    series: str | None | object = AUTO,
    iu_isin: str | None | object = AUTO,
    reg_isin: str | None | object = AUTO,
    cusip: str | None | object = AUTO,
    figi: str | None = None,
    fund_type: str = "etf",
    active: bool | None = True,
    iu_currency: str = "USD",
    fv_currency: str = "USD",
    status: str | None = "canonical",
    conflict: object = EMPTY_CONFLICT,
    instrument_type: str = "fund",
    class_id: str | None = None,
) -> tuple[dict, dict, dict]:
    """(IU, funds_v, registry) rows; funds_v projects the registry exactly."""
    identifier = uuid.UUID(int=n)
    ticker = f"T{n}" if ticker is AUTO else ticker
    series = f"S{n:09d}" if series is AUTO else series
    iu_isin = synthetic_isin(n) if iu_isin is AUTO else iu_isin
    reg_isin = synthetic_isin(n) if reg_isin is AUTO else reg_isin
    cusip = synthetic_cusip(n) if cusip is AUTO else cusip
    conflict = {} if conflict is EMPTY_CONFLICT else conflict
    iu = {
        "instrument_id": identifier,
        "instrument_type": instrument_type,
        "ticker": ticker,
        "isin": iu_isin,
        "currency": iu_currency,
        "is_active": active,
    }
    fund = {
        "instrument_id": identifier,
        "series_id": series,
        "ticker": ticker,
        "isin": reg_isin,
        "cusip": cusip,
        "currency": fv_currency,
        "fund_type": fund_type,
    }
    registry = {
        "instrument_id": identifier,
        "sec_series_id": series,
        "sec_class_id": class_id,
        "ticker": ticker,
        "isin": reg_isin,
        "cusip_9": cusip,
        "figi": figi,
        "resolution_status": status,
        "conflict_state": conflict,
    }
    return iu, fund, registry


def sec_row(
    n: int,
    *,
    class_id: str | None | object = AUTO,
    series: str | None | object = AUTO,
    ticker: str | None | object = AUTO,
    synced: dt.datetime = SEC_AT,
) -> dict:
    """One ``public.sec_company_tickers_mf`` projection row (aware ``synced_at``)."""
    return {
        "class_id": f"C{n:09d}" if class_id is AUTO else class_id,
        "series_id": f"S{n:09d}" if series is AUTO else series,
        "ticker": f"T{n}" if ticker is AUTO else ticker,
        "synced_at": synced,
    }


def auto_sec(*entities, synced: dt.datetime = SEC_AT) -> list[dict]:
    """One consistent complete SEC row per entity that has a registry row."""
    rows = []
    for _iu, _fund, registry in entities:
        if registry is None or registry["ticker"] is None:
            continue
        if registry["sec_series_id"] is None:
            continue
        number = uuid.UUID(str(registry["instrument_id"])).int
        rows.append(
            sec_row(
                number,
                class_id=registry["sec_class_id"] or f"C{number:09d}",
                series=registry["sec_series_id"],
                ticker=registry["ticker"],
                synced=synced,
            )
        )
    return rows


def catalog(
    *entities, iu=(), funds=(), registry=(), sec=None, sec_extra=(), sec_at=SEC_AT
) -> tuple[list, list, list, list]:
    """Flatten entities (a ``None`` slot omits that source row) plus extra rows.

    ``sec=None`` derives one consistent SEC row per registry entity (``auto_sec``);
    an explicit list replaces them. ``sec_extra`` rows are appended either way.
    """
    instruments, fund_rows, identity = [], [], []
    for entry in entities:
        a, b, c = entry
        if a is not None:
            instruments.append(dict(a))
        if b is not None:
            fund_rows.append(dict(b))
        if c is not None:
            identity.append(dict(c))
    instruments.extend(dict(row) for row in iu)
    fund_rows.extend(dict(row) for row in funds)
    identity.extend(dict(row) for row in registry)
    sec_rows = auto_sec(*entities, synced=sec_at) if sec is None else list(sec)
    sec_rows = [dict(row) for row in (*sec_rows, *sec_extra)]
    return instruments, fund_rows, identity, sec_rows


def only(entry, *, iu: bool = True, fund: bool = True, registry: bool = True):
    a, b, c = entry
    return (a if iu else None, b if fund else None, c if registry else None)
