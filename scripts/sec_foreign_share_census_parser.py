"""Positive, fail-closed annual-cover share census using W1c text extraction.

Only the response to the outstanding-shares cover statement is admitted.  A
12(b) listing, a receipt title, or a financial-statement balance is never a
substitute for a class named in that response.  Unknown grammar is retained as
incomplete evidence rather than silently dropped.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

try:
    from scripts.sec_foreign_listing_parser import _Document
except ModuleNotFoundError:  # Direct script/loader execution.
    from sec_foreign_listing_parser import _Document

PARSER_VERSION = "foreign-share-census-v1"
_MONTH = ("January|February|March|April|May|June|July|August|September|October|"
          "November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec")
_DATE = re.compile(rf"\b(?:{_MONTH})\.?\s+\d{{1,2}}\s*,?\s*\d{{4}}\b|\b\d{{1,2}}\s+(?:{_MONTH})\.?\s+\d{{4}}\b|\b\d{{4}}-\d{{2}}-\d{{2}}\b", re.I)
_PROMPT = re.compile(
    r"\bIndicate\s+the\s+number\s+of\s+(?:the\s+)?outstanding\s+shares\s+"
    r"of\s+each\s+of\s+the\s+issuer[’']?s\s+classes\s+of\s+capital\s+or\s+"
    r"common\s+stock\s+as\s+of\s+the\s+(?:close|end)\s+of\s+the\s+period\s+"
    r"covered\s+by\s+(?:the|this)\s+annual\s+report\s*[.:]?", re.I)
_NEXT = re.compile(r"\bIndicate\s+by\s+check\s*mark|\bPART\s+I\b|\bITEM\s+1[.\s]", re.I)
_CUSTOM = re.compile(r"\b(?:The\s+)?number\s+of\s+(?:the\s+)?outstanding\s+(?:[A-Za-z-]+\s+){0,4}(?:shares|stock)\b", re.I)
_ROMAN = r"(?=[IVX])X{0,3}(?:IX|IV|V?I{0,3})"
_ID = rf"(?:{_ROMAN}|[A-Za-z]-?\d{{1,2}}|\d{{1,4}}(?:-?[A-Za-z\d]{{1,2}})?|[A-Za-z]{{1,2}})"
_BARE_ID = rf"(?-i:(?:{_ROMAN}|[A-Z]{{1,2}}))"
_LABEL = rf"(?:Class|Series)\s+{_ID}"
_MOD = r"(?:(?:non[- ]?voting|voting|redeemable|convertible|cumulative|non[- ]?cumulative|participating|registered|subordinate|restricted|multiple|limited|subordinated)\s+)*"
_KIND = r"(?:ordinary|common|preferred|preference|deferred|founder(?:s|[’']s)?)"
_NAME = re.compile(
    rf"\b(?:(?:{_LABEL})\s+{_MOD}(?:{_KIND}\s+)?(?:shares?|stock)"
    rf"|{_MOD}{_KIND}\s+(?:shares?|stock)(?:\s+(?:of\s+)?{_LABEL})?"
    rf"|{_BARE_ID}\s+{_MOD}(?:{_KIND}\s+)?(?:shares?|stock)\b|{_LABEL}\b)", re.I)
_NUM = re.compile(r"(?<![\w.,])(?>\d{1,3}(?:[,\u202f ]\d{3})+|\d+)(?:\.\d+)?(?:\s+(?:million|billion|thousand))?(?!\w|[.,]\d)|\bnil\b|\bnone\b", re.I)
_PAR = re.compile(
    r"\b(?:par|nominal)\s+value\s*(?:of\s+)?"
    r"(?:(?:[A-Z]{1,4}\s*[$€£¥]?|[$€£¥])\s*)?"
    r"\d+(?:[.,]\d+)?\s*(?:(?:[A-Z]{1,4}|cents?)\s+)?"
    r"(?:per\s+(?:share|stock)|each|(?=[:;)]))", re.I)


def normalize_class_key(name: str) -> str | None:
    """Match #186/W1 Class and Series normalization, including I..XXXIX.

    Bare capital-letter share names are explicit classes (for example NVO's
    ``A shares``); plain ordinary/common names retain distinct canonical keys.
    """
    text = re.sub(r"[_:]", " ", name)
    text = re.sub(r"([a-z])([A-Z0-9])", r"\1 \2", text)
    text = re.sub(r"([A-Z])([A-Z][a-z])", r"\1 \2", text)
    match = re.search(rf"\b(class|series)\s+({_ID})(?![A-Za-z0-9])", text, re.I)
    if match:
        token = match.group(2)
        if len(token) == 2 and token.isalpha():
            if token.upper() in {"OF", "TO", "IN", "ON", "AS", "AN", "OR", "BY", "NO", "IS", "IT", "AT", "BE", "DO", "IF", "SO", "UP", "WE", "US", "PA"}:
                return None
            if not token.isupper() and token.lower() not in {"ii", "iv", "vi", "ix", "xi", "xv", "xx"}:
                return None
        roman = token.lower()
        if re.fullmatch(r"x{0,3}(ix|iv|v?i{0,3})", roman):
            tens = len(roman) - len(roman.lstrip("x"))
            rest = roman.lstrip("x")
            token = str(tens * 10 + ({"ix": 9, "iv": 4}.get(rest, (5 if rest.startswith("v") else 0) + rest.count("i"))))
        return match.group(1).lower() + ":" + token.lower().replace("-", "")
    bare = re.fullmatch(rf"({_BARE_ID})\s+{_MOD}(?:{_KIND}\s+)?(?:shares?|stock)", name, re.I)
    if bare:
        return normalize_class_key("Class " + bare.group(1))
    if re.search(r"\bordinary\b", name, re.I):
        return "ordinary"
    if re.search(r"\bcommon\b", name, re.I):
        return "common"
    return None


def _kind(name: str) -> str:
    if re.search(r"\b(?:preferred|preference)\b", name, re.I):
        return "preferred"
    if re.search(r"\b(?:ordinary|common)\b", name, re.I):
        return "ordinary"
    return "other"


def _parse_date(value: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", value.replace(",", "").replace(".", "")).strip()
    cleaned = re.sub(r"\bSept\b", "Sep", cleaned, flags=re.I)
    for fmt in ("%Y-%m-%d", "%B %d %Y", "%b %d %Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _count(value: str) -> int | None:
    if value.lower() in ("nil", "none"):
        return 0
    parts = value.lower().split()
    scale = {"thousand": 1000, "million": 1000000, "billion": 1000000000}.get(parts[-1], 1)
    if scale != 1:
        parts.pop()
    try:
        number = Decimal("".join(parts).replace(",", "").replace("\u202f", "")) * scale
        return int(number) if number >= 0 and number == number.to_integral_value() else None
    except (InvalidOperation, ValueError):
        return None


def _mask(text: str, spans: list[tuple[int, int]]) -> str:
    chars = list(text)
    for start, end in spans:
        chars[start:end] = " " * (end - start)
    return "".join(chars)


def _period_end(document_text: str, raw: str | None) -> str | None:
    if raw:
        found = re.findall(r"<[^>]*\bname\s*=\s*['\"]dei:DocumentPeriodEndDate['\"][^>]*>\s*(\d{4}-\d{2}-\d{2})\s*<", raw, re.I)
        dates = {_parse_date(d) for d in found} - {None}
        if len(dates) == 1:
            return dates.pop()
    dates = set()
    for match in re.finditer(r"(?:fiscal\s+)?(?:year|period)\s+ended\s*:?\s*", document_text[:14000], re.I):
        following = _DATE.match(document_text, match.end())
        if following:
            value = _parse_date(following.group())
            if value:
                dates.add(value)
    return next(iter(dates)) if len(dates) == 1 else None


def parse_share_census(document: _Document | str, period_end: str | date | None = None) -> dict:
    """Return one auditable census, with unusable partial facts kept visible.

    ``document`` may be W1c's extracted ``_Document`` or HTML, including the
    inert HTML produced by ``pdf_parser_input``.  Caller-provided period end
    must come from the same filing; no filing-date or calendar-year guess is
    made here.  Cross-checks against W1 counts are performed by the loader.
    """
    raw = document if isinstance(document, str) else None
    doc = _Document(document) if raw is not None else document
    text = doc.text
    period = period_end.isoformat() if isinstance(period_end, date) else period_end
    period = _parse_date(period) if period else _period_end(text, raw)
    result = dict(parser_version=PARSER_VERSION, classes=[], shares_as_of=None,
                  period_end=period, date_explicit=False, stated_total=None,
                  computed_total=None, complete=False, conflicting=False,
                  status="none", reasons=[], numeric_residue=[], text_residue=[],
                  source_text=None, source_location=None)
    prompts = list(_PROMPT.finditer(text[:18000]))
    prompt = prompts[0] if prompts else _CUSTOM.search(text[:14000])
    if prompt is None:
        result["reasons"] = ["cover_statement_not_found"]
        return result
    start = prompt.start()
    response_start = prompt.end() if prompts else start
    following = _NEXT.search(text, response_start)
    end = following.start() if following else min(len(text), response_start + 8000)
    response = text[response_start:end].strip()
    # Trim whitespace without losing the source offset of the evidence span.
    result["source_text"] = text[start:end].strip()
    result["source_location"] = f"cover-share-statement;text-offset={start};text-span={start}:{end}"
    result["status"] = "incomplete"
    reasons = result["reasons"]
    if len(prompts) > 1:
        reasons.append("multiple_cover_statements")
    if end == response_start + 8000:
        reasons.append("statement_boundary_unresolved")
    date_matches = list(_DATE.finditer(response))
    dates = {_parse_date(m.group()) for m in date_matches} - {None}
    if len(dates) == 1:
        result["shares_as_of"] = dates.pop()
        result["date_explicit"] = True
    elif len(dates) > 1:
        reasons.append("multiple_statement_dates")
    else:
        result["shares_as_of"] = period
    if not result["shares_as_of"]:
        reasons.append("statement_date_unresolved")
    if any(_parse_date(m.group()) is None for m in date_matches):
        reasons.append("invalid_statement_date")
    ignored = [(m.start(), m.end()) for m in date_matches]
    ignored.extend((m.start(), m.end()) for m in _PAR.finditer(response))
    ignored.extend((m.start(), m.end()) for m in re.finditer(r"\bwithout\s+nominal\s+value\b", response, re.I))
    cleaned = _mask(response, ignored)
    names = list(_NAME.finditer(cleaned))
    # Reserved cover-question nouns cannot be interpreted as unnamed classes.
    names = [m for m in names if normalize_class_key(m.group()) or _kind(m.group()) != "other"
             or re.search(r"deferred|founder|preferred|preference", m.group(), re.I)]
    numbers = [m for m in _NUM.finditer(cleaned)
               if not any(name.start() <= m.start() < name.end() for name in names)]
    consumed = set()
    assignments: dict[int, int] = {}
    total_numbers = []
    for n, number in enumerate(numbers):
        before = cleaned[max(0, number.start() - 65):number.start()]
        if re.search(r"\b(?:total(?:\s+(?:of|shares|outstanding|share\s+capital)){0,3}|in\s+aggregate)\s*[:=]?\s*$", before, re.I):
            total_numbers.append(n)
            consumed.add(n)
    if total_numbers:
        totals = {_count(numbers[n].group()) for n in total_numbers}
        if len(totals) == 1 and None not in totals:
            result["stated_total"] = totals.pop()
        else:
            reasons.append("multiple_or_invalid_totals")
    # Each count must be adjacent to exactly one named class.  Direction is
    # elected for the statement as a whole: count-first prose or name-first
    # rows. Mixed directions are allowed only where a separator binds them.
    for i, name in enumerate(names):
        left = names[i - 1].end() if i else 0
        right = names[i + 1].start() if i + 1 < len(names) else len(cleaned)
        before = [(n, m) for n, m in enumerate(numbers) if n not in consumed and left <= m.start() < name.start()]
        after = [(n, m) for n, m in enumerate(numbers) if n not in consumed and name.end() <= m.start() < right]
        choices = []
        if before:
            n, m = before[-1]
            bridge = cleaned[m.end():name.start()]
            if not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"\s*(?:outstanding\s+)?", bridge, re.I):
                choices.append((n, m, "before"))
        if after:
            n, m = after[0]
            bridge = cleaned[name.end():m.start()]
            if not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"[\s,:;()]*((?:were|was|are|is|outstanding|as\s+of|without\s+nominal\s+value)[\s,:;()]*)*", bridge, re.I):
                choices.append((n, m, "after"))
        # A bare Class A followed by "and Class B common shares" describes
        # a combined number. Do not assign that number to A or B.
        if i + 1 < len(names) and re.fullmatch(r"\s*(?:and|&)\s*", cleaned[name.end():names[i + 1].start()], re.I):
            reasons.append("combined_class_count")
            if len(choices) == 1 and choices[0][2] == "before":
                n, m, _ = choices[0]
                result["stated_total"] = _count(m.group())
                consumed.add(n)
            choices = []
        if i and re.fullmatch(r"\s*(?:and|&)\s*", cleaned[names[i - 1].end():name.start()], re.I) and not assignments.get(i - 1):
            choices = []
        if len(choices) == 2:
            # In count-first A+B prose the following count belongs to the
            # next class; a colon in name-first rows establishes the opposite.
            colon = ":" in cleaned[name.end():choices[1][1].start()]
            choices = [choices[1] if colon else choices[0]]
        shares = None
        if len(choices) == 1:
            n, m, _ = choices[0]
            shares = _count(m.group())
            consumed.add(n)
            assignments[i] = n + 1
        if shares is None:
            reasons.append("class_count_unresolved")
        exact = response[name.start():name.end()]
        key = normalize_class_key(exact)
        aggregate_prefix = re.search(r"\b(?:total|aggregate)\b", cleaned[left:name.start()], re.I)
        aggregate_suffix = re.search(r"\bin\s+aggregate\b", cleaned[name.end():right], re.I)
        if key in ("ordinary", "common") and (aggregate_prefix or aggregate_suffix):
            # A total described only as ordinary/common supply can aggregate
            # unnamed A/B classes. Keep its literal label/count auditable, but
            # do not let it establish a positive sole-class census. An
            # explicitly named ``Total Class A ordinary shares`` is different.
            reasons.append("aggregate_only_statement")
            if shares is not None:
                if result["stated_total"] is None:
                    result["stated_total"] = shares
                elif result["stated_total"] != shares:
                    reasons.append("multiple_or_invalid_totals")
        result["classes"].append(dict(class_name=exact, class_key=key,
                                      class_kind=_kind(exact), shares=shares))
    if not names:
        reasons.append("no_named_classes")
    keys = [row["class_key"] or row["class_name"].lower() for row in result["classes"]]
    if len(keys) != len(set(keys)):
        reasons.append("duplicate_class_identity")
    residue = [m.group() for n, m in enumerate(numbers) if n not in consumed]
    # The strict count lexer must not silently truncate or skip malformed
    # grouping.  Any remaining digit is numeric residue, even when its token
    # was not an admissible integer count.
    recognized = [(m.start(), m.end()) for m in numbers]
    recognized.extend((m.start(), m.end()) for m in names)
    malformed = _mask(cleaned, recognized)
    residue.extend(m.group() for m in re.finditer(r"\d[\d.,]*", malformed))
    result["numeric_residue"] = residue
    if residue:
        reasons.append("unparsed_numeric_residue")
    # Completeness also requires positive coverage of the *text*, not just
    # digits. A non-numeric, unfamiliar class row must never disappear while
    # an ordinary row makes the census appear complete. Only grammar glue and
    # explicit table headers are allowed outside class/count/date/value spans.
    text_remainder = _mask(cleaned, recognized)
    text_remainder = re.sub(r"\b(?:title\s+of\s+(?:each\s+)?class|class\s+of\s+shares|(?:number\s+of\s+)?shares?\s+outstanding|number\s+outstanding|share\s+capital|outstanding\s+shares?|number\s+of\s+shares?)\b", " ", text_remainder, flags=re.I)
    glue = {"the", "number", "of", "as", "at", "on", "in", "a", "an", "and", "were", "was", "are", "is", "there", "outstanding", "total", "aggregate", "each", "without", "nominal", "value", "for", "period", "ended", "end", "close", "year", "fiscal", "report", "annual", "covered", "by", "issuer", "s", "stock", "class", "shares"}
    # Bare class/share/stock words are allowed only if consumed by an exact
    # table header above; otherwise they may name an unparsed security.
    glue -= {"stock", "class", "shares"}
    residual_words = [w for w in re.findall(r"[A-Za-z]+", text_remainder) if w.lower() not in glue]
    if residual_words:
        result["text_residue"] = residual_words
        reasons.append("unparsed_class_or_text_residue")
    if re.search(r"\b(?:or|either|alternatively|if|unless|provided|conditional(?:ly)?)\b", cleaned, re.I):
        reasons.append("alternative_or_conditional_statement")
    # Numeric punctuation, receipt counts and qualification words need source
    # review.  They cannot become a apparently exact ordinary census.
    if re.search(r"\b(?:approximately|approx\.?|about|excluding|including|treasury|issued\s+and|authorized|authorised|American\s+depositary|ADSs?|ADRs?|less|net\s+of)\b|(?<!\w)[-−]\s*\d", cleaned, re.I):
        reasons.append("qualified_or_nonordinary_statement")
    if result["classes"] and all(c["shares"] is not None for c in result["classes"]):
        result["computed_total"] = sum(c["shares"] for c in result["classes"])
    if result["stated_total"] is not None and result["computed_total"] is not None and result["stated_total"] != result["computed_total"]:
        reasons.append("stated_total_mismatch")
        result["conflicting"] = True
    result["reasons"] = sorted(set(reasons))
    result["complete"] = bool(result["classes"]) and not result["reasons"]
    result["status"] = "conflicting" if result["conflicting"] else "complete" if result["complete"] else "incomplete"
    return result
