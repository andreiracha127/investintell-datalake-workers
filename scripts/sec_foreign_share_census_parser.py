"""Positive, fail-closed annual-cover share census using W1c text extraction.

Only the response to the outstanding-shares cover statement is admitted.  A
12(b) listing, a receipt title, or a financial-statement balance is never a
substitute for a class named in that response.  Unknown grammar is retained as
incomplete evidence rather than silently dropped.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from html import unescape

try:
    from scripts.sec_foreign_listing_parser import _Document, _HTML_TOKEN
except ModuleNotFoundError:  # Direct script/loader execution.
    from sec_foreign_listing_parser import _Document, _HTML_TOKEN

PARSER_VERSION = "foreign-share-census-v2"
_MONTH = ("January|February|March|April|May|June|July|August|September|October|"
          "November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec")
_DATE = re.compile(rf"\b(?:{_MONTH})\.?\s+\d{{1,2}}\s*,?\s*\d{{4}}\b|\b\d{{1,2}}\s+(?:{_MONTH})\.?\s+\d{{4}}\b|\b\d{{4}}-\d{{2}}-\d{{2}}\b", re.I)
_PROMPT = re.compile(
    r"\bIndicate\s+the\s+number\s+of\s+(?:the\s+)?outstanding\s+shares\s+"
    r"of\s+each\s+of\s+the\s+issuer[’']?s\s+classes\s+of\s+capital\s+or\s+"
    r"common\s+stock\s+as\s+of\s+the\s+(?:close|end)\s+of\s+the\s+period\s+"
    r"covered\s+by\s+(?:the|this)\s+annual\s+report\s*[.:]?", re.I)
_NEXT = re.compile(r"\bIndicate\s*,?\s+by\s+check\s*mark|\bPART\s+I\b|\bITEM\s+1[.\s]", re.I)
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
_NUM = re.compile(r"(?<![\w.,])(?>\d{1,3}(?:[,\u202f]\d{3})+|\d+)(?:\.\d+)?(?:\s+(?:million|billion|thousand))?(?!\w|[.,]\d)|\bnil\b|\bnone\b", re.I)
_PAR = re.compile(
    r"\b(?:par|nominal)\s+value\s*(?:of\s+)?"
    r"(?:(?:[A-Z]{1,4}\s*[$€£¥]?|[$€£¥])\s*)?"
    r"\d+(?:[.,]\d+)?\s*(?:(?:[A-Z]{1,4}|cents?)\s+)?"
    r"(?:per\s+(?:share|stock)|each|(?=[:;)\t\n]|$))", re.I)


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
    if parts[-1] in ("thousand", "thousands", "million", "millions", "billion", "billions"):
        return None
    try:
        number = Decimal("".join(parts).replace(",", "").replace("\u202f", ""))
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


def _extract_candidate(response: str, period: str | None) -> dict:
    """Extract a literal proposal; certification is a separate source check."""
    result = dict(parser_version=PARSER_VERSION, classes=[], shares_as_of=None,
                  period_end=period, date_explicit=False, stated_total=None,
                  computed_total=None, complete=False, conflicting=False,
                  status="incomplete", reasons=[], numeric_residue=[], text_residue=[],
                  source_text=None, source_location=None)
    reasons = result["reasons"]
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
    numbers = _source_numbers(cleaned, names)
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
        left = max(left, cleaned.rfind("\n", 0, name.start()) + 1)
        row_end = cleaned.find("\n", name.end())
        if row_end >= 0:
            right = min(right, row_end)
        before = [(n, m) for n, m in enumerate(numbers) if n not in consumed and left <= m.start() < name.start()]
        after = [(n, m) for n, m in enumerate(numbers) if n not in consumed and name.end() <= m.start() < right]
        choices = []
        if before:
            n, m = before[-1]
            bridge = cleaned[m.end():name.start()]
            if "\n" not in bridge and (not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"\s*(?:outstanding\s+)?", bridge, re.I)):
                choices.append((n, m, "before"))
        if after:
            n, m = after[0]
            bridge = cleaned[name.end():m.start()]
            if "\n" not in bridge and (not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"[\s,:;()]*((?:were|was|are|is|outstanding|as\s+of|without\s+nominal\s+value)[\s,:;()]*)*", bridge, re.I)):
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
        if "\t" in response or "\n" in response:
            row_start = response.rfind("\n", 0, name.start()) + 1
            row_end = response.find("\n", name.end())
            row_end = len(response) if row_end < 0 else row_end
            _, row_names, row_numbers, _ = _source_lex(response[row_start:row_end])
            if len(row_numbers) > len(row_names):
                choices = []
            if (row_start, row_end) in _invalid_columns(response):
                choices = []
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
    result["extraction_notes"] = sorted(set(reasons))
    result.update(reasons=[], complete=False, conflicting=False, status="incomplete")
    return result


_SECTION = re.compile(r"\bPART\s+I{1,3}\b|\bITEM\s+1(?:\s*[.:]|\s+IDENTITY\b)|\b(?:STRATEGIC\s+REPORT|EXCHANGE\s+RATES|PRINCIPAL\s+DOCUMENTS|PRESENTATION\s+OF\s+INFORMATION|EXPLANATORY\s+NOTE|ANNUAL\s+INFORMATION\s+FORM|DISCLOSURE\s+CONTROLS\s+AND\s+PROCEDURES|INTEGRATED\s+REPORT|INTRODUCTION|FORWARD[- ]LOOKING\s+STATEMENTS|CONVENTIONS)\b", re.I)
_HEADERS = re.compile(r"\b(?:title\s+of\s+(?:each\s+)?class|class\s+of\s+shares|(?:number\s+of\s+)?shares?\s+outstanding|number\s+outstanding|share\s+capital|outstanding\s+shares?|number\s+of\s+shares?)\b", re.I)
_GLUE = {"the", "number", "of", "as", "at", "on", "in", "a", "an", "and", "were", "was", "are", "is", "there", "outstanding", "total", "aggregate", "each", "without", "nominal", "value", "for", "period", "ended", "end", "close", "year", "fiscal", "report", "annual", "covered", "by", "issuer", "s"}
_NOTE_QUALIFIER = r"(?:includ(?:e[ds]?|ing)|exclud(?:e[ds]?|ing)|treasury|issued|held\s+by|net\s+of|combined|combines?|aggregate|employee\s+options?)"
_NOTE_SUBJECT = r"(?:class(?:es)?|counts?|capital\s+stock|share\s+(?:amounts?|counts?)|(?:ordinary|common|preferred|preference|deferred|founders?|treasury)\s+shares?|(?:these|those|such|our)\s+shares)"
_RECEIPT_NOTE = r"(?:ADSs?|ADRs?|depositary)"
_COUNT_LINK = r"(?:counts?|amounts?|totals?|number|outstanding|issued|held\s+by|net\s+of|including|excluding)"
_GLOBAL_SHARE_UNITS = re.compile(
    r"\bAll\s+(?:(?:common|ordinary)\s+)?share(?:s|\s+and\s+per[- ]share)?\s+(?:amounts?|counts?|numbers?|data|figures?)\b[^.!?]{0,180}\b(?:reflect|adjusted|restated)\b[^.!?]{0,100}\b(?:splits?|consolidation|reclassification)\b"
    r"|\ball\s+references\s+(?:in|throughout)\b[^.!?]{0,80}\b(?:annual\s+report|this\s+report)\b[^.!?]{0,60}\bshare\s+and\s+per[- ]share\s+data\b[^.!?]{0,180}\b(?:adjusted|restated)\b[^.!?]{0,160}\b(?:splits?|consolidation|reclassification)\b", re.I)


def _report_wide_unit_notes(text: str) -> list[re.Match]:
    """Explicit financial-statement or MD&A policies do not cover counts."""
    notes = []
    for match in _GLOBAL_SHARE_UNITS.finditer(text):
        quote = match.group()
        section_scope = re.search(
            r"\b(?:in|within|for)\s+(?:(?:the|these|this|our|its|accompanying)\s+)*"
            r"(?:(?:(?:audited|unaudited|consolidated|condensed|combined|interim)\s+)*financial\s+statements"
            r"|management[’']?s\s+discussion\s+and\s+analysis|MD&A)\b", quote, re.I)
        # Unqualified "all share amounts/data" or "herein" declarations
        # cover the reported share quantities. An explicit narrower financial-
        # statement scope is preserved; its location under a financial heading
        # alone is not an invented limitation on an otherwise all-share note.
        if not section_scope:
            notes.append(match)
    return notes


def _space(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ").replace("\u200b", "")).strip()


def _declarations(text: str) -> list[tuple[int, int, bool]]:
    """Enumerate both supported declaration forms, in literal source order."""
    standard = list(_PROMPT.finditer(text))
    found = [(m.start(), m.end(), True) for m in standard]
    found.extend((m.start(), m.end(), False) for m in _CUSTOM.finditer(text)
                 if not any(p.start() <= m.start() < p.end() for p in standard))
    return sorted(found)


def _response_span(text: str, declaration: tuple[int, int, bool], next_start: int | None = None) -> tuple[int, int]:
    start, prompt_end, standard = declaration
    response_start = prompt_end if standard else start
    boundary = _NEXT.search(text, response_start)
    end = boundary.start() if boundary else len(text)
    if next_start is not None and (not standard or _PROMPT.match(text, next_start)):
        end = min(end, next_start)
    return response_start, end


def _cover_region(text: str, first_declaration: int) -> tuple[str, int, bool]:
    """Keep later cover notes; running TOC links are not section boundaries.

    A finite source EOF is a valid boundary (notably for 40-F covers). A hard
    safety limit is visibly marked as unresolved and cannot certify a census.
    """
    section = _SECTION.search(text, first_declaration)
    if section and section.start() <= 60000:
        return text[:section.end()], section.end(), True
    if len(text) <= 60000:
        return text, len(text), True
    return text[:60000] + " [COVER REGION TRUNCATED]", 60000, False


def _pdf_source_lines(raw: str | None) -> list[str] | None:
    """Recover untouched lines from W1c's inert HTML PDF page transport."""
    if raw is None:
        return None
    pages = re.findall(r"<div>(.*?)</div>", raw, re.I | re.S)
    if not pages or any("<" in page for page in pages) or not any("\n" in page for page in pages):
        return None
    if re.sub(r"<div>.*?</div>", "", raw, flags=re.I | re.S).strip():
        return None
    return [line for page in pages for line in unescape(page).splitlines()]


def _has_source_superscript(raw: str, start: int, end: int) -> bool:
    """Locate superscripts in W1c text offsets without reading the body twice."""
    cursor = length = hidden = 0
    opened = []
    for token in _HTML_TOKEN.finditer(raw):
        if cursor < token.start() and not hidden:
            value = _space(unescape(raw[cursor:token.start()]))
            length += len(value) + 1 if value else 0
        tag = (token.group("tag") or "").lower()
        closing = bool(token.group("closing"))
        if tag in ("script", "style", "ix:header"):
            hidden = max(0, hidden - 1) if closing else hidden + 1
        if tag == "sup" and not hidden:
            if closing and opened:
                first = opened.pop()
                if first < end and length > start:
                    return True
            elif not closing:
                opened.append(length)
        cursor = token.end()
        if length > end:
            break
    return False


def _reading_span(doc: _Document, start: int, end: int,
                  source_lines: list[str] | None = None) -> tuple[str, dict]:
    """Serialize proven row/cell/line boundaries instead of flattening them.

    Row strings are located in the W1c normalized source before replacement.
    An unlocatable intersecting table fails closed. Nested layout tables are
    discarded when their inner rows already provide the literal structure.
    """
    replacements: list[tuple[int, int, str]] = []
    provenance = []
    failed_regions = []
    if source_lines is not None:
        cursor = 0
        for line in source_lines:
            clean = _space(line)
            if not clean:
                continue
            position = doc.text.find(clean, cursor)
            if position < 0:
                return doc.text[start:end] + "\n[UNRESOLVED SOURCE LINE]", {"kind": "pdf_lines", "unresolved": True}
            cursor = position + len(clean)
            if position < end and cursor > start:
                lo, hi = max(start, position), min(end, cursor)
                value = doc.text[lo:hi]
                replacements.append((lo, hi, value + "\n"))
                provenance.append({"start": lo, "end": hi, "line": clean})
        kind = "pdf_lines"
    else:
        kind = "html_tables" if doc.tables else "text"
        rows = []
        for table in doc.tables:
            flat = _space(" ".join(cell for row in table.rows for cell in row))
            if not flat:
                continue
            table_start = doc.text.find(flat, table.offset, table.offset + len(flat) + 300)
            if table_start < 0:
                # A nested outer layout row duplicates child cells. Inner
                # table rows remain independently locatable; do not elect the
                # duplicated outer table as structure.
                if table.offset < end and table.offset + len(flat) > start:
                    failed_regions.append((table.offset, table.offset + len(flat)))
                continue
            cursor = table_start
            for cells in table.rows:
                row_text = _space(" ".join(cells))
                if not row_text:
                    continue
                position = doc.text.find(row_text, cursor, table_start + len(flat) + 1)
                if position < 0:
                    if cursor < end and cursor + len(row_text) > start:
                        failed_regions.append((cursor, cursor + len(row_text)))
                    continue
                cursor = position + len(row_text)
                if position >= end or cursor <= start:
                    continue
                cell_cursor = position
                clipped = []
                for cell in cells:
                    if not cell:
                        clipped.append("")
                        continue
                    cell_start = doc.text.find(cell, cell_cursor, cursor)
                    if cell_start < 0:
                        clipped.append("[UNRESOLVED TABLE CELL]")
                        continue
                    cell_end = cell_start + len(cell)
                    cell_cursor = cell_end
                    if cell_start < end and cell_end > start:
                        clipped.append(doc.text[max(start, cell_start):min(end, cell_end)])
                lo, hi = max(start, position), min(end, cursor)
                rows.append((lo, hi, "\n" + "\t".join(clipped) + "\n", clipped))
        # Prefer the smallest exact row where nested tables overlap.
        for lo, hi, value, cells in sorted(rows, key=lambda row: (row[1] - row[0], row[0])):
            if any(lo < old_hi and hi > old_lo for old_lo, old_hi, _ in replacements):
                continue
            replacements.append((lo, hi, value))
            provenance.append({"start": lo, "end": hi, "cells": cells})
    pieces = []
    cursor = start
    for lo, hi, value in sorted(replacements):
        pieces.extend((doc.text[cursor:lo], value))
        cursor = hi
    pieces.append(doc.text[cursor:end])
    if failed_regions:
        flat_response = _source_response(doc.text[start:end])
        response_offset = doc.text.find(flat_response, start, end) if flat_response else start
        _, literal_names, literal_numbers, _ = _source_lex(flat_response)
        required = [(response_offset + m.start(), response_offset + m.end()) for m in literal_names + literal_numbers]
        # Only independently located inner rows covering every class/count
        # token can justify ignoring an unlocatable outer layout table.
        inner_proven = required and all(any(lo <= a and b <= hi for lo, hi, _ in replacements)
                                        for a, b in required)
        if not inner_proven:
            pieces.append("\n[UNRESOLVED TABLE STRUCTURE]")
            provenance.append({"unresolved_regions": failed_regions})
    return "".join(pieces).strip(" \r"), {"kind": kind, "rows": provenance}


def _source_response(span_text: str) -> str:
    prompt = _PROMPT.search(span_text)
    start = prompt.end() if prompt else 0
    following = _NEXT.search(span_text, start)
    return span_text[start:following.start() if following else len(span_text)].strip(" \r")


def _source_lex(response: str) -> tuple[str, list[re.Match], list[re.Match], list[re.Match]]:
    dates = list(_DATE.finditer(response))
    ignored = [(m.start(), m.end()) for m in dates]
    ignored.extend((m.start(), m.end()) for m in _PAR.finditer(response))
    ignored.extend((m.start(), m.end()) for m in re.finditer(r"\bwithout\s+nominal\s+value\b", response, re.I))
    cleaned = _mask(response, ignored)
    names = [m for m in _NAME.finditer(cleaned)
             if normalize_class_key(m.group()) or _kind(m.group()) != "other"
             or re.search(r"deferred|founder|preferred|preference", m.group(), re.I)]
    numbers = _source_numbers(cleaned, names)
    return cleaned, names, numbers, dates


def _source_numbers(cleaned: str, names: list[re.Match]) -> list[re.Match]:
    """Space grouping is supported only in one proven, purely numeric cell.

    ASCII whitespace between independent nodes or PDF lines is never grouping.
    Tabs serialize actual HTML cell boundaries, so a cell containing exactly
    ``1 234 567`` supplies one integer field without merging another cell.
    """
    grouped = []
    if "\t" in cleaned:
        for match in re.finditer(r"\d{1,3}(?: \d{3})+", cleaned):
            lo = max(cleaned.rfind("\t", 0, match.start()), cleaned.rfind("\n", 0, match.start())) + 1
            ends = [p for p in (cleaned.find("\t", match.end()), cleaned.find("\n", match.end())) if p >= 0]
            hi = min(ends) if ends else len(cleaned)
            if cleaned[lo:match.start()].strip() == cleaned[match.end():hi].strip() == "":
                grouped.append(match)
    matches = [m for m in _NUM.finditer(cleaned)
               if not any(n.start() <= m.start() < n.end() for n in names)
               and not any(g.start() <= m.start() < g.end() for g in grouped)]
    return sorted(matches + grouped, key=lambda m: m.start())


def _declaration_signature(response: str, period: str | None = None) -> tuple:
    """Compare literal declarations without invoking candidate extraction."""
    cleaned, names, numbers, dates = _source_lex(response)
    source_dates = {_parse_date(m.group()) for m in dates} - {None}
    if not source_dates and period:
        source_dates.add(period)
    return (tuple((normalize_class_key(m.group()) or m.group().lower(), _kind(m.group())) for m in names),
            tuple(_count(m.group()) for m in numbers),
            tuple(sorted(source_dates)))


def _invalid_columns(response: str) -> set[tuple[int, int]]:
    """Check explicit class/count header roles and consistent cell columns."""
    invalid = set()
    response = _source_lex(response)[0]
    class_column = count_column = None
    inferred = None
    logical_two_columns = False
    offset = 0
    for line in response.splitlines(keepends=True):
        row = line.rstrip("\r\n")
        row_end = offset + len(row)
        if "\t" not in row:
            offset += len(line)
            continue
        cells = row.split("\t")
        labels = [_space(cell).lower().strip(":") for cell in cells]
        class_headers = [i for i, cell in enumerate(labels)
                         if re.fullmatch(r"class(?:\s+of\s+(?:shares|stock))?|title\s+of\s+(?:each\s+)?class|security\s+class", cell)]
        count_headers = [i for i, cell in enumerate(labels)
                         if re.fullmatch(r"number(?:\s+of)?(?:\s+outstanding)?(?:\s+shares)?(?:\s+outstanding)?|shares\s+outstanding", cell)]
        if len(class_headers) == len(count_headers) == 1:
            nonempty_columns = [i for i, label in enumerate(labels) if label]
            logical_two_columns = len(nonempty_columns) == 2
            if logical_two_columns:
                class_column = nonempty_columns.index(class_headers[0])
                count_column = nonempty_columns.index(count_headers[0])
            else:
                class_column, count_column = class_headers[0], count_headers[0]
            inferred = None
            offset += len(line)
            continue
        if logical_two_columns:
            # Two declared semantic fields can have empty spacer cells and
            # colspan presentation. Compact padding only for column-role
            # checks; original rows and blank quantities remain untouched.
            cells = [cell for cell in cells if _space(cell)]
            labels = [_space(cell).lower().strip(":") for cell in cells]
        name_cells = []
        number_cells = []
        for index, cell in enumerate(cells):
            _, names, numbers, _ = _source_lex(cell)
            name_cells.extend([index] * len(names))
            number_cells.extend([index] * len(numbers))
        if len(name_cells) == len(number_cells) == 1:
            pair = (name_cells[0], number_cells[0])
            expected = (class_column, count_column) if class_column is not None else inferred
            if expected is not None and pair != expected:
                invalid.add((offset, row_end))
            elif inferred is None and class_column is None and pair[0] != pair[1]:
                inferred = pair
        elif class_column is not None and name_cells:
            if any(index != class_column for index in name_cells) or any(index != count_column for index in number_cells):
                invalid.add((offset, row_end))
        elif class_column is not None and class_column < len(cells) and labels[class_column]:
            if labels[class_column] not in ("total", "in aggregate"):
                invalid.add((offset, row_end))
        offset += len(line)
    return invalid


def _unsupported_sign(cleaned: str, names: list[re.Match]) -> bool:
    """Normalize notation only for refusal; preserve all literal source text."""
    outside_names = _mask(cleaned, [(m.start(), m.end()) for m in names])
    normalized = unicodedata.normalize("NFKC", outside_names)
    if re.search(r"\(\s*\d[\d, .]*\s*\)", normalized):
        return True
    for position, character in enumerate(normalized):
        if (unicodedata.category(character) == "Pd" or character in "+−⁻₋") and re.match(r"\s*\d", normalized[position + 1:]):
            return True
    return False


def _structural_reasons(response: str) -> set[str]:
    """Validate the actual source rows, independently of a supplied shape."""
    reasons = set()
    if "\n" not in response and "\t" not in response:
        return reasons
    if _invalid_columns(response):
        reasons.update(("count_class_row_mismatch", "inconsistent_table_columns", "class_count_unresolved"))
    # Dates and explicit nominal/par-value clauses can continue across rows.
    # Mask them on the whole source before inspecting individual row fields.
    source_cleaned = _source_lex(response)[0]
    for line in source_cleaned.splitlines():
        line_cleaned, line_names, line_numbers, _ = _source_lex(line)
        line_remainder = _HEADERS.sub(" ", _mask(line_cleaned, [(m.start(), m.end()) for m in line_names + line_numbers]))
        if any(re.fullmatch(_BARE_ID, word) and normalize_class_key("Class " + word) for word in re.findall(r"[A-Za-z]+", line_remainder)):
            reasons.add("unresolved_bare_class_token")
        if not line_names and line_numbers and not re.search(r"\b(?:total|aggregate)\b", line_cleaned, re.I):
            reasons.update(("count_class_row_mismatch", "ambiguous_numeric_cells"))
        elif line_names and len(line_numbers) < len(line_names):
            reasons.update(("count_class_row_mismatch", "class_count_unresolved"))
        elif line_names and len(line_numbers) > len(line_names):
            reasons.add("ambiguous_numeric_cells")
            if "\t" in line:
                reasons.add("ambiguous_row_numbers")
    return reasons


def _semantic_source_reasons(response: str) -> set[str]:
    """Apply source refusals even when a proposal strips their literal text."""
    reasons = set()
    cleaned, names, numbers, dates = _source_lex(response)
    if re.search(r"[*†‡]|[⁰¹²³⁴⁵⁶⁷⁸⁹]|\d[\d,]*\s*\(\s*\d{1,2}\s*\)", response):
        reasons.add("footnoted_or_qualified_count")
    if re.search(r"\b(?:thousands?|millions?|billions?)\b", cleaned, re.I):
        reasons.add("scaled_quantity")
    if _unsupported_sign(cleaned, names):
        reasons.add("unsupported_sign_notation")
    if re.search(r"\b(?:or|either|alternatively|if|unless|provided|conditional(?:ly)?)\b", cleaned, re.I):
        reasons.add("alternative_or_conditional_statement")
    if re.search(r"\b(?:approximately|approx\.?|about|excluding|including|treasury|issued|held\s+by|combined|combines?|authorized|authorised|American\s+depositary|ADSs?|ADRs?|less|net\s+of)\b", cleaned, re.I):
        reasons.update(("qualified_or_nonordinary_statement", "footnoted_or_qualified_count"))
    if not names:
        reasons.add("no_named_classes")
    for i, name in enumerate(names):
        left = names[i - 1].end() if i else 0
        right = names[i + 1].start() if i + 1 < len(names) else len(cleaned)
        if normalize_class_key(name.group()) in ("ordinary", "common") and (
            re.search(r"\b(?:total|aggregate)\b", cleaned[left:name.start()], re.I)
            or re.search(r"\bin\s+aggregate\b", cleaned[name.end():right], re.I)
        ):
            reasons.add("aggregate_only_statement")
        if i + 1 < len(names) and re.fullmatch(r"\s*(?:and|&)\s*", cleaned[name.end():names[i + 1].start()], re.I):
            reasons.add("combined_class_count")
    remainder = _HEADERS.sub(" ", _mask(cleaned, [(m.start(), m.end()) for m in names + numbers]))
    if any(ord(character) > 127 and unicodedata.category(character).startswith("L") for character in remainder):
        reasons.update(("unparsed_class_or_text_residue", "class_count_unresolved"))
    for word in re.findall(r"[A-Za-z]+", remainder):
        if re.fullmatch(_BARE_ID, word) and normalize_class_key("Class " + word):
            reasons.add("unresolved_bare_class_token")
        elif word.lower() not in _GLUE:
            reasons.add("unparsed_class_or_text_residue")
    if re.search(r"\d", remainder):
        reasons.add("unparsed_numeric_residue")
    source_totals = sum(bool(re.search(r"\b(?:total(?:\s+(?:of|shares|outstanding|share\s+capital)){0,3}|in\s+aggregate)\s*[:=]?\s*$",
                                      cleaned[max(0, number.start() - 65):number.start()], re.I))
                        for number in numbers)
    if len(numbers) > len(names) + source_totals:
        reasons.add("unparsed_numeric_residue")
    if any(_parse_date(m.group()) is None for m in dates):
        reasons.add("invalid_statement_date")
    if len({_parse_date(m.group()) for m in dates} - {None}) > 1:
        reasons.add("multiple_statement_dates")
    return reasons


def check_reading(span_text: str, cover_region_text: str, candidate: dict,
                  *, period_end: str | None = None) -> tuple[str, list[str]]:
    """Independently validate a supplied reading against literal source.

    Candidate ``complete``, ``reasons``, source snippets and structural claims
    are ignored. Raw HTML is accepted and its actual retained table cells are
    serialized here; plain source can carry tab/cell and newline/row boundaries.
    A false or stale extraction cannot certify itself by setting a status flag.
    """
    reasons = set()
    raw_span = span_text
    if re.search(r"</?[A-Za-z][^>]*>", span_text):
        source_doc = _Document(span_text)
        span_text, _ = _reading_span(source_doc, 0, len(source_doc.text), _pdf_source_lines(span_text))
    if re.search(r"</?[A-Za-z][^>]*>", cover_region_text):
        raw_cover = cover_region_text
        cover_doc = _Document(raw_cover)
        actual_declarations = _declarations(cover_doc.text)
        for i, declaration in enumerate(actual_declarations):
            lo, hi = _response_span(cover_doc.text, declaration,
                                    actual_declarations[i + 1][0] if i + 1 < len(actual_declarations) else None)
            if "<sup" in raw_cover.lower() and _has_source_superscript(raw_cover, lo, hi):
                reasons.add("footnoted_or_qualified_count")
        cover_region_text, _ = _reading_span(cover_doc, 0, len(cover_doc.text), _pdf_source_lines(raw_cover))
    response = _source_response(span_text)
    cleaned, names, numbers, dates = _source_lex(response)
    declarations = _declarations(cover_region_text)
    source_responses = []
    response_regions = []
    for i, declaration in enumerate(declarations):
        lo, hi = _response_span(cover_region_text, declaration,
                                declarations[i + 1][0] if i + 1 < len(declarations) else None)
        source_responses.append(cover_region_text[lo:hi].strip(" \r"))
        response_regions.append((lo, hi))
    for actual_response in source_responses:
        reasons.update(_structural_reasons(actual_response))
        reasons.update(_semantic_source_reasons(actual_response))
    reasons.update(_semantic_source_reasons(response))
    # A caller cannot remove a count-linked marker from its proposed span.
    # Qualifiers are checked on the real declarations and the later cover.
    qualified_sources = [response, *source_responses]
    if any(re.search(r"[*†‡]|[⁰¹²³⁴⁵⁶⁷⁸⁹]|\d[\d,]*\s*\(\s*\d{1,2}\s*\)", s) for s in qualified_sources) or re.search(r"<sup\b", raw_span, re.I):
        reasons.add("footnoted_or_qualified_count")
    late_cover = _mask(cover_region_text, response_regions)
    if response_regions:
        late_cover = late_cover[min(hi for _, hi in response_regions):]
    else:
        position = cover_region_text.find(response)
        if position >= 0:
            late_cover = _mask(late_cover, [(position, position + len(response))])
    if re.search(rf"\b{_NOTE_QUALIFIER}\b.{{0,100}}\b{_NOTE_SUBJECT}\b|\b{_NOTE_SUBJECT}\b.{{0,100}}\b{_NOTE_QUALIFIER}\b", late_cover, re.I):
        reasons.add("footnoted_or_qualified_count")
    # A 12(b) note merely explaining the receipt wrapper (TSM, for example)
    # does not qualify the ordinary count. Receipt notes must actually refer
    # to a count, outstanding supply, or a count qualification.
    if re.search(rf"\b{_RECEIPT_NOTE}\b.{{0,100}}\b{_COUNT_LINK}\b|\b{_COUNT_LINK}\b.{{0,100}}\b{_RECEIPT_NOTE}\b", late_cover, re.I):
        reasons.add("footnoted_or_qualified_count")
    # An unmarked later class/count note also changes the purported exhaustive
    # statement. Refuse it without inventing or adding a class to the reading.
    late_cleaned, late_names, late_numbers, _ = _source_lex(late_cover)
    _, initial_names, _, _ = _source_lex(source_responses[0] if source_responses else response)
    initial_class_keys = {normalize_class_key(m.group()) for m in initial_names}
    for late_name in late_names:
        late_key = normalize_class_key(late_name.group())
        preceding = late_cleaned[max(0, late_name.start() - 100):late_name.start()]
        if _kind(late_name.group()) == "other" and re.search(r"\b(?:medium[- ]term\s+notes?|bonds?|debt\s+securities|index\s+warrants?)\s*[,;:]?\s*$", preceding, re.I):
            continue
        for late_number in late_numbers:
            context_start = max(0, min(late_name.start(), late_number.start()) - 260)
            context_end = max(late_name.end(), late_number.end()) + 30
            quantity_context = late_cleaned[context_start:context_end]
            if re.search(r"\b(?:ADSs?|ADRs?|American\s+Depositary\s+(?:Shares|Receipts))\b.{0,220}\b(?:represents?|representing|evidences?)\b", quantity_context, re.I):
                continue
            if late_number.end() <= late_name.start():
                bridge = late_cleaned[late_number.end():late_name.start()]
            elif late_name.end() <= late_number.start():
                bridge = late_cleaned[late_name.end():late_number.start()]
            else:
                continue
            if len(bridge) <= 100 and (not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"[\s,:;()]*((?:were|was|are|is|outstanding)[\s,:;()]*)*", bridge, re.I)):
                reasons.add("footnoted_or_qualified_count")
                break
            if len(bridge) <= 100 and late_key is not None and late_key.startswith(("class:", "series:")) and late_key not in initial_class_keys:
                reasons.add("footnoted_or_qualified_count")
                break
    if late_names and re.search(r"\b(?:another|other)\s+class(?:es)?\b", late_cover, re.I):
        reasons.add("footnoted_or_qualified_count")
    if "[COVER REGION TRUNCATED]" in cover_region_text:
        reasons.add("cover_boundary_unresolved")
    if _report_wide_unit_notes(cover_region_text):
        reasons.add("footnoted_or_qualified_count")
    source_period = _period_end(cover_region_text, None)
    trusted_period = source_period or (_parse_date(period_end) if period_end else None)
    if source_responses:
        literal_equal = any(_space(response) == _space(r) for r in source_responses)
        if not literal_equal:
            reasons.add("candidate_source_mismatch")
    elif _space(response) not in _space(cover_region_text):
        reasons.add("candidate_source_mismatch")
    if len(source_responses) > 1 and len({_declaration_signature(r, trusted_period) for r in source_responses}) > 1:
        reasons.add("multiple_cover_statements")
        reasons.add("disagreeing_cover_statements")
    if re.search(r"\b(?:thousands?|millions?|billions?)\b", cleaned, re.I):
        reasons.add("scaled_quantity")
    if _unsupported_sign(cleaned, names):
        reasons.add("unsupported_sign_notation")
    if re.search(r"\b(?:or|either|alternatively|if|unless|provided|conditional(?:ly)?)\b", cleaned, re.I):
        reasons.add("alternative_or_conditional_statement")
    if re.search(r"\b(?:approximately|approx\.?|about|excluding|including|treasury|issued\s+and|authorized|authorised|American\s+depositary|ADSs?|ADRs?|less|net\s+of)\b", cleaned, re.I):
        reasons.add("qualified_or_nonordinary_statement")
        reasons.add("footnoted_or_qualified_count")
    if "[UNRESOLVED" in response:
        reasons.add("count_class_row_mismatch")
    classes = candidate.get("classes")
    if not isinstance(classes, list):
        classes = []
        reasons.add("candidate_class_mismatch")
    if not names:
        reasons.add("no_named_classes")
    if len(classes) != len(names):
        reasons.add("candidate_class_mismatch")
    used = set()
    totals = []
    for n, number in enumerate(numbers):
        before = cleaned[max(0, number.start() - 65):number.start()]
        if re.search(r"\b(?:total(?:\s+(?:of|shares|outstanding|share\s+capital)){0,3}|in\s+aggregate)\s*[:=]?\s*$", before, re.I):
            totals.append(_count(number.group()))
            used.add(n)
    read_counts = []
    for i, name in enumerate(names):
        literal = response[name.start():name.end()]
        key = normalize_class_key(literal)
        row = classes[i] if i < len(classes) and isinstance(classes[i], dict) else {}
        if row.get("class_name") != literal or row.get("class_key") != key or row.get("class_kind") != _kind(literal):
            reasons.add("candidate_class_mismatch")
        left = names[i - 1].end() if i else 0
        right = names[i + 1].start() if i + 1 < len(names) else len(cleaned)
        left = max(left, cleaned.rfind("\n", 0, name.start()) + 1)
        row_end = cleaned.find("\n", name.end())
        if row_end >= 0:
            right = min(right, row_end)
        before = [(n, m) for n, m in enumerate(numbers) if n not in used and left <= m.start() < name.start()]
        after = [(n, m) for n, m in enumerate(numbers) if n not in used and name.end() <= m.start() < right]
        possibilities = []
        if before:
            n, number = before[-1]
            bridge = cleaned[number.end():name.start()]
            if "\n" not in bridge and (not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"\s*(?:outstanding\s+)?", bridge, re.I)):
                possibilities.append((n, number, "before"))
        if after:
            n, number = after[0]
            bridge = cleaned[name.end():number.start()]
            if "\n" not in bridge and (not re.search(r"[A-Za-z]", bridge) or re.fullmatch(r"[\s,:;()]*((?:were|was|are|is|outstanding|as\s+of|without\s+nominal\s+value)[\s,:;()]*)*", bridge, re.I)):
                possibilities.append((n, number, "after"))
        combined = (i + 1 < len(names) and re.fullmatch(r"\s*(?:and|&)\s*", cleaned[name.end():names[i + 1].start()], re.I))
        if combined:
            reasons.add("combined_class_count")
            if len(possibilities) == 1 and possibilities[0][2] == "before":
                n, number, _ = possibilities[0]
                totals.append(_count(number.group()))
                used.add(n)
            possibilities = []
        if i and re.fullmatch(r"\s*(?:and|&)\s*", cleaned[names[i - 1].end():name.start()], re.I) and read_counts[-1] is None:
            possibilities = []
        if len(possibilities) == 2:
            colon = ":" in cleaned[name.end():possibilities[1][1].start()]
            possibilities = [possibilities[1] if colon else possibilities[0]]
        if "\t" in response or "\n" in response:
            row_start = response.rfind("\n", 0, name.start()) + 1
            row_end = response.find("\n", name.end())
            row_end = len(response) if row_end < 0 else row_end
            _, row_names, row_numbers, _ = _source_lex(response[row_start:row_end])
            if len(row_numbers) > len(row_names):
                possibilities = []
            if (row_start, row_end) in _invalid_columns(response):
                possibilities = []
        value = None
        if len(possibilities) == 1:
            n, number, _ = possibilities[0]
            value = _count(number.group())
            used.add(n)
        if value is None:
            reasons.add("class_count_unresolved")
        if row.get("shares") != value:
            reasons.add("candidate_count_mismatch")
        read_counts.append(value)
        if key in ("ordinary", "common") and (re.search(r"\b(?:total|aggregate)\b", cleaned[left:name.start()], re.I) or re.search(r"\bin\s+aggregate\b", cleaned[name.end():right], re.I)):
            reasons.add("aggregate_only_statement")
            if value is not None:
                totals.append(value)
    recognized = [(m.start(), m.end()) for m in names + numbers]
    remainder = _mask(cleaned, recognized)
    if any(n not in used for n in range(len(numbers))) or re.search(r"\d", remainder):
        reasons.add("unparsed_numeric_residue")
    remainder = _HEADERS.sub(" ", remainder)
    for token in re.finditer(r"[A-Za-z]+", remainder):
        word = token.group()
        if re.fullmatch(_BARE_ID, word) and normalize_class_key("Class " + word):
            reasons.add("unresolved_bare_class_token")
        elif word.lower() not in _GLUE:
            reasons.add("unparsed_class_or_text_residue")
    keys = [normalize_class_key(m.group()) or m.group().lower() for m in names]
    if len(keys) != len(set(keys)):
        reasons.add("duplicate_class_identity")
    stated_dates = {_parse_date(m.group()) for m in dates} - {None}
    if any(_parse_date(m.group()) is None for m in dates):
        reasons.add("invalid_statement_date")
    expected_date = next(iter(stated_dates)) if len(stated_dates) == 1 else trusted_period
    if len(stated_dates) > 1:
        reasons.add("multiple_statement_dates")
    if expected_date is None:
        reasons.add("statement_date_unresolved")
    elif candidate.get("shares_as_of") != expected_date:
        reasons.add("candidate_date_mismatch")
    if source_period is not None and candidate.get("period_end") not in (None, source_period):
        reasons.add("candidate_date_mismatch")
    # A row or line is a hard association boundary. Do not let a count in the
    # next row fill a blank cell, and do not let separate cells become grouping.
    reasons.update(_structural_reasons(response))
    computed = sum(read_counts) if read_counts and all(v is not None for v in read_counts) else None
    if candidate.get("computed_total") not in (None, computed):
        reasons.add("candidate_count_mismatch")
    source_totals = set(totals)
    if len(source_totals) > 1 or None in source_totals:
        reasons.add("multiple_or_invalid_totals")
    expected_total = next(iter(source_totals)) if len(source_totals) == 1 else None
    if candidate.get("stated_total") != expected_total:
        reasons.add("candidate_total_mismatch")
    if expected_total is not None and computed is not None and expected_total != computed:
        reasons.add("stated_total_mismatch")
    status = "conflicting" if "stated_total_mismatch" in reasons else "incomplete" if reasons else "complete"
    return status, sorted(reasons)


def parse_share_census(document: _Document | str, period_end: str | date | None = None,
                      *, source_lines: list[str] | None = None) -> dict:
    """Extract a candidate and certify it against the whole retained cover."""
    raw = document if isinstance(document, str) else None
    doc = _Document(document) if raw is not None else document
    text = doc.text
    period = period_end.isoformat() if isinstance(period_end, date) else period_end
    period = _parse_date(period) if period else _period_end(text, raw)
    initial = _declarations(text[:18000])
    if not initial:
        result = _extract_candidate("", period)
        result.update(status="none", complete=False, reasons=["cover_statement_not_found"])
        return result
    region, region_end, boundary_verified = _cover_region(text, initial[0][1])
    declarations = _declarations(text[:region_end])
    selected = declarations[0]
    lo, end = _response_span(text[:region_end], selected,
                             declarations[1][0] if len(declarations) > 1 else None)
    start = selected[0]
    flat_span = text[start:end].strip()
    line_source = source_lines if source_lines is not None else _pdf_source_lines(raw)
    reading_span, structure = _reading_span(doc, start, end, line_source)
    response = _source_response(reading_span)
    result = _extract_candidate(response, period)
    result.update(source_text=flat_span,
                  source_location=f"cover-share-statement;text-offset={start};text-span={start}:{end}",
                  reading_span_text=reading_span, cover_region_text=region,
                  cover_region_location=f"cover-region;text-offset=0;text-span=0:{region_end}",
                  structure=structure)
    reading_cover, _ = _reading_span(doc, 0, region_end, line_source)
    if not boundary_verified:
        reading_cover += " [COVER REGION TRUNCATED]"
    unit_notes = []
    for match in _report_wide_unit_notes(text):
        quote = match.group()
        location = f"share-count-unit-note;text-offset={match.start()};text-span={match.start()}:{match.end()}"
        unit_notes.append({"text": quote, "location": location})
        if match.start() >= region_end:
            reading_cover += "\n[Global share-count unit declaration] " + quote
            result["cover_region_text"] += "\n[Global share-count unit declaration; " + location + "] " + quote
    result["share_unit_notes"] = unit_notes
    result["reading_cover_region_text"] = reading_cover
    # Raw superscripts are count-footnote evidence even when HTML flattening
    # emits an ordinary digit. The retained reading gets an explicit marker.
    if raw and "<sup" in raw.lower() and _has_source_superscript(raw, start, end):
        reading_span += " †"
        result["reading_span_text"] = reading_span
    status, reasons = check_reading(reading_span, reading_cover, result, period_end=period)
    if not boundary_verified and "cover_boundary_unresolved" not in reasons:
        reasons.append("cover_boundary_unresolved")
    result.update(status=status, complete=status == "complete", conflicting=status == "conflicting",
                  reasons=sorted(set(reasons)))
    return result
