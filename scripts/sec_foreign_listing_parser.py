"""Extract auditable foreign listing facts from SEC filing HTML or text.

This module performs no network or database access. A parser finding is an
observation, not an admission decision. In particular, disagreement is retained
as separate evidence for the dated resolver rather than resolved by this parser.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from fractions import Fraction
from hashlib import sha256
from html import unescape
from html.parser import HTMLParser
import re
from typing import Iterable

PARSER_VERSION = "foreign-listing-v1"
_SPACE = re.compile(r"\s+")
_ADS = r"(?:(?:American|Global)\s+depositary\s+(?:shares?|receipts?)|American\s+shares?\s*\(evidenced\s+by\s+depositary\s+receipts\)|[AG]D[SR]s?)"
_SHARES = r"(?:(?:(?:class|series)\s+[A-Z0-9]+\s+)?(?:ordinary|common)\s+shares?|shares?\s+of\s+common\s+stock|(?:class|series)\s+[A-Z0-9]+\s+shares?|shares?)"
_WORDS = "one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|half|third|quarter"
_QUANTITY = rf"(?:\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?|(?:{_WORDS})(?:[ -]+(?:{_WORDS}|and)){{0,5}})(?:\s*\(\s*\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?\s*\))?"
_RATIO = re.compile(
    rf"(?P<adsn>each|{_QUANTITY})\s+{_ADS}\b(?:[^.;]|\.(?=\d)){{0,100}}?\b(?:represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?|to|per)\s+(?P<ordinary>{_QUANTITY})(?:\s+of(?:\s+(?:one|an?|the))?)?\s+(?:(?:of\s+)?(?:our|the|its|company.s)\s+)?{_SHARES}",
    re.I,
)
_RATIO_TITLE = re.compile(
    rf"{_ADS}(?!\w)[^.;]{{0,100}}?\b(?:each\s+(?:of\s+which\s+)?|which\s+)represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?\s+(?P<ordinary>{_QUANTITY})(?:\s+of(?:\s+(?:one|an?|the))?)?\s+{_SHARES}",
    re.I,
)
_NOT_TRADING = re.compile(r"not\s+(?:for\s+trading|listed\s+for\s+trading)|non[- ]traded|without\s+trading\s+privileges", re.I)
_DEPOSITARY_NOTE = re.compile(_NOT_TRADING.pattern + r"|traded\s+in\s+the\s+form\s+of", re.I)
_EXCHANGE = re.compile(r"New York Stock Exchange|NYSE|Nasdaq|NASDAQ|NYSE American|NYSE MKT|NYSE Arca|NYSEAMERICAN", re.I)
# Attribute contents are quote-aware; atomic groups avoid pathological
# backtracking on incomplete downloaded HTML. Hidden blocks are tokens, so a
# '<script>' string inside an attribute or comment never hides visible content.
_HTML_TOKEN = re.compile(
    r'''<(?P<hidden>ix:header|script|style)\b(?>[^"'<>]+|"[^"]*"|'[^']*')*>.*?</(?P=hidden)\s*>'''
    r'''|<!--.*?--\s*>|<!\[CDATA\[.*?\]\]>'''
    r'''|<(?P<closing>/?)(?P<tag>[A-Za-z][A-Za-z0-9:_-]*)(?>[^"'<>]+|"[^"]*"|'[^']*')*>'''
    r'''|<![^>]*>|<\?[^>]*>''', re.S | re.I,
)


def _clean(value: str) -> str:
    return _SPACE.sub(" ", value.replace("\xa0", " ").replace("\u200b", "")).strip()


@dataclass
class _Table:
    offset: int
    line: int
    rows: list[list[str]] = field(default_factory=list)


class _Document(HTMLParser):
    """Stdlib HTML reader; HTML lines are original, offsets are normalized text."""

    def __init__(self, content: str):
        super().__init__(convert_charrefs=True)
        self._clear_document()
        if not self._read_fast(content):
            # Preserve HTMLParser's tolerant handling for malformed markup or
            # literal '<' text. SEC's well-formed filing HTML uses the fast path.
            self.reset()
            self._clear_document()
            self.feed(content)
        self.text = "".join(self.parts)

    def _clear_document(self) -> None:
        self.parts: list[str] = []
        self.length = 0
        self.tables: list[_Table] = []
        self.table_stack: list[_Table] = []
        self.row_stack: list[tuple[_Table | None, list[list[str]]]] = []
        self.cells: list[list[str]] = []
        self.hidden = 0

    def _read_fast(self, content: str) -> bool:
        cursor = 0
        line = 1
        for token in _HTML_TOKEN.finditer(content):
            if cursor < token.start():
                data = content[cursor:token.start()]
                if "<" in data:
                    return False
                self.handle_data(unescape(data))
            # Only cover tables are queried. Preserve an already open table,
            # but avoid collecting thousands of financial-statement tables.
            collect = self.length <= 45000 or bool(self.table_stack)
            if collect:
                line += content.count("\n", cursor, token.start())
            tag = token.group("tag")
            if tag:
                tag = tag.lower()
                if tag in ("ix:header", "script", "style"):
                    return False  # An incomplete hidden block needs tolerance.
                if collect and tag in ("table", "tr", "td", "th"):
                    self.lineno = line
                    if token.group("closing"):
                        self.handle_endtag(tag)
                    else:
                        self.handle_starttag(tag, [])
                        if content[token.end() - 2:token.end()] == "/>":
                            self.handle_endtag(tag)
            if collect:
                line += content.count("\n", token.start(), token.end())
            cursor = token.end()
        if cursor < len(content):
            data = content[cursor:]
            if "<" in data:
                return False
            self.handle_data(unescape(data))
        return True

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style", "ix:header"):
            self.hidden += 1
        if tag == "table":
            table = _Table(self.length, self.getpos()[0])
            self.tables.append(table)
            self.table_stack.append(table)
        elif tag == "tr":
            self.row_stack.append((self.table_stack[-1] if self.table_stack else None, []))
        elif tag in ("td", "th"):
            cell: list[str] = []
            self.cells.append(cell)
            if self.row_stack:
                self.row_stack[-1][1].append(cell)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "ix:header"):
            self.hidden = max(0, self.hidden - 1)
        elif tag in ("td", "th") and self.cells:
            self.cells.pop()
        elif tag == "tr" and self.row_stack:
            table, cells = self.row_stack.pop()
            if table is not None:
                table.rows.append([_clean(" ".join(cell)) for cell in cells])
        elif tag == "table" and self.table_stack:
            self.table_stack.pop()

    def handle_data(self, data: str) -> None:
        if self.hidden:
            return
        value = _clean(data)
        if not value:
            return
        self.parts.append(value + " ")
        self.length += len(value) + 1
        for cell in self.cells:
            cell.append(value)


def _number(value: str) -> Fraction:
    value = value.lower().replace(",", "").strip()
    parenthetical = re.search(r"\(([\d./\s]+)\)", value)
    if parenthetical:
        return Fraction(parenthetical.group(1).replace(" ", ""))
    if value == "each":
        return Fraction(1)
    if re.fullmatch(r"[\d./\s]+", value):
        return Fraction(value.replace(" ", ""))
    words = value.replace("-", " ").split()
    fractions = {"half": 2, "third": 3, "quarter": 4}
    if words[-1] in fractions:
        if "and" in words:
            split = len(words) - 1 - words[::-1].index("and")
            whole = _number(" ".join(words[:split]))
            fraction = _number(" ".join(words[split + 1:]))
            return whole + fraction
        return Fraction(_number(" ".join(words[:-1]) or "one"), fractions[words[-1]])
    units = dict(zip("one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen".split(), range(1, 20)))
    units.update(dict(zip("twenty thirty forty fifty sixty seventy eighty ninety".split(), range(20, 100, 10))))
    total = current = 0
    for word in words:
        if word == "and":
            continue
        if word == "hundred":
            current = (current or 1) * 100
        elif word == "thousand":
            total += (current or 1) * 1000
            current = 0
        else:
            current += units[word]
    return Fraction(total + current)


def _ratios(text: str):
    seen: set[tuple[int, int, int]] = set()
    for pattern in (_RATIO, _RATIO_TITLE):
        for match in pattern.finditer(text):
            if re.search(r"\b(?:preferred|preference|units?|CPOs?|baskets?)\b|participation\s+certificates?", match.group(), re.I) or re.match(
                r"\s*(?:of\s+(?:(?:our|the|its)\s+)?)?(?:preferred|preference|units?)\b", text[match.end():match.end() + 80], re.I,
            ):
                continue
            try:
                ordinary = _number(match.group("ordinary"))
                ads = _number(match.groupdict().get("adsn") or "each")
                # F-6 registration tables can place the previous row's fee
                # immediately before the next ADS title. A nested explicit
                # 'each ADS' is the contractual unit, never that fee amount.
                if re.search(rf"\beach\s+(?:{_ADS}|(?:of\s+which\s+)?represent(?:s|ing)?)", match.group(), re.I):
                    ads = Fraction(1)
                # Aggregate outstanding/registered counts are not contractual
                # per-ADS ratios, even when both totals occur in one sentence.
                if ads > 10000:
                    continue
                ratio = ordinary / ads
            except (KeyError, ValueError, ZeroDivisionError):
                continue
            if ratio <= 0:
                continue
            key = (match.end(), ratio.numerator, ratio.denominator)
            if key not in seen:
                seen.add(key)
                yield match, ratio


def _explicit_symbols(text: str) -> list[str]:
    """Only quoted trading-symbol declarations, never bare ticker-shaped words."""
    return re.findall(r"(?:trading\s+symbol|ticker\s+symbol|symbol)\s*(?:is|of|:)?\s*[\"“']([A-Z][A-Z0-9.\-]{0,14})[\"”']", text, re.I)


def _cell_symbols(value: str) -> list[str]:
    value = value.strip(" \"“”'()*†‡")
    if value.upper() in ("NONE", "N/A", "NA", "NOT APPLICABLE"):
        return []
    return [value] if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", value) else []


def _canonical_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    # Filings sometimes place sentence punctuation inside a quoted symbol.
    # Preserve class separators (AB.A), but never store the final full stop.
    normalized = value.strip().rstrip(".").upper().replace(".", "-")
    return normalized if re.fullmatch(r"[A-Z0-9]+(?:-[A-Z0-9]+)*", normalized) else None


def _prior_ratio(text: str, offset: int) -> bool:
    return bool(re.search(
        r"(?:from|previous|current|existing|old|former|then)\s+(?:(?:ADS\s+)?ratio\s+(?:(?:of|was|is)\s+)?)?$",
        text[max(0, offset - 70):offset], re.I,
    ))


def _superseded_ratio(text: str, start: int, end: int) -> bool:
    context = text[max(0, start - 700):end + 700]
    return _prior_ratio(text, start) and bool(re.search(
        r"(?:new|former|previous)\s+(?:ADS\s+)?ratio|(?:chang\w*|amend\w*).{0,100}ratio|ratio.{0,100}(?:chang\w*|amend\w*)",
        context, re.I,
    ))


def _underlying_class(text: str) -> str | None:
    classes = {f"{kind.lower()}_{label.lower()}" for kind, label in
               re.findall(r"\b(class|series)\s+([A-Z]|[IVX]{1,4}|\d{1,3})\b", text, re.I)}
    return next(iter(classes)) if len(classes) == 1 else None


def _ordinary_candidate(title: str) -> bool:
    """Preserve unfamiliar equity titles, exclude explicitly non-ordinary lines."""
    # A depositary's contractual "right to receive" the underlying shares is
    # not a separately listed subscription right.
    title = re.sub(r"\b(?:the\s+)?right\s+to\s+receive\b", "", title, flags=re.I)
    return not bool(re.search(
        r"\b(?:warrants?|rights?|units?|debentures?|notes?|bonds?|preferred|preference|regulatory|CPOs?|baskets?)\b|\btier\s*[12]\b|participation\s+certificates?",
        title, re.I,
    ))


def _effective_date(text: str) -> str | None:
    months = "January|February|March|April|May|June|July|August|September|October|November|December"
    # Avoid the announcement date: an explicit effective/event verb must be adjacent.
    pattern = rf"(?:effective\s+date\s+for\s+the\s+(?:ADS\s+)?ratio\s+change\s+is|effective(?:\s+(?:date|on|as\s+of|from|beginning|at|will\s+be|is|was|became)){{0,3}}|take\s+effect\s+on|with\s+effect\s+from|implemented\s+on|change\s+[^.;]{{0,60}}?ratio[^.;]{{0,220}}?\bon)\s+(?P<date>(?:{months})\s+\d{{1,2}}(?:st|nd|rd|th)?,?\s+\d{{4}}|\d{{1,2}}\s+(?:{months})\s+\d{{4}}|\d{{4}}-\d{{2}}-\d{{2}})"
    matches = list(re.finditer(pattern, text, re.I))
    dates = set()
    for match in matches:
        value = re.sub(r"(?<=\d)(?:st|nd|rd|th)\b", "", match.group("date"), flags=re.I)
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                parsed = date.fromisoformat(value)
            else:
                from datetime import datetime
                parsed = datetime.strptime(value.replace(",", ""), "%d %B %Y" if value[0].isdigit() else "%B %d %Y").date()
            dates.add(parsed.isoformat())
        except ValueError:
            continue
    return next(iter(dates)) if len(dates) == 1 else None


def parse_filing(
    content: str,
    *,
    cik: str | int,
    form_type: str,
    accession_number: str,
    filing_date: date | str,
    source_url: str,
    symbols: Iterable[str] = (),
    document_role: str = "primary",
) -> list[dict]:
    """Return independent, source-grounded observations, ready for reconciliation.

    Ratio-only documents without a literal trading symbol have ``symbol=None``.
    Callers must establish a unique security link before persisting such facts.
    The supplied CIK must be the issuer CIK; F-6 depositary registrant metadata
    is not itself proof of issuer identity. ``symbols`` narrows literal matching
    and never supplies a missing symbol by itself.

    ``document_role='securities_description'`` is for a verified securities
    description attached to the identified annual report. It emits independent
    ratio corroboration only, never a new cover listing-type observation.
    """
    if document_role not in ("primary", "securities_description"):
        raise ValueError("Unsupported filing document role")
    filed = date.fromisoformat(str(filing_date)[:10])
    public = (filed + timedelta(days=1)).isoformat()
    form = form_type.upper().strip()
    base_form = form.removesuffix("/A")
    if document_role == "securities_description" and base_form not in ("20-F", "40-F", "20FR12B", "40FR12B"):
        raise ValueError("A securities description must be attached to an annual report")
    document = _Document(content)
    text = document.text
    candidates = tuple(dict.fromkeys(value for s in symbols if s for value in [_canonical_symbol(str(s))] if value))
    source_hash = sha256(content.encode("utf-8")).hexdigest()
    rows: list[dict] = []

    def emit(kind: str, source_kind: str, symbol: str | None, evidence: str, location: str,
             listed_type: str | None = None, ratio: Fraction | None = None,
             effective: str | None = None, class_text: str = "") -> None:
        if document_role == "securities_description":
            if kind == "listed_type":
                return
            source_kind = "securities_description"
            location = "exhibit/securities-description;" + location
        rows.append({
            "cik": int(cik), "symbol": _canonical_symbol(symbol), "adsh": accession_number,
            "form": form, "filed": filed.isoformat(), "source_url": source_url,
            "source_sha256": source_hash,
            "source_kind": source_kind, "evidence_kind": kind,
            "listed_type": listed_type,
            "underlying_class": _underlying_class(class_text),
            "ordinary_candidate": _ordinary_candidate(class_text),
            "ratio_numerator": ratio.numerator if ratio else None,
            "ratio_denominator": ratio.denominator if ratio else None,
            "effective_from": effective or public, "effective_to": None,
            "available_on": public, "evidence_text": _clean(evidence),
            "evidence_location": location, "parser_version": PARSER_VERSION,
        })

    if base_form in ("20-F", "40-F", "20FR12B", "40FR12B"):
        # The first 12(b) table plus nearby footnotes is the cover, not later
        # financial tables or an Item 12 discussion of a different share class.
        cover_end = min(len(text), 45000)
        section = re.search(r"Securities\s+registered\s+(?:or\s+to\s+be\s+registered\s+)?pursuant\s+to\s+(?:Section\s+)?12\s*\(b\)", text[:cover_end], re.I)
        cover_start = section.start() if section else 0
        terminator = re.search(r"TABLE\s+OF\s+CONTENTS|CAUTIONARY\s+STATEMENT|PART\s+I\b", text[cover_start + 100:cover_end], re.I)
        if terminator:
            cover_end = cover_start + 100 + terminator.start()
        cover = text[cover_start:cover_end]
        table_end_match = re.search(r"Securities\s+(?:registered|for\s+which).{0,120}?(?:12\s*\(g\)|15\s*\(d\))", cover, re.I)
        table_end = cover_start + table_end_match.start() if table_end_match else cover_end
        table_text = text[cover_start:table_end]
        footnote_markers = set()
        for note in _DEPOSITARY_NOTE.finditer(cover):
            markers = list(re.finditer(r"\*+|[†‡]|\(\d+\)", cover[max(0, note.start() - 240):note.start()]))
            if markers:
                footnote_markers.add(markers[-1].group())
        listed_symbols: set[str] = set()
        ads_symbols: set[str] = set()
        for table_index, table in enumerate(document.tables):
            if table.offset > table_end:
                continue
            header = " ".join(" ".join(row) for row in table.rows[:4])
            if table.offset < cover_start and not re.search(r"Securities\s+registered.{0,100}?12\s*\(b\)", header, re.I):
                continue
            if not (re.search(r"title\s+of\s+(?:each\s+)?class", header, re.I) and re.search(r"exchange", header, re.I)):
                continue
            has_symbol_header = bool(re.search(r"(?:trading\s+)?symbol", header, re.I))
            symbol_columns = {i for row in table.rows[:4] for i, cell in enumerate(row)
                              if re.fullmatch(r"(?:Trading\s+)?Symbol\s*(?:s|\(s\))?\s*:?", cell, re.I)}
            table_symbols = {symbol for row in table.rows for i in symbol_columns if i < len(row)
                             for symbol in _cell_symbols(row[i])}
            equity_rows = [row for row in table.rows
                           if _EXCHANGE.search(" ".join(row))
                           and re.search(rf"ordinary\s+shares?|common\s+(?:shares?|stock)|{_ADS}", " ".join(row), re.I)]
            if len(equity_rows) > 1 and any(re.search(_ADS, " ".join(row), re.I) for row in equity_rows):
                equity_rows = [row for row in equity_rows if re.search(_ADS, " ".join(row), re.I)
                               or not (set(re.findall(r"\*+|[†‡]|\(\d+\)", " ".join(row))) & footnote_markers)]
            for row_index, cells in enumerate(table.rows):
                row_text = " | ".join(cells)
                if not _EXCHANGE.search(row_text):
                    continue
                row_symbols = [symbol for i in symbol_columns if i < len(cells) for symbol in _cell_symbols(cells[i])]
                if not row_symbols:
                    # Old covers had two columns and no trading symbol. Keep
                    # issuer-scoped evidence only for a sole listed equity;
                    # the loader must obtain its symbol from dated W1 evidence.
                    if len(equity_rows) == 1 and cells == equity_rows[0] and not has_symbol_header:
                        row_symbols = [None]
                    else:
                        continue
                title = next((c for c in cells if re.search(r"shares?|stock|units?|debentures?|notes?|securities", c, re.I) and not _EXCHANGE.fullmatch(c)), "")
                if re.search(r"represent(?:s|ing)\s*$", title, re.I) and row_index + 1 < len(table.rows):
                    continuation = " ".join(table.rows[row_index + 1]).strip()
                    if continuation and not _EXCHANGE.search(continuation):
                        title += " " + continuation
                        row_text += " " + continuation
                non_equity = not _ordinary_candidate(title)
                footnote = ""
                if _DEPOSITARY_NOTE.search(cover) and re.search(_ADS, cover, re.I):
                    # Footnote association requires a marker on this title, or
                    # a unique symbol across the 12(b) table.
                    title_markers = set(re.findall(r"\*+|[†‡]|\(\d+\)", row_text))
                    if title_markers & footnote_markers or (not title_markers and not footnote_markers and len(table_symbols) == 1):
                        footnote = cover
                if footnote:
                    kind = "ads"
                elif re.search(_ADS, title, re.I):
                    kind = "ads"
                elif non_equity:
                    kind = "unknown"
                elif re.search(r"ordinary\s+shares?|common\s+(?:shares?|stock)|subordinate\s+voting\s+shares?|variable\s+voting\s+shares?", title, re.I) and not _NOT_TRADING.search(row_text):
                    kind = "ordinary_direct"
                else:
                    kind = "unknown"
                for symbol in row_symbols:
                    if symbol:
                        listed_symbols.add(symbol)
                        if kind == "ads":
                            ads_symbols.add(symbol)
                    emit("listed_type", "cover_footnote" if footnote else "cover_12b", symbol,
                         row_text + (" " + footnote if footnote else ""),
                         f"cover/section-12b/table[{table_index + 1}]/row[{row_index + 1}];html-line={table.line}", kind,
                         class_text=title)
        # Some older covers render the two-column table with positioned text,
        # not HTML <table> tags. A sole explicit ADS title and exchange remains
        # issuer-scoped until the loader supplies a contemporaneous W1 link.
        if section and not any(row["evidence_kind"] == "listed_type" for row in rows):
            ads_titles = list(re.finditer(_ADS, table_text, re.I))
            if len(ads_titles) == 1 and _EXCHANGE.search(table_text) and not re.search(r"trading\s+symbols?", table_text, re.I):
                ads_title = table_text[ads_titles[0].start():]
                following_exchange = _EXCHANGE.search(ads_title)
                if following_exchange:
                    ads_title = ads_title[:following_exchange.start()]
                emit("listed_type", "cover_12b", None, cover,
                     f"cover/section-12b/positioned-text;text-offset={cover_start}", "ads", class_text=ads_title)
            elif not ads_titles and not re.search(r"trading\s+symbols?", table_text, re.I):
                class_titles = list(re.finditer(r"\b(?:Class|Series)\s+(?:[A-Z]|\d{1,3})\b", table_text, re.I))
                if len(class_titles) == 1:
                    class_title = table_text[class_titles[0].start():]
                    following_exchange = _EXCHANGE.search(class_title)
                    if following_exchange:
                        class_title = class_title[:following_exchange.start()].strip()
                        emit("listed_type", "cover_12b", None, table_text,
                             f"cover/section-12b/positioned-text;text-offset={cover_start}", "unknown", class_text=class_title)
        # Legacy covers can put the symbol outside the table. Bind only an
        # explicit symbol declaration on that same cover to the listed title.
        for symbol in _explicit_symbols(cover):
            symbol = symbol.upper()
            if symbol in listed_symbols or (candidates and _canonical_symbol(symbol) not in candidates):
                continue
            kind = "ads" if re.search(_ADS, cover, re.I) else "unknown"
            if kind == "ads" and _EXCHANGE.search(cover):
                emit("listed_type", "cover_footnote", symbol, cover, f"cover/section-12b;text-offset={cover_start}", kind)
                listed_symbols.add(symbol)
                ads_symbols.add(symbol)
        ratio_text = text if document_role == "securities_description" else cover
        for match, ratio in _ratios(ratio_text):
            if _superseded_ratio(ratio_text, match.start(), match.end()):
                continue
            local_symbols = sorted(ads_symbols)
            ratio_context = ratio_text[max(0, match.start() - 300):match.end() + 300]
            for symbol in local_symbols if len(local_symbols) == 1 else [None]:
                evidence = ratio_context if document_role == "securities_description" else cover
                offset = match.start() if document_role == "securities_description" else cover_start + match.start()
                location = f"text-offset={offset}" if document_role == "securities_description" else f"cover/section-12b;text-offset={offset}"
                emit("ads_ratio", "cover_footnote", symbol, evidence, location, ratio=ratio, class_text=match.group(), effective=_effective_date(ratio_context))
        # Item 12.D corroboration is separate from the primary F-6 stream.
        item_matches = [] if document_role == "securities_description" else list(re.finditer(r"(?:Item\s+12\.?\s*D\.?|D\.\s*American\s+Depositary\s+Shares|Item\s+12\.?\s+Description\s+of\s+Securities)", text, re.I))
        for item in item_matches:
            end = re.search(r"\bItem\s+13\b", text[item.end():item.end() + 70000], re.I)
            item_text = text[item.start():item.end() + (end.start() if end else 40000)]
            for match, ratio in _ratios(item_text):
                if _superseded_ratio(item_text, match.start(), match.end()):
                    continue
                context = item_text[max(0, match.start() - 300):match.end() + 300]
                local_symbols = [s.upper() for s in _explicit_symbols(context)] or sorted(ads_symbols)
                for symbol in local_symbols if len(local_symbols) == 1 else [None]:
                    emit("ads_ratio", "item_12d", symbol, context, f"item-12d;text-offset={item.start() + match.start()}", ratio=ratio, class_text=match.group(), effective=_effective_date(context))
    elif base_form in ("F-6", "F-6EF", "F-6 POS"):
        explicit = [s.upper() for s in _explicit_symbols(text)]
        for match, ratio in _ratios(text):
            context = text[max(0, match.start() - 700):match.end() + 700]
            if _superseded_ratio(text, match.start(), match.end()):
                continue
            local_symbols = [s.upper() for s in _explicit_symbols(context)] or explicit
            for symbol in local_symbols if len(set(local_symbols)) == 1 else [None]:
                emit("ads_ratio", "f6", symbol, context, f"f6/depositary-description;text-offset={match.start()}", ratio=ratio,
                     effective=_effective_date(context), class_text=match.group())
    elif base_form == "6-K":
        for match, ratio in _ratios(text):
            context = text[max(0, match.start() - 500):match.end() + 700]
            if not re.search(r"(?:ratio\s+change|change.{0,100}ratio|ratio.{0,100}chang|new\s+(?:ADS\s+)?ratio|former\s+ratio)", context, re.I):
                continue
            # In 'from one ADS ... to one ADS ...', retain only the new side.
            if _prior_ratio(text, match.start()):
                continue
            effective = _effective_date(context)
            if not effective:
                continue
            local_symbols = [s.upper() for s in _explicit_symbols(context)] or [s.upper() for s in _explicit_symbols(text)]
            for symbol in local_symbols if len(set(local_symbols)) == 1 else [None]:
                emit("ads_ratio", "ratio_change_6k", symbol, context, f"6k/ratio-change;text-offset={match.start()}", ratio=ratio, effective=effective, class_text=match.group())
    unique = {}
    for row in rows:
        key = tuple((key, str(value)) for key, value in row.items())
        unique[key] = row
    return list(unique.values())
