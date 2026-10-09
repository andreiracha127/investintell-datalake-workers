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

PARSER_VERSION = "foreign-listing-v5"
_SPACE = re.compile(r"\s+")
_ADS = r"(?:(?:(?:American?|Global)\s+)?deposit[ao]ry\s+(?:shares?|receipts?)|American\s+shares?\s*\(evidenced\s+by\s+deposit[ao]ry\s+receipts\)|[AG]D[SR]s?)"
_SHARES = r"(?:(?:(?:class|series)\s+[A-Z0-9]+\s+)?(?:ordinary|common)\s+shares?|shares?\s+of\s+common\s+stock|(?:class|series)\s+[A-Z0-9]+\s+shares?|shares?)"
_WORDS = "one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|half|third|quarter"
_QUANTITY = rf"(?:\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?|(?:{_WORDS})(?:[ -]+(?:{_WORDS}|and)){{0,5}})(?:\s*\(\s*(?:\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?|(?:{_WORDS})(?:[ -]+(?:{_WORDS}|and)){{0,5}})\s*\))?"
_OF_UNIT = r"(?:\s+of(?:\s+(?:one|an?|the)(?:\s*\(\s*\d+\s*/\s*\d+\s*\))?)?)?"
_RATIO = re.compile(
    rf"(?P<adsn>each|{_QUANTITY})\s+[\"'\u201c\u201d\u2018\u2019\ufffd]*\s*{_ADS}(?!\w)(?:[^.;]|\.(?=\d)){{0,100}}?\b"
    rf"(?:represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?|to|per|for)"
    rf"(?:,\s*and\s+to\s+exercise\s+the\s+beneficial\s+ownership\s+interests\s+in,?)?\s+"
    rf"(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+(?:(?:of\s+)?(?:our|the|its|company.s)\s+)?{_SHARES}",
    re.I,
)
_RATIO_TITLE = re.compile(
    rf"{_ADS}(?!\w)[^.;]{{0,100}}?\b(?:each\s+(?:of\s+which\s+)?|which\s+)represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?\s+(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+{_SHARES}",
    re.I,
)
_RATIO_RECIPROCAL = re.compile(
    rf"(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+{_SHARES}\s+(?:to|per|for)\s+(?P<adsn>each|{_QUANTITY})\s+{_ADS}(?!\w)",
    re.I,
)
_RATIO_COMPACT_TRANSITION = re.compile(
    rf"\bfrom\s+(?:a\s+)?{_QUANTITY}\s*-\s*to\s*-\s*{_QUANTITY}\s+"
    rf"(?P<unit>CUFS|(?:ordinary|common)\s+shares?)-to-{_ADS}\s+ratio\s+to\s+(?:a\s+)?"
    rf"(?P<ordinary>{_QUANTITY})\s*-\s*to\s*-\s*(?P<adsn>{_QUANTITY})\s+ratio\b", re.I,
)
_RATIO_CHANGE = re.compile(
    r"\b(?:ratio|exchange\s+(?:ratio|rate))\b[^.;]{0,180}\b(?:chang|amend)\w*\b"
    r"|\b(?:chang|amend)\w*\b[^.;]{0,180}\b(?:ratio|exchange\s+(?:ratio|rate))\b"
    r"|\b(?:new|former)\s+(?:(?:ADS|ADR|GDS|GDR)s?\s+)?ratio\b",
    re.I,
)
_RATIO_FROM = re.compile(
    rf"\bfrom\s+(?:(?:a|the)\s+)?(?:ratio\s+(?:of\s+)?)?"
    rf"(?:each|{_QUANTITY})\s+(?:{_ADS}|{_SHARES})(?!\w)", re.I,
)
_NOT_TRADING = re.compile(r"not\s+(?:for\s+trading|listed\s+for\s+trading)|non[- ]traded|without\s+trading\s+privileges", re.I)
_DEPOSITARY_NOTE = re.compile(_NOT_TRADING.pattern + r"|traded\s+in\s+the\s+form\s+of", re.I)
_EXCHANGE = re.compile(r"New York Stock Exchange|American Stock Exchange|NYSE|Nasdaq|NASDAQ|NYSE American|NYSE MKT|NYSE Arca|NYSEAMERICAN", re.I)
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
    parenthetical = re.fullmatch(r"(.+?)\s*\(([^()]*)\)", value)
    if parenthetical:
        stated, repeated = _number(parenthetical.group(1)), _number(parenthetical.group(2))
        if stated != repeated:
            raise ValueError("Contradictory repeated ratio quantity")
        return stated
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


def _cufs_ordinary_ratio_proofs(text: str, match: re.Match, ratio: Fraction) -> list[re.Match]:
    """A CUFS count requires an explicit ordinary-share entitlement proof."""
    pattern = re.compile(
        rf"(?P<adsn>each|{_QUANTITY})\s+{_ADS}(?!\w)\s+(?:is\s+)?equivalent\s+to\s+"
        rf"(?P<ordinary>{_QUANTITY})\s+(?:ordinary|common)\s+shares?\s*/\s*CUFS\b", re.I,
    )
    proofs = list(pattern.finditer(text, max(0, match.start() - 300), min(len(text), match.end() + 700)))
    ratios = set()
    for proof in proofs:
        try:
            ratios.add(_number(proof.group("ordinary")) / _number(proof.group("adsn")))
        except (ValueError, ZeroDivisionError):
            return []
    return proofs if ratios == {ratio} else []


def _reciprocal_matches(text: str) -> list[re.Match]:
    return [match for match in _RATIO_RECIPROCAL.finditer(text)
            if not re.match(r"\s*[,;:]?\s*(?:each(?:\s+of\s+which)?\s+)?represent(?:s|ing)?\b",
                            text[match.end():match.end() + 60], re.I)
            and not re.match(rf"\s+(?:per|to|for)\s+{_QUANTITY}{_OF_UNIT}\s+{_SHARES}",
                             text[match.end():match.end() + 100], re.I)]


def _old_new_ratio_spans(text: str) -> set[tuple[int, int]]:
    """Identify former values only in explicit ordered OLD/NEW ratio tables."""
    former = set()
    for header in re.finditer(r"\b(?P<left>OLD|FORMER|PREVIOUS|NEW|CURRENT)\s+(?:RATIO\s+)?"
                              r"(?P<right>OLD|FORMER|PREVIOUS|NEW|CURRENT)\b", text, re.I):
        labels = (header.group("left").upper(), header.group("right").upper())
        if sum(label in {"OLD", "FORMER", "PREVIOUS"} for label in labels) != 1:
            continue
        field = re.search(r"\bRatio\s*:?", text[header.end():header.end() + 400], re.I)
        if not field:
            continue
        start = header.end() + field.end()
        block = text[start:start + 500]
        matches = {}
        for pattern in (_RATIO, _RATIO_TITLE):
            for match in pattern.finditer(block):
                if match.end() not in matches or match.start() < matches[match.end()].start():
                    matches[match.end()] = match
        ordered = sorted(matches.values(), key=lambda match: match.start())
        if len(ordered) >= 2:
            old = ordered[0 if labels[0] in {"OLD", "FORMER", "PREVIOUS"} else 1]
            former.add((start + old.start(), start + old.end()))
    return former


def _transition_former_ratio_spans(text: str, reciprocal: list[re.Match]) -> set[tuple[int, int]]:
    """Assign explicit from/to and changed-to roles before extracting facts."""
    by_end = {match.end(): match for match in reciprocal}
    for pattern in (_RATIO, _RATIO_TITLE):
        for match in pattern.finditer(text):
            if any(match.start() < inverse.end() and inverse.start() < match.end() for inverse in reciprocal):
                continue
            if match.end() not in by_end or match.start() < by_end[match.end()].start():
                by_end[match.end()] = match
    matches = sorted(by_end.values(), key=lambda match: match.start())
    allowed = {"a", "an", "the", "its", "our", "their", "then", "current", "existing", "original", "old", "former", "previous", "new",
               "ads", "adss", "adr", "adrs", "gds", "gdss", "gdr", "gdrs", "ratio", "of", "is", "was",
               "american", "global", "depositary", "depository", "share", "shares", "receipt", "receipts",
               "ordinary", "common", "to"}

    def wrapper(value: str) -> bool:
        value = re.sub(r"\b(ADS|ADR|GDS|GDR)s?Ratio\b", r"\1 ratio", value, flags=re.I)
        tokens = re.findall(r"[A-Za-z]+|\d+", value.lower())
        return all(token in allowed for token in tokens)

    former = set()
    for old, new in zip(matches, matches[1:]):
        if new.start() - old.end() > 450:
            continue
        gap = text[old.end():new.start()]
        if re.search(r"[.!?](?!\d)(?=\s|[\"'\u201d\u2019\ufffd])", gap):
            continue
        to_words = list(re.finditer(r"\bto\b", gap, re.I))
        if not to_words or not wrapper(gap[to_words[-1].end():]):
            continue
        lead = text[max(0, old.start() - 280):old.start()]
        from_words = list(re.finditer(r"\bfrom\b", lead, re.I))
        from_role = bool(from_words and wrapper(lead[from_words[-1].end():]))
        changed_to = bool(re.search(r"\b(?:chang(?:e|ed)|amended|adjusted|replaced)\b[^.;]{0,100}$",
                                    gap[:to_words[-1].start()], re.I))
        if from_role or changed_to:
            former.add((old.start(), old.end()))
    return former


def _defined_shares_are_ordinary(text: str) -> bool:
    definitions = list(re.finditer(
        r"\bShares\s*[\"'\u201c\u201d\u2018\u2019\ufffd]*\s+(?:mean|means|shall\s+mean)\s+(?P<title>[^.;]{1,220})",
        text, re.I,
    ))
    return bool(definitions) and all(
        re.match(r"(?:the\s+)?(?:(?:Class|Series)\s+[A-Z0-9]+\s+)?(?:ordinary|common)\s+shares?\b",
                 definition.group("title"), re.I)
        and not re.search(r"\b(?:preferred|preference|units?|baskets?|CPOs?)\b", definition.group("title"), re.I)
        for definition in definitions
    )


def _primary_share_unit(title: str) -> str | None:
    """Classify the deposited unit, before any nested component entitlement."""
    title = re.split(r";|\.(?!\d)|\b(?:including|provided|shall\s+include)\b", title, maxsplit=1, flags=re.I)[0]
    first_share = re.search(r"\bshares?\b", title, re.I)
    prefix = title[:first_share.end()] if first_share else title
    nonordinary = r"\b(?:prefer(?:red|ed|ence)|CPOs?|units?|baskets?)\b|participation\s+certificates?|certificados\s+de\s+participaci\S*\s+ordinarios"
    if re.search(nonordinary, prefix, re.I):
        return "nonordinary"
    # Registration titles can say 'one share of Series III Convertible
    # Preferred Stock'; the qualifying noun belongs to that first share.
    if first_share and re.match(r"\s+of\s+(?:(?:Class|Series)\s+[\w-]+\s+)?(?:convertible\s+)?(?:preferred|preference)\b",
                                title[first_share.end():], re.I):
        return "nonordinary"
    if re.search(r"\b(?:ordinary|common)\b", prefix, re.I) and first_share:
        return "ordinary"
    return None


def _compound_receipt_entitlement(text: str, end: int) -> bool:
    """A common-share component of a mixed receipt is not its scalar ratio."""
    suffix = re.split(r"\.(?!\d)|;", text[end:end + 220], maxsplit=1)[0]
    return bool(re.match(
        rf"\s*(?:,\s*[^.;]{{0,70}}?\s*)?\band\s+{_QUANTITY}\s+"
        r"(?:(?:Class|Series)\s+[\w-]+\s+)?(?:preferred|preference)\s+shares?\b",
        suffix, re.I,
    ))


def _ratio_primary_share_unit(text: str, match: re.Match) -> str | None:
    receipt = re.search(_ADS, match.group(), re.I)
    if receipt is None:
        return None
    if receipt.end() == len(match.group()):
        return _primary_share_unit(match.group()[:receipt.start()])  # shares-to-ADS clause
    tail = text[match.start() + receipt.end():match.end() + 100]
    verb = re.search(r"\b(?:represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?|to|per|for)\b", tail, re.I)
    return _primary_share_unit(tail[verb.end():]) if verb else None


def _named_share_class_unit(text: str, class_text: str) -> tuple[str, str] | None:
    """Resolve a bare named share class only from this source's exact class."""
    named_class = _underlying_class(class_text)
    if named_class is None:
        return None
    kind, name = named_class.split("_", 1)
    label = rf"\b{kind}\s+{re.escape(name)}\b"
    declarations = list(re.finditer(
        rf"{label}\s+(?:non[- ]voting\s+|voting\s+)?(?P<after>preferred|preference|ordinary|common)\s+shares?\b"
        rf"|\b(?P<before>preferred|preference|ordinary|common)\s+{label}\s+shares?\b",
        text, re.I,
    ))
    units = {"nonordinary" if (d.group("after") or d.group("before")).lower() in ("preferred", "preference")
             else "ordinary" for d in declarations}
    if len(units) != 1:
        return None
    return next(iter(units)), declarations[0].group()


def _f6_deposited_unit_declarations(text: str) -> list[tuple[int, int, str, str, str | None]]:
    """Collect only contractual Shares definitions and receipt unit titles.

    Preferred capital mentioned elsewhere does not define an ADS programme.
    Keeping the declaration offset and receipt family lets a later ordinary
    programme in the same source replace an earlier preferred declaration.
    """
    declarations = []
    for definition in re.finditer(
        r"\bShares\s*[\"'\u201c\u201d\u2018\u2019\ufffd]*\s+(?:mean|means|shall\s+mean)\s+(?P<title>[^.;]{1,260})",
        text, re.I,
    ):
        lead = text[max(0, definition.start() - 55):definition.start()]
        if re.search(r"\b(?:deposit[ao]ry|Class\s+[\w-]+|Series\s+[\w-]+|Preferred\s+Payment)\s+$", lead, re.I):
            continue  # Class A Shares and American Depositary Shares are different defined terms.
        unit = _primary_share_unit(definition.group("title"))
        if unit:
            declarations.append((definition.start(), definition.end(), unit, definition.group(), None))
    for declaration in re.finditer(
        rf"(?=(?P<full>(?P<receipt>{_ADS})(?!\w)(?:\s*\([^)]{{0,90}}\))?\s*,?\s+"
        r"(?:shall\s+)?represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?\s+(?P<title>[^.;]{1,230})))",
        text, re.I,
    ):
        unit = _primary_share_unit(declaration.group("title"))
        if unit:
            receipt = declaration.group("receipt")
            family = "global" if re.search(r"\bGlobal\b|\bGDS|\bGDR", receipt, re.I) else "american"
            declarations.append((declaration.start("full"), declaration.end("full"), unit, declaration.group("full"), family))
    return sorted(declarations)


def _f6_ratio_deposited_unit(text: str, match: re.Match,
                             declarations: list[tuple[int, int, str, str, str | None]]) -> tuple[str, tuple[int, int] | None] | None:
    """Bind an entitlement to its primary deposited security, not a component."""
    own_unit = _ratio_primary_share_unit(text, match)
    if own_unit == "nonordinary" or _compound_receipt_entitlement(text, match.end()):
        return None
    if own_unit == "ordinary":
        return match.group(), None
    receipt = re.search(_ADS, match.group(), re.I)
    family = "global" if receipt and re.search(r"\bGlobal\b|\bGDS|\bGDR", receipt.group(), re.I) else "american"
    relevant = [declaration for declaration in declarations if declaration[4] in (None, family)]
    primary = [declaration for declaration in relevant if declaration[4] == family]
    before = [declaration for declaration in relevant if declaration[0] <= match.start()]
    if before:
        governing = before[-1]
    elif primary and len({declaration[2] for declaration in primary}) == 1:
        governing = primary[0]
    else:
        definitions = [declaration for declaration in relevant if declaration[4] is None]
        earlier = [declaration for declaration in definitions if declaration[0] <= match.start()]
        governing = earlier[-1] if earlier else definitions[0] if definitions and len({d[2] for d in definitions}) == 1 else None
    if governing and governing[2] == "nonordinary":
        return None
    if governing and _underlying_class(governing[3]):
        primary_class = re.search(
            r"\b(?:(?:Class|Series)\s+[\w-]+\s+(?:ordinary|common)|(?:ordinary|common)\s+(?:Class|Series)\s+[\w-]+)\s+shares?\b",
            governing[3], re.I,
        )
        if primary_class:
            # Preserve ancillary rights/other-property clauses in the proof,
            # but classify only the literal primary deposited-share noun.
            return primary_class.group(), (governing[0], governing[1])
    return match.group(), None


def _ratios(text: str, *, unit_definition_text: str | None = None):
    seen: set[tuple[int, int, int]] = set()
    named_class_units: dict[str, tuple[str, str] | None] = {}
    reciprocal = _reciprocal_matches(text)
    former_table = _old_new_ratio_spans(text) | _transition_former_ratio_spans(text, reciprocal)
    for pattern in (_RATIO_RECIPROCAL, _RATIO_COMPACT_TRANSITION, _RATIO, _RATIO_TITLE):
        for match in reciprocal if pattern is _RATIO_RECIPROCAL else pattern.finditer(text):
            if any(start <= match.start() and match.end() <= end for start, end in former_table):
                continue
            if re.search(r"\b(?:previously|formerly|used\s+to)\b", match.group(), re.I):
                continue
            if (re.search(r"beneficial\s+ownership\s+interests", match.group(), re.I)
                    and not re.search(r"\b(?:ordinary|common)\s+shares?\b", match.group(), re.I)
                    and not _defined_shares_are_ordinary(text)):
                continue
            # In "from one share to two ADSs, to one share to one ADS", an
            # ADS-first match can cross the two regimes. Keep each complete
            # shares-to-ADS clause instead, so the former side remains former.
            if pattern is not _RATIO_RECIPROCAL and any(
                match.start() < inverse.end() and inverse.start() < match.end()
                for inverse in reciprocal
            ):
                continue
            if re.search(r"\b(?:preferred|preference|units?|CPOs?|baskets?)\b|participation\s+certificates?", match.group(), re.I) or re.match(
                r"\s*(?:of\s+(?:(?:our|the|its)\s+)?)?(?:preferred|preference|units?)\b", text[match.end():match.end() + 80], re.I,
            ):
                continue
            if _compound_receipt_entitlement(text, match.end()):
                continue
            if _ratio_primary_share_unit(text, match) == "nonordinary":
                continue
            if not re.search(r"\b(?:ordinary|common)\s+shares?\b", match.group(), re.I):
                named_class = _underlying_class(match.group())
                if named_class:
                    if named_class not in named_class_units:
                        named_class_units[named_class] = _named_share_class_unit(unit_definition_text or text, match.group())
                    unit = named_class_units[named_class]
                    if unit and unit[0] == "nonordinary":
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
            if (pattern is _RATIO_COMPACT_TRANSITION and match.group("unit").upper() == "CUFS"
                    and not _cufs_ordinary_ratio_proofs(text, match, ratio)):
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
        r"\b(?:from|previous(?:ly)?|current|existing|old|former(?:ly)?|then)\s+(?:(?:a|the)\s+)?(?:(?:(?:ADS|ADR)(?:[- ]to[- ]Share)?[- ]?)?ratio\s*:?[ ]*(?:(?:of|was|is)\s+)?)?[\"“'‘]*$",
        text[max(0, offset - 100):offset], re.I,
    ))


def _superseded_ratio(text: str, start: int, end: int) -> bool:
    context = text[max(0, start - 700):end + 700]
    return _prior_ratio(text, start) and bool(re.search(
        r"(?:new|former|previous)\s+(?:(?:ADS|ADR)(?:[- ]to[- ]Share)?[- ]?)?ratio|(?:chang\w*|amend\w*).{0,100}ratio|ratio.{0,100}(?:chang\w*|amend\w*)",
        context, re.I,
    ))


def _6k_ratio_context(text: str, start: int, end: int) -> tuple[str, str] | None:
    """Bind a 6-K ratio to its own transition and connected date statements."""
    left, right = max(0, start - 1200), min(len(text), end + 1400)
    boundaries = [left]
    for boundary in re.finditer(r"[.!?;](?!\d)[\"'\u201d\u2019\ufffd]*(?=\s|$)", text[left:right]):
        position = left + boundary.start()
        if text[position] == "." and re.search(
            r"(?:\b(?:Co|Ltd|Inc|Corp|PLC|Mr|Dr|No)|\bU\.S|\bN\.A)$",
            text[max(left, position - 12):position], re.I,
        ):
            continue
        boundaries.append(left + boundary.end())
    boundaries.append(right)
    index = next(i for i in range(len(boundaries) - 1)
                 if boundaries[i] <= start < boundaries[i + 1])
    own_start, own_end = boundaries[index], boundaries[index + 1]
    own = text[own_start:own_end]
    transitions = list(_RATIO_FROM.finditer(own))
    if len(transitions) > 1:
        # Multiple numeric transitions in one sentence need their own explicit
        # date links. Never attach the first event's date to the second regime.
        return None
    previous_start = boundaries[index - 1] if index else own_start
    previous = text[previous_start:own_start]
    date_lead = text[own_start:start]
    direct_effective_lead = bool(_effective_date(date_lead) and re.fullmatch(
        r"\s*(?:effective(?:\s+(?:on|as\s+of|from))?|with\s+effect\s+(?:on|from))\s*:?\s*"
        r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})\s*,?\s*",
        date_lead, re.I,
    ))
    linked_previous = bool(_RATIO_CHANGE.search(previous) and re.search(_ADS, previous, re.I)
                           and not any(pattern.search(previous) for pattern in
                                       (_RATIO, _RATIO_TITLE, _RATIO_RECIPROCAL, _RATIO_COMPACT_TRANSITION)))
    linked_previous = linked_previous or bool(direct_effective_lead and re.search(
        r"\bratio\s+change[.;]?\s*$", previous, re.I,
    ))
    dated_ratio_clause = bool(re.search(r"\bratio\b", own, re.I)
                              and re.search(_ADS, own, re.I) and _effective_date(own))
    if not _RATIO_CHANGE.search(own) and not linked_previous and not dated_ratio_clause:
        return None
    # "Concurrently" explicitly connects Honda's reciprocal ratio transition
    # to the preceding dated stock split. An unrelated tax/charter date does
    # not qualify merely because it lies within the old 1,200-character window.
    scope_start = previous_start if linked_previous or re.match(r"\s*Concurrently\b", own, re.I) else own_start
    scope_end = own_end
    anaphor_connected = True
    for following_index in range(index + 1, min(index + 5, len(boundaries) - 1)):
        following_end = boundaries[following_index + 1]
        if following_end - own_end > 900:
            break
        following = text[boundaries[following_index]:following_end]
        if any(_ratios(following)) or re.match(r"\s*(?:Also|Separately|In\s+addition|Another)\b", following, re.I):
            break
        unchanged_underlying = bool(re.fullmatch(
            r"\s*(?:There\s+(?:is|are|was|were|will\s+be)\s+)?no\s+changes?\s+to\s+"
            r"[^.;]{0,100}\bunderlying\s+(?:(?:ordinary|common)\s+)?shares?\s*[.;]?\s*", following, re.I,
        ))
        related_anaphor_date = bool(anaphor_connected and re.fullmatch(
            r"\s*This\s+(?:action|change)\s+(?:is|was|will\s+be)\s+effective(?:\s+(?:on|as\s+of|from))?\s+"
            r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})\s*[.;]?\s*",
            following, re.I,
        ))
        if _effective_date(following) and not (
            related_anaphor_date
            or
            re.search(r"\bratio\s+change\b|\bchange\b[^.;]{0,100}\bratio\b|\bratio\b[^.;]{0,100}\beffective\b", following, re.I)
            or (re.search(_ADS, following, re.I)
                and re.search(r"\b(?:holders?|exchange|surrender|cancell\w*)\b", following, re.I)
                and re.search(r"\b(?:exchange|surrender|cancell\w*)\b", following, re.I))
            or re.fullmatch(r"\s*(?:The\s+)?(?:(?:anticipated|expected)\s+)?effective\s+date\s+(?:is|will\s+be)\s+[^.;]+[.;]?\s*", following, re.I)
        ):
            break
        scope_end = following_end
        if not unchanged_underlying and not related_anaphor_date:
            anaphor_connected = False
    return text[scope_start:scope_end], own


def _6k_ratio_class(match: re.Match, context: str) -> str:
    class_text = match.group()
    if _underlying_class(class_text) is not None:
        return class_text
    # A declared "ADS to Class A common share ('Share') ratio" defines the
    # abbreviated Shares in this same transition. Do not inherit a class from
    # an unrelated capital table or a neighboring common/preferred program.
    prefix = context[:context.find(class_text)]
    declarations = list(re.finditer(
        rf"{_ADS}(?:\s*\([^)]{{0,40}}\))?[\s-]+to[\s-]+(?:(?:its|our|the)\s+)?"
        r"(?P<class>(?:Class|Series)\s+[A-Z0-9]+\s+(?:ordinary|common)\s+shares?)"
        r"(?:\s*\([^)]{0,40}\))?\s+ratio\b", prefix, re.I,
    ))
    return class_text + " " + declarations[-1].group("class") if declarations else class_text


def _6k_named_event_date(text: str, match: re.Match, own: str) -> tuple[str, re.Match] | None:
    """Resolve an explicitly linked Share Consolidation within the same source."""
    links = list(re.finditer(r"\b(?:conditional\s+)?upon\s+(?:the\s+)?Share\s+Consolidation\b", own, re.I))
    if not links or any(re.search(r"\b(?:not|unrelated|independent)\b", own[max(0, link.start() - 30):link.start()], re.I)
                        for link in links):
        return None
    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    clock = r"(?:at\s+\d{1,2}\s*:\s*\d{2}\s*[AP]\.?\s*M\.?\s*,?\s*)?"
    pattern = re.compile(
        rf"\b(?:(?:the|such)\s+)?Share\s+Consolidation\s+(?:will\s+be|shall\s+be|to\s+be|is|was)\s+effective\s+"
        rf"{clock}(?:on\s+)?(?P<date>{date_value})", re.I,
    )
    matches = list(pattern.finditer(text, max(0, match.start() - 2500), min(len(text), match.end() + 2500)))
    dated = [(proof, _effective_date("effective on " + proof.group("date"))) for proof in matches]
    dated = [(proof, value) for proof, value in dated if value]
    if len({value for _, value in dated}) != 1:
        return None
    proof, value = dated[0]
    return value, proof


def _6k_reverse_split_date(text: str, match: re.Match, own: str, context: str) -> tuple[str, re.Match] | None:
    if not re.search(r"\bConcurrently\s+with\s+(?:the\s+)?reverse\s+(?:share\s+|stock\s+)?split\b", own, re.I):
        return None
    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    own_spans = [span for span in re.compile(re.escape(own)).finditer(
        text, max(0, match.start() - 1200), min(len(text), match.end() + 1400))
        if span.start() <= match.start() and span.end() >= match.end()]
    if len(own_spans) != 1:
        return None
    own_start, own_end = own_spans[0].span()
    # A repeated label such as Reverse Split is a discourse reference. Resolve
    # the immediately adjacent dated declaration, not a later split nearby.
    prefix_start = max(0, own_start - 1200)
    prefix = text[prefix_start:own_start]
    boundaries = [0] + [boundary.end() for boundary in re.finditer(
        r"[.!?](?!\d)[\"'\u201d\u2019\ufffd]*(?=\s|$)", prefix)] + [len(prefix)]
    adjacent = []
    date_declaration = re.compile(
        rf"\breverse\s+(?:share\s+|stock\s+)?split\b[^.;]{{0,150}}?"
        rf"\b(?:went|became|was|is|will\s+be)\s+effective(?:\s+(?:on|as\s+of|from))?\s+(?P<date>{date_value})",
        re.I,
    )
    for start, end in reversed(list(zip(boundaries, boundaries[1:]))):
        if not prefix[start:end].strip():
            continue
        proofs = list(date_declaration.finditer(text, prefix_start + start, prefix_start + end))
        if not proofs:
            break
        adjacent.extend((value, proof) for proof in proofs
                        if (value := _effective_date("effective on " + proof.group("date"))))
    if adjacent:
        return adjacent[0] if len({value for value, _ in adjacent}) == 1 else None
    patterns = (
        rf"\bThe\s+effective\s+date\s+of\s+(?:this|the)\s+reverse\s+(?:share\s+|stock\s+)?split\s+(?:was|is|will\s+be)\s+(?P<date>{date_value})",
        rf"\bThe\s+first\s+date\b[^.;]{{0,220}}\bafter\s+implementation\s+of\s+the\s+reverse\s+(?:share\s+|stock\s+)?split\s+"
        rf"and\s+(?:the\s+)?concurrent\s+(?:ADS\s+)?ratio\s+change\s+will\s+be\s+(?P<date>{date_value})",
    )
    dated = []
    scope_start = text.find(context, max(0, match.start() - 1200), match.end() + 1400)
    if scope_start < 0:
        return None
    scope_end = scope_start + len(context)
    for pattern in patterns:
        for proof in re.compile(pattern, re.I).finditer(text, own_end, scope_end):
            gap = text[own_end:proof.start()]
            statements = [statement.strip() for statement in re.split(r"[.!?](?!\d)\s+", gap) if statement.strip()]
            if any(not re.search(rf"{_ADS}|\bratio\b|\breverse\s+(?:share\s+|stock\s+)?split\b", statement, re.I)
                   for statement in statements) or any(_ratios(gap)):
                continue
            value = _effective_date("effective on " + proof.group("date"))
            if value:
                dated.append((value, proof))
    return dated[0] if len({value for value, _ in dated}) == 1 else None


def _6k_completed_event_date(own: str) -> str | None:
    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    dates = set()
    for declaration in re.finditer(
        rf"\bOn\s+(?P<date>{date_value})\s*,?\s*(?P<subject>[^.;]{{0,100}}?)\b(?:effected|implemented|completed)\b"
        r"[^.;]{0,100}\b(?:change\b[^.;]{0,80}\bratio\b|ratio\s+change\b)", own, re.I,
    ):
        if re.search(r"\b(?:announced|reported|said|disclosed|expected|intended|proposed)\b", declaration.group("subject"), re.I):
            continue
        value = _effective_date("effective on " + declaration.group("date"))
        if value:
            dates.add(value)
    return next(iter(dates)) if len(dates) == 1 else None


def _f6_ratio_interval(text: str, ratio: Fraction, class_text: str,
                       candidates: list[tuple[re.Match, Fraction]]) -> tuple[str | None, str | None, str] | str | None:
    """Keep a literal prior/commencing entitlement's two operative intervals."""
    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    marker = re.compile(rf"\b(?P<role>prior\s+to|commencing(?:\s+on)?)\s+(?P<date>{date_value})", re.I)
    if not marker.search(text):
        return None
    current_class = _underlying_class(class_text)
    intervals = []
    for candidate, value in candidates:
        if value != ratio:
            continue
        candidate_class = _underlying_class(candidate.group())
        if current_class is not None and candidate_class is not None and current_class != candidate_class:
            continue
        prefix_start = max(0, candidate.start() - 220)
        for declaration in marker.finditer(text, prefix_start, candidate.start()):
            tail = text[declaration.end():candidate.start()]
            if not re.fullmatch(r"[\s,:()\[\]\"'\u201c\u201d\u2018\u2019\ufffd]*(?:each\s+)?", tail, re.I):
                continue
            day = _effective_date("effective on " + declaration.group("date"))
            if day:
                role = "prior" if declaration.group("role").lower().startswith("prior") else "commencing"
                intervals.append((role, day, text[declaration.start():candidate.end()]))
    regimes = {(role, day) for role, day, _ in intervals}
    if len(regimes) > 1:
        return "ambiguous"
    if not regimes:
        return None
    role, day, proof = intervals[0]
    return (None, day, proof) if role == "prior" else (day, None, proof)


def _6k_correction_metadata(text: str, context: str) -> dict:
    # An Old CUSIP is a literal program pin for this transition. A /A suffix,
    # newer date, or merely matching issuer never establishes replacement.
    cusips = set(re.findall(r"\bOld\s+CUSIP\s*[:#]?\s*([A-Z0-9]{9})(?!\w)", context, re.I))
    if len(cusips) != 1:
        return {}
    metadata = {"ratio_change_program_key": "old_cusip:" + next(iter(cusips)).upper()}
    heading = re.search(
        r"\bCORRECTING\s+and\s+REPLACING\b\s*[-\u2013\u2014:]?\s*.{0,220}?\b(?:ADR|ADS|GDR|GDS)\s+Ratio\s+Change\b",
        text[:4000], re.I,
    )
    if heading and not re.search(r"\b(?:not|previously|formerly|quoted)\b", text[max(0, heading.start() - 60):heading.start()], re.I):
        metadata.update({"ratio_change_correction_kind": "correcting_and_replacing",
                         "ratio_change_correction_text": _clean(heading.group())})
    return metadata


def _f6_pending_effectiveness(text: str, ratio: Fraction, class_text: str) -> dict:
    """Flag only a source's operative ratio condition awaiting depositary notice."""
    current_class = _underlying_class(class_text)

    def matches_ratio(fragment: str) -> bool:
        observations = [(value, _underlying_class(match.group())) for match, value in _ratios(fragment)]
        if not observations and ratio.denominator == 1:
            # XIN's operative sections replace the ADR's quoted "two Shares"
            # with "twenty Shares", rather than repeating the complete ADS
            # definition. Tie that replacement to the source's per-one-ADS
            # entitlement, never to an arbitrary multi-ADS group quantity.
            for change in re.finditer(
                rf"\breplacing\b[^.;]{{0,80}}?{_QUANTITY}\s+{_SHARES}[^.;]{{0,40}}?\bwith\s+"
                rf"[\"'\u201c\u201d\u2018\u2019\ufffd]*\s*(?P<new>(?P<quantity>{_QUANTITY})\s+{_SHARES})", fragment, re.I,
            ):
                observations.append((_number(change.group("quantity")), _underlying_class(change.group("new"))))
        return (bool(observations) and {value for value, _ in observations} == {ratio}
                and all(value is None or current_class is None or value == current_class for _, value in observations))

    for proof in re.finditer(
        r"\bEffective\s+on\s+the\s+date\s+announced\s+by\s+the\s+Depositary\b[^.;]{0,400}", text, re.I,
    ):
        if matches_ratio(proof.group()):
            return {"ratio_effectiveness_pending": True, "ratio_effectiveness_pending_text": _clean(proof.group())}
    for proof in re.finditer(
        r"\b(?:ratio\s+change\s+amendments?|the\s+amendment\s+reflected\s+in\s+Section\s+\d+\.\d+)"
        r"(?:[^.;]|\.(?=\d)){0,200}?\bshall\s+not\s+become\s+effective\s+until\b[^.;]{0,200}"
        r"\bannounced\s+by\s+the\s+Depositary\b", text, re.I,
    ):
        reference = re.search(r"\bSections?\s+(\d+\.\d+(?:\s+and\s+\d+\.\d+)?)", proof.group(), re.I)
        if not reference:
            continue
        fragments = []
        for section in re.findall(r"\d+\.\d+", reference.group(1)):
            heading = re.search(r"\bSECTION\s+" + re.escape(section) + r"\s*\.", text, re.I)
            if not heading:
                continue
            tail = text[heading.end():heading.end() + 4000]
            next_heading = re.search(r"\b(?:SECTION\s+\d+\.\d+\s*\.|ARTICLE\s+[IVXLC]+)\s", tail, re.I)
            fragments.append(tail[:next_heading.start()] if next_heading else tail)
        if matches_ratio(" ".join(fragments)):
            return {"ratio_effectiveness_pending": True, "ratio_effectiveness_pending_text": _clean(proof.group())}
    return {}


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
    boundary = r"(?:the\s+)?(?:(?:open(?:ing)?|clos(?:e|ing)|beginning|commencement|start)\s+of\s+(?:trading|business)|market\s+(?:open|close))"
    instrument = r"(?:\s+of\s+(?:the\s+)?(?:ADSs?|ADRs?|GDSs?|GDRs?|American\s+deposit[ao]ry\s+shares?))?"
    venue = r"(?:\s+(?:on|at)\s+(?:the\s+)?(?:Nasdaq(?:\s+(?:Global\s+Select|Global|Capital))?(?:\s+(?:Stock\s+)?Market)?|New\s+York\s+Stock\s+Exchange|NYSE(?:\s+(?:American|Arca))?))?"
    zone = r"(?:New\s+York(?:\s+City)?|Eastern|Pacific|EST|EDT|PST|PDT|local)(?:\s+(?:Standard|Daylight))?(?:\s+time)?"
    clock = rf"(?:\s*,?\s*(?:\(\s*)?(?:at\s+)?\d{{1,2}}(?::\d{{2}})?\s*(?:a\.?m\.?|p\.?m\.?)(?:\s+{zone})?\s*\)?,?)?"
    zone_only = rf"(?:\s*\(\s*{zone}\s*\))?"
    market_effect = rf"(?:effective|with\s+effect|take\s+effect)(?:\s+(?:as\s+of|at|from|on))?\s+{boundary}{instrument}{venue}{clock}{zone_only}\s*,?\s+(?:on|as\s+of)"
    declared_date = r'''["“]?effective\s+date["”]?\s*(?:shall\s+mean|means|is|will\s+be|:)'''
    dated_transition = r"(?:will|shall|desires?\s+to|intends?\s+to|agrees?\s+to|agreed\s+to)\s+change\s+[^.;]{0,60}?ratio[^.;]{0,220}?\bon"
    ordinary_effect = rf"effective\s+date\s+for\s+the\s+(?:ADS\s+)?ratio\s+change\s+is|effective(?:\s+(?:date|on|as\s+of|from|beginning|at|will\s+be|is|was|became)){{0,3}}|take\s+effect\s+on|with\s+effect\s+(?:on|from)|implemented\s+on|{dated_transition}"
    pattern = rf"(?:{market_effect}|{declared_date}|{ordinary_effect})\s*:?\s*(?P<date>(?:{months})\s+\d{{1,2}}(?:st|nd|rd|th)?,?\s+\d{{4}}|\d{{1,2}}\s+(?:{months})\s+\d{{4}}|\d{{4}}-\d{{2}}-\d{{2}})"
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


def _f6_amendment_date(text: str) -> str | None:
    """Date the operative amendment, not the annexed agreement's old terms.

    Registration exhibits often reproduce a revised ADR and a holder notice
    many pages after the amendment itself. Its explicit ratio-change date
    governs those replacement clauses even when it is outside a local excerpt.
    Restrict the date search to an identified amendment's operative articles;
    dates in signatures, specimen ADRs and legacy agreement annexes do not set it.
    """
    heading = re.search(r"\bamendment\s+no\.?\s*\d+(?:\s*,\s*dated\s+as\s+of[^;]{0,180}?)?\s*,?\s*to\s+(?:the\s+)?(?:(?:amended|and|restated)\s+)*deposit\s+agreement\b", text[:8000], re.I)
    if not heading:
        return None
    body = text[heading.start():]
    end = re.search(r"\bIN\s+WITNESS\s+WHEREOF\b|\bEXHIBIT\s+[A-Z]\s*\[?\s*FORM\b", body, re.I)
    if end:
        body = body[:end.start()]
    if not re.search(rf"(?:chang\w*|amend\w*).{{0,100}}ratio|ratio.{{0,100}}(?:chang\w*|amend\w*)|each\s+{_ADS}\s+shall\s+represent", body, re.I):
        return None
    return _effective_date(body)


def _inline_item_reference(text: str, offset: int) -> bool:
    before = text[max(0, offset - 100):offset].rstrip()
    return bool(before and before[-1] in {'"', "'", '\u201c', '\u2018', '\u2014', '\u2013', '-'}) or bool(re.search(
        r"\b(?:see(?:\s+also)?|under|in|to|of|and|this|that|refer\s+to|referred\s+to|described\s+in|set\s+forth\s+in|captioned|entitled)\s*$",
        before, re.I,
    ))


def _item_12d_sections(text: str):
    """Yield actual Item 12.D spans, excluding quoted cross-references and TOCs.

    A bare 'D. American Depositary Shares' is only meaningful inside an Item
    12 section. Every span ends at the next item heading; there is no character
    window that can spill from an inline Item 12 reference into Item 9 Markets.
    """
    headings = list(re.finditer(
        r"\bItem\s+(?P<number>\d{1,2})(?P<suffix>[A-Z])?\s*\.?", text, re.I,
    ))
    headings = [heading for heading in headings if not _inline_item_reference(text, heading.start())]
    for index, heading in enumerate(headings):
        if heading.group("number") != "12":
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        section = text[heading.end():end]
        if not re.match(r"\s*(?:D\.?\s*)?(?:Description\s+of\s+Securities|American\s+Deposit[ao]ry\s+Shares)", section, re.I):
            continue
        if (heading.group("suffix") or "").upper() == "D" or re.match(r"\s*D\.?\s*Description", section, re.I):
            start = heading.start()
        else:
            subsection = next((match for match in re.finditer(r"\bD\s*\.\s*(?:American|Global)\s+Deposit[ao]ry\s+Shares\b", section, re.I)
                               if not _inline_item_reference(section, match.start())), None)
            if not subsection:
                continue
            start = heading.end() + subsection.start()
        yield start, text[start:end]


def _exchange_key(text: str) -> str | None:
    if re.search(r"nasdaq", text, re.I):
        return "nasdaq"
    if re.search(r"NYSE\s+(?:American|MKT)|American\s+Stock\s+Exchange", text, re.I):
        return "nyse_american"
    if re.search(r"NYSE\s+Arca", text, re.I):
        return "nyse_arca"
    if re.search(r"New\s+York\s+Stock\s+Exchange|\bNYSE\b", text, re.I):
        return "nyse"
    return None


def _item_9_ads_listings(text: str):
    """Explicit exchange/symbol declarations within the actual listing item."""
    headings = [heading for heading in re.finditer(r"\bItem\s+(?P<number>\d{1,2})(?:[A-Z])?\s*\.?", text, re.I)
                if not _inline_item_reference(text, heading.start())]
    for index, heading in enumerate(headings):
        if heading.group("number") != "9":
            continue
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        section = text[heading.end():end]
        # Inline XBRL spans occasionally split words ("Off er", "Li sting").
        if not re.match(r"\s*The\s+Off\s*er\s+and\s+Li\s*sting\b", section, re.I):
            continue
        for depositary in re.finditer(_ADS, section, re.I):
            statement = section[depositary.start():depositary.start() + 550]
            sentence_end = re.search(r"[.!?;](?:\s|$)", statement)
            if sentence_end:
                statement = statement[:sentence_end.end()]
            symbols = {_canonical_symbol(value) for value in _explicit_symbols(statement)} - {None}
            exchange = _exchange_key(statement)
            if len(symbols) != 1 or not exchange or not re.search(r"\b(?:listed|traded|trading|trade)\b", statement, re.I):
                continue
            if re.search(r"\b(?:not|no|will|may(?!\s+\d)|expect\w*|intend\w*|plan\w*|seek\w*|appl\w*|delist\w*|suspend\w*|ceased|formerly|previously)\b", statement, re.I):
                continue
            yield next(iter(symbols)), exchange, statement, heading.end() + depositary.start()


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
    description attached to the identified annual report or registration
    statement. It emits independent ratio corroboration only, never a new cover
    listing-type observation.
    """
    if document_role not in ("primary", "securities_description"):
        raise ValueError("Unsupported filing document role")
    filed = date.fromisoformat(str(filing_date)[:10])
    public = (filed + timedelta(days=1)).isoformat()
    form = form_type.upper().strip()
    base_form = form.removesuffix("/A")
    if document_role == "securities_description" and base_form not in ("20-F", "40-F", "20FR12B", "20FR12G", "40FR12B"):
        raise ValueError("A securities description must be attached to an annual report or registration statement")
    document = _Document(content)
    text = document.text
    candidates = tuple(dict.fromkeys(value for s in symbols if s for value in [_canonical_symbol(str(s))] if value))
    source_hash = sha256(content.encode("utf-8")).hexdigest()
    rows: list[dict] = []

    def emit(kind: str, source_kind: str, symbol: str | None, evidence: str, location: str,
             listed_type: str | None = None, ratio: Fraction | None = None,
             effective: str | None = None, class_text: str = "", metadata: dict | None = None,
             until: str | None = None) -> None:
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
            "effective_from": effective or public, "effective_to": until,
            "effective_date_explicit": effective is not None,
            "available_on": public, "evidence_text": _clean(evidence),
            "evidence_location": location, "parser_version": PARSER_VERSION,
            "ratio_change_program_key": None, "ratio_change_correction_kind": None,
            "ratio_change_correction_text": None, "ratio_effectiveness_pending": None,
            "ratio_effectiveness_pending_text": None, **(metadata or {}),
        })

    if base_form in ("20-F", "40-F", "20FR12B", "40FR12B") or document_role == "securities_description":
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
        depositary_notes: list[tuple[str | None, str]] = []
        for note in _DEPOSITARY_NOTE.finditer(cover):
            markers = list(re.finditer(r"\*+|[†‡]|\(\d+\)", cover[max(0, note.start() - 240):note.start()]))
            marker_value = markers[-1].group() if markers else None
            if markers:
                footnote_markers.add(marker_value)
            note_text = cover[note.start():]
            next_note = re.search(r"\*+|[†‡]|\(\d+\)|Securities\s+(?:registered|for\s+which)|Indicate\s+by\s+check", note_text, re.I)
            if next_note:
                note_text = note_text[:next_note.start()]
            depositary_notes.append((marker_value, note_text))
        listed_symbols: set[str] = set()
        ads_symbols: set[str] = set()
        cover_lines: dict[str, list[tuple[str, str]]] = {}
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
                           and re.search(rf"ordinary\s+shares?|common\s+(?:shares?|stock)|subordinate\s+voting\s+shares?|variable\s+voting\s+shares?|{_ADS}", " ".join(row), re.I)]
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
                associated_notes = ""
                if depositary_notes:
                    # Footnote association requires a marker on this title, or
                    # a unique symbol across the 12(b) table.
                    title_markers = set(re.findall(r"\*+|[†‡]|\(\d+\)", row_text))
                    if title_markers & footnote_markers or (not title_markers and not footnote_markers and len(table_symbols) == 1):
                        footnote = cover
                        associated_notes = " ".join(note_text for marker, note_text in depositary_notes
                                                    if marker in title_markers or (marker is None and not title_markers))
                exchange_wrapper = bool(re.search(rf"in\s+connection\s+with\s+(?:the\s+)?(?:listing|registration).{{0,140}}?{_ADS}", row_text, re.I))
                exchange_wrapper = exchange_wrapper or any(
                    _EXCHANGE.search(cell) and re.search(rf"\(\s*{_ADS}\s*[,;:]?\s+(?:each\s+)?represent(?:s|ing)?\b", cell, re.I)
                    for cell in cells
                )
                if re.search(_ADS, title, re.I) or re.search(_ADS, associated_notes, re.I) or exchange_wrapper:
                    kind = "ads"
                elif footnote:
                    kind = "unknown"
                elif non_equity:
                    kind = "unknown"
                elif re.search(r"ordinary\s+shares?|common\s+(?:shares?|stock)|subordinate\s+voting\s+shares?|variable\s+voting\s+shares?", title, re.I) and not _NOT_TRADING.search(row_text):
                    kind = "ordinary_direct"
                else:
                    kind = "unknown"
                for symbol in row_symbols:
                    if symbol:
                        listed_symbols.add(symbol)
                        normalized_symbol = _canonical_symbol(symbol)
                        exchange = next((_exchange_key(cell) for cell in cells if _exchange_key(cell)), None)
                        if normalized_symbol and exchange:
                            cover_lines.setdefault(normalized_symbol, []).append((exchange, title))
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
                ordinary_titles = list(re.finditer(
                    r"ordinary\s+shares?|common\s+(?:shares?|stock)|subordinate\s+voting\s+shares?|variable\s+voting\s+shares?",
                    table_text, re.I,
                ))
                exchanges = list(_EXCHANGE.finditer(table_text))
                # Plain-text legacy covers can name one ordinary class without
                # a Class/Series label. Require the actual two-column headings,
                # a sole title followed by a sole exchange, and no disqualifying
                # security or trading qualifiers. Keep the symbol issuer-scoped.
                if (not class_titles and len(ordinary_titles) == len(exchanges) == 1
                        and ordinary_titles[0].start() < exchanges[0].start()
                        and re.search(r"title\s+of\s+(?:each\s+)?class", table_text, re.I)
                        and re.search(r"name\s+of\s+(?:each\s+)?exchange", table_text, re.I)
                        and _ordinary_candidate(table_text) and not depositary_notes):
                    class_title = table_text[ordinary_titles[0].start():exchanges[0].start()].strip()
                    emit("listed_type", "cover_12b", None, table_text,
                         f"cover/section-12b/positioned-text;text-offset={cover_start}",
                         "ordinary_direct", class_text=class_title)
                elif len(class_titles) == 1:
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
        if document_role == "primary":
            for symbol, exchange, statement, offset in _item_9_ads_listings(text):
                associated_titles = [title for venue, title in cover_lines.get(symbol, []) if venue == exchange]
                if associated_titles:
                    # Keep the literal cover assertion. If it says ordinary
                    # direct, these co-effective sources correctly disagree;
                    # the resolver returns ambiguous instead of choosing one.
                    emit("listed_type", "listing_description", symbol, statement,
                         f"item-9/listing-description;text-offset={offset}", "ads",
                         class_text=" ".join(associated_titles) + " " + statement)
        ratio_text = text if document_role == "securities_description" else cover
        for match, ratio in _ratios(ratio_text, unit_definition_text=text):
            if _superseded_ratio(ratio_text, match.start(), match.end()):
                continue
            local_symbols = sorted(ads_symbols)
            ratio_context = ratio_text[max(0, match.start() - 300):match.end() + 300]
            for symbol in local_symbols if len(local_symbols) == 1 else [None]:
                evidence = ratio_context if document_role == "securities_description" else cover
                offset = match.start() if document_role == "securities_description" else cover_start + match.start()
                location = f"text-offset={offset}" if document_role == "securities_description" else f"cover/section-12b;text-offset={offset}"
                if document_role == "primary":
                    registration = list(re.finditer(
                        r"\bSecurities\s+(?:registered\s+(?:or\s+to\s+be\s+registered\s+)?|for\s+which\s+there\s+is\s+a\s+reporting\s+obligation\s+)"
                        r"[^.;]{0,120}?\b(?:Section\s+)?(?P<section>12\s*\([bg]\)|15\s*\(d\))", cover[:match.start()], re.I,
                    ))
                    if registration and re.fullmatch(r"12\s*\(g\)", registration[-1].group("section"), re.I):
                        # An ordinary entitlement in 12(g) corroborates its
                        # ratio; it does not establish a 12(b) listed line.
                        location = f"cover/section-12g;text-offset={offset}"
                emit("ads_ratio", "cover_footnote", symbol, evidence, location, ratio=ratio, class_text=match.group(), effective=_effective_date(ratio_context))
        # Item 12.D corroboration is separate from the primary F-6 stream.
        item_sections = [] if document_role == "securities_description" else _item_12d_sections(text)
        for item_offset, item_text in item_sections:
            for match, ratio in _ratios(item_text, unit_definition_text=text):
                if _superseded_ratio(item_text, match.start(), match.end()):
                    continue
                context = item_text[max(0, match.start() - 300):match.end() + 300]
                local_symbols = [s.upper() for s in _explicit_symbols(context)] or sorted(ads_symbols)
                for symbol in local_symbols if len(local_symbols) == 1 else [None]:
                    emit("ads_ratio", "item_12d", symbol, context, f"item-12d;text-offset={item_offset + match.start()}", ratio=ratio, class_text=match.group(), effective=_effective_date(context))
    elif base_form in ("F-6", "F-6EF", "F-6 POS"):
        explicit = [s.upper() for s in _explicit_symbols(text)]
        amendment_date = _f6_amendment_date(text)
        amendment_date_match = None
        if amendment_date:
            stated_date = date.fromisoformat(amendment_date)
            month = stated_date.strftime("%B")
            amendment_date_match = re.search(
                rf"\b(?:{month}\s+{stated_date.day}(?:st|nd|rd|th)?,?\s+{stated_date.year}|{stated_date.day}\s+{month}\s+{stated_date.year}|{amendment_date})\b",
                text, re.I,
            )
        ratio_matches = list(_ratios(text))
        deposited_units = _f6_deposited_unit_declarations(text)
        interval_cache: dict[tuple[Fraction, str | None], tuple[str | None, str | None, str] | str | None] = {}
        for match, ratio in ratio_matches:
            deposited_unit = _f6_ratio_deposited_unit(text, match, deposited_units)
            if deposited_unit is None:
                continue
            class_text, unit_proof = deposited_unit
            context = text[max(0, match.start() - 700):match.end() + 700]
            if _superseded_ratio(text, match.start(), match.end()):
                continue
            interval_key = (ratio, _underlying_class(class_text))
            if interval_key not in interval_cache:
                interval_cache[interval_key] = _f6_ratio_interval(text, ratio, class_text, ratio_matches)
            interval = interval_cache[interval_key]
            if interval == "ambiguous":
                continue  # Contradictory operative dates cannot become filing-date fallbacks.
            if interval and interval[1] is not None and interval[1] <= public:
                continue  # This source first becomes public after the prior regime expired.
            location = f"f6/depositary-description;text-offset={match.start()}"
            if unit_proof:
                proof = text[unit_proof[0]:unit_proof[1]]
                if proof not in context:
                    context += " " + proof
                location += ";deposited-share-definition-text-offset=" + str(unit_proof[0])
            if amendment_date_match and not (max(0, match.start() - 700) <= amendment_date_match.start() <= match.end() + 700):
                context += " " + text[max(0, amendment_date_match.start() - 300):amendment_date_match.end() + 150]
                location += f";amendment-date-text-offset={amendment_date_match.start()}"
            local_symbols = [s.upper() for s in _explicit_symbols(context)] or explicit
            for symbol in local_symbols if len(set(local_symbols)) == 1 else [None]:
                effective = interval[0] if interval else amendment_date or _effective_date(context)
                until = interval[1] if interval else None
                if interval:
                    location += ";operative-ratio-interval=" + ("prior" if until else "commencing")
                    if interval[2] not in context:
                        context += " " + interval[2]
                emit("ads_ratio", "f6", symbol, context, location, ratio=ratio,
                     effective=effective, until=until, class_text=class_text,
                     metadata=_f6_pending_effectiveness(text, ratio, class_text))
    elif base_form == "6-K":
        for match, ratio in _ratios(text):
            scope = _6k_ratio_context(text, match.start(), match.end())
            if scope is None:
                continue
            context, own = scope
            # In 'from one ADS ... to one ADS ...', retain only the new side.
            dated_current = (re.search(r"\bcurrent\b[^.;]{0,60}\bratio\b[^.;]{0,30}$",
                                       text[max(0, match.start() - 120):match.start()], re.I)
                             and not _RATIO_FROM.search(context))
            if _prior_ratio(text, match.start()) and not dated_current:
                continue
            named_event = _6k_named_event_date(text, match, own)
            event_name = "Share Consolidation"
            if named_event is None:
                named_event = _6k_reverse_split_date(text, match, own, context)
                event_name = "Reverse Split"
            reverse_linked = bool(re.search(r"\bConcurrently\s+with\s+(?:the\s+)?reverse\s+(?:share\s+|stock\s+)?split\b", own, re.I))
            own_date = _6k_completed_event_date(own) or _effective_date(own)
            if named_event and own_date and named_event[0] != own_date:
                continue
            effective = named_event[0] if named_event else own_date or (None if reverse_linked else _effective_date(context))
            if not effective:
                continue
            location = f"6k/ratio-change;text-offset={match.start()}"
            if named_event:
                proof = named_event[1]
                context_start = text.find(context, max(0, match.start() - 1200), match.end() + 1400)
                if context_start >= 0:
                    context = text[min(context_start, proof.start()):max(context_start + len(context), proof.end())]
                location += ";effective-date-named-event=" + event_name + ";named-event-date-text-offset=" + str(proof.start())
            if match.groupdict().get("unit", "").upper() == "CUFS":
                proofs = _cufs_ordinary_ratio_proofs(text, match, ratio)
                context_start = text.find(context, max(0, match.start() - 1200), match.end() + 1400)
                if proofs and context_start >= 0:
                    context = text[context_start:max(context_start + len(context), max(proof.end() for proof in proofs))]
                    location += ";ordinary-equivalence-text-offset=" + str(proofs[0].start())
            local_symbols = [s.upper() for s in _explicit_symbols(context)] or [s.upper() for s in _explicit_symbols(text)]
            for symbol in local_symbols if len(set(local_symbols)) == 1 else [None]:
                emit("ads_ratio", "ratio_change_6k", symbol, context, location, ratio=ratio, effective=effective,
                     class_text=_6k_ratio_class(match, context), metadata=_6k_correction_metadata(text, context))
    unique = {}
    for row in rows:
        key = tuple((key, str(value)) for key, value in row.items())
        unique[key] = row
    return list(unique.values())
