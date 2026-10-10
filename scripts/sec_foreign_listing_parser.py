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

PARSER_VERSION = "foreign-listing-v11"
_SPACE = re.compile(r"\s+")
_ADS = r"(?:(?:(?:American?|Global)\s+)?deposit[ao]ry\s+(?:shares?|receipts?)|American\s+shares?\s*\(evidenced\s+by\s+deposit[ao]ry\s+receipts\)|[AG]D[SR]s?)"
_SHARES = r"(?:(?:(?:class|series)\s+[A-Z0-9]+\s+)?(?:ordinary|common)\s+shares?|shares?\s+of\s+common\s+stock|(?:class|series)\s+[A-Z0-9]+\s+shares?|shares?)"
_WORDS = "one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|thousand|half|third|quarter"
_QUANTITY = rf"(?:\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?|(?:{_WORDS})(?:[ -]+(?:{_WORDS}|and)){{0,5}})(?:\s*\(\s*(?:\d[\d,]*(?:\.\d+)?(?:\s*/\s*\d+)?|(?:{_WORDS})(?:[ -]+(?:{_WORDS}|and)){{0,5}})\s*\))?"
_OF_UNIT = r"(?:\s+of(?:\s+(?:one|an?|the)(?:\s*\(\s*\d+\s*/\s*\d+\s*\))?)?)?"
_RATIO = re.compile(
    rf"(?P<adsn>each|{_QUANTITY})\s+[\"'\u201c\u201d\u2018\u2019\ufffd]*\s*{_ADS}(?!\w)(?:[^.;]|\.(?=\d)){{0,100}}?\b"
    rf"(?:represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?|to|per|for(?:\s+every)?)"
    rf"(?:,\s*and\s+to\s+exercise\s+the\s+beneficial\s+ownership\s+interests\s+in,?)?\s+"
    rf"(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+(?:(?:of\s+)?(?:our|the|its|company.s)\s+)?{_SHARES}",
    re.I,
)
_RATIO_TITLE = re.compile(
    rf"{_ADS}(?!\w)[^.;]{{0,100}}?\b(?:each\s+(?:of\s+which\s+)?|which\s+)represent(?:s|ing)?(?:\s+the\s+right\s+to\s+receive)?\s+(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+{_SHARES}",
    re.I,
)
_RATIO_RECIPROCAL = re.compile(
    rf"(?P<ordinary>{_QUANTITY}){_OF_UNIT}\s+{_SHARES}\s+(?:to|per|for(?:\s+every)?)\s+(?P<adsn>each|{_QUANTITY})\s+{_ADS}(?!\w)",
    re.I,
)
_RATIO_COMPACT_TRANSITION = re.compile(
    rf"\bfrom\s+(?:a\s+)?{_QUANTITY}\s*-\s*to\s*-\s*{_QUANTITY}\s+"
    rf"(?P<unit>CUFS|(?:ordinary|common)\s+shares?)-to-{_ADS}\s+ratio\s+to\s+(?:a\s+)?"
    rf"(?P<ordinary>{_QUANTITY})\s*-\s*to\s*-\s*(?P<adsn>{_QUANTITY})\s+ratio\b", re.I,
)
_RATIO_CHANGE = re.compile(
    r"\b(?:ratio|exchange\s+(?:ratio|rate))\b[^.;]{0,180}\b(?:chang|amend|adjust)\w*\b"
    r"|\b(?:chang|amend|adjust)\w*\b[^.;]{0,180}\b(?:ratio|exchange\s+(?:ratio|rate))\b"
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

# These are grammatical objects, not proximity searches. Completing a budget,
# preparations, or notices for a ratio change does not complete the change.
_RATIO_NOUN = rf"(?:{_ADS}(?:\s*\([^)]{{0,40}}\))?(?:[\s-]+to[\s-]+{_SHARES})?[\s-]+)?ratio"
_RATIO_CHANGE_OBJECT = rf"(?:(?:(?:first|second|proposed|planned|concurrent)\s+)?{_RATIO_NOUN}\s+change|change\s+(?:of|in|to)\s+(?:(?:the|a)\s+)?(?:new\s+)?{_RATIO_NOUN})"
_EVENT_DETERMINER = r"(?:(?:the|a|an|its|our|this|that)\s+)?"
_RATIO_OBJECT_TAIL = rf"(?=$|[.,;:()\"'\u201c\u201d\u2018\u2019\ufffd]|\s+(?:from|to|of|in|on|effective|whereby|which|that|with|and|as|for|representing|proportionate|so)\b|\s+(?:our|its|the)\s+{_ADS}\b)"
_FINANCIAL_CONFIRMATION = re.compile(
    r"\b(?:EPS|weighted\s+average|(?:basic|diluted)\s+(?:(?:net\s+)?(?:income|loss|earnings)|and\s+diluted)|"
    r"(?:income|loss|earnings)\s+per\s+(?:(?:ordinary|equivalent)\s+)?(?:shares?|ADS)|fair\s+value\s+measurement|"
    r"total\s+(?:assets|liabilities)|cash\s+flows?)\b", re.I,
)
# Modal and negative words must govern an event predicate. An ancillary clause
# saying new ADSs will be available, or ownership does not change, is not a
# qualification of an already completed ratio change.
_CONFIRMATION_QUALIFIER = re.compile(
    r"\b(?:if|unless|assume|assuming|assumed|assumptions?|suppose|supposing|hypothetical(?:ly)?|pro\s+forma|"
    r"financial\s+statement\s+presentation|for\s+purposes\s+of|as\s+(?:if|though)|"
    r"retrospectively\s+adjusted|had\s+been\s+effective|"
    r"(?:would|could|should|may|might|will|shall)\s+(?:have\s+|be\s+|become\s+)?"
    r"(?:effective|approved|adopted|represent\w*|chang\w*|amend\w*|adjust\w*|"
    r"complet\w*|effect\w*|implement\w*|take\s+effect)|"
    r"expects?|expected|intends?|anticipated|subject\s+to|conditional(?:ly)?|conditioned|contingent|"
    r"provided\s+that|on\s+condition\s+that|(?:pending|awaiting)\s+(?:regulatory|shareholder)\s+approval|"
    r"not\s+(?:yet\s+)?(?:effective|effected|completed|implemented|approved|adopted)|"
    r"never\s+(?:became\s+effective|completed|effected|implemented))\b", re.I,
)
# These premises always inspect the full owning sentence, including text that
# will later be trimmed from the auditable narrative proof.
_CONFIRMATION_PREMISE = re.compile(
    r"\b(?:if|unless|provided\s+that|on\s+condition\s+that|"
    r"(?:pending|awaiting)\s+(?:regulatory|shareholder)\s+approval|"
    r"assume|assuming|assumed|assumptions?|suppose|supposing|hypothetical(?:ly)?|pro\s+forma|"
    r"financial\s+statement\s+presentation|for\s+(?:the\s+)?purposes?\s+of|"
    r"for\s+(?:EPS|accounting|financial\s+reporting)\s+purposes?|as\s+(?:if|though)|"
    r"retro(?:spect|act)ively\s+(?:adjusted|revised)|(?:adjusted|revised)\s+retro(?:spect|act)ively|"
    r"had\s+been\s+effective|denied|disputed|without\s+establishing|"
    r"no\s+(?:evidence|confirmation)|yet\s+to\s+confirm|incorrectly\s+suggests|"
    r"(?:cannot|could\s+not|unable\s+to)\s+confirm|"
    r"(?:would|could|should|might)\s+have|"
    r"(?:may|would|could|might)\s+(?:become\s+effective|take\s+effect))\b", re.I,
)
_NEGATED_RATIO_EVENT = re.compile(
    r"\b(?:(?:did|does|do|has|have|had|is|are|was|were)\s+not|"
    r"(?:did|does|do|has|have|had|is|are|was|were)n['\u2019]t)(?:\s+yet)?\s+"
    r"(?:(?:legally|actually|in\s+fact)\s+)?"
    r"(?:be(?:en|come)?\s+)?(?:effective|effected|completed|implemented|obtained|received|approved|adopted|passed|resolved|granted|taken?\s+effect|"
    rf"(?:change[ds]?|amend(?:ed)?|adjust(?:ed)?)\s+{_EVENT_DETERMINER}{_RATIO_NOUN})\b", re.I,
)


def _confirmation_is_affirmative(statement: str) -> bool:
    """Reject a qualified event predicate or an accounting premise."""
    return not (_CONFIRMATION_QUALIFIER.search(statement) or _CONFIRMATION_PREMISE.search(statement)
                or _NEGATED_RATIO_EVENT.search(statement))



def _clean(value: str) -> str:
    return _SPACE.sub(" ", value.replace("\xa0", " ").replace("\u200b", "")).strip()


@dataclass
class _Table:
    offset: int
    line: int
    rows: list[list[str]] = field(default_factory=list)


@dataclass(frozen=True)
class _DateEvidence:
    dates: tuple[str, ...]
    proof: str
    spans: tuple[tuple[int, int], ...] = ()

    @property
    def unique(self) -> str | None:
        return self.dates[0] if len(self.dates) == 1 else None

    @property
    def conflict(self) -> bool:
        return len(self.dates) > 1

    def metadata(self) -> dict:
        return {"operative_date_conflict": True, "operative_date_candidates": list(self.dates),
                "operative_date_conflict_text": _clean(self.proof)} if self.conflict else {}


def _merge_date_evidence(*evidence: _DateEvidence | None) -> _DateEvidence:
    present = [item for item in evidence if item and item.dates]
    return _DateEvidence(tuple(sorted({day for item in present for day in item.dates})),
                         " ".join(dict.fromkeys(item.proof for item in present)),
                         tuple(span for item in present for span in item.spans))


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
            and not re.match(rf"\s+(?:per|to|for(?:\s+every)?)\s+{_QUANTITY}{_OF_UNIT}\s+{_SHARES}",
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


def _6k_ratio_is_transition_target(text: str, match: re.Match) -> bool:
    """A TO-current/new ratio is the target even though CURRENT alone is former."""
    return bool(re.search(r"\bfrom\b", text[max(0, match.start() - 700):match.start()], re.I) and re.search(
        r"\bto\s+(?:(?:the|a)\s+)?(?:current|new)\s+(?:ADS\s+)?ratio\s+(?:of\s+)?$",
        text[max(0, match.start() - 100):match.start()], re.I,
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
    # The bounded discourse window may cut a long owning sentence in half.
    # Expand only that sentence, so a distant assumption cannot disappear
    # before the confirmation qualifier screen.
    if index == 0 and left:
        prior_boundaries = list(re.finditer(r"[.!?;](?!\d)(?=\s|$)", text[:left]))
        boundaries[0] = 0
        for boundary in reversed(prior_boundaries):
            if text[boundary.start()] == "." and re.search(
                    r"(?:\b(?:Co|Ltd|Inc|Corp|PLC|Mr|Dr|No)|\bU\.S|\bN\.A)$",
                    text[max(0, boundary.start() - 12):boundary.start()], re.I):
                continue
            boundaries[0] = boundary.end()
            break
    if index == len(boundaries) - 2 and right < len(text):
        following_boundary = re.search(r"[.!?;](?!\d)(?=\s|$)", text[right:])
        boundaries[-1] = right + following_boundary.end() if following_boundary else len(text)
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
                              and re.search(_ADS, own, re.I) and _operative_date_evidence(own).dates)
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
        if _operative_date_evidence(following).dates and not (
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
    declarations.extend(re.finditer(
        rf"\bratio\s+of\s+(?:(?:its|our|the)\s+)?{_ADS}(?:\s*\([^)]{{0,40}}\))?\s+to\s+"
        r"(?P<class>(?:Class|Series)\s+[A-Z0-9]+\s+(?:ordinary|common)\s+shares?)"
        r"(?:\s*\([^)]{0,40}\))?\s+from\b", prefix, re.I,
    ))
    declarations.extend(re.finditer(
        rf"{_ADS}(?:\s*\([^)]{{0,40}}\))?\s*,?\s*representing\s+"
        r"(?P<class>(?:Class|Series)\s+[A-Z0-9]+\s+(?:ordinary|common)\s+shares?)"
        r"\s+from\b", prefix, re.I,
    ))
    declarations.sort(key=lambda declaration: declaration.start())
    return class_text + " " + declarations[-1].group("class") if declarations else class_text


def _6k_named_event_date(text: str, match: re.Match, own: str) -> _DateEvidence | None:
    """Resolve a ratio date explicitly linked to its consolidation or subdivision event."""
    relation = re.compile(
        r"\b(?:conditional\s+(?:upon|on)|conditioned\s+(?:upon|on)|upon|following|after|"
        r"proportionate\s+to|concurrently\s+with(?:\s+(?:the\s+)?effectiveness\s+of)?|"
        r"concurrently\s+on\s+(?:the\s+)?same\s+day)\s+(?:the\s+)?"
        r"(?P<event>Share\s+(?:Consolidation|Subdivision))\b", re.I)
    events = []
    for link in relation.finditer(own):
        prefix = own[max(0, link.start() - 45):link.start()]
        if not re.search(r"\b(?:not|unrelated|independent)\b", prefix, re.I):
            events.append(link.group("event"))
    if not events:
        return None

    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    clock = r"(?:at\s+\d{1,2}\s*:\s*\d{2}\s*[AP]\.?\s*M\.?\s*,?\s*)?"
    window_start, window_end = max(0, match.start() - 2500), min(len(text), match.end() + 2500)
    current_class = _underlying_class(_6k_ratio_class(match, own))
    matches: list[re.Match] = []
    for event in dict.fromkeys(events):
        event_name = re.escape(event)
        patterns = [
            re.compile(rf"\b(?:(?:the|such)\s+)?{event_name}\s+"
                       rf"(?:will\s+become|will\s+be|shall\s+be|to\s+be|is|was|has\s+become|became)\s+"
                       rf"effective\s+(?:(?:on|from|as\s+of)\s+)?{clock}(?:on\s+)?(?P<date>{date_value})", re.I),
        ]
        if event.lower().endswith("subdivision"):
            patterns.append(re.compile(
                rf"\b(?:one-to-[A-Za-z0-9-]+\s+)?subdivision\s+of\s+(?:ordinary\s+)?shares?\s+"
                rf"(?:has\s+(?:become|been)|have\s+(?:become|been)|will\s+(?:become|be)|"
                rf"shall\s+be|was|were|is|became)\s+effective\s+(?:on|from|as\s+of)\s*(?P<date>{date_value})", re.I))
            patterns.append(re.compile(
                rf"\b(?:[^.;]|\.(?=\d)){{0,450}}\bsubdivided\s+into\b(?:[^.;]|\.(?=\d)){{0,450}}?"
                rf"\beffective\s+(?:on|from|as\s+of)\s*(?P<date>{date_value})"
                rf"(?:[^.;]|\.(?=\d)){{0,100}}\bShare\s+Subdivision\b", re.I))
        for pattern in patterns:
            matches.extend(pattern.finditer(text, window_start, window_end))
    dated = []
    for proof in matches:
        lead = text[max(0, proof.start() - 80):proof.start()]
        declared_class = _underlying_class(lead + proof.group())
        if current_class is not None and declared_class is not None and current_class != declared_class:
            continue
        value = _effective_date("effective on " + proof.group("date"))
        if value:
            dated.append((proof, value))
    if not dated:
        return None
    return _DateEvidence(tuple(sorted({value for _, value in dated})),
                         " ".join(dict.fromkeys(proof.group() for proof, _ in dated)),
                         tuple(proof.span() for proof, _ in dated))


def _6k_reverse_split_date(text: str, match: re.Match, own: str, context: str) -> _DateEvidence | None:
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
        return _DateEvidence(tuple(sorted({value for value, _ in adjacent})),
                             " ".join(dict.fromkeys(proof.group() for _, proof in adjacent)),
                             tuple(proof.span() for _, proof in adjacent))
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
    return _DateEvidence(tuple(sorted({value for value, _ in dated})),
                         " ".join(dict.fromkeys(proof.group() for _, proof in dated)),
                         tuple(proof.span() for _, proof in dated)) if dated else None


def _6k_completed_event_dates(own: str) -> _DateEvidence:
    date_value = r"(?:[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})"
    completion = rf"(?:completed|effected|implemented)\s+{_EVENT_DETERMINER}{_RATIO_CHANGE_OBJECT}\b{_RATIO_OBJECT_TAIL}"
    adjustment = rf"(?:adjusted|changed|amended)\s+{_EVENT_DETERMINER}{_RATIO_NOUN}\b{_RATIO_OBJECT_TAIL}"
    invalid_subject = re.compile(
        r"\b(?:announced|reported|said|disclosed|expect\w*|intend\w*|propos\w*|plan\w*|"
        r"anticipat\w*|not|never|if|will|would|could|should|may|might)\b", re.I,
    )
    patterns = (
        rf"\b(?:On|Effective(?:\s+(?:on|as\s+of|from))?)\s+(?P<date>{date_value})\s*,?\s*"
        rf"(?P<subject>[^.;]{{0,100}}?)\b(?:{completion}|{adjustment})",
        rf"(?P<subject>[^.;]{{0,100}}?)\b{completion}"
        rf"(?:[^.;]|\.(?=\d)){{0,450}}?\bon\s+(?P<date>{date_value})",
        rf"(?P<subject>[^.;]{{0,100}}?)\b{adjustment}"
        rf"(?:[^.;]|\.(?=\d)){{0,450}}?\beffective\s+(?:on\s+)?(?P<date>{date_value})",
        rf"(?P<subject>(?:the\s+)?Company|we)\s+(?:has\s+)?(?:effected|completed|implemented)\s+"
        rf"{_EVENT_DETERMINER}(?:(?:Ordinary|Common)\s+Share|Share)\s+Consolidation\b"
        rf"(?:[^.;]|\.(?=\d)){{0,120}}?\balong\s+with\s+{_EVENT_DETERMINER}"
        rf"(?!(?:planned|proposed|expected)\b){_RATIO_CHANGE_OBJECT}\b{_RATIO_OBJECT_TAIL}"
        rf"(?:[^.;]|\.(?=\d)){{0,400}}?\b(?:this\s+)?(?:was|became)\s+effective\s+"
        rf"(?:(?:on|from|as\s+of)\s+)?(?P<date>{date_value})",
    )
    dated = []
    for pattern in patterns:
        for declaration in re.finditer(pattern, own, re.I):
            if invalid_subject.search(declaration.group("subject")):
                continue
            value = _effective_date("effective on " + declaration.group("date"))
            if not value:
                continue
            # Keep the narrative declaration and its numerical transition, not
            # the flattened financial table or heading before the declaration.
            start = declaration.start()
            if not re.match(r"\s*(?:On|Effective)\b", declaration.group(), re.I):
                subject = declaration.group("subject")
                narrative_subject = re.search(
                    r"\b(?:(?:the|our)\s+)?(?:Company|we|[A-Z][A-Za-z0-9&.-]+)(?:\s+(?:has|had))?\s*$",
                    subject, re.I,
                )
                start += narrative_subject.start() if narrative_subject else len(subject)
            boundary = re.search(r"[.!?;](?!\d)(?:\s|$)", own[declaration.end():])
            end = declaration.end() + boundary.end() if boundary else len(own)
            accounting = re.search(
                r",?\s+and\s+(?:basic\b|diluted\b|EPS\b|all\s+(?:ADS|per[- ]ADS)\b)", own[start:end], re.I,
            )
            if (accounting and re.search(r"\b(?:revised|adjusted|retrospective\w*)\b", own[start + accounting.end():end], re.I)
                    and not re.search(r"\b(?:assuming|assumed|hypothetical\w*|pro\s+forma|presentation\s+only)\b", own[start:end], re.I)):
                # The completed legal event remains factual when followed by
                # a separate clause describing its retrospective EPS effect.
                end = start + accounting.start()
            dated.append((value, start, end))
    return _DateEvidence(tuple(sorted({value for value, _, _ in dated})),
                         " ".join(dict.fromkeys(own[start:end] for _, start, end in dated)),
                         tuple((start, end) for _, start, end in dated))


def _6k_completed_event_date(own: str) -> str | None:
    return _6k_completed_event_dates(own).unique


def _f6_ratio_interval(text: str, ratio: Fraction, class_text: str,
                       candidates: list[tuple[re.Match, Fraction]]) -> tuple[str | None, str | None, str] | _DateEvidence | None:
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
    if len({day for _, day, _ in intervals}) > 1:
        return _DateEvidence(tuple(sorted({day for _, day, _ in intervals})),
                             " ".join(dict.fromkeys(proof for _, _, proof in intervals)))
    if len(regimes) > 1:
        return None  # The same entitlement before and after one date is continuous coverage.
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


def _f6_ratio_date_scope(text: str, match: re.Match,
                         candidates: list[tuple[re.Match, Fraction]]) -> str:
    """Keep the entitlement's own and explicitly connected date statements."""
    left, right = max(0, match.start() - 700), min(len(text), match.end() + 700)
    boundaries = [left]
    for boundary in re.finditer(r"[.!?;](?!\d)[\"'\u201d\u2019\ufffd]*(?=\s|$)", text[left:right]):
        position = left + boundary.start()
        if text[position] == "." and re.search(r"(?:\b(?:Co|Ltd|Inc|Corp|PLC|Mr|Dr|No)|\bU\.S|\bN\.A)$",
                                                text[max(left, position - 12):position], re.I):
            continue
        boundaries.append(left + boundary.end())
    boundaries.append(right)
    index = next(i for i in range(len(boundaries) - 1) if boundaries[i] <= match.start() < boundaries[i + 1])
    start, end = boundaries[index], boundaries[index + 1]
    if re.search(r"\b(?:as\s+of|on|from)\s+(?:the\s+)?Effective\s+Date\b", text[start:end], re.I):
        for previous_index in range(index - 1, max(-1, index - 4), -1):
            previous = text[boundaries[previous_index]:boundaries[previous_index + 1]]
            if _RATIO_CHANGE.search(previous) and re.search(_ADS, previous, re.I) and _operative_date_evidence(previous).dates:
                prior_class, current_class = _underlying_class(previous), _underlying_class(match.group())
                if prior_class is None or current_class is None or prior_class == current_class:
                    start = boundaries[previous_index]
                break
    neighbors = sorted([candidate for candidate, _ in candidates if start <= candidate.start() < end],
                       key=lambda candidate: candidate.start())
    preceding = [candidate for candidate in neighbors if candidate.end() <= match.start()]
    following = [candidate for candidate in neighbors if candidate.start() >= match.end()]
    connector = re.compile(r"\b(?:and|but|while|whereas|then)\b", re.I)
    if preceding:
        prior = preceding[-1]
        links = list(connector.finditer(text, prior.end(), match.start()))
        start = links[-1].end() if links else prior.end()
    if following:
        following_match = following[0]
        links = list(connector.finditer(text, match.end(), following_match.start()))
        end = links[-1].start() if links else following_match.start()
        lead = text[match.end():end]
        for date_span in _operative_date_evidence(lead).spans:
            if _RATIO_FROM.search(lead[date_span[1]:]):
                # A dated historical from/to clause belongs to its later
                # entitlement, not a current cover title preceding it.
                end = match.end() + date_span[0]
                break
    for following_index in range(index + 1, min(index + 4, len(boundaries) - 1)):
        statement = text[boundaries[following_index]:boundaries[following_index + 1]]
        if any(candidate.start() < boundaries[following_index + 1] and candidate.end() > boundaries[following_index]
               for candidate, _ in candidates):
            break
        if not re.search(r"\b(?:this|the)\s+(?:ADS\s+)?ratio\s+change\b|\bratio\b[^.;]{0,100}\beffective\b|\b(?:the\s+)?effective\s+date\b",
                         statement, re.I):
            break
        if not _operative_date_evidence(statement).dates:
            break
        end = boundaries[following_index + 1]
    return text[start:end]


def _6k_effectiveness_metadata(text: str, match: re.Match, own: str, context: str,
                               class_text: str, named_event: _DateEvidence | None, effective: str,
                               named_event_name: str | None = None, ratio: Fraction | None = None,
                               program_symbol: str | None = None) -> dict:
    """Distinguish an outstanding condition from an affirmative event result."""
    current_class = _underlying_class(class_text)
    # Contract boilerplate and pro-forma accounting assumptions are not
    # outstanding approval conditions. They neither defer a dated change nor
    # require a later confirmation.
    _BOILERPLATE_CONDITION = re.compile(
        r"\b(?:terms?(?:\s+and\s+conditions?)?|provisions)\s+of\s+(?:the\s+)?(?:Deposit(?:ary)?\s+)?Agreement\b"
        r"|\b(?:in\s+accordance\s+with|pursuant\s+to)\s+(?:the\s+)?(?:Deposit(?:ary)?\s+)?Agreement\b"
        r"|\boperational\s+procedures\s+of\s+the\s+Depository\s+Trust\s+Company\b"
        r"|\bassuming\b(?:(?:[^.;]|\.(?=\d)){0,200})\bat\s+the\s+beginning\s+of\s+the\s+earliest\s+period\s+presented\b",
        re.I,
    )

    def same_class(fragment: str) -> bool:
        stated = _underlying_class(fragment)
        # A named class cannot discharge a transition with unknown class.
        named = set((kind.lower(), label.lower()) for kind, label in re.findall(
            r"\b(class|series)\s+([A-Z0-9]+)\b", fragment, re.I)
                    if label.casefold() not in {"of", "ordinary", "common", "par", "shares", "share", "stock"})
        return len(named) <= 1 and (not named or (stated is not None and stated == current_class))

    def same_event(fragment: str, *, allow_financial_lead: bool = False) -> bool:
        if (not allow_financial_lead and _FINANCIAL_CONFIRMATION.search(fragment)) or not same_class(fragment):
            return False
        def receipt_programs(value: str) -> set[str]:
            programs = set()
            if re.search(r"\b(?:ADSs?|ADRs?|American\s+deposit[ao]ry\s+(?:shares?|receipts?))\b", value, re.I):
                programs.add("american")
            if re.search(r"\b(?:GDSs?|GDRs?|Global\s+deposit[ao]ry\s+(?:shares?|receipts?))\b", value, re.I):
                programs.add("global")
            return programs
        stated_programs = receipt_programs(fragment)
        current_programs = receipt_programs(class_text)
        if stated_programs and not stated_programs.issubset(current_programs):
            return False
        stated_symbols = {_canonical_symbol(value) for value in _explicit_symbols(fragment)}
        if stated_symbols and stated_symbols != {program_symbol}:
            return False
        for former_subject in re.finditer(
            rf"\b(?:(?:the|its|our)\s+)?(?:former|previous|prior|old|original|existing|"
            rf"current[- ]before[- ]change)\s+{_RATIO_NOUN}\b", fragment, re.I,
        ):
            if not re.search(r"\bfrom\s+$", fragment[max(0, former_subject.start() - 20):former_subject.start()], re.I):
                return False
        # Explicit named programs must be established in this transition;
        # generic references to the/its ADS program are anaphoric.
        programs = re.findall(r"\b((?:Class|Series)\s+[A-Z0-9]+|[A-Z][A-Za-z0-9.-]{1,20})\s+(?:ADS\s+)?programs?\b", fragment, re.I)
        own_programs = {value.casefold() for value in re.findall(r"\b((?:Class|Series)\s+[A-Z0-9]+|[A-Z][A-Za-z0-9.-]{1,20})\s+(?:ADS\s+)?programs?\b", own, re.I)}
        programs = [value.casefold() for value in programs if value.casefold() not in
                    {"ads", "adr", "gds", "gdr", "the", "its", "our", "this", "that"}]
        if any(value not in own_programs for value in programs):
            return False
        stated_ratios = {value for proof, value in _ratios(fragment)
                         if (not _prior_ratio(fragment, proof.start())
                             or _6k_ratio_is_transition_target(fragment, proof))
                         and not re.search(r"\bfrom\b", proof.group(), re.I)}
        # “for every” can otherwise produce a reciprocal match spanning the
        # OLD share quantity and the NEW ADS quantity. Read the direct units
        # here so a 25 -> 5 transition cannot confirm the crossed 25/1 row.
        direct = list(re.finditer(
            rf"(?P<adsn>each|{_QUANTITY})\s+{_ADS}\s+for\s+every\s+"
            rf"(?P<ordinary>{_QUANTITY})\s+{_SHARES}", fragment, re.I,
        ))
        if direct:
            stated_ratios = {_number(proof.group("ordinary")) / _number(proof.group("adsn"))
                            for proof in direct if not re.search(
                                r"\bfrom\s+(?:(?:the|previous|old|former|current|existing)\s+)*$",
                                fragment[max(0, proof.start() - 80):proof.start()], re.I)}
        for compact in re.finditer(
            rf"\bratio\b[^.;]{{0,100}}?(?P<left>{_QUANTITY})\s*(?:[:/]|[- ]to[- ])\s*(?P<right>{_QUANTITY})\b",
            fragment, re.I,
        ):
            left, right = _number(compact.group("left")), _number(compact.group("right"))
            if not right or not left:
                return False
            if re.search(rf"{_ADS}\s+to\s+(?:ordinary|common)\s+shares?", fragment, re.I):
                stated_ratios.add(right / left)
            elif re.search(rf"(?:ordinary|common)\s+shares?\s+to\s+{_ADS}", fragment, re.I):
                stated_ratios.add(left / right)
            else:
                # A bare 20:1 does not establish which unit is the numerator.
                return False
        # An explicit unfamiliar numerical ratio is evidence to withhold, not
        # a reason to treat the confirmation as omitting its ratio.
        declared = re.search(rf"\bratio\s+(?:of\s+)?(?:{_QUANTITY})\b", fragment, re.I)
        if declared and not stated_ratios:
            return False
        if not stated_ratios and re.search(
            rf"\b(?:former|previous|prior|old|original|existing|current[- ]before[- ]change)\b"
            rf"(?:[^.;]|\.(?=\d)){{0,80}}\b{_RATIO_NOUN}\b"
            rf"|\bcurrent\b[^.;]{{0,50}}\bratio\b[^.;]{{0,80}}\bbefore\s+(?:the\s+)?change\b",
            fragment, re.I,
        ):
            # An unnumbered reference to the old side cannot establish the
            # planned target, even when both sides share an operative date.
            return False
        return ratio is None or not stated_ratios or stated_ratios == {ratio}

    def statement_for(proof: re.Match, *, source: str = context, trim_financial: bool = True) -> str:
        boundaries = [0]
        for boundary in re.finditer(r"[.!?;](?!\d)(?=\s|$)", source[:proof.start()]):
            if source[boundary.start()] == "." and re.search(
                    r"(?:\b(?:Co|Ltd|Inc|Corp|PLC)|\bU\.S|\bN\.A)$",
                    source[max(0, boundary.start() - 12):boundary.start()], re.I):
                continue
            boundaries.append(boundary.end())
        suffix = re.search(r"[.!?;](?!\d)(?:\s|$)", source[proof.end():])
        end = proof.end() + suffix.end() if suffix else len(source)
        start = boundaries[-1]
        prefix = source[start:proof.start()]
        if trim_financial and _FINANCIAL_CONFIRMATION.search(prefix):
            # A prose footnote following flattened table cells has its own
            # narrative subject. Numeric cells before that subject are not
            # part of the statement asserting completion.
            introductions = list(re.finditer(
                rf"\b(?:Each\s+{_ADS}|The\s+(?:Company|change|ratio)|On\s+\w+\s+\d{{1,2}}|Effective\s+\w+\s+\d{{1,2}})\b",
                prefix, re.I,
            ))
            if introductions:
                candidate = start + introductions[-1].start()
                if not _FINANCIAL_CONFIRMATION.search(source[candidate:proof.start()]):
                    start = candidate
        return source[start:end]

    def affirmative_statement(statement: str) -> bool:
        return _confirmation_is_affirmative(statement)

    def actual_named_date() -> bool:
        if not named_event:
            return False
        for start, end in named_event.spans:
            proof = re.compile(re.escape(text[start:end])).match(text, start, end)
            if proof:
                statement = statement_for(proof, source=text, trim_financial=False)
                if _CONFIRMATION_PREMISE.search(statement) or _NEGATED_RATIO_EVENT.search(statement):
                    return False
        return True

    def condition_codes(proof: str) -> set[str]:
        proof = re.split(r",\s+to\s+(?:change|amend)\b", proof, maxsplit=1, flags=re.I)[0]
        codes = set()
        if re.search(r"\bshareholders?\b|\bEGM\b|\bgeneral\s+meeting\b", proof, re.I):
            codes.add("shareholder_approval")
        if re.search(r"\b(?:Share\s+(?:Consolidation|Subdivision)|reverse\s+(?:stock\s+|share\s+)?split)\b", proof, re.I):
            codes.add("consolidation")
        if re.search(r"\bSEC\b|\bSecurities\s+and\s+Exchange\s+Commission\b|\bregulator\w*\b|\bForm\s+F-6\b", proof, re.I):
            codes.add("regulatory_approval")
        if re.search(r"\b(?:notice|notification|announc\w*|approval|confirm\w*|date)\b[^.;]{0,70}\bDepositary\b(?!\s+(?:shares?|receipts?))"
                     r"|\bDepositary\b(?!\s+(?:shares?|receipts?))[^.;]{0,70}\b(?:notice|notification|announc\w*|approval|confirm\w*)\b", proof, re.I):
            codes.add("depositary_notice")
        if not codes and re.search(r"\bapprov\w*\b", proof, re.I):
            codes.add("other_approval")
        if not codes:
            if _BOILERPLATE_CONDITION.search(proof):
                return set()
            codes.add("unknown_condition")
        return codes

    condition = re.compile(r"(?=(?P<condition>\b(?:subject\s+to|conditional\s+(?:upon|on)|conditioned\s+(?:upon|on)|contingent\s+(?:upon|on)|assuming|(?:if|when|once)(?=(?:[^.;]|\.(?=\d)){0,100}\bapprov\w*\b))\b(?:[^.;]|\.(?=\d)){0,300}))", re.I)
    conditions = []
    for proof in condition.finditer(context):
        start, end = proof.span("condition")
        if re.search(r"\b(?:not|no\s+longer)\s+$", context[max(0, start - 25):start], re.I):
            continue
        own_start = context.find(own)
        statement_boundaries = [0]
        for boundary in re.finditer(r"[.!?;](?!\d)\s+", context[:start]):
            position = boundary.start()
            if context[position] == "." and re.search(r"(?:\b(?:Co|Ltd|Inc|Corp|PLC|Mr|Dr|No)|\bU\.S|\bN\.A)$",
                                                     context[max(0, position - 12):position], re.I):
                continue
            statement_boundaries.append(boundary.end())
        statement_start = statement_boundaries[-1]
        statement_end = re.search(r"[.!?;](?!\d)(?:\s|$)", context[end:])
        statement = context[statement_start:end + statement_end.end() if statement_end else len(context)]
        linked = own_start >= 0 and own_start <= start < own_start + len(own)
        linked = linked or bool(re.search(r"\b(?:this|the)\s+(?:foregoing\s+)?(?:ADS\s+)?(?:ratio\s+change|change\s+in\s+(?:the\s+)?ADS\s+ratio)\b", statement, re.I))
        linked = linked or bool(named_event and re.search(r"\bupon\s+(?:the\s+)?Share\s+Consolidation\b", own, re.I)
                                and re.search(r"\bShare\s+Consolidation\b", proof.group("condition"), re.I))
        if linked:
            conditions.append(proof.group("condition"))
    prospective_plan = None
    if not conditions and _RATIO_CHANGE.search(own):
        prospective_plan = re.search(
            r"\b(?:plans?\s+to\s+(?:change|adjust|amend)\b[^.;]{0,180}\bratio|"
            r"(?:the\s+)?(?:ADS\s+)?ratio\s+change\s+is\s+expected\s+to\s+"
            r"(?:take\s+place|be\s+effective|become\s+effective))\b",
            context, re.I,
        )
    confirmation = None
    confirmed_conditions: set[str] = set()
    # A completed legal ratio event, not an announced/proposed action, is
    # affirmative evidence that its operative prerequisite was met.
    rejected_confirmation = bool(
        _CONFIRMATION_PREMISE.search(own) or _NEGATED_RATIO_EVENT.search(own)
        or re.search(rf"\b{_RATIO_NOUN}\s+(?:change\s+)?"
                     r"(?:announcements?|estimates?|notices?|filings?|agreements?|budgets?|preparations?|plans?)\b", own, re.I)
        or re.search(rf"\b{_RATIO_CHANGE_OBJECT}\b[^.;]{{0,450}}\b"
                     r"(?:will\s+become|is\s+expected\s+to\s+become)\s+effective\b", own, re.I)
    )
    completed = _6k_completed_event_dates(own)
    own_start = text.find(own, max(0, match.start() - len(own)), match.end() + len(own))
    completion_owns_ratio = own_start >= 0 and any(
        start <= match.start() - own_start and match.end() - own_start <= end
        for start, end in completed.spans
    )
    completion_lead = own[:min((start for start, _ in completed.spans), default=0)]
    completion_identity = completed.proof if _FINANCIAL_CONFIRMATION.search(completion_lead) else own
    conditional_completion = bool(
        re.search(r"\b(?:if|assuming|assumed|hypothetical)\b", completion_lead, re.I)
        or re.search(r"\b(?:would|could|should|not|never)\b", completion_lead[-40:], re.I)
    )
    if (completed.unique == effective and completion_owns_ratio and same_event(completed.proof)
            and not conditional_completion and not _CONFIRMATION_PREMISE.search(own)
            and not _NEGATED_RATIO_EVENT.search(own)
            and affirmative_statement(completed.proof)
            and same_event(completion_identity, allow_financial_lead=True)):
        confirmation = completed.proof
        confirmed_conditions.add("ratio_effective")
    elif completed.unique == effective:
        rejected_confirmation = True
    past_pattern = re.compile(
        rf"\b(?P<object>{_RATIO_CHANGE_OBJECT}|{_RATIO_NOUN})\b"
        r"(?:,?\s+(?:for|of|to|on|from)\b(?:[^.;]|\.(?=\d)){0,450}?)?"
        r"(?:,?\s+(?:that|which|this))?\s+(?:was|became|has\s+become)\s+effective\b[^.;]{0,120}",
        re.I,
    )
    candidates = [past_pattern.match(context, candidate.start()) for candidate in re.finditer(
        "(?=" + past_pattern.pattern + ")", context, re.I,
    )]
    # The immediate ratio subject owns its effective predicate. An earlier
    # table's ratio label cannot span a later, separate legal ratio object.
    # Keep ADS + ratio-change together when their object ends are identical.
    past_effect = max(candidates, key=lambda candidate: (candidate.end("object"), -candidate.start()), default=None)
    past_statement = statement_for(past_effect) if past_effect else ""
    full_past_statement = statement_for(past_effect, trim_financial=False) if past_effect else ""
    past_dates = _operative_date_evidence(past_statement)
    past_start = text.find(past_statement, max(0, match.start() - len(context)), match.end() + len(context)) if past_statement else -1
    past_owns_ratio = past_start >= 0 and past_start <= match.start() and match.end() <= past_start + len(past_statement)
    legal_start = text.find(past_effect.group(), max(0, match.start() - len(context)), match.end() + len(context)) if past_effect else -1
    legal_clause_owns_ratio = bool(legal_start >= 0 and legal_start <= match.start()
                                  and match.end() <= legal_start + len(past_effect.group()))
    own_relative = match.start() - own_start if own_start >= 0 else -1
    own_introductions = list(re.finditer(
        rf"\b(?:Each\s+{_ADS}|The\s+Company|On\s+\w+\s+\d{{1,2}}|Effective\s+\w+\s+\d{{1,2}})\b",
        own[:max(0, own_relative)], re.I,
    ))
    own_narrative_ratio = bool(own_introductions and not _FINANCIAL_CONFIRMATION.search(
        own[own_introductions[-1].start():],
    ))
    linked_completed_date = bool(named_event and named_event.unique == effective and re.search(
        r"\bConcurrently\s+with\s+(?:the\s+)?(?:reverse\s+(?:share\s+|stock\s+)?split|Share\s+(?:Consolidation|Subdivision))\b",
        own, re.I,
    ))
    if (past_effect and same_event(past_statement, allow_financial_lead=legal_clause_owns_ratio)
            and not _CONFIRMATION_PREMISE.search(full_past_statement)
            and not _NEGATED_RATIO_EVENT.search(full_past_statement)
            and affirmative_statement(past_statement)
            and (not _FINANCIAL_CONFIRMATION.search(own) or past_owns_ratio or own_narrative_ratio
                 or legal_clause_owns_ratio)
            and (past_dates.unique == effective or (not past_dates.dates and linked_completed_date))):
        # Identity guards inspect the complete sentence; stored dated proofs
        # end at the operative date rather than swallowing an adjacent table.
        proof_start = max(0, past_statement.find(past_effect.group()))
        confirmation = confirmation or (past_statement[proof_start:max(end for _, end in past_dates.spans)] if past_dates.spans
                                        else named_event.proof + " " + past_statement[proof_start:])
        confirmed_conditions.add("ratio_effective")
    elif past_effect and not _BOILERPLATE_CONDITION.search(own):
        rejected_confirmation = True
    # Some meeting results report a completed ratio change as taking effect
    # concurrently on the same day as an already-dated share subdivision.
    # Preserve that literal same-day link; do not infer it from nearby dates.
    taken_effect = re.search(
        rf"\b{_RATIO_CHANGE_OBJECT}\b{_RATIO_OBJECT_TAIL}[^.;]{{0,450}}\bhas\s+taken\s+effect\b", own, re.I,
    )
    same_day_link = re.search(r"\bconcurrently\b[^.;]{0,80}\b(?:on\s+)?the\s+same\s+day\b", own, re.I)
    context_dates = _operative_date_evidence(context)
    if (taken_effect and same_day_link and same_event(own) and affirmative_statement(own)
            and (not named_event or actual_named_date())
            and context_dates.unique == effective):
        date_proof = " ".join(context[start:end] for start, end in context_dates.spans)
        confirmation = _clean(date_proof + " " + taken_effect.group())
        confirmed_conditions.add("ratio_effective")
    depositary = re.search(
        rf"\b(?:the\s+)?Depositary\s+(?:has\s+)?(?:announced|confirmed|notified)\s+"
        rf"(?:that\s+)?{_EVENT_DETERMINER}(?:{_RATIO_CHANGE_OBJECT}|{_RATIO_NOUN})\b"
        r"(?:\s+(?:of|for|to)\b[^.;]{0,120}?)?\s+(?:(?:is|was|will\s+be|became|has\s+become)\s+)?"
        r"effective\b[^.;]{0,100}", context, re.I,
    )
    depositary_statement = statement_for(depositary) if depositary else ""
    if (depositary and same_event(depositary_statement)
            and affirmative_statement(statement_for(depositary, trim_financial=False))
            and _operative_date_evidence(depositary_statement).unique == effective):
        confirmation = depositary_statement
        confirmed_conditions.add("depositary_notice")
    # ANTE's meeting result explicitly fulfils the named consolidation
    # approval. A proposed RESOLVED THAT or Board approval is not that result.
    if named_event and actual_named_date() and re.search(r"\bupon\s+(?:the\s+)?Share\s+Consolidation\b", own, re.I):
        for approval in re.finditer(
            r"\b(?:the\s+)?shareholders\s+(?:have\s+)?(?:"
            rf"(?:approved|adopted|passed)\s+(?:(?:the|a|an|proposed)\s+)*Share\s+Consolidation\b{_RATIO_OBJECT_TAIL}"
            r"|resolved\s+(?:by\s+ordinary\s+resolution\s+)?to\s+consolidate\s+"
            rf"(?:every\s+{_QUANTITY}\s+of\s+(?:the\s+)?(?:authori[sz]ed\s+(?:\([^)]{{0,60}}\)\s+)?)?shares"
            r"|(?:(?:the|issued|ordinary|common|outstanding)\s+)*shares)\b"
            r"(?:[^.;]|\.(?=\d)){0,700}?\bShare\s+Consolidation\b)[^.;]{0,80}",
            text[max(0, match.start() - 2500):match.end() + 2500], re.I,
        ):
            window = text[max(0, match.start() - 2500):match.end() + 2500]
            approval_sentence = statement_for(approval, source=window, trim_financial=False)
            if affirmative_statement(approval_sentence) and same_event(approval_sentence):
                confirmation = approval.group() + " " + named_event.proof + " " + own
                confirmed_conditions.update(("shareholder_approval", "consolidation"))
    # TCOM's meeting-result 6-K states that resolutions submitted for
    # shareholder approval were adopted, then links the same Share Subdivision
    # to the ADS event. The adoption is affirmative evidence of that named
    # prerequisite; the earlier proposal wording alone remains pending.
    if (named_event and actual_named_date() and named_event_name and named_event_name.casefold() == "share subdivision"
            and re.search(r"\bconcurrently\s+with\s+(?:the\s+)?effectiveness\s+of\s+(?:the\s+)?Share\s+Subdivision\b",
                          own, re.I)):
        window_start, window_end = max(0, match.start() - 2500), min(len(text), match.end() + 2500)
        window = text[window_start:window_end]
        adopted = re.search(
            r"\bproposed\s+resolutions?\s+submitted\s+for\s+shareholder\s+approval\b"
            r"(?:(?![.!?;](?!\d))[^\n]){0,700}?\b(?:has|have)\s+been\s+adopted\s+as\s+an?\s+ordinary\s+resolution\b"
            r"(?:(?![.!?;](?!\d))[^\n]){0,300}?:\s*Each\b"
            r"(?:[^.;]|\.(?=\d)){0,350}?\bordinary\s+shares\b"
            r"(?:[^.;]|\.(?=\d)){0,200}?\bbe\s+and\s+is\s+hereby\s+subdivided\s+into\b"
            r"(?:[^.;]|\.(?=\d)){0,300}?\bShare\s+Subdivision\b", window, re.I,
        )
        if (adopted and same_event(adopted.group())
                and affirmative_statement(statement_for(adopted, source=window, trim_financial=False))):
            confirmation = adopted.group() + " " + named_event.proof + " " + own
            confirmed_conditions.update(("shareholder_approval", "consolidation"))
    direct_approval = re.search(
        rf"\b(?:the\s+)?shareholders\s+(?:have\s+)?(?:approved|resolved|adopted|passed)\s+"
        rf"{_EVENT_DETERMINER}(?:proposed\s+)?{_RATIO_CHANGE_OBJECT}\b{_RATIO_OBJECT_TAIL}[^.;]{{0,350}}",
        context, re.I,
    )
    approval_statement = statement_for(direct_approval) if direct_approval else ""
    if (direct_approval and same_event(approval_statement)
            and affirmative_statement(statement_for(direct_approval, trim_financial=False)) and not re.search(
            r"\b(?:if|assuming|when|once|will|would|should|propos\w*)\b",
            context[max(0, direct_approval.start() - 40):direct_approval.start()], re.I)):
        confirmation = approval_statement
        confirmed_conditions.add("shareholder_approval")
    elif direct_approval:
        rejected_confirmation = True
    if (prospective_plan and not ({"ratio_effective", "depositary_notice"} & confirmed_conditions)
            and effective is not None and _operative_date_evidence(context).unique == effective):
        # “Plans to change” and “expected to take place/be effective” describe
        # a dated intention, not proof that the ratio actually changed. No
        # prerequisite is named, so keep it pending as unknown until a later
        # public completion or definitive Depositary confirmation.
        conditions.append(prospective_plan.group())
    outstanding = [(proof, condition_codes(proof) - confirmed_conditions) for proof in conditions]
    outstanding = [(proof, codes) for proof, codes in outstanding if codes]
    if outstanding:
        return {"ratio_effectiveness_pending": True,
                "ratio_effectiveness_pending_text": _clean(" ".join(dict.fromkeys(proof for proof, _ in outstanding))),
                "ratio_effectiveness_conditions": sorted({code for _, codes in outstanding for code in codes})}
    if not confirmation and (rejected_confirmation or named_event):
        return {"ratio_effectiveness_pending": True,
                "ratio_effectiveness_pending_text": _clean(own),
                "ratio_effectiveness_conditions": ["unknown_condition"]}
    if confirmation:
        return {"ratio_effectiveness_confirmed": True,
                "ratio_effectiveness_confirmation_text": _clean(confirmation),
                "ratio_effectiveness_confirmed_conditions": sorted(confirmed_conditions)}
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


def _operative_date_evidence(text: str) -> _DateEvidence:
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
    return _DateEvidence(tuple(sorted(dates)), text,
                         tuple(match.span() for match in matches))


def _effective_date(text: str) -> str | None:
    return _operative_date_evidence(text).unique


def _f6_amendment_scope(text: str) -> str | None:
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
    return body


def _f6_amendment_date(text: str) -> str | None:
    body = _f6_amendment_scope(text)
    return _effective_date(body) if body else None


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
            "ratio_effectiveness_pending_text": None, "operative_date_conflict": None,
            "operative_date_candidates": None, "operative_date_conflict_text": None,
            "ratio_effectiveness_confirmed": None, "ratio_effectiveness_confirmation_text": None,
            "ratio_effectiveness_conditions": None, "ratio_effectiveness_confirmed_conditions": None,
            **(metadata or {}),
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
        cover_ratio_matches = list(_ratios(ratio_text, unit_definition_text=text))
        for match, ratio in cover_ratio_matches:
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
                dates = _operative_date_evidence(_f6_ratio_date_scope(ratio_text, match, cover_ratio_matches))
                if dates.conflict:
                    evidence += " " + dates.proof
                    location += ";operative-date-conflict=" + ",".join(dates.dates)
                emit("ads_ratio", "cover_footnote", symbol, evidence, location, ratio=ratio, class_text=match.group(),
                     effective=dates.dates[0] if dates.dates else None, metadata=dates.metadata())
        # Item 12.D corroboration is separate from the primary F-6 stream.
        item_sections = [] if document_role == "securities_description" else _item_12d_sections(text)
        for item_offset, item_text in item_sections:
            item_ratio_matches = list(_ratios(item_text, unit_definition_text=text))
            for match, ratio in item_ratio_matches:
                if _superseded_ratio(item_text, match.start(), match.end()):
                    continue
                context = item_text[max(0, match.start() - 300):match.end() + 300]
                local_symbols = [s.upper() for s in _explicit_symbols(context)] or sorted(ads_symbols)
                dates = _operative_date_evidence(_f6_ratio_date_scope(item_text, match, item_ratio_matches))
                for symbol in local_symbols if len(local_symbols) == 1 else [None]:
                    location = f"item-12d;text-offset={item_offset + match.start()}"
                    if dates.conflict:
                        context += " " + dates.proof
                        location += ";operative-date-conflict=" + ",".join(dates.dates)
                    emit("ads_ratio", "item_12d", symbol, context, location, ratio=ratio, class_text=match.group(),
                         effective=dates.dates[0] if dates.dates else None, metadata=dates.metadata())
    elif base_form in ("F-6", "F-6EF", "F-6 POS"):
        explicit = [s.upper() for s in _explicit_symbols(text)]
        amendment_scope = _f6_amendment_scope(text)
        amendment_dates = _operative_date_evidence(amendment_scope) if amendment_scope else None
        amendment_date = amendment_dates.unique if amendment_dates else None
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
        interval_cache: dict[tuple[Fraction, str | None], tuple[str | None, str | None, str] | _DateEvidence | None] = {}
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
            if isinstance(interval, tuple) and interval[1] is not None and interval[1] <= public:
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
                date_scope = _f6_ratio_date_scope(text, match, ratio_matches)
                local_dates = _operative_date_evidence(date_scope)
                dates = (interval if isinstance(interval, _DateEvidence) else _DateEvidence((), "")
                         if isinstance(interval, tuple) else local_dates
                         if amendment_dates and amendment_dates.conflict and local_dates.dates
                         else amendment_dates or local_dates)
                effective = interval[0] if isinstance(interval, tuple) else dates.dates[0] if dates.dates else None
                until = interval[1] if isinstance(interval, tuple) else None
                if isinstance(interval, tuple):
                    location += ";operative-ratio-interval=" + ("prior" if until else "commencing")
                    if interval[2] not in context:
                        context += " " + interval[2]
                elif dates.conflict:
                    context += " " + dates.proof
                    location += ";operative-date-conflict=" + ",".join(dates.dates)
                effectiveness = _f6_pending_effectiveness(text, ratio, class_text)
                if effective and not dates.conflict and re.search(
                    r"\b(?:Depositary\s+(?:has\s+)?(?:announced|confirmed|notified)|(?:ratio|ADS(?:s)?)\b[^.;]{0,140}\b(?:was|became|has\s+become)\s+effective|(?:effected|implemented|completed)\b[^.;]{0,100}\bratio)\b",
                    date_scope, re.I,
                ):
                    affirmative = _6k_effectiveness_metadata(text, match, date_scope, date_scope, class_text, None, effective,
                                                            ratio=ratio, program_symbol=symbol)
                    fulfilled_notice = bool({"depositary_notice", "ratio_effective"}
                                            & set(affirmative.get("ratio_effectiveness_confirmed_conditions", [])))
                    if affirmative.get("ratio_effectiveness_confirmed") and not affirmative.get("ratio_effectiveness_pending") and (
                        not effectiveness.get("ratio_effectiveness_pending") or fulfilled_notice
                    ):
                        if fulfilled_notice:
                            effectiveness.pop("ratio_effectiveness_pending", None)
                            effectiveness.pop("ratio_effectiveness_pending_text", None)
                        effectiveness.update(affirmative)
                emit("ads_ratio", "f6", symbol, context, location, ratio=ratio,
                     effective=effective, until=until, class_text=class_text,
                     metadata={**effectiveness, **dates.metadata()})
    elif base_form == "6-K":
        ratio_matches = list(_ratios(text))
        for match, ratio in ratio_matches:
            scope = _6k_ratio_context(text, match.start(), match.end())
            if scope is None:
                continue
            context, own = scope
            narrative_completion = _6k_completed_event_dates(own)
            own_offset = text.find(own, max(0, match.start() - len(own)), match.end() + len(own))
            if narrative_completion.dates and own_offset >= 0 and not any(
                    start <= match.start() - own_offset and match.end() - own_offset <= end
                    for start, end in narrative_completion.spans):
                # Flattened financial tables can share a punctuation-delimited
                # scope with a later prose footnote. Their numeric ADS counts
                # are not the footnote's ratio transition or a pending event.
                continue
            # In 'from one ADS ... to one ADS ...', retain only the new side.
            dated_current = _6k_ratio_is_transition_target(text, match) or (re.search(r"\bcurrent\b[^.;]{0,60}\bratio\b[^.;]{0,30}$",
                                       text[max(0, match.start() - 120):match.start()], re.I)
                             and not _RATIO_FROM.search(context))
            if _prior_ratio(text, match.start()) and not dated_current:
                continue
            named_event = _6k_named_event_date(text, match, own)
            named_events = set(re.findall(r"\bShare\s+(?:Consolidation|Subdivision)\b", own, re.I))
            event_name = next(iter(named_events)) if named_event and len(named_events) == 1 else None
            if named_event is None:
                named_event = _6k_reverse_split_date(text, match, own, context)
                event_name = "Reverse Split"
            reverse_linked = bool(re.search(r"\bConcurrently\s+with\s+(?:the\s+)?reverse\s+(?:share\s+|stock\s+)?split\b", own, re.I))
            scope_dates = _operative_date_evidence(_f6_ratio_date_scope(text, match, ratio_matches))
            # An explicit effective date governs a separately dated completion
            # ("changed its ratio on July 31 ... became effective on August 5").
            # Only without one does the completed-event date supply the clock.
            own_dates = scope_dates if scope_dates.dates else _6k_completed_event_dates(own)
            dates = _merge_date_evidence(named_event, own_dates)
            if not dates.dates and not reverse_linked:
                dates = _operative_date_evidence(context)
            effective = dates.dates[0] if dates.dates else None
            if not effective:
                continue
            location = f"6k/ratio-change;text-offset={match.start()}"
            if named_event:
                context_start = text.find(context, max(0, match.start() - 1200), match.end() + 1400)
                if context_start >= 0 and named_event.spans:
                    context = text[min(context_start, min(start for start, _ in named_event.spans)):
                                   max(context_start + len(context), max(end for _, end in named_event.spans))]
                location += ";effective-date-named-event=" + event_name
                if named_event.spans:
                    location += ";named-event-date-text-offset=" + str(named_event.spans[0][0])
            if dates.conflict:
                context += " " + dates.proof
                location += ";operative-date-conflict=" + ",".join(dates.dates)
            if match.groupdict().get("unit", "").upper() == "CUFS":
                proofs = _cufs_ordinary_ratio_proofs(text, match, ratio)
                context_start = text.find(context, max(0, match.start() - 1200), match.end() + 1400)
                if proofs and context_start >= 0:
                    context = text[context_start:max(context_start + len(context), max(proof.end() for proof in proofs))]
                    location += ";ordinary-equivalence-text-offset=" + str(proofs[0].start())
            local_symbols = [s.upper() for s in _explicit_symbols(context)] or [s.upper() for s in _explicit_symbols(text)]
            for symbol in local_symbols if len(set(local_symbols)) == 1 else [None]:
                class_text = _6k_ratio_class(match, context)
                effectiveness = _6k_effectiveness_metadata(text, match, own, context, class_text, named_event,
                                                          effective, event_name, ratio=ratio,
                                                          program_symbol=symbol)
                if (_FINANCIAL_CONFIRMATION.search(own) and not effectiveness.get("ratio_effectiveness_confirmed")):
                    # Arithmetic assumptions and prospective financial-table
                    # clauses cannot enter SQL as unconditional announcements.
                    continue
                if (not dates.conflict and effectiveness.get("ratio_effectiveness_pending")
                        and effectiveness.get("ratio_effectiveness_conditions") == ["unknown_condition"]
                        and narrative_completion.dates and not re.search(
                            r"\b(?:plans?\s+to|expects?\s+to|expected\s+to|subject\s+to|conditional|contingent)\b",
                            own, re.I)):
                    # An unsupported table/former-side observation is not an
                    # announced transition. Do not manufacture a control that
                    # could override the independently stated completion.
                    continue
                if dates.conflict:
                    effectiveness.pop("ratio_effectiveness_confirmed", None)
                    effectiveness.pop("ratio_effectiveness_confirmation_text", None)
                    effectiveness.pop("ratio_effectiveness_confirmed_conditions", None)
                emit("ads_ratio", "ratio_change_6k", symbol, context, location, ratio=ratio, effective=effective,
                     class_text=class_text, metadata={**_6k_correction_metadata(text, context),
                                                    **effectiveness, **dates.metadata()})
    unique = {}
    for row in rows:
        key = tuple((key, str(value)) for key, value in row.items())
        unique[key] = row
    return list(unique.values())
