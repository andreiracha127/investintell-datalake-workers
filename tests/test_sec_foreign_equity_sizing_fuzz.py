"""Independent set-based admission oracle for synthetic foreign cover filings.

The independent Round 7 specification is deliberately narrow: ordinary proof
belongs to the count's own context; a nonordinary veto must concern that same
raw member or its own stock subject class. An unrelated Class B, a warrant's
target class, or another context's coordinated title cannot scope-veto a Class A
count. Count-owned coordination remains ambiguous. Differential equality alone
does not establish this specification; separate real-data golden floors do.

One legal class label cannot identify two ordinary stock lines in the issuer
source cohort: its latest complete cover and subsequent incomplete covers
through the count's source date. A newer complete cover resets that cohort.
Repeated contexts or ticker aliases of one raw class are not competing lines.
Named historical alias controls explicitly declare one logical stock line;
the reference does not reimplement W1's separate canonical-line election.
If the selected raw count already has its own observed stock line, the sole
legal-label carrier must also belong to that line. An unobserved count-only tag
can map to the sole registered stock; two observed lines cannot be conflated.
Count-date factors additionally require the historical elected class to equal
the selected count's class, even if the numerical ratio is unchanged. These
factor checks are separate from the current count's admission decision.

Benign par/nominal-value metadata is a finite suffix, never an identity. A
numeric percentage and finite rate description may precede an observation's
stock subject, but cannot turn another class or an instrument target into the
count's class. Neither extension accepts coordinated identities or residue.

The reference never calls SQL label/unit helpers, reads resolver bodies, or
imports their grammar. Python token lists and sets implement this contract.
Each case calls the public sizing API. COPY and rollback batches bound storage.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path

import pytest

from test_sec_foreign_equity_sizing import sql_database


SEED = 0xB2052026
ORACLE_VERSION = 13
ADS_MEMBER = "ClassOfStock=AmericanDepositaryShares;"
DAY = "2025-12-31"
ROMANS = {
    "i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6",
    "vii": "7", "viii": "8", "ix": "9", "x": "10", "xi": "11",
    "xii": "12", "xiii": "13", "xiv": "14", "xv": "15", "xvi": "16",
    "xvii": "17", "xviii": "18", "xix": "19", "xx": "20",
    "xxi": "21", "xxii": "22", "xxiii": "23", "xxiv": "24",
    "xxv": "25", "xxvi": "26", "xxvii": "27", "xxviii": "28",
    "xxix": "29", "xxx": "30", "xxxi": "31", "xxxii": "32",
    "xxxiii": "33", "xxxiv": "34", "xxxv": "35", "xxxvi": "36",
    "xxxvii": "37", "xxxviii": "38", "xxxix": "39",
}
DESCRIPTORS = {"ordinary", "common", "voting", "non-voting", "nonvoting", "subordinate", "multiple"}
COORDINATORS = {"and", "or", "&", ",", "/", ";"}
NONORDINARY_WORDS = {
    "preferred", "preference", "deferred", "founder", "founders", "debt",
    "note", "notes", "debenture", "debentures", "bond", "bonds", "option", "options",
    "warrant", "warrants", "unit", "units", "right", "rights", "unknown", "conflicting",
}
MEMBERS = (
    "ClassOfStock=ClassACommonShares;", "ClassOfStock=CommonClassA;",
    "ClassOfStock=ClassAOrdinaryShares;", "ClassOfStock=cLaSs A Ordinary Shares;",
    "ClassOfStock=ClassBCommonShares;", "ClassOfStock=ClassIIOrdinaryShares;",
    "ClassOfStock=ClassIIICommonShares;", "ClassOfStock=SeriesACommonStock;",
    "ClassOfStock=ClassA;", "ClassOfStock=OpaqueMember;",
    "ClassOfStock=CommonShares;", "ClassOfStock=OrdinaryShares;",
    "ClassOfStock=ClassAAndClassBCommonShares;",
    "ClassOfStock=Class A and B Common Shares;",
    "ClassOfStock=ClassIIAndIIIOrdinaryShares;",
    "ClassOfStock=ClassAPreferredShares;", ADS_MEMBER,
    "ClassOfStock=ClassACommonlyHeldShares;",
    "ClassOfStock=ClassACommONlyHeldShares;",
    "ClassOfStock=ClassAExtraordinaryShares;",
    "ClassOfStock=Class A cOmMoN Shares;",
    "ClassOfStock=AAndBCommonShares;", "ClassOfStock=A and B Common Shares;",
    "ClassOfStock=IIAndIIIOrdinaryShares;",
)
TITLES = (
    "Class A ordinary shares", "Class A common shares", "Class A shares",
    "Class B ordinary shares", "Series A ordinary shares", "Class II ordinary shares",
    "cLaSs ii ordinary shares", "cLaSs ii and III ordinary shares",
    "Class A and Series A ordinary shares", "Class A and B ordinary shares",
    "Series A and Series B ordinary shares", "A and B shares", "A and B ordinary shares",
    "Class A & B ordinary shares", "Class A, B ordinary shares",
    "Class A / B ordinary shares", "Class A or B ordinary shares",
    "Class A; B ordinary shares", "Class A ordinary shares outstanding",
    "Class A ordinary shares par value $0.01", "Class A mystery shares",
    "Common shares", "Ordinary shares", "Class ordinary shares", None,
    "Class A non-voting common shares", "Class A subordinate voting common stock",
    'Class "A" ordinary shares', "Class «A» ordinary shares",
    'Class "A ordinary shares', 'Class A" ordinary shares', "Class «A ordinary shares",
    "ClassACommonShares",
    "Class A common shares, par value of $0.01 per share",
    "Class A Common Stock, without par value",
    "Class A ordinary shares, nominal value of EUR .01 per share",
    "Class A ordinary shares, USD 0.01 par value",
    "Class A ordinary shares, no-par value",
    "Class A ordinary shares, par value of $0.01 and Class B shares",
    "Class A ordinary shares, without par value; Series A shares",
)


@dataclass(frozen=True)
class Observation:
    member: str
    dimh: str
    kind: str
    title: str | None


@dataclass(frozen=True)
class Registration:
    member: str
    title: str
    symbol_suffix: str = ""
    other_filing: bool = False
    canonical_line: str | None = None


@dataclass(frozen=True)
class Filing:
    member: str
    line: str
    observations: tuple[Observation, ...]
    generic_ads_context: bool = True
    registrations: tuple[Registration, ...] = ()
    historical_line: str | None = None
    current_complete: bool = True
    line_member: str | None = None


def canonical_id(token: str) -> str:
    lowered = token.lower().replace("-", "")
    return ROMANS.get(lowered, lowered)


def member_tokens(member: str) -> list[str]:
    # Dimension axis names are not identities. Deliberately tokenize member
    # values separately from titles; QName camel spelling is common in members.
    value = member.split("=", 1)[-1].rstrip(";").lower()
    vocabulary = (
        "depositary", "depository", "preferred", "preference", "ordinary", "common",
        "deferred", "founder", "american", "member", "series", "class", "shares",
        "share", "stock", "issuer", "and", "or", "ads", "adr",
    )
    # One lexical pass avoids rescanning the 'or' inside an already recognized
    # 'ordinary' token. This is a simple vocabulary lexer, not camel splitting.
    value = re.sub("|".join(vocabulary), lambda match: " " + match[0] + " ", value)
    return re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*|[&,/;]", value)


def member_identities(member: str) -> set[str]:
    tokens = member_tokens(member)
    result: set[str] = set()
    namespace = None
    wanted = False
    for token in tokens:
        if token in {"class", "series"}:
            namespace, wanted = token, True
        elif token in COORDINATORS:
            wanted = namespace is not None
        elif wanted:
            if token not in DESCRIPTORS | NONORDINARY_WORDS | {"shares", "share", "stock", "member"}:
                result.add(namespace + ":" + canonical_id(token))
            wanted = False
    return result


def _caption_identity(title: str | None, descriptors: set[str], terminals: set[str]) -> str | None:
    if not title:
        return None
    # Case-insensitive whitespace tokenisation cannot split cLaSs into letters.
    # Optional quotes surround the identifier only; no punctuation or residue
    # is removed from the rest of a source title.
    words = title.lower().split()
    if len(words) < 3 or words[0] not in {"class", "series"}:
        return None
    identifier = words[1]
    quote_pairs = {'"': '"', "'": "'", "“": "”", "‘": "’", "«": "»"}
    all_quotes = set(quote_pairs) | set(quote_pairs.values())
    if identifier[0] in quote_pairs:
        if len(identifier) < 3 or identifier[-1] != quote_pairs[identifier[0]]:
            return None
        identifier = identifier[1:-1]
    elif identifier[0] in all_quotes or identifier[-1] in all_quotes:
        return None
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identifier):
        return None
    if identifier in DESCRIPTORS | COORDINATORS | NONORDINARY_WORDS | {"class", "series", "shares", "share", "stock"}:
        return None
    if words[-1] not in terminals:
        return None
    if any(word not in descriptors for word in words[2:-1]):
        return None
    return words[0] + ":" + canonical_id(identifier)


def strict_title_identity(title: str | None) -> str | None:
    valid, label = own_title_scope(title)
    return label if valid else None


def benign_value_metadata(tail: str) -> bool:
    """Consume a finite metadata word sequence, independently of SQL regexes."""
    words = tail.lower().split()
    if words in (["no", "par", "value"], ["no-par", "value"], ["without", "par", "value"]):
        return True
    if words[-2:] == ["per", "share"]:
        words = words[:-2]
    if len(words) >= 3 and words[:2] in (["par", "value"], ["nominal", "value"]):
        words = words[2:]
        if words and words[0] == "of":
            words = words[1:]
    elif len(words) >= 3 and words[-2:] in (["par", "value"], ["nominal", "value"]):
        words = words[:-2]
    else:
        return False
    amount = " ".join(words)
    currencies = ("us$", "hk$", "nt$", "a$", "c$", "usd", "eur", "gbp", "cad", "aud", "hkd", "jpy", "chf", "$", "€", "£", "¥")
    for currency in currencies:
        if amount.startswith(currency):
            amount = amount[len(currency):].strip()
            break
    return re.fullmatch(r"(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)", amount) is not None


def own_title_scope(title: str | None) -> tuple[bool, str | None]:
    if not title:
        return True, None
    text = " ".join(title.lower().split())
    text = re.sub(r"\*{1,4}$", "", text).rstrip()
    if "," in text:
        text, tail = text.split(",", 1)
        tail = tail.strip()
        if not benign_value_metadata(tail):
            return False, None
    text = re.sub(r"^(class|series)(?=[a-z0-9])", r"\1 ", text.strip())
    label = _caption_identity(text, DESCRIPTORS, {"share", "shares", "stock"})
    if label is not None:
        return True, label
    words = text.split()
    generic = (len(words) >= 2 and words[0] in {"common", "ordinary"}
               and words[-1] in {"share", "shares", "stock"}
               and all(word in DESCRIPTORS for word in words[1:-1]))
    return generic, None


def strip_coupon_prefix(text: str) -> str:
    """A rate requires an explicit percent marker; an unmarked year is inert."""
    rate = re.match(r"^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)\s*(?:%|percent|per[\s-]+cent)\s+", text)
    if rate is None:
        return text
    remainder = text[rate.end():]
    rate_words = re.match(
        r"^(?:fixed[\s-]+to[\s-]+floating(?:[\s-]+rate)?|fixed[\s-]+rate|floating[\s-]+rate)[\s-]+",
        remainder,
    )
    return remainder[rate_words.end():] if rate_words else remainder


def stock_subjects(text: str | None, *, member: bool = False) -> list[tuple[set[str], str]]:
    if not text:
        return []
    if member:
        value = text.split("=", 1)[-1].rstrip(";")
    else:
        # Each clause has its own subject. Strip a rate only at that clause's
        # start; never search through an instrument's target wording.
        parts = re.split(r"(\b(?:and|or)\b|[&,/;])", text.lower())
        value = "".join(
            part if index % 2 else part[:len(part) - len(part.lstrip())] + strip_coupon_prefix(part.lstrip())
            for index, part in enumerate(parts)
        )
    tokens = member_tokens("=" + value) if member else re.findall(
        r'''"[^"\n]*"|'[^'\n]*'|“[^”\n]*”|‘[^’\n]*’|«[^»\n]*»|[a-z0-9]+(?:-[a-z0-9]+)*|[&,/;]|["'“”‘’«»]''',
        re.sub(r"\b(class|series)(?=[a-z0-9])", r"\1 ", value.lower()),
    )
    prefixes = {"class", "classes", "series"}
    stock_descriptors = DESCRIPTORS | {"preferred", "preference", "deferred", "founder", "founders", "issuer", "capital"}
    stop = DESCRIPTORS | NONORDINARY_WORDS | COORDINATORS | prefixes | {"share", "shares", "stock", "stocks", "member"}

    def identifier(index):
        if index >= len(tokens):
            return None
        token = tokens[index]
        pairs = {'"': '"', "'": "'", "“": "”", "‘": "’", "«": "»"}
        quotes = set(pairs) | set(pairs.values())
        if token[0] in pairs:
            if len(token) < 3 or token[-1] != pairs[token[0]]:
                return None
            token = token[1:-1].strip()
        elif token[0] in quotes or token[-1] in quotes or (index + 1 < len(tokens) and tokens[index + 1] in quotes):
            return None
        return token if token not in stop and re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", token) else None

    def start(index):
        while index < len(tokens) and tokens[index] in stock_descriptors:
            index += 1
        return index if index < len(tokens) and tokens[index] in prefixes else None

    first = start(0)
    if first is None:
        return []
    result = []
    cursor = first
    modifier_start = 0
    while cursor < len(tokens):
        namespace = "series" if tokens[cursor] == "series" else "class"
        cursor += 1
        first_id = identifier(cursor)
        if first_id is None:
            break
        keys = {namespace + ":" + canonical_id(first_id)}
        cursor += 1
        while cursor < len(tokens) and tokens[cursor] in COORDINATORS:
            following = cursor + 1
            while following < len(tokens) and tokens[following] in COORDINATORS:
                following += 1
            next_namespace = namespace
            if following < len(tokens) and tokens[following] in prefixes:
                next_namespace = "series" if tokens[following] == "series" else "class"
                following += 1
            next_id = identifier(following)
            if next_id is not None:
                keys.add(next_namespace + ":" + canonical_id(next_id))
                cursor = following + 1
            else:
                break
        body_start, next_subject, next_boundary = cursor, None, len(tokens)
        for index in range(cursor, len(tokens)):
            if tokens[index] in COORDINATORS:
                candidate = start(index + 1)
                if candidate is not None:
                    next_subject, next_boundary = candidate, index
                    break
        wording = " ".join(tokens[modifier_start:next_boundary])
        result.append((keys, wording))
        if member or next_subject is None:
            break
        modifier_start, cursor = next_boundary + 1, next_subject
    return result


def negative_observation_evidence(observation: Observation) -> tuple[set[str], bool]:
    keys = set().union(*(keys for keys, _ in stock_subjects(observation.member, member=True)))
    # Depositary captions describe underlying shares. Explicit member keys may
    # link a veto, but caption labels and coordinators never identify ADS units.
    if observation.kind == "depositary" or has_depositary(observation.member) or has_depositary(observation.title):
        return keys, False
    declared = set().union(*(keys for keys, _ in stock_subjects(observation.title)))
    return keys | declared, False


def negative_observation_keys(observation: Observation) -> set[str]:
    return negative_observation_evidence(observation)[0]


def has_depositary(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return "depositary" in lower or "depository" in lower or bool(
        re.search(r"(?<![a-z])(ads|adss|adr|adrs|gds|gdss|gdr|gdrs)(?![a-z])", lower)
    )


def has_nonordinary(text: str | None) -> bool:
    if not text:
        return False
    return bool(set(re.findall(r"[a-z]+", text.lower())) & NONORDINARY_WORDS)


def ordinary_wording(title: str | None) -> bool:
    return bool(title and set(re.findall(r"[a-z]+", title.lower())) & {"common", "ordinary"})


def ordinary_member_hint(member: str) -> bool:
    # Positive unit words need their own lexical proof. The finite vocabulary
    # used for canonical identities must never turn Commonly into Common.
    # This independent camel word scanner does not call the frozen W1 helper.
    value = member.split("=", 1)[-1].rstrip(";")
    raw_words = set(re.findall(r"[a-z0-9_]+", value.lower()))
    camel_words = {word.lower() for word in re.findall(
        r"[A-Z]?[a-z]+[0-9]*|[A-Z]+(?![a-z])[0-9]*|[0-9]+", value,
    )}
    return bool((raw_words | camel_words) & {"common", "ordinary"})


def count_member_coordinated(member: str) -> bool:
    value = member.split("=", 1)[-1].rstrip(";")
    raw = re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*|[&,/;]", value.lower())
    camel = [word.lower() for word in re.findall(
        r"[A-Z]?[a-z]+[0-9]*|[A-Z]+(?![a-z])[0-9]*|[0-9]+|[&,/;]", value,
    )]
    excluded = DESCRIPTORS | NONORDINARY_WORDS | COORDINATORS | {
        "class", "classes", "series", "share", "shares", "stock", "stocks", "member", "of", "the",
    }
    for tokens in (raw, camel):
        for index, token in enumerate(tokens):
            if token not in COORDINATORS or index == 0:
                continue
            following = index + 1
            if following < len(tokens) and tokens[following] in {"class", "classes", "series"}:
                following += 1
            if following < len(tokens) and tokens[index - 1] not in excluded and tokens[following] not in excluded:
                return True
    return False


def stock_carriers(filing: Filing, label: str) -> set[str]:
    """Positive stock lines in an explicitly declared source cohort.

    A registration caption is source evidence; a count-only member is not a
    second carrier. Symbols and contexts do not multiply one raw stock class.
    """
    observations = [(o.member, o) for o in filing.observations] + [
        (r.canonical_line or r.member, Observation(r.member, "registration", "equity", r.title))
        for r in filing.registrations if not r.other_filing or not filing.current_complete
    ]
    carriers = set()
    for canonical_line, observation in observations:
        if observation.kind not in {"equity", "unknown"} or not observation.title:
            continue
        caption = observation.title.lower()
        if not re.search(r"\b(?:shares?|stock)\b", caption):
            continue
        if has_depositary(caption) or has_depositary(observation.member):
            continue
        if has_nonordinary(caption) or has_nonordinary(" ".join(member_tokens(observation.member))):
            continue
        subjects = set().union(*(keys for keys, _ in stock_subjects(caption)))
        raw_identities = member_identities(observation.member)
        if len(subjects) > 1 or len(raw_identities) > 1 or count_member_coordinated(observation.member):
            continue
        # A legal caption identifies the stock before a raw QName spelling;
        # a singleton member label supplies only a generic-caption fallback.
        generic_valid, generic_label = own_title_scope(caption)
        identities = subjects or (raw_identities if generic_valid and generic_label is None else set())
        if identities == {label}:
            carriers.add(canonical_line)
    return carriers


def reference_decision(filing: Filing) -> str:
    """Brute-force positive set, contradictory set and strict scope set.

    An optional generic ADS observation remains unlinked to an ordinary count
    but suppresses spelling-only proof, just as in the real 75 refused rows.
    """
    observations = filing.observations + tuple(
        Observation(r.member, "registration", "equity", r.title)
        for r in filing.registrations if not r.other_filing
    )
    own = [o for o in observations if o.member == filing.member and o.dimh == "cover"]
    identities = member_identities(filing.member)
    title_labels = [strict_title_identity(o.title) for o in own if o.title and o.title.strip()]
    positive_ids = identities | {label for label in title_labels if label is not None}
    linked = [o for o in observations
              if o.member == filing.member or positive_ids & negative_observation_keys(o)]
    own_ordinary = any(o.kind == "equity" and ordinary_wording(o.title) for o in own)
    ads = has_depositary(filing.member) or any(
        o.kind == "depositary" or has_depositary(o.title) or has_depositary(o.member) for o in linked
    )
    own_ads = has_depositary(filing.member) or any(
        o.kind == "depositary" or has_depositary(o.title) or has_depositary(o.member) for o in own
    )
    other = has_nonordinary(" ".join(member_tokens(filing.member))) or any(
        o.kind not in {"equity", "depositary"}
        or ((o.kind == "depositary" or has_depositary(o.member) or has_depositary(o.title))
            and has_nonordinary(o.title))
        or has_nonordinary(" ".join(member_tokens(o.member)))
        or any(positive_ids & keys and has_nonordinary(wording) for keys, wording in stock_subjects(o.title))
        # Once the member or its stock class links this observation, an
        # unanchored caption's contradictory unit still belongs to that source.
        # The target in a warrant caption never establishes the link itself.
        or (not stock_subjects(o.title) and has_nonordinary(o.title))
        for o in linked
    )
    filing_ads = filing.generic_ads_context or has_depositary(filing.member) or any(
        o.kind == "depositary" or has_depositary(o.title) or has_depositary(o.member)
        for o in observations
    )
    ordinary = own_ordinary or (
        ordinary_member_hint(filing.member)
        and not filing_ads and not ads and not other
    )
    if other or (ads and ordinary) or (not ads and not ordinary):
        return "share_count_unit_unverified"
    if ads:
        # Negative related-context evidence blocks ordinary supply but cannot
        # positively establish the elected count's depositary units.
        return "ordinary_class_shares_unavailable" if own_ads else "share_count_unit_unverified"
    own_scopes = [own_title_scope(o.title) for o in own if o.title and o.title.strip()]
    if count_member_coordinated(filing.member) or any(not valid for valid, _ in own_scopes):
        return "foreign_listing_class_ambiguous"
    if any(label is None for _, label in own_scopes) and len(identities) != 1:
        return "foreign_listing_class_ambiguous"
    identities.update(label for valid, label in own_scopes if valid and label is not None)
    if len(identities) != 1:
        return "foreign_listing_class_ambiguous"
    count_label = next(iter(identities))
    carriers = stock_carriers(filing, count_label)
    count_line_established = (
        filing.line_member is None
        or any(o.member == filing.member for o in observations)
    )
    if len(carriers) > 1 or (carriers and count_line_established and filing.member not in carriers):
        return "foreign_listing_class_ambiguous"
    if identities != {filing.line}:
        return "foreign_listing_class_mismatch"
    return "resolved"


def reference_count_ratio(filing: Filing) -> tuple[int | None, int | None]:
    """Equal numbers cannot make a historical different-class entitlement usable."""
    if reference_decision(filing) != "resolved":
        return None, None
    historical_class = filing.historical_line or filing.line
    return (5, 1) if historical_class == filing.line else (None, None)


def make_cases(size: int, seed: int = SEED) -> list[Filing]:
    rng = random.Random(seed)
    a = MEMBERS[0]
    valid = "Class A ordinary shares"
    cases = [
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),)),
        Filing(a, "class:b", (Observation(a, "cover", "equity", valid),)),
        Filing(a, "class:a", (Observation(a, "other", "equity", valid),)),
        Filing(a, "class:a", (Observation(a, "other", "preferred", "Class A preferred shares"),)),
        Filing(a, "class:a", (Observation(a, "other", "preferred", "Class A preferred shares"),), False),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(a, "other", "preferred", "Class A preferred shares")), False),
        Filing(a, "class:a", (Observation(a, "cover", "depositary", "Class A American Depositary Shares"),)),
        Filing(a, "class:a", (Observation(a, "other", "depositary", "Class A American Depositary Shares"),)),
        Filing(a, "class:a", (Observation(a, "cover", "equity", "A and B ordinary shares"),)),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(a, "other", "equity", "Class B ordinary shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassesOfShareCapital=CommonClassA;", "other", "equity", "Class B ordinary shares"))),
        Filing("ClassOfStock=OpaqueMember;", "class:a", (
            Observation("ClassOfStock=OpaqueMember;", "cover", "equity", valid),
            Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class B ordinary shares"))),
        Filing("ClassOfStock=OpaqueMember;", "class:a", (
            Observation("ClassOfStock=OpaqueMember;", "cover", "equity", valid),
            Observation("ClassOfStock=ClassA;", "other", "preferred", "Class A preferred shares"))),
        Filing("ClassOfStock=CommonShares;", "class:a", (
            Observation("ClassOfStock=CommonShares;", "cover", "equity", valid),
            Observation("ClassOfStock=ClassA;", "other", "unknown", "Class A ordinary shares"))),
        Filing("ClassOfStock=OpaqueMember;", "class:a", (
            Observation("ClassOfStock=OpaqueMember;", "cover", "equity", valid),
            Observation("ClassOfStock=ClassB;", "other", "preferred", "Class B preferred shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "preferred", "Class A preferred shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "unknown", "Class A ordinary shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "preferred", "Class B preferred shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "depositary", valid))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(ADS_MEMBER, "other", "depositary", "Class A American Depositary Shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=ClassB;", "other", "preferred", "Class A preferred shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class A notes"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(a, "other", "equity", "Class A and B ordinary shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(a, "other", "equity", "A and B ordinary shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class A and B ordinary shares"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=ClassB;", "other", "equity", valid))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "preferred", "Class A preferred shares, par value $0.01"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class A debentures"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation(a, "other", "equity", "Class A ordinary shares, par value $0.01"))),
        Filing("ClassOfStock=ClassACommonlyHeldShares;", "class:a", (
            Observation("ClassOfStock=ClassACommonlyHeldShares;", "cover", "equity", "Class A shares"),), False),
        Filing("ClassOfStock=ClassACommONlyHeldShares;", "class:a", (
            Observation("ClassOfStock=ClassACommONlyHeldShares;", "cover", "equity", "Class A shares"),), False),
        Filing("ClassOfStock=ClassAExtraordinaryShares;", "class:a", (
            Observation("ClassOfStock=ClassAExtraordinaryShares;", "cover", "equity", "Class A shares"),), False),
        Filing(a, "class:a", (Observation(a, "cover", "equity", "Class A shares"),), False),
        Filing("ClassOfStock=Class A cOmMoN Shares;", "class:a", (
            Observation("ClassOfStock=Class A cOmMoN Shares;", "cover", "equity", "Class A shares"),), False),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class A bonds"))),
        Filing(a, "class:a", (Observation(a, "cover", "equity", valid),
                              Observation("ClassOfStock=OpaqueMember;", "other", "equity", "Class A options"))),
    ]
    cases.extend(case for case, _ in narrow_acceptance_cases())
    cases.extend(case for case, _ in round7_acceptance_cases())
    while len(cases) < size:
        member = rng.choice(MEMBERS)
        observations = []
        if rng.randrange(8) != 0:
            observations.append(Observation(member, rng.choice(("cover", "cover", "other")),
                                            rng.choice(("equity", "equity", "equity", "depositary", "preferred", "unknown")),
                                            rng.choice(TITLES)))
        for _ in range(rng.randrange(3)):
            relation = rng.randrange(4)
            related_member = member if relation == 0 else rng.choice(MEMBERS)
            if relation == 1 and member_identities(member) == {"class:a"}:
                related_member = "ClassesOfShareCapital=ClassAOrdinaryShares;"
            observations.append(Observation(
                related_member, rng.choice(("cover", "other", "third")),
                rng.choice(("equity", "depositary", "preferred", "unknown", "debt", "warrant", "unit", "right")),
                rng.choice(TITLES + ("Class A preferred shares", "Class A deferred shares", "Class A founder shares",
                                     "Class A unknown shares", "Class A conflicting shares", "Class A American Depositary Shares",
                                     "8.250% Series A Preferred Stock", ".50 percent Class A preferred shares",
                                     "8.25% Fixed-to-Floating Rate Class B preferred shares",
                                     "2019 Series A Preferred Stock", "Warrants to purchase 8.25% Class A shares")),
            ))
        line = rng.choice(("class:a", "class:a", "class:b", "class:2", "series:a"))
        registrations = ()
        if rng.randrange(5) == 0:
            registration_label = rng.choice((line, "class:a", "class:b", "series:a"))
            namespace, identifier = registration_label.split(":")
            registrations = (Registration(
                rng.choice((member, "ClassOfStock=OtherStockCarrier;")),
                f"{namespace.title()} {identifier.upper()} ordinary shares",
                rng.choice(("", "ALT")), bool(rng.randrange(4) == 0),
            ),)
        historical_line = rng.choice((line, "class:a", "class:b", "series:a")) if rng.randrange(4) == 0 else None
        cases.append(Filing(member, line, tuple(observations), bool(rng.randrange(2)),
                            registrations, historical_line))
    return cases[:size]


def narrow_acceptance_cases() -> list[tuple[Filing, str]]:
    a, b = MEMBERS[0], "ClassOfStock=ClassBCommonShares;"
    own_a = Observation(a, "cover", "equity", "Class A ordinary shares")
    own_b = Observation(b, "cover", "equity", "Class B ordinary shares")
    opaque = "ClassOfStock=OpaqueNegative;"
    mixed = "Class A common shares, Class B preferred shares"
    return [
        (Filing(a, "class:a", (own_a, Observation(b, "other", "preferred", "Class B preferred shares"))), "resolved"),
        (Filing(a, "class:a", (own_a, Observation(b, "other", "unknown", "Class B ordinary shares"))), "resolved"),
        (Filing(a, "class:a", (own_a, Observation(opaque, "other", "equity", "Class A and B ordinary shares"))), "resolved"),
        (Filing(a, "class:a", (own_a, Observation(opaque, "other", "equity", mixed))), "resolved"),
        (Filing(b, "class:b", (own_b, Observation(opaque, "other", "equity", mixed))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation(opaque, "other", "preferred", "Class A and B preferred shares"))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation(opaque, "other", "equity", "Preferred Class A shares"))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation("ClassOfStock=WarrantsToPurchaseClassA;", "other", "warrant", "Warrants to purchase Class A ordinary shares"))), "resolved"),
        (Filing(b, "class:b", (own_b, Observation("ClassOfStock=CommonClassAWarrantsToPurchaseClassB;", "other", "warrant", "Warrants to purchase Class B ordinary shares"))), "resolved"),
        (Filing(a, "class:a", (Observation(b, "other", "preferred", "Class B preferred shares"),), False), "resolved"),
        (Filing(a, "class:a", (own_a, Observation("ClassOfStock=ClassAPreferredShares;", "other", "equity", "Class A ordinary shares"))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation("ClassOfStock=ClassAPreferredShares;", "other", "equity", None))), "share_count_unit_unverified"),
        (Filing(b, "class:b", (own_b, Observation("ClassOfStock=ClassAAndClassBCommonShares;", "other", "unknown", None))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation("ClassOfStock=ClassIIOrdinaryShares;", "other", "preferred", 'Class "A ordinary shares'))), "resolved"),
        (Filing("ClassOfStock=AmericanDepositaryShares;", "class:a", (
            Observation("ClassOfStock=AmericanDepositaryShares;", "cover", "depositary", "Class A unknown shares"),)), "share_count_unit_unverified"),
        (Filing("ClassOfStock=AmericanDepositaryShares;", "class:a", (
            Observation("ClassOfStock=AmericanDepositaryShares;", "other", "equity", "Class A founder shares"),)), "share_count_unit_unverified"),
        *[(Filing(member, "class:a", (Observation(member, "cover", "equity", "Class A ordinary shares"),)),
           "foreign_listing_class_ambiguous") for member in (
               "ClassOfStock=AAndBCommonShares;", "ClassOfStock=A and B Common Shares;",
               "ClassOfStock=IIAndIIIOrdinaryShares;",
           )],
    ]


def round7_acceptance_cases() -> list[tuple[Filing, str]]:
    """Independent named controls for the four review patterns and their limits."""
    a = MEMBERS[0]
    own_a = Observation(a, "cover", "equity", "Class A ordinary shares")
    tracking = "ClassOfStock=TrackingGroupOne;"
    other_tracking = "ClassOfStock=TrackingGroupTwo;"
    own_tracking = Observation(tracking, "cover", "equity", "Series A ordinary shares")
    series_b = "ClassOfStock=SeriesBCommonStock;"
    own_b = Observation(series_b, "cover", "equity", "Series B ordinary shares")
    opaque = "ClassOfStock=OpaquePreferred;"
    cases = [
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(other_tracking, "Series A ordinary shares", "OTHER"),)), "foreign_listing_class_ambiguous"),
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(other_tracking, "Series A ordinary shares"),)), "foreign_listing_class_ambiguous"),
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(tracking, "Series A ordinary shares", "ALT"),)), "resolved"),
        (Filing(tracking, "series:a", (own_tracking, Observation(tracking, "other", "equity", "Series A ordinary shares"))), "resolved"),
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(other_tracking, "Series B ordinary shares", "OTHER"),)), "resolved"),
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(other_tracking, "Series A ordinary shares", "OLD", True),)), "resolved"),
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(tracking, "Series A ordinary shares", "", True),
            Registration(other_tracking, "Series A ordinary shares", "OTHER", True),
        ), current_complete=False), "foreign_listing_class_ambiguous"),
        # A current complete cover ends the prior two-line cohort.
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration(tracking, "Series A ordinary shares", "", True),
            Registration(other_tracking, "Series A ordinary shares", "OTHER", True),
        )), "resolved"),
        # The old and renamed raw members explicitly describe one logical
        # stock line in this fixture; no alias engine is copied into Python.
        (Filing(tracking, "series:a", (own_tracking,), registrations=(
            Registration("ClassOfStock=StandardClassA;", "Series A ordinary shares", "", True, tracking),
        ), generic_ads_context=False, current_complete=False), "resolved"),
        # Scope proof precedes binding: a duplicate Series A is ambiguous
        # even when the current foreign listing asks for a different class.
        (Filing(tracking, "class:b", (own_tracking,), registrations=(
            Registration(other_tracking, "Series A ordinary shares", "OTHER"),
        )), "foreign_listing_class_ambiguous"),
        # One count-only QName and one actual stock caption are a mapping,
        # rather than two positively evidenced coexisting stock classes.
        (Filing(a, "class:a", (), False, (Registration("ClassOfStock=RegisteredStock;", "Class A ordinary shares"),),
                line_member="ClassOfStock=RegisteredStock;"), "resolved"),
        # The same registration cannot validate a separately observed count
        # line merely because both raw members can be called Class A.
        (Filing(a, "class:a", (), False, (Registration("ClassOfStock=RegisteredStock;", "Class A ordinary shares"),)), "foreign_listing_class_ambiguous"),
        (Filing(a, "class:b", (), False, (Registration("ClassOfStock=RegisteredStock;", "Class A ordinary shares", "ALT"),)), "foreign_listing_class_ambiguous"),
        (Filing(a, "class:a", (own_a,), historical_line="class:b"), "resolved"),
        (Filing(a, "class:a", (own_a,), historical_line="class:a"), "resolved"),
    ]
    accepted_metadata = (
        "par value of $0.01 per share", "without par value", "no-par value",
        "nominal value of EUR .01 per share", "USD 0.01 par value",
        "nominal value HK$0.01", "par value NT$10", "par value £0.01",
        "nominal value CHF 0.01", "par value of JPY 1 per share",
    )
    for metadata in accepted_metadata:
        cases.append((Filing(a, "class:a", (Observation(a, "cover", "equity", f"Class A ordinary shares, {metadata}"),)), "resolved"))
    for metadata in (
        "par value of $0.01 and Class B shares", "without par value; Series A shares",
        "par value of mystery $0.01", "par value 0.01 USD", "no par value Class B",
    ):
        cases.append((Filing(a, "class:a", (Observation(a, "cover", "equity", f"Class A ordinary shares, {metadata}"),)), "foreign_listing_class_ambiguous"))
    for prefix in ("8.250%", ".50 percent", "8 per cent", "8.25% Fixed-to-Floating Rate", "8.25% Fixed Rate", "8.25% floating-rate"):
        cases.append((Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "preferred", f"{prefix} Series B Preferred Stock"))), "share_count_unit_unverified"))
        cases.append((Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "preferred", f"{prefix} Series C Preferred Stock"))), "resolved"))
    cases.extend([
        (Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "preferred", "2019 Series B Preferred Stock"))), "resolved"),
        (Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "preferred", "8.25 Series B Preferred Stock"))), "resolved"),
        (Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "warrant", "Warrants to purchase 8.25% Series B ordinary shares"))), "resolved"),
        (Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "equity", "Series C ordinary shares, 8.250% Series B Preferred Stock"))), "share_count_unit_unverified"),
        (Filing(series_b, "series:b", (own_b, Observation(opaque, "other", "equity", "Series B ordinary shares, 8.250% Series C Preferred Stock"))), "share_count_unit_unverified"),
        (Filing(series_b, "series:b", (own_b, Observation("ClassOfStock=OpaqueOther;", "other", "equity", "Series B ordinary shares, 8.250% Series C Preferred Stock"))), "resolved"),
        (Filing(a, "class:a", (own_a, Observation("ClassesOfShareCapital=ClassAOrdinaryShares;", "other", "equity", "Warrants to purchase 8.25% Class A shares"))), "share_count_unit_unverified"),
        (Filing(a, "class:a", (own_a, Observation("ClassOfStock=ClassACommonlyHeldShares;", "other", "equity", "2019 Series A Preferred Stock"))), "share_count_unit_unverified"),
    ])
    return cases


def _fact_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _insert_batch(conn, cases: list[Filing], start: int) -> list[dict]:
    observations, counts, foreign, requests = [], [], [], []
    for local, case in enumerate(cases):
        number = start + local
        cik, symbol = 30_000_000 + number, "FZ" + str(number)
        adsh = f"{cik:010d}-25-{number:06d}"
        base = (_fact_hash(f"count:{number}"), adsh, cik, "cover", case.member, case.member)
        count_form = "20-F" if case.current_complete else "6-K"
        counts.append(base + ("2024-12-31", "2024-12-31", 100_000, count_form, "2025-03-01", "2025-03-02", "2026-10-10", "fuzz"))
        # This registration anchor makes the member callable without supplying
        # an own-context title or ordinary-unit proof to a cover count.
        all_observations = case.observations + (
            Observation(case.line_member or case.member, "registration-anchor", "equity", None),
        ) + (
            (Observation(ADS_MEMBER, "ads-line", "depositary", "American Depositary Shares"),)
            if case.generic_ads_context else ()
        )
        for j, o in enumerate(all_observations):
            observations.append((_fact_hash(f"observation:{number}:{j}"), adsh, cik, o.dimh, o.member, o.member,
                                 symbol, symbol, o.title, "New York Stock Exchange", o.kind, 2, case.current_complete, count_form,
                                 "2025-03-01", "2025-03-02", "2026-10-10", "fuzz"))
        for j, registration in enumerate(case.registrations):
            registration_adsh = f"{cik:010d}-24-{number:06d}" if registration.other_filing else adsh
            filed, available = ("2024-03-01", "2024-03-02") if registration.other_filing else ("2025-03-01", "2025-03-02")
            registered_symbol = symbol + registration.symbol_suffix
            observations.append((_fact_hash(f"registration:{number}:{j}"), registration_adsh, cik,
                                 f"registration-{j}", registration.member, registration.member,
                                 registered_symbol, registered_symbol, registration.title, "New York Stock Exchange",
                                 "equity", 2, True if registration.other_filing else case.current_complete,
                                 "20-F" if registration.other_filing else count_form,
                                 filed, available, "2026-10-10", "fuzz"))
        contracts = [(case.line, "2020-01-01", "2020-01-02", None, adsh)]
        if case.historical_line is not None:
            contracts = [
                (case.historical_line, "2020-01-01", "2020-01-02", "2025-06-01", f"{cik:010d}-20-{number:06d}"),
                (case.line, "2025-05-31", "2025-06-01", None, adsh),
            ]
        for contract_number, (contract_class, filed, effective, until, source_adsh) in enumerate(contracts):
            for j, source in enumerate(("cover_12b", "f6", "item_12d")):
                listed = j == 0
                foreign.append((_fact_hash(f"foreign:{number}:{contract_number}:{j}"), cik, symbol,
                                contract_class.replace(":", "_"), source_adsh, "20-F", filed,
                                "https://www.sec.gov/Archives/fuzz", "a" * 64, source,
                                "listed_type" if listed else "ads_ratio", "ads" if listed else None, True,
                                None if listed else 5, None if listed else 1, effective, until, False,
                                "Synthetic differential oracle", "test", "fuzz-v1", effective, "2026-10-10", "fuzz"))
        requests.append({"number": number, "cik": cik, "symbol": symbol, "members": [case.line_member or case.member]})
    for table, columns, rows in (
        ("sec_cover_share_counts", "fact_hash,adsh,cik,dimh,segments,class_key,stated_on,ddate_rounded,shares,form,filed,available_on,loaded_on,source_package", counts),
        ("sec_ticker_cik_observations", "fact_hash,adsh,cik,dimh,segments,class_key,ticker,ticker_raw,security_title,exchange,security_kind,filing_equity_classes,filing_complete,form,filed,available_on,loaded_on,source_package", observations),
        ("sec_foreign_listing_evidence", "fact_hash,cik,symbol,underlying_class,adsh,form,filed,source_url,source_sha256,source_kind,evidence_kind,listed_type,ordinary_candidate,ratio_numerator,ratio_denominator,effective_from,effective_to,effective_date_explicit,evidence_text,evidence_location,parser_version,available_on,loaded_on,source_package", foreign),
    ):
        with conn.cursor().copy(f"COPY public.{table} ({columns}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
    return requests


def case_coverage(cases: list[Filing]) -> dict:
    all_observations = [o for case in cases for o in case.observations]
    positive_keys = [member_identities(case.member) | {
        label for o in case.observations
        if o.member == case.member and o.dimh == "cover"
        and (label := strict_title_identity(o.title)) is not None
    } for case in cases]
    return {
        "count_member_spellings": len({case.member for case in cases}),
        "count_source_titles": len({o.title for o in all_observations}),
        "observation_kinds": sorted({o.kind for o in all_observations}),
        "observation_contexts": sorted({o.dimh for o in all_observations}),
        "line_classes": sorted({case.line for case in cases}),
        "with_generic_ads_context": sum(case.generic_ads_context for case in cases),
        "without_generic_ads_context": sum(not case.generic_ads_context for case in cases),
        "same_member_observations": sum(o.member == case.member for case in cases for o in case.observations),
        "different_member_same_class_observations": sum(
            o.member != case.member and bool(negative_observation_keys(o) & keys)
            for case, keys in zip(cases, positive_keys, strict=True) for o in case.observations
        ),
        "unlinked_member_observations": sum(
            o.member != case.member and not (negative_observation_keys(o) & keys)
            for case, keys in zip(cases, positive_keys, strict=True) for o in case.observations
        ),
        "competing_stock_carrier_cases": sum(len(stock_carriers(case, case.line)) > 1 for case in cases),
        "same_ratio_historical_class_changes": sum(case.historical_line is not None and case.historical_line != case.line for case in cases),
        "extra_registrations": sum(len(case.registrations) for case in cases),
        "other_filing_registrations": sum(r.other_filing for case in cases for r in case.registrations),
        "incomplete_count_filings": sum(not case.current_complete for case in cases),
        "declared_historical_aliases": sum(r.canonical_line is not None for case in cases for r in case.registrations),
        "count_only_registered_member_mappings": sum(case.line_member is not None and case.line_member != case.member for case in cases),
        "coupon_caption_observations": sum(strip_coupon_prefix((o.title or "").lower()) != (o.title or "").lower() for o in all_observations),
    }


def compare_cases(conn, cases: list[Filing], *, batch_size: int = 64) -> dict:
    disagreements, expected_counts, actual_counts = [], Counter(), Counter()
    expected_ratio_counts, actual_ratio_counts = Counter(), Counter()
    disagreement_count = 0
    for start in range(0, len(cases), batch_size):
        batch = cases[start:start + batch_size]
        conn.execute("BEGIN")
        conn.execute("SET LOCAL jit = off")
        conn.execute("SET LOCAL statement_timeout = '60s'")
        try:
            requests = _insert_batch(conn, batch, start)
            rows = conn.execute(
                "SELECT q.number,s.status,s.refusal,s.share_unit,s.evidence,s.count_ratio_numerator,s.count_ratio_denominator "
                "FROM jsonb_to_recordset(%s::jsonb) AS q(number integer,cik bigint,symbol text,members text[]) "
                "CROSS JOIN LATERAL public.sec_cover_ticker_size_basis_at(q.symbol,q.cik,q.members,%s::date) s "
                "ORDER BY q.number", (json.dumps(requests), DAY),
            ).fetchall()
            assert len(rows) == len(batch), "Public sizing API must return exactly one row per request"
            for number, status, refusal, unit, evidence, count_numerator, count_denominator in rows:
                case = cases[number]
                expected = reference_decision(case)
                actual = "resolved" if status == "resolved" else refusal.split(":", 1)[0]
                expected_ratio = reference_count_ratio(case)
                actual_ratio = count_numerator, count_denominator
                expected_counts[expected] += 1
                actual_counts[actual] += 1
                if expected == "resolved":
                    expected_ratio_counts[str(expected_ratio)] += 1
                if actual == "resolved":
                    actual_ratio_counts[str(tuple(int(value) if value is not None else None for value in actual_ratio))] += 1
                historical_mismatch = expected == "resolved" and case.historical_line is not None and case.historical_line != case.line
                historical_audit_valid = not historical_mismatch or (
                    evidence["count_class_binding_status"] == "mismatch"
                    and (evidence["count_ratio_refusal"] or "").startswith("foreign_listing_class_mismatch:")
                )
                ratio_disagrees = expected == actual == "resolved" and expected_ratio != actual_ratio
                if expected != actual or ratio_disagrees or not historical_audit_valid:
                    disagreement_count += 1
                    if len(disagreements) < 30:
                        disagreements.append({"number": number, "expected": expected, "actual": actual,
                                              "unit": unit, "expected_count_ratio": expected_ratio,
                                              "actual_count_ratio": actual_ratio,
                                              "historical_audit_valid": historical_audit_valid,
                                              "filing": asdict(case), "audit": evidence})
        finally:
            conn.execute("ROLLBACK")
    input_json = json.dumps([asdict(case) for case in cases], sort_keys=True, separators=(",", ":"))
    return {"oracle_version": ORACLE_VERSION, "seed": SEED, "cases": len(cases), "input_sha256": hashlib.sha256(input_json.encode()).hexdigest(),
            "expected_decision_counts": dict(sorted(expected_counts.items())),
            "actual_decision_counts": dict(sorted(actual_counts.items())),
            "expected_count_ratio_counts": dict(sorted(expected_ratio_counts.items())),
            "actual_count_ratio_counts": dict(sorted(actual_ratio_counts.items())),
            "count_ratio_comparison_scope": "current_resolved_rows",
            "coverage": case_coverage(cases), "distinct_decision_classes": len(expected_counts), "disagreements": disagreement_count,
            "examples": disagreements}


def test_seeded_full_sizing_admission_matches_independent_set_oracle(sql_database):
    size = int(os.environ.get("B2_SIZING_FUZZ_CASES", "384"))
    assert 384 <= size <= 20_000
    result = compare_cases(sql_database, make_cases(size))
    report = os.environ.get("B2_SIZING_FUZZ_REPORT")
    if report:
        Path(report).write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8", newline="\n")
    assert result["distinct_decision_classes"] == 5
    concise = [{"number": e["number"], "expected": e["expected"], "actual": e["actual"],
                "member": e["filing"]["member"], "line": e["filing"]["line"],
                "observations": e["filing"]["observations"]} for e in result["examples"][:5]]
    assert result["disagreements"] == 0, json.dumps(
        {"disagreements": result["disagreements"], "first_five": concise}, indent=2, default=str,
    )


def test_narrow_specification_explicit_acceptance_controls():
    for case, expected in narrow_acceptance_cases():
        assert reference_decision(case) == expected, (case, expected)


def test_round7_specification_explicit_review_controls():
    for case, expected in round7_acceptance_cases():
        assert reference_decision(case) == expected, (case, expected)
        if expected == "resolved":
            assert reference_count_ratio(case) == ((None, None) if case.historical_line not in {None, case.line} else (5, 1))
