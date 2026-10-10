"""Independent set-based admission oracle for synthetic foreign cover filings.

The independent Round 6 specification is deliberately narrow: ordinary proof
belongs to the count's own context; a nonordinary veto must concern that same
raw member or its own stock subject class. An unrelated Class B, a warrant's
target class, or another context's coordinated title cannot scope-veto a Class A
count. Count-owned coordination remains ambiguous. Differential equality alone
does not establish this specification; separate real-data golden floors do.

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
ORACLE_VERSION = 10
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
)


@dataclass(frozen=True)
class Observation:
    member: str
    dimh: str
    kind: str
    title: str | None


@dataclass(frozen=True)
class Filing:
    member: str
    line: str
    observations: tuple[Observation, ...]
    generic_ads_context: bool = True


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


def own_title_scope(title: str | None) -> tuple[bool, str | None]:
    if not title:
        return True, None
    text = " ".join(title.lower().split())
    text = re.sub(r"\*{1,4}$", "", text).rstrip()
    if "," in text:
        text, tail = text.split(",", 1)
        tail = tail.strip()
        if tail != "no par value" and not re.fullmatch(
            r"(?:par|nominal) value\s+(?:us\$|\$)?\s*[0-9]+(?:\.[0-9]+)?(?:\s+(?:per share|pershare))?", tail,
        ):
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


def stock_subjects(text: str | None, *, member: bool = False) -> list[tuple[set[str], str]]:
    if not text:
        return []
    value = text.split("=", 1)[-1].rstrip(";") if member else text
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


def reference_decision(filing: Filing) -> str:
    """Brute-force positive set, contradictory set and strict scope set.

    An optional generic ADS observation remains unlinked to an ordinary count
    but suppresses spelling-only proof, just as in the real 75 refused rows.
    """
    own = [o for o in filing.observations if o.member == filing.member and o.dimh == "cover"]
    identities = member_identities(filing.member)
    title_labels = [strict_title_identity(o.title) for o in own if o.title and o.title.strip()]
    positive_ids = identities | {label for label in title_labels if label is not None}
    linked = [o for o in filing.observations
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
        or ((o.member == filing.member or any(
            positive_ids & keys for keys, _ in stock_subjects(o.member, member=True)
        )) and has_nonordinary(" ".join(member_tokens(o.member))))
        or any(positive_ids & keys and has_nonordinary(wording) for keys, wording in stock_subjects(o.title))
        or (o.member == filing.member and not stock_subjects(o.title) and has_nonordinary(o.title))
        for o in linked
    )
    filing_ads = filing.generic_ads_context or has_depositary(filing.member) or any(
        o.kind == "depositary" or has_depositary(o.title) or has_depositary(o.member)
        for o in filing.observations
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
    if identities != {filing.line}:
        return "foreign_listing_class_mismatch"
    return "resolved"


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
                                     "Class A unknown shares", "Class A conflicting shares", "Class A American Depositary Shares")),
            ))
        cases.append(Filing(member, rng.choice(("class:a", "class:a", "class:b", "class:2", "series:a")),
                            tuple(observations), bool(rng.randrange(2))))
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


def _fact_hash(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _insert_batch(conn, cases: list[Filing], start: int) -> list[dict]:
    observations, counts, foreign, requests = [], [], [], []
    for local, case in enumerate(cases):
        number = start + local
        cik, symbol = 30_000_000 + number, "FZ" + str(number)
        adsh = f"{cik:010d}-25-{number:06d}"
        base = (_fact_hash(f"count:{number}"), adsh, cik, "cover", case.member, case.member)
        counts.append(base + ("2024-12-31", "2024-12-31", 100_000, "20-F", "2025-03-01", "2025-03-02", "2026-10-10", "fuzz"))
        # This registration anchor makes the member callable without supplying
        # an own-context title or ordinary-unit proof to a cover count.
        all_observations = case.observations + (
            Observation(case.member, "registration-anchor", "equity", None),
        ) + (
            (Observation(ADS_MEMBER, "ads-line", "depositary", "American Depositary Shares"),)
            if case.generic_ads_context else ()
        )
        for j, o in enumerate(all_observations):
            observations.append((_fact_hash(f"observation:{number}:{j}"), adsh, cik, o.dimh, o.member, o.member,
                                 symbol, symbol, o.title, "New York Stock Exchange", o.kind, 2, True, "20-F",
                                 "2025-03-01", "2025-03-02", "2026-10-10", "fuzz"))
        class_token = case.line.replace(":", "_")
        for j, source in enumerate(("cover_12b", "f6", "item_12d")):
            listed = j == 0
            foreign.append((_fact_hash(f"foreign:{number}:{j}"), cik, symbol, class_token, adsh, "20-F", "2020-01-01",
                            "https://www.sec.gov/Archives/fuzz", "a" * 64, source,
                            "listed_type" if listed else "ads_ratio", "ads" if listed else None, True,
                            None if listed else 5, None if listed else 1, "2020-01-02", False,
                            "Synthetic differential oracle", "test", "fuzz-v1", "2020-01-02", "2026-10-10", "fuzz"))
        requests.append({"number": number, "cik": cik, "symbol": symbol, "members": [case.member]})
    for table, columns, rows in (
        ("sec_cover_share_counts", "fact_hash,adsh,cik,dimh,segments,class_key,stated_on,ddate_rounded,shares,form,filed,available_on,loaded_on,source_package", counts),
        ("sec_ticker_cik_observations", "fact_hash,adsh,cik,dimh,segments,class_key,ticker,ticker_raw,security_title,exchange,security_kind,filing_equity_classes,filing_complete,form,filed,available_on,loaded_on,source_package", observations),
        ("sec_foreign_listing_evidence", "fact_hash,cik,symbol,underlying_class,adsh,form,filed,source_url,source_sha256,source_kind,evidence_kind,listed_type,ordinary_candidate,ratio_numerator,ratio_denominator,effective_from,effective_date_explicit,evidence_text,evidence_location,parser_version,available_on,loaded_on,source_package", foreign),
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
    }


def compare_cases(conn, cases: list[Filing], *, batch_size: int = 64) -> dict:
    disagreements, expected_counts, actual_counts = [], Counter(), Counter()
    disagreement_count = 0
    for start in range(0, len(cases), batch_size):
        batch = cases[start:start + batch_size]
        conn.execute("BEGIN")
        conn.execute("SET LOCAL jit = off")
        conn.execute("SET LOCAL statement_timeout = '60s'")
        try:
            requests = _insert_batch(conn, batch, start)
            rows = conn.execute(
                "SELECT q.number,s.status,s.refusal,s.share_unit,s.evidence "
                "FROM jsonb_to_recordset(%s::jsonb) AS q(number integer,cik bigint,symbol text,members text[]) "
                "CROSS JOIN LATERAL public.sec_cover_ticker_size_basis_at(q.symbol,q.cik,q.members,%s::date) s "
                "ORDER BY q.number", (json.dumps(requests), DAY),
            ).fetchall()
            assert len(rows) == len(batch), "Public sizing API must return exactly one row per request"
            for number, status, refusal, unit, evidence in rows:
                case = cases[number]
                expected = reference_decision(case)
                actual = "resolved" if status == "resolved" else refusal.split(":", 1)[0]
                expected_counts[expected] += 1
                actual_counts[actual] += 1
                if expected != actual:
                    disagreement_count += 1
                    if len(disagreements) < 30:
                        disagreements.append({"number": number, "expected": expected, "actual": actual,
                                              "unit": unit, "filing": asdict(case), "audit": evidence})
        finally:
            conn.execute("ROLLBACK")
    input_json = json.dumps([asdict(case) for case in cases], sort_keys=True, separators=(",", ":"))
    return {"oracle_version": ORACLE_VERSION, "seed": SEED, "cases": len(cases), "input_sha256": hashlib.sha256(input_json.encode()).hexdigest(),
            "expected_decision_counts": dict(sorted(expected_counts.items())),
            "actual_decision_counts": dict(sorted(actual_counts.items())),
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
