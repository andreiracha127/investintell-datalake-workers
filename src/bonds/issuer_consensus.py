"""Issuer attribution by REPORTED-NAME consensus (pure, DB-free, fail-closed).

Why this module exists (measured on the live cohort 2026-08-07)
--------------------------------------------------------------
``bond_security_v1`` published an issuer name for 384 of the 10,073 curated
securities.  The other 9,689 were not unnamed: every one of them carried
reported issuer names, but the security master compared them as EXACT STRINGS,
so a single security reported by many funds as ``AFLAC INC`` / ``Aflac, Inc.`` /
``AFLAC INCORPORATED`` / ``AFLAC INC 3.6% 04/01/30`` looked like conflicting
evidence, the name was nulled, and — worse — the whole IDENTITY was flagged
``ambiguous``.  9,688 of 9,689 gaps had ``identity_reason_code =
'conflicting_issuer_evidence'``; exactly ONE was a real identity collision.

Reported spelling variance is REPORTING NOISE, not competing evidence about
which instrument this is.  This module resolves it the honest way: normalise,
collapse truncations, vote, and ABSTAIN when the vote is genuinely split or the
reported legal entities (LEIs) actually disagree.

The layered, fail-closed rule (pre-registered before the build; do not tune)
---------------------------------------------------------------------------
1. **Layer 1 — exact CUSIP9.**  The votes are the reported issuer names of the
   holding lots that reference THIS security's own CUSIP9.  Same CUSIP9 means
   the same instrument, hence the same legal issuer, so consensus here is a
   spelling decision and nothing more.
2. **Layer 2 — CUSIP6 fallback, ONLY when layer 1 abstains.**  The votes widen
   to every holding lot in the same 6-character issuer prefix.  This INFERS
   across securities, so it carries two extra guards: the vehicle-name guard
   (below) and the same LEI validation.
3. **Abstain (issuer stays NULL) when** — any of:
   * more than one distinct reported LEI in the vote scope (``multiple_lei``);
   * no single name cluster reaches ``CONSENSUS_THRESHOLD`` (``no_consensus``);
   * layer 2 only: the winning name looks like a financing vehicle / trust
     (``vehicle_name_at_cusip6``) — an SPV name does not generalise across a
     CUSIP6 block the way an operating company's does;
   * there are no named holdings at all (``no_named_source``).
4. **Never** substitute a parent, sponsor or guarantor for the legal issuer: the
   winner is always one of the REPORTED names, never a synthesised or
   externally-supplied one.

Normalisation strips what filers vary (case, punctuation, corporate/instrument
suffix tokens) and what they append (the coupon and the maturity, e.g. ``AFLAC
INC 3.6% 04/01/30``).  Truncations are then folded by PREFIX CONTAINMENT: a
filer that reports ``AMC ENTERTAINMEN`` where another reports ``AMC
ENTERTAINMENT HOLDINGS`` wrote the same string through a narrower field, not a
different claim.  The attributed value is the modal RAW spelling inside the
winning cluster, so the served name is one a filer actually wrote.

Every verdict — attributed or abstained — carries its evidence (layer,
candidates with vote counts, winning share, LEIs seen, abstain reason) so the
attribution is auditable from the published row alone.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

# Pre-registered before the build (measured band 0.50-0.60 left as declared
# upside, deliberately NOT tuned post hoc).
CONSENSUS_THRESHOLD = 0.60

ATTRIBUTION_CUSIP9 = "cusip9_consensus"
ATTRIBUTION_CUSIP6 = "cusip6_consensus"

ABSTAIN_NO_NAMED_SOURCE = "no_named_source"
ABSTAIN_MULTIPLE_LEI = "multiple_lei"
ABSTAIN_NO_CONSENSUS = "no_consensus"
ABSTAIN_VEHICLE_NAME = "vehicle_name_at_cusip6"

# Corporate/instrument tokens filers add or drop freely. Stripping them is a
# spelling normalisation, never an identity decision: they are removed only for
# COMPARISON, and the attributed value is always a raw reported spelling.
_SUFFIX_TOKENS = (
    "INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY", "COMPANIES",
    "LTD", "LIMITED", "PLC", "LLC", "LLP", "LP", "NV", "BV", "SA", "SAS", "AG",
    "GMBH", "AB", "ASA", "OYJ", "SPA", "PTE", "PTY", "THE", "AND", "OF", "SR",
    "JR", "UNSECURED", "SECURED", "SENIOR", "SUBORDINATED", "SUB", "NOTES",
    "NOTE", "BOND", "BONDS", "DEBENTURE", "DEBENTURES", "144A", "REGS", "MTN",
    "GLBL", "FRN", "CALLABLE", "DUE",
)

# Tokens that mark a financing vehicle rather than an operating company. Used
# ONLY to veto a layer-2 (CUSIP6) inference: at the exact CUSIP9 grain an SPV
# name IS the issuer of that bond and must be published.
_VEHICLE_TOKENS = frozenset({
    "TRUST", "FUNDING", "SPV", "SPC", "CLO", "CDO", "SECURITIZATION",
    "RECEIVABLES", "ISSUER", "VEHICLE", "MASTER",
})

_COUPON_RE = re.compile(r"[0-9]+(?:\.[0-9]+)?\s*%")
_DATE_RE = re.compile(r"[0-9]{1,4}[/-][0-9]{1,4}(?:[/-][0-9]{2,4})?")
_NON_ALNUM_RE = re.compile(r"[^A-Z0-9 ]")
_WS_RE = re.compile(r"\s+")
_SUFFIX_RE = re.compile(r"\b(?:%s)\b" % "|".join(_SUFFIX_TOKENS))


def normalize_issuer_name(raw: object) -> str:
    """Comparison key for one reported issuer name (never a published value).

    Upper-cases, drops an appended coupon (``3.6%``) and maturity/date token,
    reduces punctuation to spaces and removes corporate/instrument suffix
    tokens. A name made ENTIRELY of suffix tokens falls back to its
    punctuation-stripped form rather than collapsing to an empty key.
    """
    if raw is None:
        return ""
    text = str(raw).strip().upper()
    if not text:
        return ""
    text = _COUPON_RE.sub(" ", text)
    text = _DATE_RE.sub(" ", text)
    text = _NON_ALNUM_RE.sub(" ", text)
    punct = _WS_RE.sub(" ", text).strip()
    core = _WS_RE.sub(" ", _SUFFIX_RE.sub(" ", punct)).strip()
    return core or punct


def collapse_prefix_clusters(names: Iterable[str]) -> dict[str, str]:
    """Map each normalised name to the LONGEST name it is a whitespace prefix of.

    A truncated filing (``AMC ENTERTAINMEN``) folds into its complete sibling
    (``AMC ENTERTAINMENT HOLDINGS``); a name with no superstring maps to itself.
    Deterministic: ties break on the longest, then lexicographically smallest.
    """
    unique = sorted({name for name in names if name})
    canon: dict[str, str] = {}
    for name in unique:
        supersets = [other for other in unique
                     if other != name and other.startswith(name + " ")]
        if supersets:
            canon[name] = min(supersets, key=lambda value: (-len(value), value))
        else:
            canon[name] = name
    return canon


@dataclass(frozen=True)
class IssuerVerdict:
    """The attribution decision for one security, with its full evidence."""

    issuer_name: str | None
    attribution: str | None
    abstain_reason: str | None
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def attributed(self) -> bool:
        return self.issuer_name is not None


def _empty_verdict(reason: str, *, layer: str, leis: Sequence[str]) -> IssuerVerdict:
    return IssuerVerdict(
        issuer_name=None, attribution=None, abstain_reason=reason,
        evidence={
            "layer": layer, "abstain_reason": reason, "candidates": [],
            "n_sources": 0, "top_share": None, "distinct_lei": list(leis),
        },
    )


def resolve_issuer(
    votes: Mapping[str, int],
    *,
    layer: str,
    leis: Iterable[str] = (),
    threshold: float = CONSENSUS_THRESHOLD,
) -> IssuerVerdict:
    """Decide one issuer name from weighted reported spellings (fail-closed).

    ``votes`` maps a RAW reported name to how many holding lots reported it.
    ``layer`` is ``cusip9_consensus`` or ``cusip6_consensus``; the CUSIP6 layer
    additionally vetoes a financing-vehicle winner. ``leis`` are the distinct
    reported LEIs in the same vote scope — more than one means the reported legal
    entities genuinely disagree and the verdict abstains.
    """
    lei_list = sorted({str(value).strip() for value in leis if str(value).strip()})
    positive = {name: int(count) for name, count in votes.items()
                if str(name).strip() and int(count) > 0}
    if not positive:
        return _empty_verdict(ABSTAIN_NO_NAMED_SOURCE, layer=layer, leis=lei_list)
    if len(lei_list) > 1:
        verdict = _empty_verdict(ABSTAIN_MULTIPLE_LEI, layer=layer, leis=lei_list)
        return IssuerVerdict(
            None, None, ABSTAIN_MULTIPLE_LEI,
            {**verdict.evidence, "n_sources": sum(positive.values())},
        )

    # raw -> normalised -> prefix-collapsed cluster key.
    normalised = {raw: normalize_issuer_name(raw) for raw in positive}
    canon = collapse_prefix_clusters(normalised.values())
    cluster_votes: Counter[str] = Counter()
    raw_votes_by_cluster: dict[str, Counter[str]] = {}
    for raw, count in positive.items():
        key = canon.get(normalised[raw], normalised[raw])
        cluster_votes[key] += count
        raw_votes_by_cluster.setdefault(key, Counter())[raw] += count

    total = sum(cluster_votes.values())
    # Deterministic winner: most votes, then the lexicographically smallest key.
    winner, top_votes = min(
        cluster_votes.items(), key=lambda item: (-item[1], item[0])
    )
    top_share = top_votes / total
    candidates = [
        {"name": key, "votes": count}
        for key, count in sorted(cluster_votes.items(), key=lambda i: (-i[1], i[0]))
    ]
    evidence: dict[str, Any] = {
        "layer": layer,
        "candidates": candidates,
        "clusters": len(cluster_votes),
        "n_sources": total,
        "top_share": round(top_share, 4),
        "distinct_lei": lei_list,
        "threshold": threshold,
    }

    if len(cluster_votes) > 1 and top_share < threshold:
        return IssuerVerdict(None, None, ABSTAIN_NO_CONSENSUS,
                            {**evidence, "abstain_reason": ABSTAIN_NO_CONSENSUS})

    if layer == ATTRIBUTION_CUSIP6 and _looks_like_vehicle(winner):
        return IssuerVerdict(None, None, ABSTAIN_VEHICLE_NAME,
                            {**evidence, "abstain_reason": ABSTAIN_VEHICLE_NAME})

    # Publish a spelling a filer actually wrote, choosing among the winning
    # cluster's RAW names. Two filters, in order:
    #   1. keep only names whose OWN normalised form IS the cluster key, which
    #      drops the truncations that merely folded into it (``AMC ENTERTAINMEN``
    #      never gets served when ``AMC ENTERTAINMENT HOLDINGS`` was reported);
    #   2. then most votes, then the SHORTEST raw string, then lexicographic —
    #      shortest is what discards the filers who append the coupon and the
    #      maturity (``AFLAC INC 3.6% 04/01/30``) while keeping ``AFLAC INC``.
    # Deterministic at every step, so a replay rebuilds the same publication.
    raw_counts = raw_votes_by_cluster[winner]
    complete = {raw: count for raw, count in raw_counts.items()
                if normalised[raw] == winner} or raw_counts
    chosen = min(complete.items(), key=lambda item: (-item[1], len(item[0]), item[0]))[0]
    return IssuerVerdict(
        issuer_name=chosen, attribution=layer, abstain_reason=None,
        evidence={**evidence, "chosen_cluster": winner, "abstain_reason": None},
    )


def _looks_like_vehicle(normalized_name: str) -> bool:
    return bool(_VEHICLE_TOKENS.intersection(normalized_name.split()))


__all__ = [
    "ABSTAIN_MULTIPLE_LEI",
    "ABSTAIN_NO_CONSENSUS",
    "ABSTAIN_NO_NAMED_SOURCE",
    "ABSTAIN_VEHICLE_NAME",
    "ATTRIBUTION_CUSIP6",
    "ATTRIBUTION_CUSIP9",
    "CONSENSUS_THRESHOLD",
    "IssuerVerdict",
    "collapse_prefix_clusters",
    "normalize_issuer_name",
    "resolve_issuer",
]
