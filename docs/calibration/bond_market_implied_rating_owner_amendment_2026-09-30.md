# Market-implied rating — owner amendment, 2026-09-30

**Status: authorized local code preparation only; calibration reproduction unexecuted; not a PASS, publication or rollout approval.** This is a new append-only decision record. Historical failed results and the [original declaration](bond_market_implied_rating_round_declaration.md) are not rewritten.

## Authority and identities

Source: the direct owner response in the DSH session on 2026-09-30, recorded in [owner decisions](https://github.com/andreiracha127/investintell-light/blob/feat/bond-default-review-market-el/docs/planning/bond-default-el-owner-decisions-2026-09-30.md). The isolated Workers source baseline is `b8b9503d25739b9a68a2e93f69b322e69a78cbc8`. No commit, push, PR, deploy, publication, schema installation, production query/write, credentials or tunnel operation is authorized by this amendment.

| Identity | Value |
| --- | --- |
| Product | `bond_market_implied_rating_v1` |
| Policy version | `bond_market_implied_rating_policy_v1` |
| Historical unpinned policy digest | `28f70b9bd8f617fedf6104deb86cd518d43307e88b3bbd1fcf53aa1fae869a3b` |
| New local fixed-authoritative policy digest | `4b752a3fc5d5222b398f1e2a073b7c428b068e77968f8fb79edd20957202a08a` |
| Canonical `market_level.anchor.kind` | `fixed_authoritative` |
| Canonical decimal-string `market_level.anchor.l_anchor` | `-0.8864114120812487` |
| Frozen diagnostic window | 2023-09 through 2026-08, 36 months |
| Owner-reported existing production publication | `a8b9a3d2-03a9-5c82-bf21-f9d1b9298803`, not independently verified here |

The new policy identity requires a later, separately authorized republication. Publication IDs bind the fixed value through the canonical policy digest, not through an inherited-anchor suffix determined by diagnostic drift. Historical artifact-loader contracts, identities and source pins are unchanged; a frozen historical artifact is not automatically valid under this new policy.

## Fixed anchor is the source of authority

The selected anchor is exactly `-0.8864114120812487`. The recomputed median of observed chained market levels in the frozen window is a diagnostic only. Data corrections, including filling previously dark July/August months, may change that diagnostic without `anchor_not_reproduced` or `anchor_drift` invalidating this fixed policy.

This does not fabricate market information:

- A closed history with no genuine finite observed market level still refuses as `no_market_level_observation`.
- If observations exist elsewhere but the frozen window is empty, the fixed pin remains usable, with `resolved_l_anchor=null` and `anchor_diagnostic_reason=anchor_window_empty`.
- Nonfinite/malformed selected, policy, diagnostic or inherited pins refuse as `invalid_anchor`.
- A caller cannot override the official anchor, even with a nearby finite value. The worker always chooses the policy value; a foreign current publication pin outside the absolute `1e-9` compatibility tolerance refuses as `anchor_policy_mismatch`. Compatible inherited float roundoff never replaces the official value.
- The unpinned historical policy path, exercised only with explicit synthetic test policies, retains inherited-anchor identity and median-drift checks. This is not an operator option to bypass the new fixed policy.

No witness thresholds, cuts, hysteresis, source-exit, D confirmation, cure or carry parameters are retuned. Synthetic comparisons verify unchanged buckets/scores when the old measured anchor equals the official pin; they do not establish equality of the full production rows digest, which also includes the intentionally changed policy digest.

## Economic source and blocking readings

Primary expected loss is PD from market-implied transitions to D times the declared analytical model LGD `0.60`. Legal defaults adjudicated by the sole owner are auxiliary/protective evidence, not the primary PD. Neither market prices nor market recovery proxies are realized legal-default recovery. Missing or stale auxiliary event publication alone does not invalidate otherwise valid market EL. No agency vendor/licence, dual reviewer or held-out document custody is required.

| Reading | Owner-authorized treatment |
| --- | --- |
| G1 determinism | Blocking; executed reproduction still required |
| G2–G4 static-rating agreement | Descriptive, not blocking |
| G5/G6 default discrimination/false-D | Partial readings; do not invent sample-size/recall thresholds |
| G7 migration | No additional blocking authority is inferred |
| G8 / revised DG-3 | Typed per-issue exclusions plus the systemic guard below |
| G9 positive control/default capacity | Blocking; executed evidence still required |
| G10 anchor reproduction | Blocking calibration evidence, not a per-build requirement that the corrected diagnostic median match the official pin |

DG-3 excludes D from new-buy candidates and excludes WITHDRAWN/NOT_RATED for insufficient recent evidence. Run evidence must record reason, identifier and face amount. Existing holdings are flagged for review, never silently removed or sold. More than 5% excluded by count **or** face value blocks the entire solve as systemic/data quality; equality at 5% does not cross this guard. After exclusions, a remaining required bucket without EL still blocks; no zero fallback. These are Light registry decisions, not added Workers rating-policy parameters.

## Calendar and frozen-export amendment

Economic T is always the elected internal-rating pointer/header `last_month`. The intended first live T is September 2026 **only after** confirming that September closed with witnessed volume; do not hardcode August or use an open month/browser date. The owner's expected daily run is 2026-10-01 at 07:30 UTC; this document does not run or verify it.

The calibration measurement/export remains limited to months through 2026-08. A later panel head is allowed by this amendment only with an export constrained to `month <= 2026-08-01` and evidence tying that export to the historical production inputs. G1 must reproduce the historical rows digest of the owner-reported `a8b9a3d2…` publication using its historical source/policy identity, and G10 must reproduce its anchor. The full historical rows digest must be captured from authorized, executed evidence; it is intentionally not guessed here. A replay stamped with the new policy digest is a new identity and cannot pretend to have the same full historical digest.

| Required evidence | Status |
| --- | --- |
| Bounded export and historical source/input identity | Not executed here |
| G1 historical rows-digest reproduction | Not executed here |
| G9 positive control/default capacity | Not executed here |
| G10 reproduction of the historical anchor | Not executed here |
| September closed witnessed volume and current header | Not verified here |
| Owner acceptance of executed round | Not claimed |
| Production republication/Light activation | Not authorized |

The original failed rounds remain failures. This amendment authorizes only the scoped local source/test change and records the owner's revised decision contract; synthetic green tests cannot substitute for calibration or live evidence.

## Separate Git publication authorization — 2026-10-01

The owner subsequently requested committing this implementation and opening review PRs. That authorizes publication of the prepared source branches only. It does not authorize merge, schema installation, Stage 7 republication, deployment, pointer changes or economic activation, and it does not revise the historical failed-round evidence.
