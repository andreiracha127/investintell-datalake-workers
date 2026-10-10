# W1c B1 round 2: final7 confirmation detector validation

This report supersedes [final6](sec-foreign-listing-final6.md) after the
read-only gate blocked PR #181 at `546904287e05c6ca081e8dd520ebc3f44fbae97c`.
It records the detector correction, the new offline artifact and the local
PG18 validation. Production was not accessed. Final6 remains immutable.

## Detector corrections and regression evidence

Parser version: `foreign-listing-v11`, SHA-256
`05aaa42f47560f34d9b25f9152a5d0cf9624e7bc96e58b1d1f6d16216e1f36bf`.
Implementation commits: `12228b8bebc0993b7ebd74201b9e466bd3e955d9` and
`c3f65887b7b58244059f1abb51b5241698e96917` and
`deb77fbe33f3dfa1d1b9789838bee1e96a2f6b68`
on [PR #181](https://github.com/andreiracha127/investintell-datalake-workers/pull/181).
The unchanged schema SHA-256 is
`f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957`.
SQL settlement still requires a later accession, publication by D, and matching
program, exact class and target ratio. Round 2 changes no schema or database
apply code. The runbook remains unchanged because the stacked schema v2 work
owns it; this document records the final7 pins.

| Gate finding and correction | Regression |
|---|---|
| Qualifier checks retain the full owning sentence before proof trimming. Assumed/assuming, as if/as though, purposes clauses, retrospective adjustment and had-been-effective statements cannot supply legal completion. | `test_b1_round2_full_accounting_sentence_cannot_confirm`; `test_b1_round2_legal_completion_and_separate_eps_sentence_still_confirm` preserves a separate genuine legal sentence followed by an EPS sentence. |
| Amendments and adjustments act on the ratio itself. Announcements, estimates, notices, filings and agreements fail the object check; future/modal owning clauses cannot confirm. | `test_b1_round2_adjustment_requires_ratio_object_and_completed_predicate`, with completion and adjustment adversaries in the frozen matrix. |
| A numberless confirmation cannot bind to the target when its sentence names the former, previous, prior, old, existing or current-before-change side. Explicit numbers still require exact target identity. | `test_b1_round2_numberless_former_ratio_cannot_confirm_target`, plus matrix former-side and other class/program cases. |
| Class A is extracted from ADSs representing Class A ordinary shares, so Baidu's reciprocal target retains its actual class. | `test_b1_round2_baidu_representing_class_a_keeps_completed_reciprocal_ratio`; original accession `0000950123-10-067501` source check. |
| The unambiguous “for every N” ratio grammar works in both directions, preserving NetEase's completed 25/1 to 5/1 change. | `test_b1_round2_for_every_ratio_directions_keep_only_completed_target`; both original NetEase sources checked. |
| Every confirmation entry point uses the shared qualifier and identity guards. Rejected dated target observations are either dropped or pending. | `test_b1_round2_adversarial_confirmation_matrix`, `test_b1_round2_confirmation_matrix_genuine_controls`, and five cases in `test_b1_round2_rejected_later_authority_keeps_sql_ratio_ambiguous`. |

The historical 406-case core round 2 regression selection, before the
supplemental admission matrix was added, fails against the blocked `5469042` parser:
**250 failed, 156 passed, 410 deselected**. It includes each requested defect
and omission regression. Current required test files pass completely; the
selection and baseline/current logs are preserved in
`C:/investintell-data/w1c-final7-work/`.

Source review of the rejected first replay added 17 targeted follow-up cases:
10 negatives, five genuine completions and two SEC checkbox scope-equivalence
controls, all passing. They cover the actual ARBK completed ratio change with a
future result clause, NNDM's completed corresponding ratio change with an
explicit concurrent split date, common-share/ADS unit direction, basic/diluted
EPS labels, retroactive market-price comparisons and the exact Form 6-K
reporting checkboxes. These checkboxes cannot create a ratio prerequisite;
actual conditional language in the narrative remains screened.

The relevant tests are
`test_b1_round2_completed_numbered_arbk_transition_owns_its_future_result`,
`test_b1_round2_explicit_concurrent_completed_ratio_uses_actual_following_split_date`,
`test_b1_round2_concurrent_completion_requires_actual_named_split_date`,
`test_b1_round2_explicit_common_share_compact_transition_preserves_unit_direction`,
`test_b1_round2_form_6k_checkboxes_cannot_supply_ratio_conditions`,
`test_b1_round2_per_basic_and_diluted_ads_accounting_is_never_a_pending_control`
and `test_b1_round2_retroactively_adjusted_market_prices_are_never_a_pending_control`.
The owning completion clause must be past and actual; a separately stated
resulting ADS entitlement may use future wording without serving as the
completion proof.

## Adversarial confirmation matrix

The committed synthetic fixture is
`tests/fixtures/sec_foreign_listing_evidence/b1_round2_confirmation_matrix.json`,
SHA-256 `4166d7d295c4997d8b5258b132d4d923f45492d669e106caa71f68e63e4a3861`.
Its **362 cases** comprise **335 negatives and 27 genuine controls**.
The initial independent in-memory review of the exact frozen cases found
**163 false confirmations and 2 genuine omissions on `5469042`**. The final independent review repeats both committed fixture sets with
**472/472 passing cases: 437 negatives and 35 genuine controls**. The final
required test run repeats both sets with zero failures. Every surviving rejected target is pending; none is admitted as an
unconditional completed target.

| Confirmation entry family | Negative | Genuine | Baseline false confirmations | Baseline genuine omissions | Final failures |
|---|---:|---:|---:|---:|---:|
| Dated direct completion | 71 | 9 | 38 | 2 | 0 |
| Dated direct adjustment | 36 | 5 | 21 | 0 | 0 |
| Past effective predicate | 84 | 3 | 53 | 0 | 0 |
| Depositary notice | 30 | 3 | 6 | 0 | 0 |
| Direct shareholder approval | 32 | 4 | 8 | 0 | 0 |
| Named consolidation approval and date | 29 | 0 | 25 | 0 | 0 |
| Adopted subdivision resolution | 5 | 0 | 0 | 0 | 0 |
| Rejected event admission without a positive predicate | 30 | 0 | 3 | 0 | 0 |
| Taken effect with same-day link | 10 | 1 | 3 | 0 | 0 |
| F-6 reuse of effectiveness metadata | 8 | 2 | 6 | 0 | 0 |
| **Total** | **335** | **27** | **163** | **2** | **0** |

The supplemental `b1_round2_admission_matrix.json` has SHA-256
`4cb89a99144099e50275118180e510c0a8d42207ad22c63234fd5bd7e70b908d`.
Its 110 cases contain 102 negatives and eight genuine controls, all passing.
The 115-case post-candidate regression selection is red on `c3f6588` / parser
`d5eaa720ee0d6b79f9ee88efe2f2be27acb8a7749439504806915d3f012413ff`:
**47 failed, 68 passed, 821 deselected**, including all initial six reported
compact-object and checkbox escapes. This historical selection includes the
supplemental matrix and related owner-scope tests; its log is
`C:/investintell-data/w1c-final7-work/admission-base-c3f6588.log`.

| Supplemental entry/scope family | Negative | Genuine | Final failures |
|---|---:|---:|---:|
| Linked corresponding completion | 20 | 1 | 0 |
| Weekday compact completion | 18 | 1 | 0 |
| Named compact direction | 34 | 2 | 0 |
| Form 6-K checkbox boundary | 10 | 1 | 0 |
| Form 6-K complete answer-list boundary | 4 | 2 | 0 |
| Financial table lead with compact completion | 1 | 0 | 0 |
| Full owning sentence across semicolon | 4 | 0 | 0 |
| Full owning sentence accounting assumption | 1 | 0 | 0 |
| Full owning sentence with separate EPS sentence | 0 | 1 | 0 |
| Numberless other transition side | 5 | 0 | 0 |
| Negated/modal event with adverbs | 5 | 0 | 0 |
| **Total supplemental** | **102** | **8** | **0** |

The supplements check every dated target admission as well as confirmation.
A rejected compact target cannot survive as an unconfirmed, unconditional
fact usable by SQL `announced_changes`. Exact reporting checkboxes and their
complete answer lists are separated from legal conditions; prefix extensions
cannot bypass the full-sentence screen. Semicolons and dotted abbreviations
retain governing premises. Financial class labels cannot be trimmed into an
unknown-class match. Initial, pre-change, pre-split, pre-adjustment and unchanged
ratio references cannot bind a numberless new target; adverbs cannot hide a
negated or modal event. Tests are
`test_b1_round2_new_compact_and_checkbox_admission_matrix` and
`test_b1_round2_full_sentence_qualifier_survives_dotted_depositary_abbreviation`.

The reviewed patterns are `completed`, `effected` and `implemented` with a
ratio-change object; `adjusted`, `changed` and `amended` with the ratio itself;
`was effective`, `became effective` and `has become effective`; same-day
`has taken effect`; Depositary `announced`, `confirmed` and `notified`; and
shareholder `approved`, `resolved`, `adopted` or `passed` acting on the actual
ratio change, consolidation or subdivision. Leading and trailing dates,
relative clauses, coordinated completions, named share-operation dates and
F-6 metadata reuse are all included.

| Disqualifier group | Negative cases |
|---|---:|
| Hypothetical or accounting | 168 |
| Negation or disputed proposition | 58 |
| Former side | 30 |
| Future or modal | 30 |
| Announcement, notice or other non-ratio object | 28 |
| Conditional | 13 |
| Other class or program | 8 |
| **Total** | **335** |

These groups aggregate the original 335 negative fixture's labelled cases;
the supplemental families above extend the same disqualifiers. Exact individual inputs
remain in the JSON fixture. Qualifiers include long sentence prefixes/suffixes
beyond the previous windows, accounting-purpose variants, negated contractions,
interrupted negation, denial/no-evidence clauses, unsupported multi-letter
class names, and pending/awaiting approvals. Genuine controls preserve target
binding, legal completion followed by a separate accounting sentence, May
calendar dates, future availability of ADSs after actual completion, and
ancillary statements that underlying shares or ownership did not change.

Five original hash-pinned legal fixtures remain genuine: ANPC, AKTX, TAL,
ANTE and TCOM. The final independent review, exact probes, original fixture checks and
summary are at `C:/investintell-data/w1c-final7-work/review/`, including
`final-independent-review.md` and `final-independent-review-summary.json`.

## Artifact identities and offline replay

Final7 directory: `C:/investintell-data/w1c-final7/`.

| Artifact | SHA-256 |
|---|---|
| `manifest.json` | `3ebbe15ed8339f59abe3bd0b85f74445b25b391c8eba8f74e9d5e790b9458aa4` |
| `evidence.jsonl` | `5580abc486d88c872d42fd5b53a51aaf04a942b66ca0c6b5c853174b1ab2b977` |
| `SHA256SUMS` | `91f44eb8f6eaf3555cda777e62d6baaebada55dd3aaa00f732d00ed20e8de241` |

Final7 contains **47,329 source entries, 29,636 evidence rows and 398 confirmed
rows**, with zero failed sources and complete parse/discovery flags. Source
dispositions remain 46,388 parsed, 593 issuer-binding-unverified and 348
not-securities-description, over 47,050 unique URLs.

Measured full replay: **530.80 seconds (8 minutes 50.80 seconds)**,
**16 workers**, peak simultaneous process-tree RSS **4,633,329,664
bytes (4.315 GiB)**, and peak 28 processes including
launchers/PDF tools. The replay ended at `2026-10-10T08:34:05Z`. These are the
accepted third full-run metrics. Two rejected full generations remain at
`C:/investintell-data/w1c-final7-candidate1/` and `w1c-final7-candidate2/`,
with separate records under the work directory; their metrics and pins do not
identify final7.

Input checks verify both original final5 pins and all three final6 artifact pins
unchanged. Exact final metrics, implementation pins and test results are in
`C:/investintell-data/w1c-final7-work/artifact-verification.json`; command,
stdout/stderr, telemetry and metrics are retained alongside it, and the metrics
are copied into the final7 directory.



Pinned inputs are unchanged:

- Raw cache: `E:/investintell-data/w1c-20261009-run2/final5`, read in place through `--raw-cache-dir`.
- Final5 manifest: `793cc435a00d41309a5b1b72724eb940ae43b8c86b97e2d7412d5fe0439a5646`.
- Final5 evidence: `9ca17573dd649db4075a7eb40065274e7e0b1d536649b553c4b790c3dfc1accc`.
- Universe: `E:/investintell-data/w1c-20261009-baseline/universe.json`, byte SHA-256 `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b`.
- Collector observations: `E:/investintell-data/w1c-20261009-baseline/foreign_observations.json`, canonical JSON SHA-256 `69dbccf9710ecda27132a4eb7389ac3bb2ed8b3ec4810fd9481b8650cae8f73a`.
- Coverage observations: saved `foreign_observations_all_versions.json`, SHA-256 `0cef1602ff75bb3582680d1b633ddf25f4738c0ceb3abd7a178ebb920917852b`.
- Saved `current_status_rows.json`, SHA-256 `8091c2570436a032b7fd696becfabfc74945d86f11f3b0ab48cea6f32315b3b4`.

```powershell
python scripts/load_sec_foreign_listing_evidence.py `
  --offline `
  --universe E:/investintell-data/w1c-20261009-baseline/universe.json `
  --manifest E:/investintell-data/w1c-20261009-run2/final5/manifest.json `
  --raw-cache-dir E:/investintell-data/w1c-20261009-run2/final5 `
  --cache-dir C:/investintell-data/w1c-final7 `
  --output C:/investintell-data/w1c-final7/evidence.jsonl `
  --observations E:/investintell-data/w1c-20261009-baseline/foreign_observations.json `
  --workers 16
```

The telemetry wrapper is
`C:/investintell-data/w1c-final7-work/replay_final7.py`.
It blocks socket connections in parent/spawned workers, restricts the parse to
CPUs 0–19 of the 24 logical cores, and records simultaneous process-tree RSS
once per second. The worker memory estimate is 256 MiB, with a 2 GiB reserve;
Python 3.12.12 and pypdf 6.20.0 are pinned in `replay-command.json`.
Input and final6 immutability checks precede the timed child-launch-to-exit
interval. No discovery runs, raw-cache writes or raw-document copies occur.
All new artifacts and temporary output are on C:.

## Confirmation audit and source review

| Comparison | Before | After | Removed | New | Revised/cleaned proof | Unchanged |
|---|---:|---:|---:|---:|---:|---:|
| final5 to final7 | 408 | 398 | 62 | 52 | 166 | 180 |
| final6 to final7 | 415 | 398 | 32 | 15 | 0 | 383 |

All **318 original documents** in both comparison unions pass decompressed
SHA-256, URL metadata and literal/component quote checks. Every removed, added,
revised and cleaned proof is source-checked; no manual flags remain. Pairing
preserves duplicate multiplicity and permits only a unique same-event relocation
with an unchanged or strictly narrowed narrative quote. A cleaned BABA footnote
is a retained/revised event, not an invalid removal.

| Removed category | final5 to final7 | final6 to final7 | Why inadmissible |
|---|---:|---:|---|
| Earliest-period EPS assumption | 18 | 0 | The quantity belongs to an assumption about the earliest accounting period, rather than the separately asserted legal ratio-change event. |
| Numeric financial table | 9 | 0 | Table quantities cannot inherit completion of a later narrative ratio object; the NetEase table rows additionally record the former 25/1. |
| EPS/share-compensation adjustment | 28 | 28 | The full owning sentence normalizes EPS, weighted shares or share compensation. Its trimmed date tail cannot supply independent legal completion. |
| Retroactive market-price adjustment | 4 | 4 | The owning sentence describes price-table comparability; it does not independently complete the ratio change. |
| Named class for unknown plan | 1 | 0 | The row has unknown class while the proof names Class A. The corrected Baidu event now has Class A explicitly. |
| Former ratio | 2 | 0 | The number belongs to the former side: NetEase 25/1 instead of target 5/1, or SaverOne 1200/1 instead of target 3600/1. |

The **32 new financial-ownership removals** consist of 28 EPS/share-compensation
sentences and four market-price adjustment sentences. Their historical date
claims need not be false; they fail the required independent narrative authority
for completion. Separate genuine legal statements are retained where present.

Final5 has **18 contaminated numeric financial-table confirmation rows**:
OTLY/1843586 13, NetEase/1110646 2, ASLN/1722926 1, LGHL/1806524 1 and
CIK 1743340 1. Nine are discarded and nine become clean independent legal
narrative proofs. Final7 has **zero numeric financial-table or financial-heading
confirmation proofs**. The full original list remains in
`C:/investintell-data/w1c-final6-audit/final5-invalid-financial-confirmations.json`;
its final dispositions and every current proof are verified in the final7 audit.

Because final5 has more than 40 removals, the following exact 40-row sample is
provided with the categories above. All 62 individual owning sentences, source
hashes, URLs and reasons are in
`C:/investintell-data/w1c-final7-audit/5-to-7/removed-confirmations-reviewed.json`.
The exact sample is in `removed-confirmations-sample40-reviewed.json` in that
directory. The full 32-row final6 delta follows it.

| Final5 line | CIK | Accession | Ratio/class | Reason | Source |
|---:|---:|---|---|---|---|
| 47 | 1780531 | 0001104659-25-024790 | 30/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1780531/000110465925024790/tm259628d2_ex99-1.htm) |
| 175 | 1843586 | 0001193125-25-256284 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000119312525256284/otly_6k_3q25.htm) |
| 734 | 1843586 | 0001843586-26-000012 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000184358626000012/otly-ex99_1.htm) |
| 850 | 1372920 | 0001193125-23-194044 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523194044/d509403dex991.htm) |
| 851 | 1372920 | 0001193125-23-194044 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523194044/d509403dex991.htm) |
| 1239 | 1372920 | 0001193125-22-269567 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522269567/d412317dex991.htm) |
| 1240 | 1372920 | 0001193125-22-269567 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522269567/d412317dex991.htm) |
| 1312 | 1372920 | 0001193125-23-008943 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523008943/d430427dex991.htm) |
| 1313 | 1372920 | 0001193125-23-008943 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523008943/d430427dex991.htm) |
| 1615 | 1372920 | 0001193125-22-118753 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522118753/d315717dex991.htm) |
| 1616 | 1372920 | 0001193125-22-118753 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522118753/d315717dex991.htm) |
| 2499 | 1110646 | 0001104659-21-027746 | 25/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1110646/000110465921027746/a21-7816_1ex99d1.htm) |
| 3393 | 1592560 | 0001104659-23-037306 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465923037306/tm2310637d1_ex99-1.htm) |
| 3962 | 1592560 | 0001410578-24-002119 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000141057824002119/tctm-20241227xex99d2.htm) |
| 4050 | 1843586 | 0000950170-25-061038 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000095017025061038/otly_6k_1q25.htm) |
| 4051 | 1843586 | 0000950170-25-061038 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000095017025061038/otly_6k_1q25.htm) |
| 4078 | 1696355 | 0001213900-22-076019 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390022076019/ea169379ex99-2_brightscholar.htm) |
| 6071 | 1372920 | 0001193125-23-105618 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523105618/d447235dex991.htm) |
| 6072 | 1372920 | 0001193125-23-105618 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523105618/d447235dex991.htm) |
| 6692 | 1696355 | 0001213900-23-080118 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023080118/ea185877ex99-1_brightscholar.htm) |
| 7767 | 1592560 | 0001104659-22-122963 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465922122963/tm2231528d1_ex99-1.htm) |
| 7949 | 1713923 | 0001104659-21-059001 | 20/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1713923/000110465921059001/a21-14433_16k.htm) |
| 8499 | 1329099 | 0000950123-10-067501 | 1/10; unknown | Named class for unknown plan | [SEC](https://www.sec.gov/Archives/edgar/data/1329099/000095012310067501/c03699exv99w1.htm) |
| 8694 | 1816007 | 0001193125-24-225400 | 2/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1816007/000119312524225400/d855645dex991.htm) |
| 9071 | 1780531 | 0001104659-23-092183 | 30/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1780531/000110465923092183/tm2323915d2_ex99-1.htm) |
| 9683 | 1780531 | 0001104659-25-038307 | 30/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1780531/000110465925038307/tm2513082d1_ex99-1.htm) |
| 9711 | 1592560 | 0001104659-22-091534 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465922091534/tm2223565d1_ex99-1.htm) |
| 10920 | 1485538 | 0001144204-17-063933 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417063933/tv481320_ex99-1.htm) |
| 10921 | 1485538 | 0001144204-17-063933 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417063933/tv481320_ex99-1.htm) |
| 12028 | 1816007 | 0001193125-26-197190 | 2/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1816007/000119312526197190/d128745dex995.pdf) |
| 12730 | 1843586 | 0000950170-25-097884 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000095017025097884/otly_6k_2q25.htm) |
| 13319 | 1485538 | 0001144204-17-039702 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417039702/v471250_ex99-1.htm) |
| 13320 | 1485538 | 0001144204-17-039702 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417039702/v471250_ex99-1.htm) |
| 13739 | 1780531 | 0001104659-23-097916 | 30/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1780531/000110465923097916/tm2325125d1_ex99-1.htm) |
| 15255 | 1696355 | 0001213900-23-050587 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023050587/ea180742ex99-1_brightscholar.htm) |
| 16152 | 1843586 | 0001193125-25-213996 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000119312525213996/otly-20250630.htm) |
| 16357 | 1843586 | 0001193125-26-046546 | 20/1; unknown | Numeric financial table | [SEC](https://www.sec.gov/Archives/edgar/data/1843586/000119312526046546/otly_year-end_report_202.htm) |
| 17321 | 1780531 | 0001104659-24-050313 | 30/1; unknown | Earliest-period EPS assumption | [SEC](https://www.sec.gov/Archives/edgar/data/1780531/000110465924050313/tm2412296d1_ex99-1.htm) |
| 17438 | 1372920 | 0001193125-25-216438 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312525216438/d35269dex991.pdf) |
| 17938 | 1894693 | 0001213900-25-082099 | 1200/1; unknown | Former ratio | [SEC](https://www.sec.gov/Archives/edgar/data/1894693/000121390025082099/ea025457401ex99-2_saverone.htm) |

| Final6 line | CIK | Accession | Ratio/class | Reason | Source |
|---:|---:|---|---|---|---|
| 848 | 1372920 | 0001193125-23-194044 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523194044/d509403dex991.htm) |
| 849 | 1372920 | 0001193125-23-194044 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523194044/d509403dex991.htm) |
| 1237 | 1372920 | 0001193125-22-269567 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522269567/d412317dex991.htm) |
| 1238 | 1372920 | 0001193125-22-269567 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522269567/d412317dex991.htm) |
| 1310 | 1372920 | 0001193125-23-008943 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523008943/d430427dex991.htm) |
| 1311 | 1372920 | 0001193125-23-008943 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523008943/d430427dex991.htm) |
| 1613 | 1372920 | 0001193125-22-118753 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522118753/d315717dex991.htm) |
| 1614 | 1372920 | 0001193125-22-118753 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522118753/d315717dex991.htm) |
| 3392 | 1592560 | 0001104659-23-037306 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465923037306/tm2310637d1_ex99-1.htm) |
| 3961 | 1592560 | 0001410578-24-002119 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000141057824002119/tctm-20241227xex99d2.htm) |
| 4075 | 1696355 | 0001213900-22-076019 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390022076019/ea169379ex99-2_brightscholar.htm) |
| 6066 | 1372920 | 0001193125-23-105618 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523105618/d447235dex991.htm) |
| 6067 | 1372920 | 0001193125-23-105618 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523105618/d447235dex991.htm) |
| 6687 | 1696355 | 0001213900-23-080118 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023080118/ea185877ex99-1_brightscholar.htm) |
| 7762 | 1592560 | 0001104659-22-122963 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465922122963/tm2231528d1_ex99-1.htm) |
| 7943 | 1713923 | 0001104659-21-059001 | 20/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1713923/000110465921059001/a21-14433_16k.htm) |
| 9699 | 1592560 | 0001104659-22-091534 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465922091534/tm2223565d1_ex99-1.htm) |
| 10906 | 1485538 | 0001144204-17-063933 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417063933/tv481320_ex99-1.htm) |
| 10907 | 1485538 | 0001144204-17-063933 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417063933/tv481320_ex99-1.htm) |
| 13300 | 1485538 | 0001144204-17-039702 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417039702/v471250_ex99-1.htm) |
| 13301 | 1485538 | 0001144204-17-039702 | 3/1; unknown | Retroactive market-price adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1485538/000114420417039702/v471250_ex99-1.htm) |
| 15233 | 1696355 | 0001213900-23-050587 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023050587/ea180742ex99-1_brightscholar.htm) |
| 17411 | 1372920 | 0001193125-25-216438 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312525216438/d35269dex991.pdf) |
| 19551 | 1696355 | 0001213900-23-090347 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023090347/ea189118ex99-1_bright.htm) |
| 21789 | 1372920 | 0001193125-23-240998 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523240998/d515693dex991.pdf) |
| 21792 | 1372920 | 0001193125-23-240998 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312523240998/d515693dex991.pdf) |
| 24972 | 1979887 | 0001193125-26-395514 | 800/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1979887/000119312526395514/ck0001979887-ex99_2.htm) |
| 24973 | 1979887 | 0001193125-26-395514 | 800/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1979887/000119312526395514/ck0001979887-ex99_2.htm) |
| 25949 | 1592560 | 0001104659-23-070538 | 5/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1592560/000110465923070538/tm2318452d1_ex99-1.htm) |
| 29015 | 1372920 | 0001193125-22-202595 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522202595/d365280dex991.htm) |
| 29016 | 1372920 | 0001193125-22-202595 | 10/1; unknown | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1372920/000119312522202595/d365280dex991.htm) |
| 29213 | 1696355 | 0001213900-23-068338 | 4/1; class_a | EPS/share-compensation adjustment | [SEC](https://www.sec.gov/Archives/edgar/data/1696355/000121390023068338/ea183720ex99-1_brightscholar.htm) |

Both full comparison directories contain individually reviewed removed/new
confirmations and revised proofs. The aggregate review is
`C:/investintell-data/w1c-final7-audit/final-confirmation-review.json`, SHA-256
`f8a74adc823c3a4b7c08bf3564247019dbf23726e1418054fe6747015ddec03d`.
The audit file inventory is `final-audit-artifact-hashes.json`, SHA-256
`22e4ce2b6cae1c08fbe8e3eeac47e53567c37b8f336336f500a3afb4d290fdb6`.


The gate's three omitted-source checks establish genuine completed targets.
All three original decompressed documents match their pinned SHA-256 and the
quoted legal clauses occur in the originals. The following accounting sentence
is separate from each legal completion and supplies no confirmation itself.

| Source | Target and operative date | Raw-source SHA-256 |
|---|---|---|
| [Baidu `0000950123-10-067501`](https://www.sec.gov/Archives/edgar/data/1329099/000095012310067501/c03699exv99w1.htm) | Class A ADS 1/10, 2010-05-12 | `e85cf9319276138bb5b28024c0d7655144f19e7da0c15b27cd7f7b5361795211` |
| [NetEase `0001104659-20-127089`](https://www.sec.gov/Archives/edgar/data/1110646/000110465920127089/a20-36341_1ex99d1.htm) | ADS 5/1 from 25/1, 2020-10-01 | `4b59c4cfcb6a67faed1521d812304a6e1fc700ca60e449bfc383c06bfabed0b4` |
| [NetEase `0001104659-21-027746`](https://www.sec.gov/Archives/edgar/data/1110646/000110465921027746/a21-7816_1ex99d1.htm) | ADS 5/1 from 25/1, 2020-10-01 | `2da896c9c4579b05c5bec219b12c2dd4b4cd8cb2a825a874f11ea2fe2cc8c6d4` |

Baidu states “Effective on May 12, 2010, Baidu adjusted the ratio”, identifies
ADSs “representing Class A ordinary shares”, and changes to “ten (10) ADSs for
one (1) share”. Both NetEase sources describe the transition “from the previous
one ADS for every twenty-five ordinary shares to one ADS for every five
ordinary shares.” Their full original context is in
`C:/investintell-data/w1c-final7-audit/omission-source-review.json`.

The 15 new confirmations versus final6 comprise ten NetEase rows (five actual
25/1 completions in 2006 and five actual 5/1 completions in 2020), the correctly
classified Baidu row, and four genuine sources: ARBK 2160/1 effective
2025-12-12, NNDM 1/1 effective 2020-06-29, China Mass Media 300/1 effective
2011-11-28, and PSNY 30/1 effective 2025-12-09. The NetEase 2006 target 25/1
is distinct from the former 25/1 side of its 2020 transition to 5/1.
All four additional sources preserve unknown class/program scope. The
independent four-source semantic review verifies original bytes, metadata and
quarterly master-index dates, at
`compact-independent-review/four-source-semantic-review.json` in the audit
directory, SHA-256 `0044106ec9a5bdfd4355b975b587178a43509edeb20bcac616a4b53d4c871ba1`.
PSNY is first governed available on 2026-01-10 and cannot settle a 2025 query.
The existing 20-F body role boundary is retained; this report does not claim
newly emitted MTL/HQCL/GRVY body-completion facts or a global zero-gap result.


## Changed answers

Each artifact is initially loaded into a separate fresh database in the same
single task-owned PG18 container. Final7 is not reconciled onto an older
artifact for historical comparison. Both fresh baselines match their pinned
saved historical semantic snapshots for all **12,155 distinct line/date
queries**: 3,036 lines at 2010, 2015, 2020 and 2025 year-ends, plus the same
17 acceptance cases whose dates overlap six year-end queries.

The accepted full replay changes **8/12,155 answers across seven lines versus
final5**, and **6/12,155 answers across five lines versus final6**. Every changed
after-answer has a NULL ratio. Five changes move towards fail-closed; the three
remaining final5 changes (TRIB, NTES and SVRE) are explicitly source-justified
non-numeric state corrections. Versus final6, NTES is the only such correction;
TRIB and SVRE retain their final6 answers. OSN changes its internal ratio status
while overall/listing status remain ambiguous, which the tables show explicitly.

Final5 to final7:

| CIK | Line | Date | final5 | final7 | Direction |
|---|---|---|---|---|---|
| 888721 | TRIB | 2025-12-31 | ambiguous ads | none ads | source_justification_required |
| 1110646 | NTES | 2025-12-31 | ambiguous ads | none ads | source_justification_required |
| 1485538 | OSN | 2020-12-31 | ambiguous (ratio_status=resolved) | ambiguous (ratio_status=none) | towards_fail_closed |
| 1485538 | OSN | 2025-12-31 | ambiguous (ratio_status=resolved) | ambiguous (ratio_status=none) | towards_fail_closed |
| 1592560 | VSA | 2025-12-31 | resolved ads 5/1 | ambiguous ads | towards_fail_closed |
| 1792267 | BNR | 2025-12-31 | none ads | ambiguous ads | towards_fail_closed |
| 1894693 | SVRE | 2025-12-31 | ambiguous ads | none ads | source_justification_required |
| 1935172 | AIXI | 2025-12-31 | resolved ads 3/1 | ambiguous ads | towards_fail_closed |

Final6 to final7:

| CIK | Line | Date | final6 | final7 | Direction |
|---|---|---|---|---|---|
| 1110646 | NTES | 2025-12-31 | ambiguous ads | none ads | source_justification_required |
| 1485538 | OSN | 2020-12-31 | ambiguous (ratio_status=resolved) | ambiguous (ratio_status=none) | towards_fail_closed |
| 1485538 | OSN | 2025-12-31 | ambiguous (ratio_status=resolved) | ambiguous (ratio_status=none) | towards_fail_closed |
| 1592560 | VSA | 2025-12-31 | resolved ads 5/1 | ambiguous ads | towards_fail_closed |
| 1792267 | BNR | 2025-12-31 | none ads | ambiguous ads | towards_fail_closed |
| 1935172 | AIXI | 2025-12-31 | resolved ads 3/1 | ambiguous ads | towards_fail_closed |

Every changed answer has a completed source verdict with exact raw hash and
quoted evidence public by D. The two comparison directories preserve all
before/after facts, control checks and source verdicts. Final5-to-final7 has
84 fact/control checks, final6-to-final7 59; all quote/component checks pass.

**TRIB / CIK 888721**: The actual 2024 ratio change retires the obsolete 2004 1/1 registration. Current same-class independent corroboration remains insufficient, so the answer supplies no numeric ratio. This is the same source-justified final6 correction.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/888721/000117891325003264/exhibit_99-1.htm); filed 2025-09-08, governed available 2025-09-09. Raw SHA-256: `b0ff3ed4f51d5d04ecbeaba11f7fe76671c2db37e904e69db0f79b214fecdde4`.

> we effected an ADS Ratio Change on February 23, 2024

**NTES / CIK 1110646**: The genuine completion states the target 5/1 and former 25/1 in a standalone narrative sentence. The following as-if accounting-adjustment sentence does not qualify that completed legal sentence. The new actual event removes obsolete 25/1 competition, while eligible current independent corroboration remains insufficient. No numeric ratio is newly supplied.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1110646/000110465920127089/a20-36341_1ex99d1.htm); filed 2020-11-19, governed available 2020-11-20. Raw SHA-256: `4b59c4cfcb6a67faed1521d812304a6e1fc700ca60e449bfc383c06bfabed0b4`.

**OSN / CIK 1485538**: The owning sentence describes retrospective market-price table adjustment. It supplies no legal ratio-change completion. Removing all four such table proofs changes ratio_status from resolved to none; listing_status and overall status remain ambiguous, and both before and after return no numeric ratio.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1485538/000114420417063933/tv481320_ex99-1.htm); filed 2017-12-15, governed available 2017-12-16. Raw SHA-256: `ce5c2afb23d12e0c74655a7be4269e730a1510e37eab106a1bbe84f4de3bc95c`.

> all prices have been retroactively adjusted

**VSA / CIK 1592560**: The full owning sentence is an EPS accounting adjustment, so its trimmed effective-date tail cannot discharge the pending change. The original-cache sweep found no standalone matching legal completion public before 2025 year-end; the executed 2021 amendment first made public in 2026 is excluded at D. The corrected answer refuses the prior numeric 5/1 and does not resolve the obsolete 1/1.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1592560/000110465924049422/tctm-20231231x20f.htm); filed 2024-04-19, governed available 2024-04-20. Raw SHA-256: `2eafe3a6f276fde4b4d5a72e2c7fd0d09781a2be710aa5fe8661c8a95effb2e8`.

> earnings per ADS have been retrospectively adjusted

**BNR / CIK 1792267**: This source announces that the ratio change will become effective on a future day. The pinned cache has no matching actual legal completion public before D. Retaining a pending 10/1 transition makes the previously silent none result explicitly ambiguous, with no numeric ratio.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1792267/000119312524137661/d837322dex991.htm); filed 2024-05-14, governed available 2024-05-15. Raw SHA-256: `6e93591c38e48921cfd94ed1d541aa0dd06b4dcbf61abd129526bcb4d7d08ebe`.

> will become effective on May 15, 2024

**SVRE / CIK 1894693**: The completed target is 3600/1, while 1200/1 is the former side. Correct binding ends obsolete 1200/1 competition; independent current corroboration remains insufficient. The answer supplies no numeric ratio and preserves the final6 correction.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1894693/000121390025082099/ea025457401ex99-2_saverone.htm); filed 2025-08-29, governed available 2025-08-30. Raw SHA-256: `8e5ae9996cd4846c74f5d26a6adce35f5a34d2e99a47b713d07ee449eb1097de`.

> On June 11, 2025, the Company effected the change

**AIXI / CIK 1935172**: The August 2024 6-K is a future notice and remains pending. A genuine later 20-F Item 14 narrative, public in May 2025 before D, says the 3/1 change occurred on August 3, 2024, while the held 6-K clock is August 23. The annual report and F6 registration do state 3/1; genuine public completion evidence is present. Frozen f334 settlement requires an explicit-date f6 or ratio_change_6k authority, and a settling effective_from at least the held August 23 date unless the control is already a date-conflict control. The Item 14 body is outside the admitted authority role and its stated August 3 date precedes the held clock. The governed answer therefore refuses sizing with NULL ratio. This documents the existing source role and date boundary and does not claim global completion coverage. The annual report retains its actual role, the literal dates are preserved, and the schema rules remain unchanged.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1935172/000121390024071289/ea021194001ex99-1_xiao1.htm); filed 2024-08-21, governed available 2024-08-22. Raw SHA-256: `6dd2e5cdcea14fa085aff0614cca22c6c71da80e2cebe50f7cc83187c8a67262`.

Source: [SEC filing](https://www.sec.gov/Archives/edgar/data/1935172/000121390025044439/ea0238811-20f_xiaoicorp.htm); filed 2025-05-15, governed available 2025-05-16. Raw SHA-256: `f99343fb37e2aa61081fb1e95ac4709a88c918d9916f9b1aaa26c7da7e5fb6cc`.

For AIXI, retain the literal Item 14 August 3 date and the prospective 6-K
August 23 date without normalizing either. The same annual report also has a
Corporate Actions narrative reporting implementation on August 23. These public
legal narratives exist, but the unchanged `f334d08d` settlement admits the
specified F-6/ratio-change-6-K authority roles. The deep annual-body role remains
outside that settlement; Item 14's August 3 also precedes the pending August 23
clock. The governed answer therefore refuses 3/1 and stays ambiguous. This is a
known role/date boundary, not a claim that the corpus lacks actual completion.
Full original annual context and all 37 checked issuer documents are recorded in
`C:/investintell-data/w1c-final7-audit/aixi-changed-answer-source-review.json`.
No annual evidence is mislabeled as a 6-K and no protected SQL is changed.


## Acceptance, precision and tests

**17/17 acceptance, 30/30 frozen precision and 10/10 changed precision pass**
with the exact saved cohorts and no resampling. **187/187 current supporting
facts across 114 original documents** pass fresh raw SHA-256, exact meaningful
quote/component, public-by-D and same-accession W1 binding checks. There are
179 contiguous parser excerpts; all composed components and annotations are
verified. This preserves the reviewed precision study and its source evidence.
Current source retention, exact original cohort hashes and every case are at
`C:/investintell-data/w1c-final7/validation/precision-source-retention.json`,
`precision-source-check/precision-sources.json`, and `final7/` cohort files.

| Acceptance line | CIK | Date | Actual result | Match |
|---|---:|---|---|---|
| AZN | 901832 | 2025-12-31 | resolved; 1/2 | Pass |
| QGEN | 1015820 | 2025-12-31 | resolved; 1/1 | Pass |
| CNQ | 1017413 | 2025-12-31 | resolved; 1/1 | Pass |
| TSM | 1046179 | 2025-12-31 | resolved; 5/1 | Pass |
| ZIM | 1654126 | 2025-12-31 | resolved; 1/1 | Pass |
| ANPC | 1786511 | 2022-10-24 | resolved; 1/1 | Pass |
| ANPC | 1786511 | 2022-10-25 | resolved; 1/1 | Pass |
| ANPC | 1786511 | 2022-11-03 | resolved; 1/1 | Pass |
| ANPC | 1786511 | 2022-11-04 | ambiguous; no ratio | Pass |
| AKTX | 1541157 | 2023-08-16 | resolved; 100/1 | Pass |
| AKTX | 1541157 | 2023-08-17 | ambiguous; no ratio | Pass |
| AKTX | 1541157 | 2023-08-18 | ambiguous; no ratio | Pass |
| OTLY | 1843586 | 2025-02-12 | ambiguous; no ratio | Pass |
| OTLY | 1843586 | 2025-02-13 | ambiguous; no ratio | Pass |
| OTLY | 1843586 | 2025-02-17 | ambiguous; no ratio | Pass |
| OTLY | 1843586 | 2025-02-18 | ambiguous; no ratio | Pass |
| OTLY | 1843586 | 2025-12-31 | resolved; 20/1 | Pass |

The separate genuine-completion query check passes **5/5**:

| Line | Date | Actual result |
|---|---|---|
| AMBR | 2025-12-31 | resolved; 5/1 |
| FRLN | 2023-06-01 | resolved; 15/1 |
| FRLN | 2025-12-31 | resolved; 15/1 |
| ANPC | 2022-12-16 | ambiguous; no ratio |
| ANPC | 2022-12-17 | resolved; 20/1 |

| Coverage date | Resolved both | Ambiguous | None |
|---|---:|---:|---:|
| 2010-12-31 | 2 | 0 | 3034 |
| 2015-12-31 | 83 | 1 | 2952 |
| 2020-12-31 | 513 | 26 | 2497 |
| 2025-12-31 | 1149 | 81 | 1806 |

At the 2025 year-end, 819 of the saved current refusals resolve. No fresh production export or
discovery is part of these denominators. The complete accepted capture has
snapshot SHA-256 `00b7930353fd3c026e6e133cdb5c1530e37a824d3b6a07e9b8e9cee06e45328d`.


The acceptance oracle remains each saved case's `final5_answer`; the older
`expected` field is the final4 oracle. Frozen30 and changed10 are the exact
reviewed saved cohorts, without resampling.

Required tests use `timescale/timescaledb:2.27.2-pg18` (PostgreSQL 18.4), one
file at a time, `PYTEST_WORKERS=2`, with temporary and cache artifacts on C:.

| Suite | Result |
|---|---|
| `tests/test_sec_foreign_listing_evidence.py` | 936 passed, 0 failed/errors/skipped |
| `tests/test_sec_foreign_listing_loader.py` | 138 passed, 0 failed/errors/skipped |

The five new SQL cases keep a false later authority from discharging an
unmet conditional ratio. The original genuine ANPC settlement regression is
retained. Focused Ruff passed. Tests and matrix source files are LF-only.
Schema bytes are unchanged, so round 2 introduces no migration or new
changed-schema rollback cycle. Full logs and commands are in
`C:/investintell-data/w1c-final7-work/focused-tests.json` and the adjacent logs.

The sole task-owned PG18 container, `w1c-final7-pg18`, served the tests and
separate fresh databases and was removed successfully with its volumes after
validation. Its exact ID was verified before removal and the final Docker
name-filter inventory is empty. Unrelated pre-existing containers were preserved. Final6 artifact bytes stay unchanged; its historical
document receives only the pointer at its top. Round2 changes no schema,
database apply code or runbook. The PR remains open without merge, and no
production access or Railway operation was performed.


## B1b round 2: v2 applied over the loaded final5

This section records the stacked schema/loader acceptance separately from the
B1 detector replay above. It supersedes the production target in the historical
[final6 B1b measurement](sec-foreign-listing-final6.md#b1b-v2-applied-over-the-loaded-final5).
That measurement and both immutable artifacts remain preserved. The original
B1 final7 report above is unchanged.

The project's restatement rule remains the one documented for W1 and W1b:
same-byte parser corrections make the old reading invisible at every date and
inherit the replaced reading's availability; source changes remain prospective.
Both facts and source metadata already record parser versions. The base SQL
remains byte-identical to production v1. Every resolver branch descends from the
single reason-filtered `issuer_observed` CTE, including downstream publication
ordering and public-by-registration date bounds.

| Governed SQL | SHA-256 |
|---|---|
| v1 base | `f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957` |
| v2 | `4eca964a6a3edbcfa328628904a324dfa6a363d82f4a5dd46f8b76d0012d7158` |
| v2 rollback | `1aef8c8168c40af826fb0f6b90440569c6933d0ff01ce74ee59c305e999a7bc2` |

The v2 resolver body remains MD5 `60f5d1bf86a645a41fb7e23328c7ab8a`.
Both migration and rollback now acquire transaction advisory lock `(79311, 173)`
shared with evidence application. The loader acquires it before checking the
exact v2 schema/resolver, so a loader queued behind rollback observes the
restored v1 resolver and refuses application. Rollback preserves the additive
reason data/CHECK while restoring exact v1 resolver bytes, comments, ownership,
grants and behavior. No production schema was accessed or applied.

The local sequence installed v1, applied final5 with the main loader in
`E:/investintell-datalake-workers-sep/.worktrees/w1c-prod` and
`--observed-on 2026-10-09`, applied v2 DDL, then applied pinned final7 with this
branch's loader and `--observed-on 2026-10-10`. The main checkout remained
`2905146afd69c4c27cc0e71c34f13ab383d000e9`; its loader SHA-256 was
`d56c54172645b656363d968c9653bdf39fee745a0a48d9682bd27f19db7a5762`.
This mirrors the proposed production operation. The final7 manifest/evidence
pins are the ones in the artifact table above. All 47,329 source-package hashes
agree between final5 and final7; no fetch or reparse occurred during application.

All **12,155/12,155** queries in
`C:/investintell-data/w1c-final7/validation/final7/snapshot.json` match the
isolated final7 semantic answers, with **zero mismatches**. The applied database
reproduces exactly the eight changed queries across seven lines listed under
[Changed answers](#changed-answers), including OSN's distinct ratio-status
changes at 2020 and 2025 year-ends. Every corrected after-answer returns a NULL
numeric ratio. The source quotations and verdicts above explain those changes;
no extra difference is introduced by the application history.

| Measurement | After v1/final5 | After v2/final7 |
|---|---:|---:|
| Sources | 47,329 | 47,329 |
| Total fact versions | 29,709 | 59,345 |
| Active fact versions | 29,709 | 29,636 |
| Retired: `parser_correction` | 0 | 29,709 |
| Retired: `source` | 0 | 0 |
| Retired: NULL | 0 | 0 |

Final7 application reports 29,636 inserted, 29,709 retired and zero unchanged
facts, as parser-version hashes re-version the full fact set. Every active fact
has `available_on = source_available_on`, rather than the replay date. Retired
readings record `foreign-listing-v8`; active readings record
`foreign-listing-v11`. Input hash checks confirm the immutable manifests, JSONL,
snapshot and universe remain unchanged.

All **7/7** changed lines also match an independently initialized isolated
v1/final7 database at **2026-10-11**, after reconciliation. Each row below has
NULL numeric ratio in both databases:

| Line / CIK | Applied and isolated overall status | Listed type | Listing status | Ratio status |
|---|---|---|---|---|
| TRIB / 888721 | ambiguous | ads | resolved | ambiguous |
| NTES / 1110646 | none | ads | resolved | none |
| OSN / 1485538 | ambiguous | NULL | ambiguous | none |
| VSA / 1592560 | ambiguous | ads | resolved | ambiguous |
| BNR / 1792267 | ambiguous | ads | resolved | ambiguous |
| SVRE / 1894693 | none | ads | resolved | none |
| AIXI / 1935172 | ambiguous | ads | resolved | ambiguous |

TRIB's later ambiguity is the same pinned 2026 F-6 conflict documented in the
preserved final6 B1b measurement: “Each American Depositary Share shall represent
one Share” versus “each American Depositary Share represents twenty shares.”
Those later-public assertions are absent at 2025-12-31, when its corrected
answer is `none`, but remain an exact-ratio conflict at 2026-10-11. Thus the
later answer also depends on source evidence rather than load history.
Source: [Trinity F-6 deposit agreement](https://www.sec.gov/Archives/edgar/data/890836/000101915526000024/trinityda.htm),
accession `0001019155-26-000024`, filed 2026-01-27, public 2026-01-28; raw SHA-256
`22d345b973c116eb5215f070d983ebc684234902a94f7761684d12c783538809`.

The complete applied/isolated comparison is
`C:/investintell-data/w1c-b1b-work/round2/report.json`; its reproducible sequence
is `restate_acceptance.py` (`preload`, then `replay`) in that same directory.
All new artifacts are on C:; final5, final6 and final7 remain read-only inputs.
The [runbook](../runbooks/sec-foreign-listing-evidence.md#exact-production-procedure-not-executed-by-this-pr)
now specifies: verify v1/final5, apply the pinned v2 DDL, apply the pinned final7
artifact, then read back all 12,155 historical and seven later semantic answers
and the reason counts. No production access, Railway change, deployment,
admission switch or merge was performed by this acceptance.


B1b ran the two required test files sequentially on
`timescale/timescaledb:2.27.2-pg18`, PostgreSQL 18.4, with `PYTEST_WORKERS=2`
and at most one task-owned PG container. All temporary/test artifacts are on C:.

| B1b round 2 suite | Result | Elapsed |
|---|---|---:|
| `tests/test_sec_foreign_listing_evidence.py` | 952 passed, 0 failed/errors/skipped | 9.50 s |
| `tests/test_sec_foreign_listing_loader.py` | 156 passed, 0 failed/errors/skipped | 2.84 s |

The migration cycle still covers loaded v1 -> v2 -> idempotent v2 -> rollback
-> v2, preserving relation filenodes and restoring exact v1 resolver bytes,
comments, ownership, privileges and behavior. Source-versus-parser retirement,
removed/added confirmation, republication availability, source publication
floors, empty parses, new-document protection and downstream resolver branches
remain covered.

Three new concurrency cases exercise the shared lock on actual PostgreSQL:
`test_loader_waiting_for_reconciliation_lock_refuses_a_committed_v2_rollback`
and both parametrized cases of
`test_v2_migration_and_rollback_share_the_reconciliation_lock`. They observe
an ungranted advisory lock on the blocked backend before releasing the holder;
they do not infer serialization from elapsed time. A loader queued behind an
actual committed rollback must then reject the restored v1 resolver.

The stale-guard regression was also run with the pre-fix loader at
`c6313b8`: it fails with `DID NOT RAISE RuntimeError` (1 expected failure,
155 deselected, 0.75 s), because its guard executes before waiting. The current
full loader suite then passes all 156 cases, including the three concurrency
cases. The red/green proof and full test logs are
`C:/investintell-data/w1c-b1b-work/round2/p2-old-guard.log`,
`test_sec_foreign_listing_evidence.log` and `test_sec_foreign_listing_loader.log`.
The updated runbook's read-only checker verified final7 manifest/evidence/snapshot
pins, the unchanged v2 resolver body and owner, all counts and retirement reasons,
12,155/12,155 historical answers and 7/7 later answers with zero mismatches.
Its code is mirrored exactly in `C:/investintell-data/w1c-b1b-work/readback.py`
and the Round2 artifact copy; the result is `round2/readback.log`.
Focused Ruff, whitespace and LF checks passed. The one task-owned PG18 container
was removed after validation (`round2/container-cleanup.log`); unrelated existing
containers were preserved. The final handoff is
`E:/investintell-handoffs/limitations-program/B1B-REPORT.md`, Round 2.
