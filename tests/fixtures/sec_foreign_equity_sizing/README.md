# Foreign sizing real-configuration golden fixtures

`real_configurations.json` contains exact public SEC rows from the preserved
production-equivalent export captured on 2026-10-10. The fixtures contain 13
row-dates for 12 issuers: 15 W1 observations, 28 W1 share-count rows and 34 W1c
evidence facts. They require no production access or external files in CI.

The expected outputs come from the Round 4 result at
`81a20b82f8951bc129810cdefe994e1f9820d197`, before the Round 5 coverage loss.
`quoted_observations` and `quoted_counts` retain the exact filing captions,
members, context hashes and quantities behind each expectation. The test
docstrings state this acceptance source. Expected admissions were not inferred
from the new SQL or from the fuzz oracle.

| Configuration | Preserved Round 4 expectation | Evidence retained |
|---|---|---|
| SHOP 2025 | Resolved Class A | A subordinate voting caption; separate A, B and founder counts |
| TEAM 2025 | Resolved Class A | A common-stock caption; separate A and B counts |
| TECK 2025 | Resolved Class B | B subordinate voting caption; separate A and B counts |
| TAL 2025 | Resolved Class A | Own ordinary caption; separate ADS member and A/B counts |
| TNK 2025 | Resolved Class A | A common-share caption; separate A and B counts |
| BLX 2025 | Resolved Class E | E common-stock caption; separate E, F, A1 and B1 counts |
| CPA 2025 | Resolved Class A | A common-stock caption; total plus separate A/B counts |
| LX 2025 | Resolved Class A | Own ordinary caption; separate ADS member and A/B counts |
| GSL 2015 | Resolved Class A | Explicit common A/B members; the untitled registration remains unbound |
| GSL 2020 | `share_count_unit_unverified` | Combined ordinary/depositary caption without own ordinary count proof |
| ASML 2025 | `share_total_class_scope_unverified` | Direct ordinary line with an undimensioned total |
| TSM 2025 | `share_total_class_scope_unverified` | ADS contract with an undimensioned cover total |
| DLO 2025 | `share_total_class_scope_unverified` | A listing caption with the unbound 285,475,136 A+B total |

The positive configurations must preserve their exact count, canonical class,
ordinary units and explicit binding. The refusal configurations keep the
existing fail-closed boundaries. The same test module also includes the quoted
preferred and depositary gate controls linked to the same member in another
context; both must still refuse beside the real-data admissions.

The JSON selects all observation and count versions in each elected filing and
the elected W1c evidence at the count date and cutoff. It is a focused replay of
the issuer configuration, rather than an entire issuer history. Column arrays
preserve original IDs, availability, retirement, source package and parser
version fields. Original W1c source text is retained verbatim. The loader does
not parse new facts or fabricate an ordinary-unit attestation.

Provenance is embedded in the JSON. The source binary COPY checksums are:

| Preserved export | SHA-256 |
|---|---|
| `sec_ticker_cik_observations.copy` | `7e4e3d709eab563ff9dc55fa6da215f47375ad388503125a83c3100d85f82247` |
| `sec_cover_share_counts.copy` | `2c47bd49d1992e4619c06129671c0e3dcbfeca0d1bbcb1bc962fb203e706aff7` |
| `sec_foreign_listing_evidence.copy` | `16c98b33350fea710ce313a52e2288aefe500e76e350afef07a5e8d92da325ae` |

The Round 4 expected-output artifact hashes are recorded in the JSON alongside
these export pins, so a future expectation change must identify its evidence.
