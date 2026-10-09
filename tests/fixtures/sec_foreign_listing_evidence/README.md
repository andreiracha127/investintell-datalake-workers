# SEC foreign listing evidence fixtures

These are small real SEC filing extracts, downloaded from EDGAR on 2026-10-09.
`manifest.json` gives each source URL, accession, filing date, form, CIK, expected
ratio or classification, and SHA-256 of the committed excerpt. Excerpts retain
the source HTML markup or legacy plain-text registration table; line endings are
LF. They are data, never executable input. Tests make no SEC requests.

- **TSM, 2024 annual report:** the Section 12(b) row labels TSM "Common Shares",
  but its footnote says the underlying shares are not for trading and identifies
  the listed ADS. The footnote appears after the cover's checkbox paragraphs.
- **TSM, 2007 F-6EF:** one ADS represents five common shares. The plain-text
  registration-fee table places numeric fee columns between "each ADS" and
  "representing", a layout that must not obscure the ratio.
- **TSM, 2024 annual report Exhibit 2a.1:** its registered-securities table and
  depositary description corroborate five common shares per ADS. This is
  explicitly `securities_description` evidence and never creates a listed-type
  observation. It is not mislabeled as a cover footnote or Item 12.D.
- **ZIM and QGEN, 2024 annual reports:** direct ordinary/common share listings.
  QGEN's column heading says "Title of class", without "each".
- **Canadian Natural Resources (CNQ), 2024 Form 40-F:** direct common-share
  listing on the NYSE.
- **AstraZeneca (AZN):** the 2014 F-6 describes one ordinary share per ADS; the
  2015 F-6 POS exhibit changes this to one half, effective July 27, 2015. The
  June 26, 2015 6-K announces the expected date, and the July 27 6-K confirms the
  change. The 2017-filed 20-F cover corroborates one half and separately marks
  the underlying ordinary shares as not for trading.
- **Preflight regressions:** AEM and CNI combine headings with data; Unilever's
  table contains the Section 12(b) heading; TLK uses a positioned legacy layout;
  EOCC and MTU vary the not-for-trading footnote wording; NWG writes "Trading
  Symbol (s)" with a space and lists both ADS and debt; IBA and KEP use different
  share-class and fractional-ratio wording. ELP demonstrates that an ADS over
  preferred shares is still an ADS, without proving an ordinary-share ratio.
  TV's Global Depositary Shares represent a CPO basket rather than a single
  ordinary class. RCI's abbreviated "Class B Non-Voting" title is retained as
  unknown audit evidence without inferring an unstated security type.
- **Full-run regressions:** PT's 2025 Item 12.D describes its former ratio of
  seven and its current ratio of thirty-five, effective May 16, 2022. ASX's
  2009 cover affirmatively says the common shares trade in depositary form.
  Honda's 2005 cover enumerates its ADS row as `(2)`; that row number must not
  become the denominator of its stated half-share ratio.

The 2024 cover files and preflight covers are contiguous source extracts. AZN files ending in
`combined.html` concatenate verbatim HTML elements from one source document;
the PT ratio-change fixture likewise combines its cover and Item 12.D. The
manifest identifies these selections. Fragment offsets and parser locations refer to
the excerpt, not the complete original filing.

Legacy F-6 and AZN 20-F extracts do not contain a literal trading symbol. The
parser correctly retains an unbound issuer fact; tests do not manufacture a
symbol in those source documents. The loader and dated SQL resolver separately
test the allowed linkage to a single contemporaneously evidenced ADS line.

SQL-only assertions in the test module are explicitly synthetic. They exercise
temporal conflicts, expiry, correction availability and symbol binding rather
than purporting to quote additional filings.
