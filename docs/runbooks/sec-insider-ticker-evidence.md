# SEC insider ticker evidence

`scripts/load_sec_insider_filings.py` loads issuer CIK and trading-symbol statements
from Section 16 Forms 3, 4, 5 and amendments. DERA quarterly `SUBMISSION.tsv`
provides 2006 onward; sec-api.io's monthly original-document archives provide
ownership XML from May 2003 through December 2005. The dataset catalogues contain
complete container lists; the loader consumes every requested monthly archive.
Early non-XML filings yield no fact but are recorded as package members, so XML
learned for them later is dated by reconciliation. An archive whose filings all lack
XML is still reconciled. Unusable raw symbols are kept for audit.

Each archive holds one `YYYY-MM/<accession>/` directory per filing, and each directory
must yield exactly one metadata record for its own accession. Otherwise the package is
refused before anything is retired. The catalogue's `records` field is not a filing
count (1,458,595 records against 697,814 filings).

The parser imports W1's `normalize_symbol` without changing W1. Its extra rules
unwrap filer punctuation and remove exchange, OTC, country and when-issued markers.
Placeholders and prose (`NOT LISTED`, `SEE REMARK`, `LEE ENT`, `XPEL, INC.`) are
rejected whole before any split. Since `sec_insider_v4`, `TRUE` and `OB` are symbols
(TrueCar and Centrue; OneBeacon and Outbrain), as in W1. A lowercase or mixed-case
`true` stays a boolean, `OB` beside a symbol still qualifies it (`EDLG, OB`), and
`FALSE` and `OTCBB` remain placeholders. Explicit lists and slashes split (`LTR;CG`,
`ABI/CRA`, `Z AND ZG`); a space splits only class variants of one root
(`CRDA CRDB`). Class and preferred suffixes stay attached (`BRK/A`, `HFC PrB`),
and a lone listed class names its sibling (`BWINA / B`). The source has no class mapping.

## Point-in-time query

```sql
SELECT * FROM sec_insider_ticker_issuer_at('BRK-A', DATE '2010-12-31');
```

The window is `[D - 365, D)`. Evidence must also be known by D and not retired at
D. Initial DERA facts become known at `filed + 1`; XML uses the acceptance date
in New York. A correction to a previously seen accession becomes known no
earlier than reconciliation. A package is staged completely before one atomic
transaction updates its facts and validators. Withdrawn versions are retired,
not deleted, and unchanged content keeps its original availability.

The resolver counts distinct accessions per CIK, with separator-free ticker
matching. It needs at least two filings on two dates. A sole candidate resolves;
with competing CIKs, the leading candidate must have at least three times the
runner-up's filings. Inadmissible minority CIKs still participate in conflicts.
Other conflicts return `ambiguous` with a null CIK. Empty or insufficient
single-candidate evidence returns `none`. Counts describe the leading candidate
even when it does not resolve, and `window_end` is exclusive.

This function is independent evidence. Cover precedence and admission wiring are
reserved for a later PR after W1; the consumer will use it only for `missing`.
No present-day mapping or `universe_constituents.cik` participates in resolution.

## Owner-authorized production steps

The owner authorizes production schema application and loading separately.
Configure the existing owner connection in `DATABASE_URL` and the provider key
in `SEC_API_IO_KEY` or `SEC_API_KEY`, using the environment's secret mechanism.
Run these exact commands from the repository root in PowerShell:

```powershell
psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_insider_ticker_evidence.sql
python -m scripts.load_sec_insider_filings --dsn $env:DATABASE_URL --packages-dir E:/investintell-data/w1b/dera --secapi-dir E:/investintell-data/w1b/secapi --download-dera --verify-cache --download-secapi
```

The migration is idempotent and assigns ownership to `worker_writer` where that
role exists. Runtime and read-only roles receive only read/function access. The
loader requires the schema to exist and does not apply it implicitly. For a Linux
worker, replace both cache paths with persistent paths mounted for that worker.

Use the same load command for incremental runs: package hashes and parser
versions skip unchanged parses; remote validators detect republication. A new parser
version re-parses every package. Only readings that change are retired and inserted
again, available from the reconciliation date; unchanged facts keep their rows. The
loader sets `temp_buffers` to 128 MB at connect, before the session's first temporary
table. PostgreSQL 18 fails the largest DERA quarter's staging COPY (2006q1, 83,657
filings) at the 8 MB default with "no empty local buffer available". Downloads
stage to temporary files before replacing the cache. sec-api credentials travel
in an Authorization header; console errors scrub secrets. SEC requests use
`InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)` and are sequential.

An owner-authorized rollback is:

```powershell
psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_insider_ticker_evidence.rollback.sql
```

## Local reproduction

Create a uniquely named `timescale/timescaledb:2.27.2-pg18` container/database (the
production PostgreSQL 18.4 and TimescaleDB 2.27.2), apply the schema, and
point the same loader at the task's local DERA and sec-api cache directories.
The loader can also download without connecting (`--download-only`) or parse
without loading (`--dry-run`).

Read-only production eligibility snapshots use only ticker, first price and
W1's point-in-time cover result. This script fixes the identity to `mcp_ro`, sets
`PGOPTIONS` to read-only with a 30-second statement timeout and refuses heavy
exports between 06:00 and 08:30 UTC:

```powershell
python -m scripts.validate_sec_insider_ticker_evidence --export-only --snapshots E:/investintell-data/w1b/production --psql 'C:/Program Files/PostgreSQL/18/bin/psql.exe'
python -m scripts.validate_sec_insider_ticker_evidence --local-dsn postgresql://postgres@127.0.0.1:55461/w1b --snapshots E:/investintell-data/w1b/production
```

Exports are written atomically. An interrupted export resumes only complete CSVs
(psql header and final newline) and re-exports the rest; use a new empty snapshots directory
for a new production snapshot. Validation first checks that `production_snapshot.json`
lists exactly the expected exports from `mcp_ro` and that every file matches its SHA-256.
It then writes `coverage.json`, including the
coverage table, cover agreement and numerical clean-handover evaluation.

Run the focused SQL/parse/downloader regressions one file at a time:

```powershell
$env:PYTEST_WORKERS='2'
$env:SEC_INSIDER_TEST_DSN='postgresql://postgres@127.0.0.1:55461/w1b_test'
python -m pytest tests/test_sec_insider_ticker_evidence.py -q
```

Database tests create isolated schemas in the explicitly supplied disposable
database. They never use the application's default connection or production.
