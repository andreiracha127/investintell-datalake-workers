# Owner legal-default evidence bridge — local preparation only

## Status and authority

This is the independent **bond_default_owner_evidence_v1** product, with the new
agency-free policy **bond_default_owner_event_policy_v1**. It does not change
[legacy default-event policy](../../contracts/bonds/default_event_policy_v1.json),
[legacy publication SQL](../../schemas/bond_credit_publications_v1.sql), public-PIT
agency rating contracts, rating/anchor workers, daily scheduling or any pointer.
No production data, credentials, database or live WorkOS session was accessed.

Legal-event evidence is diagnostic/protective, **never the primary market-PD
numerator**. An unreviewed proposal is not a default. Neither an implied D nor a
valid suggested CUSIP, CIK, LEI, issuer name or CUSIP-6 admits a legal event.
No agency vendor/licence, held-out/development custody split, dual reviewer,
minimum adjudication sample or automatic recall cutoff is required here.

## Boundary with Light

The owner-only Light API owns persisted proposals and append-only decisions. It
verifies the WorkOS JWT subject and accepts only its configured owner. Intended
owner: `user_01M3T6XRJPZD863AT9X0H11B6W`; the bridge requires that independently
configured subject as a parameter, **not authority inherited from JSON**.

An authorized offline operator obtains a Light `bond_default_review_export_v1`
artifact with `owner_sub`, `proposals`, `decisions`, `resolutions`,
`accepted_events`, `payload_sha256`. The importer [owner_evidence](../../src/bonds/default_events/owner_evidence.py)
verifies the canonical SHA-256 of the entire export and each full decision and
resolution request, then validates all identities and predecessor chains.
**Hashes are integrity checks, not signatures or JWT proof.** The importer does
not itself authenticate an export or fetch document bytes. Source hashes,
HTTP(S) URLs and quoted citations are retained as owner/operator provenance;
raw-document byte verification and trusted export acquisition remain operator
responsibilities. No token or backend secret should enter the artifact.

Review chains are keyed by **proposal_id**, not issuer episode. Multiple issues
in one episode retain independent heads. A nullable `source_obligation_id` is
preserved as explicit lineage; legacy episode-only exports remain supported.
A proposal without usable citations stays visible and unreviewed, but a decision
still requires cited evidence. Exact CUSIP-9 checksum, issue scope,
event date/type, seniority and security/collateral statement are required for an
accepted head. The importer independently recomputes the accepted projection
and rejects a supplied projection that disagrees; reject and not-a-default
remain distinct in the preserved complete ledger.

Market-proxy months are time-bounded: a proxy month start must be at or before
the UTC date of the decision's recording instant, the proposal's known
`source_as_of`, and the mandatory bundle knowledge cutoff, for every decision
(including superseded, rejected and not-a-default records) and every proposal,
including unreviewed ones. Owner confirmation is not time evidence, and no
month-closure rule is invented. Refusals are the sanitized codes
`market_proxy_after_recording` and `market_proxy_after_knowledge_cutoff`.

After validating each proposal's decision chain, every known proposal
`source_as_of` and every known proposal citation `public_at` must be at or before
that proposal's **first recorded decision**, regardless of disposition or later
supersession. A later head cannot make a future proposal snapshot available to an
earlier reviewer. Refusals are sanitized `proposal_after_first_decision` or
`proposal_citation_after_first_decision`. Comparisons use UTC instants; equality
is valid. Null legacy dates remain unknown, and unreviewed proposals have no
invented review-origin gate. Existing decision-citation/recording and bundle
knowledge-cutoff checks remain in force. This does not establish import time,
PIT/custody or authenticated acquisition and does not alter unsigned operator trust.

Each event is an owner-acceptance issue proof pinned to its decision, proposal,
source episode and citations. This is not automatic deduplication or incidence
ascertainment across independently created proposals. A descriptive recall
cohort must explicitly identify admitted event IDs, their buckets, model hits
and declared exposure; the caller is responsible for legal-event ascertainment
and selecting a nonduplicated incident cohort. No bulk model-D or issuer-only
matching constructs that cohort automatically.

## Optional severity and rating reference

Acceptance does not require a resolution. Resolution additions preserve estimated
versus realized valuation, form, date and citations. A current accepted event's
`realized_recovery_per_100` is populated only by its latest explicitly realized
resolution. Event-month/following-month confirmed market prices remain
`market_recovery_proxy_not_realized`; they never become realized LGD.

An optional internal rating reference contains product, publication, policy,
parent-panel and last-month pins, without agency action IDs or public-PIT grid.
Missing/stale/mismatched auxiliary binding produces a descriptive warning, not
an EL stop or a requirement to republish legal events on every month boundary.
`recall_diagnostics` requires an explicitly hash-pinned cohort and exposure;
zero accepted events yields unavailable recall, not a zero-default claim.
It never fabricates a sample floor, cutoff, PD estimate or automatic blocker.

## Deterministic offline use

[The CLI](../../src/workers/bond_default_owner_evidence.py) is directly executable
and deliberately not added to the daily dispatcher. Default `validate` and
`dry-run` have no DSN/environment lookup, network or persistence path.

```powershell
.venv\Scripts\python.exe -m src.workers.bond_default_owner_evidence `
  --input <trusted-local-export.json> `
  --owner-sub user_01M3T6XRJPZD863AT9X0H11B6W `
  --code-revision <source-revision-or-local-uncommitted-label> `
  --knowledge-cutoff <explicit-aware-ISO-timestamp>
```

Only an explicit `--output <new-path>` writes an offline bundle; existing files
are not overwritten. Output summaries omit quotes, subject, URLs and secrets.
Malformed input/semantic refusal exits 4; local artifact I/O refusal exits 3.
The JSON report states `offline_verified_not_persisted` and `pointer_changed=false`.

Bundle/publication identity pins source-export digest, producer code digest and
revision label, policy digest, optional internal reference, explicit knowledge
cutoff and event digest. The issuer-mapping digest is actually derived from all
admitted explicit owner issue links (empty links hash `[]`), never an arbitrary
non-null placeholder. The full ledger, including rejected/nondefault histories,
remains embedded in the writer-only bundle payload.

Replaying requires the producing code/policy bytes; a changed producer refuses
an old artifact rather than pretending it is reproduced. Freeze these identities
before any later operational import.

## Explicit persistence API and schema

[New SQL](../../schemas/bond_default_owner_evidence_v1.sql) is additive and was
installed **only in isolated, disposable synthetic PG16 test schemas**, never in
production. This product has its own immutable builds/events and appended
validation receipts: a Light app-DB export cannot truthfully invent the validated
SEC run/package lineage required by the shared SEC ledger. It exposes read-only
`bond_default_owner_evidence_v1_publications` and
`bond_default_owner_events_v1_current` views; legacy v1 qualification is untouched.

`PostgresOwnerEvidenceStore(connection, expected_owner_sub=..., schema=...)`
accepts an explicitly supplied, already provisioned connection. Construction
never connects or changes privileges. `prepare(bundle)` replays the bundle,
persists exact rows in an explicit transaction and verifies read-back. It never
inserts a validation receipt or elects a pointer. `validate(publication_id)`
replays the stored export and compares persisted issue IDs, count, full payload
and ordered digest before appending a receipt; repeated validation is idempotent.
No queued/prepared artifact is falsely labelled validated. The schema checks
payload/column pins, guards UPDATE/DELETE/TRUNCATE and seals issue inserts after
validation. Readers are granted only the two views, not raw proposal histories.

Install in one transaction under the trusted table/function owner, with the intended
permanent schema first in the installation search path. Private schemas are
supported: all six functions bind `pg_catalog, <installation schema>, pg_temp`
(temp last), independent of callers' public/temporary paths. Ordinary writers and
readers must neither own objects, inherit the owner nor have CREATE in that schema.
The owner can bypass protections by changing DDL; it is not an untrusted writer.

The writer gets SELECT/INSERT on immutable evidence and only
`UPDATE(publication_id)` on builds: PostgreSQL requires some UPDATE privilege for
`FOR UPDATE`, while the immutable trigger rejects even a same-value rewrite.
The separate `bond_default_owner_evidence_v1_point(target, expected_current)` is
an **explicit SECURITY DEFINER compare-and-set**, owned by that trusted installer.
Writer EXECUTE is the only pointer-write API; pointer SELECT remains available.
Tokens are created/removed only inside CAS, with no writer token access or direct
pointer INSERT/UPDATE/DELETE/TRUNCATE. Reapply revokes the old unsafe table grants,
not merely omitting them. CAS checks the expected prior publication and matching
validated receipt/bundle/event pins and refuses knowledge-cutoff regression;
zero accepted events is still valid, not an invented sample floor. Neither CLI
nor prepare/validate calls CAS. Reader roles get view SELECT only, not raw histories
or CAS. No production grant, install or pointer move is authorized here.

## Verification and remaining operational work

The focused synthetic suite covers all three dispositions, empty adjudication,
multiple issues in one episode, supersession/forks/idempotency, owner/hash spoof,
exact issue fields, citations, optional realized/estimated severity, market proxy
labels, binding warnings, explicit recall and no-write CLI behavior. Persistence
unit tests use a transactional SQL double and prove count/digest refusal and
absence of pointer calls. They do **not** establish PostgreSQL trigger, role, ACL,
concurrency, installation or live-path correctness.

Run locally without real data or secrets:

```powershell
.venv\Scripts\python.exe -m pytest -q tests/test_bond_default_owner_evidence.py
.venv\Scripts\ruff.exe check src/bonds/default_events/owner_evidence.py `
  src/workers/bond_default_owner_evidence.py tests/test_bond_default_owner_evidence.py
```

The opt-in [PG regression](../../tests/test_bond_default_owner_evidence_pg.py) connects
only to the literal authorized fixture: container `dsh-bde-el-pg-20260930`, label
`dsh.test=bde-el-20260930`, `127.0.0.1:54339`, database `bond_default_review_test`.
It verifies database/user and PG **16.15**, uses 5s lock/60s statement timeouts,
creates one unique `correction_evidence_test_<uuid>` schema and three unique
NOLOGIN owner/writer/reader roles, and cleans only those resources in `finally`.
Operational grant-recipient names are mapped only to these disposable roles;
no operational roles, other tests' schemas or container lifecycle are changed.

```powershell
$env:DSH_OWNER_EVIDENCE_PG_TEST='1' # opt-in flag only; no environment DSN
.venv\Scripts\python.exe -m pytest -q -s tests/test_bond_default_owner_evidence_pg.py `
  tests/test_bond_default_owner_evidence.py
.venv\Scripts\ruff.exe check src/bonds/default_events/owner_evidence.py `
  src/workers/bond_default_owner_evidence.py tests/test_bond_default_owner_evidence.py `
  tests/test_bond_default_owner_evidence_pg.py
```

**Historical SQL-role correction evidence** (2026-09-30): **56 unit + 4 actual PG tests passed**;
focused Ruff and in-memory Python compilation passed. Real non-owner writer
prepare/validate/replay succeeded for 0 then 2 issue events; intended CAS advance
exposed both CUSIPs with `economic_authority=false`. Validated inserts sealed,
narrow row locks succeeded, actual UPDATE/DELETE/TRUNCATE refused, and raw token
insertion/older-pointer update/deletion/truncation were denied (`42501`). CAS
mismatch, unvalidated/bad-receipt target and cutoff regression refused (`P0001`).
Reapply removed deliberately reseeded stale grants. Reader view SELECT succeeded;
raw history/DML and CAS were denied (view DELETE may return `55000` or `42501`).
Private-schema execution with public caller paths and five temp shadow relations
passed; all six fixed paths and installer ownership were queried, not inferred.

Both correction test schemas, with suffixes
`19cd1fa7930e4ba88b2ef3043369175c` and `b2772a1f4e674126ba37681219e3f73d`,
and each schema's `ce_test_<suffix>_{owner,writer,reader}` roles were removed;
absence was queried. Temporary shadows disappeared on connection close.
For that historical SQL-only correction, [producer bytes](../../src/bonds/default_events/owner_evidence.py)
were **not changed**: SHA-256
`baeca798ab24515becf1f8872455a11faac6d130b82f277b150aa12cc8c2a3d0`.
That hash and its evidence remain **historical**, not the corrected P2 source.
A producer edit changes its code digest and requires a fresh bundle and publication
identity; never rewrite source hashes, relabel an old artifact, or attribute an
old-source replay receipt to new bytes.

### P2 first-decision chronology correction (local synthetic)

The old producer reproduced an impossible hash-sealed lineage: proposal source
and a distinct proposal document dated 2020-06-01, first decision recorded
2020-01-20 citing a different valid January document; build and replay admitted
one event. Valid hashes, requests and global identities did not prove chronology.
The correction above rejects that snapshot at the per-proposal review origin.

The [unit suite](../../tests/test_bond_default_owner_evidence.py) now passes
**123 cases (56 existing + 67 new)**. Independent UTC/hash/publication oracles
cover source-only/citation-only/both futures, all three dispositions, all first/head
status combinations with supersession and out-of-order serialization, invalid
forks/predecessors/time regression, per-proposal origins within one episode,
all known citations, exact/offset-equivalent boundaries and one-microsecond
futures. Undated legacy/unreviewed proposals remain valid; bundle cutoff and
existing decision-citation restrictions still refuse futures. CLI refusals expose
only error codes and create no bundle. Fresh fixture helpers and the narrow
[PG provenance assertions](../../tests/test_bond_default_owner_evidence_pg.py)
read the runtime producer SHA instead of hardcoding a new current hash.

Executed locally on the same explicit PG16.15 fixture: **123 unit + 4 actual PG =
127 passed**, focused Ruff over bridge/CLI/unit/PG files passed, and in-memory
compilation of those four Python files passed without bytecode. SQL/store API and
safe-CAS logic were not changed. SQL SHA-256 remains
`16e5ede2a3257e21968cbda76ff66aeafc71e50f0aaaf7e237eb375e6fd8e712`.
Only this run's schema `correction_evidence_test_332475ac619f4ac48432198df96ede17`
and roles `ce_test_332475ac619f4ac48432198df96ede17_{owner,writer,reader}` were
created/removed; absence was queried, temp relations disappeared on close, and
the parent-owned container was preserved.

Corrected producer SHA-256:
`f6041a046425589860e002cf6111960ec6cff717d0639c48b127a1896b185d2a`.
A **fresh in-memory synthetic** standard unit fixture under label
`local-uncommitted-synthetic`, cutoff `2020-12-01T00:00:00Z`, produced publication
`d926a041-a23d-51ca-9927-c368612aee61` and bundle SHA-256
`ee2c664ff138e5c1e567e9086cb83d0091976a15b3918fe46582b80ae12483e4`.
These are new fixture identities, not a relabelled historical bundle, operational
export or authenticated replay receipt. Existing artifacts were preserved.

The earlier read-only, no-bytecode Light cross-wire check remains separate
**historical** synthetic serialization evidence, not authenticated acquisition or
activation. A later rerun must capture the runtime-current producer SHA and
build fresh fixtures; its results must not be attributed to old-source receipts.
Before operational use, separately authorize installation/ACL review and trusted
Light export acquisition/replay under pinned bytes. No production bridge, live
source activation, authenticated transfer, live publication or economic activation is
claimed; legacy SEC lineage, agency and custody gates were not fabricated or changed.
