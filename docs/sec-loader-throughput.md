# SEC loader throughput and provider policy

`scripts/sec_provider_transport.py` supplies one SQLite permit ledger to W1,
W1b and W1c. Independent Python processes, threads and Windows spawned workers
reserve every request in a transaction. All `sec.gov` hosts share a government
bucket, including `www.sec.gov` and `data.sec.gov`. Retries consume permits;
redirects reserve their destination before urllib sends another request.

The default state path is `%PROGRAMDATA%/Investintell/sec-provider.sqlite3` on
Windows and `/tmp/investintell-sec-provider.sqlite3` on Unix. Set
`SEC_PROVIDER_STATE_PATH` to the **same absolute local-disk path** in every
loader invocation. For the Priority A validation, use a path under
`C:/investintell-data/limitations-program/A/`. The ledger records only bucket
names, limits and timestamps; it does not contain credentials or URLs. Directory
permissions must permit participating service accounts to access that ledger.
Coordination failures stop requests rather than bypass the governor. Do not
place SQLite state on a network filesystem.

| Environment variable (`SEC_PROVIDER_` prefix) | Default | Shared scope |
| --- | ---: | --- |
| `GOVERNMENT_RPS` | 10 | Every SEC government hostname, combined |
| `MIRROR_RPS` | 150 | `edgar-mirror.sec-api.io` |
| `ARCHIVE_RPS` | 40 | `archive.sec-api.io` |
| `DOWNLOAD_BUDGET` | 15000 | Mirror, archive and API download products combined |
| `DOWNLOAD_WINDOW` | 300 seconds | Rolling account download window |
| `API_HOST_RPS` | 40 | Products sharing `api.sec-api.io` |
| `QUERY_RPS` | 2 | Query product, conservative configurable default |
| `FULLTEXT_RPS` | 20 | Full-text product |

Limits are positive; the government cap cannot exceed ten requests in one
second. The mirror and archive short caps cannot exceed the documented
technical ceilings of 200 and 50 requests per second. API `/datasets` downloads
also consume the archive short-window and combined account-download budget.
Short windows are paced as well as checked against rolling counts.
The stricter count and longer window advertised by any participant persist
in the ledger, so a concurrently started process cannot relax an established
policy. To raise a confirmed quota from 15000 to 60000, stop **all** participating
loaders, wait a full maximum quota window (at least 300 seconds after the final
request, and longer if an active Retry-After cooldown requires it), then restart
all participants with the confirmed budget and one newly agreed ledger path.
Keep the old ledger as audit evidence. An individual process restarting with a
higher limit on the original ledger cannot relax the persisted policy.
Do not change the state path to evade active permits or cooldowns.

The default paid download entitlement is 15000 per five minutes, which permits
at most 50 requests per second on average across download endpoints despite
the larger mirror short-window ceiling. A confirmed account entitlement can
configure 60000 per five minutes. A successful access probe and missing quota
headers do not establish that entitlement. Technical and pricing documentation
currently differ: the [download documentation](https://sec-api.io/docs/sec-filings-render-api)
publishes higher technical capacities, while [pricing](https://sec-api.io/pricing)
defines plan entitlements. Full-text and Query remain distinct product budgets.
The government policy follows the [SEC webmaster guidance](https://www.sec.gov/files/about/webmaster-faq.htm).

`429` publishes its numeric or HTTP-date `Retry-After` into every affected bucket,
including the shared account-download bucket. All waiting callers recheck it
before sending. A pause exceeding the configured maximum permit wait (600
seconds by default) fails that operation instead of truncating `Retry-After`.
Paid-provider authentication and plan errors (`401`/`403` on an authenticated request) stop
immediately: the transport raises `ProviderAuthError`, which no loader fallback, discovery or
parse loop absorbs, so a refused paid request never retries through sec.gov and an aborted W1c
parse marks its staging manifest incomplete. An unauthenticated sec.gov denial carries no
credential and stays an ordinary transport failure that may enter cache recovery.
Retryable transport failures, `408`, `429` and selected `5xx` statuses have a
bounded attempt count; endpoint-specific statuses can be explicitly added.

Authenticated requests require HTTPS and a trusted paid hostname. Credentials
are not forwarded across origins on redirects. Error messages contain a status
and a URL with query, fragment and userinfo removed; provider error bodies and
raw exception text are not displayed. Callers must keep canonical SEC URLs as
cache/provenance identities. `filing_download_url` selects the mirror when a key
is configured, without modifying the canonical URL or downloaded bytes. Caller
stream-integrity and maintenance-page validation still run before publication.

This coordination scope is one machine with one shared filesystem ledger.
Separate machines or replicas do not coordinate merely because they use the
same API key. Deployment policy must keep one active downloader replica or
allocate account-wide budgets conservatively across machines using a shared
external coordinator. Government source-IP limits also require deployment-wide
coordination when separate hosts share an egress IP. Local CPU counts do not
establish Railway CPU or memory limits; production counts require measured
container resources and separate rollout authorization.

One replica **per service** is insufficient when several services share a key:
the single active downloader policy must cover every participating service and
manual run. Without an external coordinator, divide both short-window and
five-minute budgets by the maximum number of simultaneously active machines,
including replicas and manual runs. Do not deploy the full account entitlement
independently to each container. Government budgets must similarly share the
ten-request ceiling across machines using the same egress IP.

## Parsing, immutable shard plans and offline replay

W1c uses Windows-compatible spawned processes for parsing. Each process loads
the parent-wide binding context once; individual tasks contain one canonical
URL and its issuer candidates. At most twice the worker count is outstanding.
Parsed rows retain source-package order and each parser's row order. A failed
reparse marks its staging manifest incomplete and retains the last complete
evidence file. Parser versions, evidence hashes and detector semantics are not
changed by this throughput work.

`--workers` is an upper bound. Effective CPU affinity, cgroup CPU quotas, current
available RAM and cgroup memory headroom cap it further. The CPU ceiling leaves
four logical cores free. `SEC_PARSE_WORKER_MEMORY_MB` defaults to 600 MiB; set it
from a representative measured pilot with a margin. The default memory reserve
is at least 2 GiB on a host with 4 GiB or more capacity and at least 25 percent
of available RAM; small containers retain a proportional reserve.
`SEC_PARSE_MEMORY_RESERVE_MB` can pin a larger reserve for CI and agents. Parsing
fails before starting a pool when no worker fits that reserve.

For shard collection, `prepare --parts N --workers TOTAL` pins both values in
the immutable version 2 plan. Every shard receives a disjoint allocation from
that one total, rather than a full pool of its own. Resume follows the prepared
values and rejects explicit changed values. Concurrent collection of the same
shard is rejected by an OS-held lock, which the kernel releases after a crash.
Different independent plans still require an operator to allocate one total
machine budget across them; this resource helper does not coordinate arbitrary
unrelated pools.

Use `--offline --manifest INPUT --raw-cache-dir ORIGINAL --cache-dir STAGING`
with a separate staging directory and output outside the original cache.
Original bytes and recovery metadata are read from ORIGINAL; PDF temporary
files, fresh parsed spools and the updated manifest stay in STAGING. Missing or
corrupt originals fail clearly. Existing `parsed/*.jsonl` and evidence files
are never reused as parse results.

`scripts/benchmark_sec_foreign_listing_throughput.py` requires `psutil` and is
the reproducible final5 benchmark harness. It freezes input identities, blocks
network access in parent and spawned children, records extractor versions and
sampled CPU/process-tree RAM, and verifies the generated evidence hash. Whole
runs must use new output directories on C:. `--pilot-count 200` selects large
documents, PDFs and source strata while retaining full parent bindings. Its
comparison hash is computed from canonical rows only as an oracle; those rows
are not used to produce the output. The implementation report records exact
commands and measured results, including the frozen old-path source.

The provider tests use a local fake HTTP server, a fake clock and independent
Windows-spawn processes; they never call provider services. Run the file alone
with `PYTEST_WORKERS=2`. Keep pytest temporary directories and new benchmark
outputs under `C:/investintell-data/limitations-program/A/`. Full offline parsing
must consume raw cached documents and reproduce canonical evidence byte for
byte, preserving extractor versions, parser versions, canonical provenance,
source ordering, shard coverage, parent-wide binding proofs and fact hashes.
Benchmark measurements and resource findings belong in the implementation
report; derived estimates must be labeled as estimates.

## Priority A measured acceptance (2026-10-09)

The chief engineer completed `full-new-w6` from the final5 raw cache, with
network disabled and fresh C: staging: 47,329 source documents, 29,709 rows,
zero failures. The evidence was independently rehashed to
`9ca17573dd649db4075a7eb40065274e7e0b1d536649b553c4b790c3dfc1accc`.
Whole wall time was 1405.732843 seconds (23 minutes 25.73 seconds); parsing
was 1404.523338 seconds. Six parse workers had affinity to 20 logical CPUs
and averaged 5.56 observed CPU cores. Peak simultaneous process-tree RSS,
sampled once per second, was 2,133,454,848 bytes (1.987 GiB). Peak OS process
count was 14, including launchers and PDF tools.

The owner intentionally interrupted the old eight-thread, two-CPU-affinity
baseline after 1896.342773 seconds. Its last progress recorded 13,558 source
documents (13,400 unique URLs), zero failures. This is **not** a completed
baseline wall time or a byte-identical full old output. Linear extrapolation
from that partial sample gives about 110.33 minutes and an **estimated**
4.71x speedup; workload variation and concurrent activity make that estimate
uncertain. The owner instructed that neither whole benchmark be restarted.

Both benchmark source pins match the delivered files: loader SHA-256
`e25245fc8a0d95e6cce1b0c414a5d30859afc77be1cc6e131d27b46d0453fb0f`,
parser SHA-256
`327e35481945681e416611404815a188de53430df9072e748001b7660edb96ce`.
The financial parser file is unchanged from the base. Python 3.12.12,
Git-bundled pdftotext 4.00 and pypdf 6.20.0 were recorded.

Local focused suites total 1,182 passing cases, including database regressions
on PostgreSQL 18.4 / TimescaleDB 2.27.2, Windows spawn, cross-process quota
coordination, offline recovery, immutable shard plans, and exact W1 fixture
bytes/facts with one and multiple download workers. CI is left to the chief
engineer's independent gate; local validation does not establish CI success.
