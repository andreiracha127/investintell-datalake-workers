# Local N-PORT fixed-income materializer

`build_nport_fixed_income_features` is intentionally unsupported in PostgreSQL.
Operate the pipeline in four separate commands: `bootstrap`, `extract`, `compute`, then
`publish`. `bootstrap` creates the local sentinel, seeds the exact prepared build
identity, prepares the target, and installs the SHA-pinned oracle. `compute`
loads the physical snapshot first, then validates/current-points the source
holdings publication before invoking the oracle. Every invocation must provide
an explicit source publication/run/
package, target publication, as-of date, contract digest, and nonzero timeout
limits. The extractor uses a read-only session and a simple relation-by-relation
COPY; the compute command runs the guarded local PostgreSQL oracle and writes
target-shaped artifacts; the publisher
only accepts the manifest's target-shaped payloads and never reads raw inputs.

`compute` connects only to a separately bootstrapped local PostgreSQL instance.
That database must have a name beginning with `nport_local_` or `fi_local_` and
an `nport_local_materializer_sentinel` row whose `local_materializer` flag is
true and whose `run_uuid` exactly matches `--local-run-uuid`. It must contain
the minimal SEC/N-PORT schema and the local-only oracle installed by
`install_local_oracle`; production DSNs, credentials, and endpoints never enter
the container or artifact.

Before starting the local instance, pin the PostgreSQL 18 image by digest and
set an explicit memory, CPU, temporary-disk, statement, lock, idle-transaction,
and client-watchdog limit. The local-build watchdog must be at least as long as
the local SQL timeout (the default envelope is eight hours). Run the container
on an internal Docker network so it has no outbound path. The materializer
loads only the extracted snapshot, invokes the
guarded local oracle, and exports deterministic gzip TSVs for all eight target
relations. It does not calculate any metric in Python or DuckDB.

Extraction writes streaming gzip COPY snapshots of only the three physical base
relations. The manifest accepts exactly those inputs and all eight contract
outputs, with exact column lists, approved oracle hash, PostgreSQL 18 image
digest, server fingerprint, and resource limits. `publish` owns one transaction;
any COPY failure rolls back rows, manifest, validation, and current-pointer work.

The example identifiers in the task brief are suitable only as operator
examples; the CLI has no defaults for identity or contract values. Keep the
artifact directory immutable between `compute` and `publish`: a payload or
manifest hash mismatch fails closed. A repeated publish is accepted only if the
same target identity, manifest, all output counts, and validated/current target
publication are already present.
