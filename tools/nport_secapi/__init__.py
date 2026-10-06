"""sec-api.io monthly bulk N-PORT -> ``sec_nport_holdings`` loader CSVs.

The DERA quarterly packages (``tools.nport_dera``) arrive once a quarter and a
month or more after it closes. sec-api.io publishes the same NPORT-P filings as
a monthly ``form-nport`` bulk dataset, refreshed daily, one parsed filing per
JSON line. These modules turn that dataset into the exact CSV the existing
loader (``tools.nport_dera.nport_parallel_load``) already consumes, so the write
path into ``sec_nport_holdings`` stays the one that is tested and verified:

* ``download`` - fetch monthly containers with the ``sec_api`` SDK.
* ``convert``  - containers -> one deterministic loader CSV per report_date.
* ``validate`` - offline sanity over the CSVs (no database).

The 2026-08-06 load of report_dates 2026-02..2026-04 was built from these same
containers by a converter that never reached a repository. This package is its
replacement, and ``src/workers/nport_secapi_monthly`` is the recurring lane that
keeps the table from going stale again.
"""
