# RR1 fee facts: what `occurrence` means, and why the consumer must not rank on it

Measured against the local mirror of the datalake on 2026-08-02, over the current
publications (`rr1_fee_profile_v1` = `0a432e7a-4755-56ec-882f-87778d48c4a4`,
`sec_regulatory_serving_v1` = `a1e9cc83-fa1a-5d39-9d3b-299891195617`).

## The question

The app found that for one `(instrument, class, concept)` there are rows tied on
`(effective_date, filing_date, accession)`, so the winner ends up decided by an
`ORDER BY` in the consumer. The ties come from rows that differ only in
`document_id` / `occurrence`. Are those a parser artefact to dedupe at the
producer, or a legitimate grain the consumer has to disambiguate?

## What the producer actually emits

`rr1_fee_profiles` is keyed by the RR1 fact **context**, not by the share class:

```
PRIMARY KEY (publication_id, source_run_id, accession_number, series_id, class_id,
             data_date, measure_id, document_id, dimensions, occurrence, canonical_concept)
```

`build_rr1_fee_profiles` takes the DISTINCT set of contexts and **cross-joins it
with all seven canonical concepts**. Every context therefore yields seven rows,
and a concept the context did not report becomes an `unavailable` row with
`reason_code = 'concept_not_reported'`.

Current publication: `2 313 955` rows = `330 565` contexts x 7 concepts.

## Where the ties come from

Grouping by `(accession, series_id, class_id, canonical_concept, effective_date,
filed_date)` over `sec_current_rr1_fee_profiles`:

| | groups | of which the value disagrees |
|---|---:|---:|
| groups total | 2 301 600 | — |
| **tied (more than one row)** | **12 271** | **257** |
| tied, varying only on `document_id` | 7 154 | 162 |
| tied, varying only on `occurrence` | 2 821 | **0** |
| tied, varying only on `data_date` | 2 191 | 50 |
| tied, varying only on `dimensions` | 98 | 42 |
| tied, varying only on `measure_id` | 7 | 3 |
| tied, varying on more than one of them | 0 | — |

Same decomposition on the surface the app actually reads
(`sec_regulatory_serving_facts`, `family='rr1_fee'`, `grain_origin='class'`,
`fact_key = concept|measure|document|dimensions|occurrence|data_date`):

| | count |
|---|---:|
| serving rows | 2 297 505 |
| groups | 2 285 178 |
| tied groups | 12 243 |
| tied where `occurrence` is the ONLY varying axis | 2 793 |
| ... of those, value disagrees | **0** |
| ... of those, exactly ONE row is `available` | 1 236 |
| tied on any other axis | 9 450 |
| ... of those, value disagrees | 257 |

## Verdict

**`occurrence` is not a semantic grain.** It is RR1 `num.tsv`'s `iprx`, the SEC's
own sequence number for facts that would otherwise be identical inside one
submission. The source rows carry no distinguishing content:

```
accession 0000894189-21-001434, S000071206 / C000226000, ManagementFeesOverAssets
  iprx=0  value=0.0030  dcml=4 dimn=2 uom=pure  measure/document/otherdims/footnote all null
  iprx=1  value=0.0030  dcml=4 dimn=2 uom=pure  measure/document/otherdims/footnote all null
```

Across the whole current publication, **zero** of the 2 821 occurrence-only tie
groups disagree on the value. So no choice between occurrences can change a served
number — the only thing at stake is whether the consumer picks a reported value or
a manufactured filler.

**`document_id`, `dimensions`, `measure_id` and `data_date` ARE legitimate grains.**
One filing can carry several prospectus documents for the same class, and 257 tie
groups genuinely disagree on the value along those axes. A consumer needs a rule
for them regardless of what happens to `occurrence`.

## The trap the app's review caught

At a context whose `occurrence` exists only because a *different* concept was
repeated, the concepts that were reported once carry nothing but their
`unavailable` filler at the higher occurrence. The example above, in full:

| concept | occ 0 | occ 1 |
|---|---|---|
| management_fee | available 0.0030 | available 0.0030 |
| distribution_12b1 | available 0.0000 | **unavailable** |
| gross_expense | available 0.0030 | **unavailable** |
| other_expense | available 0.0000 | **unavailable** |
| acquired_fund_expense | unavailable | unavailable |
| net_expense | unavailable | unavailable |
| waiver_reimbursement | unavailable | unavailable |

1 236 serving groups have exactly this shape. Appending `document_id`/`occurrence`
to the app's rank naively makes occurrence 1 win and regresses those slots to
`unavailable` — the 191 regressed slots the app-side review measured.

Note the corollary about vocabulary: `unavailable` + `concept_not_reported` on an
RR1 fee row means *not reported in this context*. It does **not** mean the class
never reported the concept.

## Recommended consumer rule (registered)

For one number per `(series, class, canonical_concept)`, rank:

1. latest `data_date` (the current fiscal period);
2. latest `effective_date`, then latest `filing_date`, then accession — as today;
3. `status IN ('available','degraded')` **before** `unavailable`;
4. lowest `occurrence`;
5. only then a deterministic tie-break on `document_id` / `dimensions` /
   `measure_id`.

Steps 3 and 4 must stay in that order. Step 3 alone already removes every
occurrence-driven regression; step 4 only makes the remaining choice
deterministic.

## Why the dedupe was NOT implemented in the producer

Collapsing `occurrence` out of the grain is provably value-neutral on the measured
data, but it is not a local change:

* `occurrence` is in the primary key of `rr1_fee_profiles` and in the
  `fact_key` of `sec_regulatory_serving_facts`, which the app pins through
  `SURFACE_DIGEST`;
* `build_rr1_fee_profiles` and `rr1_fee_profile_build_is_closed` both fail closed
  on `count(*) > 1` per context+concept, so simply dropping `occurrence` from the
  grain turns 2 821 groups into `conflicting RR1 fee facts` and aborts the build;
* both products are immutable per publication, so the change means republishing
  2.3M profile rows and re-materialising the serving surface.

That is a planned republication with an app-side contract bump, not a backlog
edit. Until it happens the semantics above are the contract, and they now ship as
`COMMENT ON COLUMN` on `rr1_fee_profiles` so a consumer can read them from the
database instead of guessing.
