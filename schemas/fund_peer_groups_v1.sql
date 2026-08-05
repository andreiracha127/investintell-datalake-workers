-- fund_peer_groups_v1 — EMPIRICAL peer groups, one row per (anchor, series).
--
-- WHAT THIS IS
--   The published output of the frozen partitioner validated over eight quarterly
--   anchors (the P1.6 partitioner, the P1.7 stability evidence): funds are grouped by
--   what they actually HOLD, not by a declared label. One row per served fund series
--   per quarterly anchor. The row says which group the fund landed in, how big that
--   group is, how much portfolio the median pair inside it shares, and — the part the
--   product cannot do without — whether the fund has an empirical group AT ALL.
--
-- THE STATE IS THE CONTRACT, AND THE NULL IS THE FAIL-SAFE
--   ~17% of the universe lands in a community whose median intra-group overlap is
--   below 5%. Those funds do NOT have peers by portfolio composition; presenting
--   their community as a peer group would be a lie the data does not support. They
--   are published with group_state = 'no_empirical_group' and group_id IS NULL.
--   NULL, not a sentinel string, on purpose: the natural peer query is a self join on
--   (anchor_date, group_id), and NULL never joins. A consumer that FORGETS to filter
--   on group_state gets zero peers for those funds rather than a wrong list. The
--   surface decides what copy a fund without an empirical group gets; this table only
--   states the fact.
--
-- granularity IS THE COPY LOCK, MACHINE-READABLE
--   Overlap is measured on a MIXED ruler: equity and everything else at the security
--   (CUSIP-9 / ISIN) level, fixed income collapsed to the ISSUER (CUSIP-6). That was
--   not a convenience — on a pure security ruler only 12 of the 26 coherent
--   fixed-income communities survive, and the two largest go from a median of 0.068
--   and 0.282 at the issuer level to 0.005 and 0.005 at the paper level. Measured on
--   all eight anchors, the ratio of the security-ruler median to the mixed-ruler
--   median sits between 0.43 and 0.62 — a permanent property, not one quarter's
--   caveat. So:
--       'issuer'   (block 'fi')     the copy may say "exposure to the same issuers".
--                                   It may NOT say "similar portfolios" or "the same
--                                   securities".
--       'security' (block 'eq')     security-level similarity. All 57 coherent equity
--                                   communities of the reference anchor stay coherent
--                                   on the pure security ruler.
--       'mixed'    (block 'mixed')  a balanced book measured on both rulers at once.
--
-- WHAT IS DELIBERATELY NOT HERE
--   * No display label, no group name, no description. Naming a group is a product
--     decision made on the surface, under the owner's rule that the surface never
--     reveals where data comes from. A name in this table would leak into every
--     consumer and could never be changed without a republication.
--   * No cross-anchor group identity. group_id is stable WITHIN an anchor and is not
--     a lineage key: a group that splits in two between quarters has no single
--     successor, and pretending otherwise would manufacture continuity that the
--     Jaccard matching measured as absent for exactly those cases.
--   * No sponsor / adviser column. Sponsor dominance was measured and refuted as an
--     explanation of the result (removing every same-sponsor pair moves the pooled
--     median by 1.6% relative); carrying it here would invite a filter nobody
--     measured.
--
-- OWNERSHIP OF THIS DDL
--   The OPERATOR applies this file; the worker NEVER creates or alters a table. It
--   verifies the catalog read-only and fails loud when the catalog and this file
--   disagree. CREATE TABLE IF NOT EXISTS is a no-op against an existing table: it
--   does NOT repair drift. If a column was added, dropped or retyped by hand, this
--   file will run green and change nothing while the worker keeps refusing to
--   publish — which is the intended outcome. Reconcile with an explicit ALTER (or
--   drop and re-apply on an anchor that can be recomputed), never by editing the
--   worker's expectation to match a drifted catalog.
CREATE TABLE IF NOT EXISTS fund_peer_groups_v1 (
    -- The quarter-end the partition was computed AT. Every fund in the row set was
    -- represented by its LAST N-PORT report at or before this date (max lag 4 months
    -- 15 days), so portfolios of slightly different dates are compared to each other
    -- — an inherited limitation, stated rather than hidden.
    anchor_date           DATE        NOT NULL,
    series_id             TEXT        NOT NULL,

    -- The hard class pre-split, at 70% of identified long weight. The three blocks
    -- are DISJOINT GRAPHS: no edge crosses them, so no group can mix a bond book with
    -- an equity book. That is structural, and it was verified as exactly zero
    -- contamination across the 165 fixed-income + equity communities of the reference
    -- anchor. 'mixed' is the residual: neither side reaches 70%.
    block                 TEXT        NOT NULL
        CHECK (block IN ('fi', 'eq', 'mixed')),

    -- '<anchor_date>:<block>:<n>' — n is the partition's own community index at this
    -- anchor (size-descending, ties broken by the smallest member position). NULL
    -- exactly when the fund has no empirical group; see the header note on why the
    -- absence is a NULL and not a sentinel.
    group_id              TEXT
        CHECK (group_id IS NULL OR group_id ~ ':(fi|eq|mixed):[0-9]+$'),

    -- 'empirical'           the fund is in a community of >= 2 members whose MEDIAN
    --                       intra-group overlap is >= 0.05 on the mixed ruler.
    -- 'no_empirical_group'  a singleton, or a community whose median pair shares less
    --                       than 5% of portfolio. The fund exists, was measured, and
    --                       has no peer group by composition. The surface falls back
    --                       to whatever it declares elsewhere; this table does not
    --                       decide that copy.
    group_state           TEXT        NOT NULL
        CHECK (group_state IN ('empirical', 'no_empirical_group')),

    -- Members of the group at this anchor, including this fund.
    group_size            INTEGER
        CHECK (group_size IS NULL OR group_size >= 2),
    -- Median pairwise overlap inside the group, RAW: sum over holdings of
    -- min(w_i, w_j) on the mixed ruler. No inverse-document-frequency discount, no
    -- normalisation — the ruler that FORMS the graph is the ruler that JUDGES it.
    -- Read it as "the median pair of this group holds this fraction of portfolio in
    -- common" (0.146 at the reference anchor, pooled).
    group_median_overlap  NUMERIC
        CHECK (group_median_overlap IS NULL
               OR (group_median_overlap >= 0 AND group_median_overlap <= 1)),

    -- The copy lock. See the header note.
    granularity           TEXT        NOT NULL
        CHECK (granularity IN ('issuer', 'security', 'mixed')),

    -- ── provenance ──────────────────────────────────────────────────────────
    -- One value for the whole anchor: the publication is atomic, so a row set with
    -- two computed_at values would mean a half-applied republication.
    computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    code_commit           TEXT        NOT NULL,
    -- sha256 over the canonical JSON of every parameter that can change the
    -- partition (seed, theta, the 0.70 block threshold, the size cap, the resolution
    -- ladder, the coherence floor, the eligibility rule, the identifier floor). Two
    -- anchors carrying different digests were not produced by the same recipe, and
    -- comparing them as if they were is the mistake this column exists to prevent.
    params_sha256         TEXT        NOT NULL
        CHECK (params_sha256 ~ '^[0-9a-f]{64}$'),

    CONSTRAINT fund_peer_groups_v1_pkey PRIMARY KEY (anchor_date, series_id),

    -- The state and the three group columns are ONE fact, stated three ways. Stated
    -- here so the database, not a comment, forbids a row that claims an empirical
    -- group with no group, or a group with no state.
    CONSTRAINT fund_peer_groups_v1_state_consistent
        CHECK ((group_state = 'empirical') = (group_id IS NOT NULL)
           AND (group_state = 'empirical') = (group_size IS NOT NULL)
           AND (group_state = 'empirical') = (group_median_overlap IS NOT NULL)),
    -- Coherence IS this inequality. A group published as empirical whose median pair
    -- shares less than 5% of portfolio would be exactly the claim the coherence layer
    -- exists to refuse.
    CONSTRAINT fund_peer_groups_v1_empirical_is_coherent
        CHECK (group_state <> 'empirical' OR group_median_overlap >= 0.05),
    -- The ruler follows the block. A fixed-income group measured at the issuer level
    -- cannot be published as security-level similarity, whatever a caller passes.
    CONSTRAINT fund_peer_groups_v1_granularity_follows_block
        CHECK (granularity = CASE block WHEN 'fi'  THEN 'issuer'
                                        WHEN 'eq'  THEN 'security'
                                        ELSE 'mixed' END)
);

-- The read path: "who are the peers of this series?" resolves series_id first, then
-- self-joins on (anchor_date, group_id). The PK already serves (anchor_date, ...);
-- this index serves the series-first lookup and the per-series history.
CREATE INDEX IF NOT EXISTS fund_peer_groups_v1_series_idx
    ON fund_peer_groups_v1 (series_id);
