# open_macro_v03 Reference Sleeve Candidate (Phase 0Q supplement 002)

Status: candidate_not_approved — quant_owner decision pending. A5=blocked, runtime_activation=false, allocator_publish=false.

## What this is (and is not)

A 6-ETF reference portfolio used ONLY to measure the quadrant decision path (turnover, max drawdown, annualized volatility, stress behavior, walk-forward OOS). It is **not** a productive allocation and **not** the official allocator — the official open_macro_v03 output is the quadrant/tilt signal; the allocator lives in a separate repository and stays untouched.

## Instruments and coverage

| Ticker | Role | eod_prices coverage |
|---|---|---|
| SPY | equities / growth risk | 1993-01-29 → current |
| TLT | nominal duration (IEF absent from eod_prices) | 2002-07-26 → current |
| TIP | inflation-linked duration | 2003-12-05 → current |
| GLD | gold / inflation hedge | 2004-11-18 → current |
| DBC | broad commodities | 2006-02-06 → current |
| SHY | cash / defensive | 2002-07-26 → current |

Full sleeve priced from **2006-02-06** → GFC_2008 is covered on the price side (vintage side is reduced-coverage there per `harness_window_policy.json`).

## Quadrant → baseline weights

| Quadrante | SPY | TLT | TIP | GLD | DBC | SHY |
|---|---|---|---|---|---|---|
| Q1 G↑ I↓ (goldilocks) | 60% | 20% | 5% | 5% | 0% | 10% |
| Q2 G↑ I↑ (reflação) | 45% | 5% | 15% | 10% | 15% | 10% |
| Q3 G↓ I↑ (estagflação) | 15% | 5% | 25% | 20% | 15% | 20% |
| Q4 G↓ I↓ (bust) | 15% | 40% | 10% | 10% | 0% | 25% |

Constraint baselines the calibration `*_delta_pp` offsets apply to: `risk_cap = 0.65` over {SPY, DBC}; `defensive_floor = 0.20` over {TLT, SHY, TIP}. Every row satisfies both (guard-tested arithmetic).

## Costs

Base candidate **5 bps one-way** (not final approval). Every scenario-grid cell runs the full sensitivity grid **0 / 5 / 10 / 25 bps** so threshold conformance is judged across the cost envelope, not at a single point.

## Rebalance

Monthly, month-end decision date, latest PIT quadrant; trade on quadrant change or weight drift > 5pp.

## Fallback / proxy policy

- Pre-inception (binding: DBC before 2006-02): drop + renormalize remaining weights (run labeled `reduced_sleeve`).
- Missing price on trade date: defer trade to next session with data; never interpolate.
- Substitutes (transparency only, switching requires a new proposal + sign-off): DBC→GSG, GLD→IAU, TLT→IEF/AGG, SHY→BIL.

## Survivorship & data quality

Survivorship bias acknowledged: currently-listed ETFs chosen ex-post — acceptable only because the sleeve measures regime-switching behavior, not instrument selection skill. Checks (any trigger marks affected cells `reduced_quality`): no negative/zero adj_close; no >3-day gaps inside coverage; split/dividend adjustment consistency; zero-volume runs >5 sessions flagged; every run emits a `data_quality` section.
