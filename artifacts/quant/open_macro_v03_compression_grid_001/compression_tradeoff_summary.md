# open_macro_v03 compression mini-grid — trade-off summary

**MEASUREMENT ONLY.** This package measures four reference-sleeve compression variants to inform the quant_owner's sleeve and OOS bounds decisions. Nothing here replaces the baseline (`replaces_baseline` is false everywhere), A5 stays **blocked**, `runtime_activation`/`activation_allowed`/`allocator_publish`/`official_result` are all false, `db_write_mode` is `none`, and no gate verdict grants activation or approval. Status: `candidate_not_approved`.

## Naming convention

`compressed_N` = **N% of the original inter-quadrant distance RETAINED**; the compression factor (fraction of the distance each quadrant weight vector is moved TOWARD the four-quadrant mean) is `1 - N/100`. `baseline_100` = factor 0.0 (no compression). `compressed_50` is numerically identical to `sleeve_compressed_50` in evidence_002 (verified as a consistency test); `compressed_25` (75% moved toward the mean) is the most compressed of the grid.

## Frontier at 5 bps (primary window 2014-03..2026-06)

| variant | factor | ann. turnover | ann. vol | max DD | window return |
|---|---|---|---|---|---|
| baseline_100 | 0.00 | 1.6103 | 0.0821 | 0.1497 | 103.48% |
| compressed_75 | 0.25 | 1.3112 | 0.0772 | 0.1503 | 109.25% |
| compressed_50 | 0.50 | 1.0271 | 0.0738 | 0.1510 | 114.73% |
| compressed_25 | 0.75 | 0.7433 | 0.0722 | 0.1564 | 120.86% |

## Cost sensitivity (annualized turnover is cost-invariant; return net of cost)

| variant | ret 0bps | ret 5bps | ret 10bps | ret 25bps | MDD 0bps | MDD 25bps |
|---|---|---|---|---|---|---|
| baseline_100 | 104.41% | 103.48% | 102.55% | 99.79% | 0.1493 | 0.1513 |
| compressed_75 | 109.99% | 109.25% | 108.52% | 106.34% | 0.1500 | 0.1515 |
| compressed_50 | 115.26% | 114.73% | 114.21% | 112.64% | 0.1507 | 0.1518 |
| compressed_25 | 121.17% | 120.86% | 120.55% | 119.64% | 0.1563 | 0.1572 |

## Monotonicity observations

- **Turnover** decreases monotonically with compression: 1.6103 (baseline_100) -> 1.0271 (compressed_50) -> 0.7433 (compressed_25).
- **Volatility** decreases monotonically: 0.0821 -> 0.0738 -> 0.0722.
- **Window return** increases monotonically: 103.48% -> 114.73% -> 120.86%.
- **Max drawdown** rises slightly with compression (0.1497 -> 0.1510 -> 0.1564), the only metric that worsens; it stays well within the 0.25 base bound for every variant.

## Does compressed_50 still (near-)dominate?

`compressed_50` improves on the baseline across turnover, volatility and return with an essentially unchanged drawdown, so it remains a strong leading alternative candidate. It is NOT a strict Pareto dominator of the whole grid: `compressed_25` posts lower turnover / vol and higher return still, at the cost of a higher (but in-bounds) drawdown. `compressed_50` is the balanced point that keeps drawdown nearest the baseline while capturing most of the turnover/vol/return gains — hence its `leading_alternative_candidate` flag. The final sleeve choice is the quant_owner's.

## OOS cross-fold dispersion (9 folds, 5 bps) — CRITICAL for the bounds decision

| variant | MDD max-dev (bound 0.08) | vol max-dev (bound 0.05) | verdict |
|---|---|---|---|
| baseline_100 | 0.10144 | 0.07079 | no_go_bounds_under_review |
| compressed_75 | 0.11027 | 0.06572 | no_go_bounds_under_review |
| compressed_50 | 0.10660 | 0.06071 | no_go_bounds_under_review |
| compressed_25 | 0.10132 | 0.05575 | no_go_bounds_under_review |

**Key finding:** compression reduces cross-fold **volatility** dispersion monotonically but NOT the **MDD** dispersion, and NEITHER clears its current bound at any compression level. The cross-fold MDD max-deviation stays around 0.10-0.11 (> 0.08 bound) for all four variants, and the volatility max-deviation stays ~0.056-0.071 (> 0.05 bound). Compression therefore does **not** by itself bring OOS dispersion inside the current bounds. The OOS verdict label stays `no_go_bounds_under_review` for every variant; the bounds are UNTOUCHED and the decision is deferred to the quant_owner.

## Constraints

Every variant satisfies the constraints for all four quadrant targets: weights sum to 1, risk assets <= risk_cap (0.65), defensive assets >= defensive_floor (0.20). Compression moves the quadrant vectors toward their common centroid, which lowers risk weight and raises defensive weight, so no renormalization enforcement was triggered for any variant (`any_renormalization_applied` is false throughout).

