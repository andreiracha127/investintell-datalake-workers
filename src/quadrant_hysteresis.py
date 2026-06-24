"""Per-axis hysteresis (freeze v1 §5) — the SAME latched-state-machine idea as the
gate's build_rows, but on the axis SIGN instead of risk_on/risk_off.

§5.1 init (no prior): set the sign only if |score| >= AXIS_ENTER AND confidence >=
min; otherwise abstain (transition pending, no quadrant).
§5.2 confirmed (prior sign h): signed_margin = h * score. MANDATORY PRECEDENCE —
evaluate the opposite-switch (signed_margin <= -AXIS_ENTER) BEFORE stability
(signed_margin >= AXIS_EXIT); else deadband. The opposite-switch case keeps the
internal memory flipped even if confidence is too low to publish (so the next day
can tell 'was up, transitioning' from 'never confirmed').

Returns (internal_sign, effective_sign, transition_pending, reason):
  internal_sign  : latched memory, preserved across deadband (persisted as *_sign)
  effective_sign : consumable sign; NULL whenever in transition / low-confidence
  transition_pending : True in init-abstain / deadband / withheld switch
  reason         : audit tag for transition_reason
"""
from __future__ import annotations

AXIS_ENTER = 0.25
AXIS_EXIT = 0.10


def axis_hysteresis(
    prev_sign: int | None,
    score: float,
    candidate_confidence: float,
    *,
    enter: float = AXIS_ENTER,
    exit_: float = AXIS_EXIT,
    min_confidence: float,
) -> tuple[int | None, int | None, bool, str | None]:
    confident = candidate_confidence >= min_confidence

    if prev_sign is None:
        # §5.1 initialization.
        if abs(score) < enter:
            return None, None, True, "init_below_enter"
        if not confident:
            return None, None, True, "init_low_confidence"
        sign = 1 if score > 0 else -1
        return sign, sign, False, "init"

    # §5.2 confirmed state — precedence: opposite-switch BEFORE stability.
    signed_margin = prev_sign * score
    if signed_margin <= -enter:
        new_sign = 1 if score > 0 else -1
        if confident:
            return new_sign, new_sign, False, "switch"
        # opposite evidence sufficient but not confident: flip memory, withhold.
        return new_sign, None, True, "switch_low_confidence"
    if signed_margin >= exit_:
        if confident:
            return prev_sign, prev_sign, False, "hold"
        return prev_sign, None, True, "hold_low_confidence"
    # deadband: keep internal memory, publish no quadrant.
    return prev_sign, None, True, "deadband"
