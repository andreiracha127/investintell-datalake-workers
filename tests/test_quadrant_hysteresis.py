from __future__ import annotations

from src.quadrant_hysteresis import AXIS_ENTER, AXIS_EXIT, axis_hysteresis

MINC = 0.70


def test_constants_frozen() -> None:
    assert AXIS_ENTER == 0.25 and AXIS_EXIT == 0.10


def test_init_confirms_when_strong_and_confident() -> None:
    # §5.1: no prior, |score|>=ENTER and confidence>=min -> initialize the sign.
    internal, effective, pending, reason = axis_hysteresis(
        None, 0.30, 0.90, min_confidence=MINC)
    assert internal == 1 and effective == 1 and pending is False
    assert reason == "init"


def test_init_abstains_when_weak() -> None:
    # |score| < ENTER on init -> no sign, transition pending, no quadrant.
    internal, effective, pending, reason = axis_hysteresis(
        None, 0.20, 0.95, min_confidence=MINC)
    assert internal is None and effective is None and pending is True
    assert reason == "init_below_enter"


def test_init_abstains_when_low_confidence() -> None:
    internal, effective, pending, reason = axis_hysteresis(
        None, 0.40, 0.60, min_confidence=MINC)
    assert effective is None and pending is True and reason == "init_low_confidence"


def test_confirmed_stays_with_aligned_signal_above_exit() -> None:
    # prior +1, signed_margin = +1 * 0.15 = 0.15 >= EXIT -> keep.
    internal, effective, pending, reason = axis_hysteresis(
        1, 0.15, 0.90, min_confidence=MINC)
    assert internal == 1 and effective == 1 and pending is False and reason == "hold"


def test_opposite_switch_takes_precedence_over_stability() -> None:
    # prior +1, score strongly negative: signed_margin = +1 * -0.30 = -0.30 <= -ENTER.
    # Opposite-switch is evaluated BEFORE stability (§5.2 precedence).
    internal, effective, pending, reason = axis_hysteresis(
        1, -0.30, 0.90, min_confidence=MINC)
    assert internal == -1 and effective == -1 and pending is False and reason == "switch"


def test_opposite_strong_but_low_confidence_does_not_switch_consumably() -> None:
    # opposite evidence sufficient but confidence < min -> internal flips memory,
    # effective sign withheld, transition pending.
    internal, effective, pending, reason = axis_hysteresis(
        1, -0.30, 0.55, min_confidence=MINC)
    assert internal == -1 and effective is None and pending is True
    assert reason == "switch_low_confidence"


def test_deadband_keeps_internal_publishes_no_quadrant() -> None:
    # prior +1, signed_margin = +1 * 0.05 = 0.05 in (-ENTER, EXIT): deadband.
    internal, effective, pending, reason = axis_hysteresis(
        1, 0.05, 0.90, min_confidence=MINC)
    assert internal == 1 and effective is None and pending is True and reason == "deadband"


def test_deadband_opposite_small_keeps_old_internal() -> None:
    # prior +1, score -0.05 -> signed_margin -0.05, |.|<ENTER and < EXIT: deadband,
    # memory of +1 preserved (so tomorrow distinguishes 'was up' from 'never set').
    internal, effective, pending, _ = axis_hysteresis(1, -0.05, 0.90, min_confidence=MINC)
    assert internal == 1 and effective is None and pending is True
