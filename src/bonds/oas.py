"""Pure, DB-free option-adjusted-spread (OAS) motor — RESEARCH GRADE ONLY.

A calibrated short-rate binomial lattice, optimal-exercise valuation of callable
bonds by backward induction, and OAS by bisection on a constant spread added to
the lattice short rates.  No I/O, no database driver, no wall-clock read, no
third-party dependency (not even ``numpy``) — only stdlib ``math`` and the
Task-2 / Task-3 pure motors.  Every result is reproducible from its inputs.

VALIDATION STATUS (read this first — Increment 3 Global Constraint #4)
---------------------------------------------------------------------
No end-to-end published OAS worked example is reproducible from memory with the
confidence Constraint #4 demands.  Therefore this engine ships with a **partial**
validation harness (see ``tests/bonds/test_oas.py``) and its status STAYS
:data:`MODEL_VALIDATION_STATUS` == ``"model_validation_incomplete"``.  Task-6's
Phase-10 gate consumes that exact string as the ``model_validation_incomplete``
reason; the metric remains ``gate_not_passed``.  Nothing here is labelled
``authoritative_published``, and no value reaches any production surface
(Constraint #3).  What IS validated, honestly:

* the calibration drift and callable backward induction are recomputed BY HAND
  in a 2-step lattice (``convention_derived``);
* an option-free bond prices on the lattice EXACTLY as the Task-3 closed-form
  ``curve_price`` (``cross_check``), and on a flat curve the lattice OAS equals
  the Task-3 periodic ``z_spread`` under the exact continuous<->periodic
  conversion;
* option cost >= 0, a monotone price response (higher price -> lower OAS),
  call-value shrinkage, deep-OTM collapse, and decaying-oscillation convergence
  (``property``).

Model choice — Ho-Lee (justification)
-------------------------------------
The short rate follows the discrete Ho-Lee model  ``dr = theta(t) dt + sigma dW``
on a recombining binomial lattice.  Ho-Lee is chosen over BDT because it is
**adequate for a research-grade OAS** and, under continuous per-step compounding,
its calibration has a CLOSED FORM (no per-step root finding) — which is exactly
what lets the calibration be written out by hand and checked.  Its known
limitations are stated honestly: (a) a constant (input, NOT calibrated)
volatility, (b) Gaussian rates that can go negative (a genuine Ho-Lee feature,
harmless here because discounting is ``exp(-r*dt)`` — never ``1/(1+r*dt)`` — so a
negative rate never divides by a non-positive base), and (c) no mean reversion
(that would be Hull-White; out of scope).

Lattice construction (documented conventions)
---------------------------------------------
* **Nodes.** Node ``(i, j)`` — ``i`` the time step (``0..N-1`` for rate-setting
  nodes, times ``tau_i = i*dt``), ``j`` the number of up-moves (``0..i``).  The
  short rate is  ``r(i, j) = a_i + j * rate_step``  with
  ``rate_step = 2*sigma*sqrt(dt)``.  Risk-neutral up/down probability is 0.5.

* **Discounting.** One step at node ``(i, j)`` discounts by
  ``exp(-(r(i, j) + spread) * dt)``.  Continuous compounding is deliberate: a
  CONSTANT ``spread`` then factors out of the path expectation, so an option-free
  zero to node ``k`` discounts by ``DF_0(tau_k) * exp(-spread * tau_k)`` — i.e.
  the OAS is a *continuously-compounded* spread.  (The Task-3 ``z_spread`` is a
  *periodic* spread; on a flat curve the two map exactly by
  ``s_cont = m*ln((1+(z+s_per)/m)/(1+z/m))``.  The relationship, and its lack of
  step dependence for the option-free part, is documented in the report.)

* **Calibration (Jamshidian forward induction, closed form).**  With state
  prices ``Q(i, j)`` (Arrow-Debreu: PV at time 0 of $1 iff node ``(i, j)`` is
  reached), the drift that reprices the target zero ``DF*_{i+1}`` is
  ``a_i = -(1/dt) * ln( DF*_{i+1} / sum_j Q(i, j) * exp(-j * rate_step * dt) )``
  because ``exp(-a_i*dt)`` factors out of the one-step zero sum.  The target
  zeros come from the Task-3 :class:`~src.bonds.pricing.SpotCurve` on the SAME
  ACT/365F time basis and the SAME periodic curve DF as ``curve_price`` — so the
  calibrated tree reprices the curve at every node.

* **Valuation (backward induction, optimal exercise).**  Cash flows sit on
  nodes.  ``cont(k, j) = exp(-(r(k, j)+spread)*dt) * 0.5*(V(k+1, j+1)+V(k+1, j))``
  is the PV of everything strictly after ``tau_k``.  At a call node the issuer
  minimises the bond value, so the redemption value is ``min(cont, call_price)``;
  the coupon due at ``tau_k`` (if any) is paid in addition.  **Declared accrued
  treatment:** calls are restricted to coupon dates (inherited from Task-3's
  ``call_not_on_coupon_grid``), so accrued at a call is 0 and the call payment is
  exactly the clean call price (``call.price * face / 100``).

Every degenerate input raises a typed :class:`~src.bonds.errors.BondError` with a
stable ``code`` — never a NaN or a silent zero.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

from .cashflows import Schedule
from .errors import BondError
from .pricing import (
    SpotCurve,
    _CURVE_DAYS_PER_YEAR,
    _accrued_amount,
    _solve_decreasing,
    _validate_settlement,
)

# --------------------------------------------------------------------------- #
# Module constants (declared once).
# --------------------------------------------------------------------------- #
# Task-6's Phase-10 gate consumes this exact string as the metric reason.
MODEL_VALIDATION_STATUS = "model_validation_incomplete"
# Risk-neutral up/down probability of the recombining Ho-Lee binomial.
RISK_NEUTRAL_PROB = 0.5
# OAS bisection bracket (continuously-compounded spread, decreasing in value).
OAS_SPREAD_FLOOR = -0.5
OAS_SPREAD_CEILING = 2.0
# A cash flow time must sit on a lattice node to this ACT/365F tolerance.
_NODE_ALIGN_TOL = 1e-9


# --------------------------------------------------------------------------- #
# The calibrated lattice.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HoLeeLattice:
    """A Ho-Lee short-rate binomial lattice calibrated to target zero DFs.

    ``target_dfs`` are the zero-coupon discount factors to nodes ``1..N`` (the
    node-0 DF is 1 by definition).  ``dt`` is the uniform ACT/365F step and
    ``sigma`` the constant (INPUT, not calibrated) short-rate volatility.  On
    construction the drift ``a_0..a_{N-1}`` is calibrated in closed form so the
    tree reprices ``target_dfs`` at every node.
    """

    dt: float
    sigma: float
    target_dfs: tuple[float, ...]
    drift: tuple[float, ...] = field(default=(), compare=False)
    rate_step: float = field(default=0.0, compare=False)
    _state_prices: tuple[tuple[float, ...], ...] = field(default=(), compare=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.dt, (int, float)) or self.dt <= 0.0:
            raise BondError("non_positive_lattice_step", {"dt": self.dt})
        if not isinstance(self.sigma, (int, float)) or self.sigma < 0.0:
            raise BondError("invalid_volatility", {"sigma": self.sigma})
        if not self.target_dfs:
            raise BondError("empty_lattice", {})
        for df in self.target_dfs:
            if not isinstance(df, (int, float)) or df <= 0.0:
                raise BondError("non_positive_discount_factor", {"df": df})

        rate_step = 2.0 * self.sigma * math.sqrt(self.dt)
        object.__setattr__(self, "rate_step", rate_step)
        drift, state_prices = self._calibrate(rate_step)
        object.__setattr__(self, "drift", drift)
        object.__setattr__(self, "_state_prices", state_prices)

    @property
    def steps(self) -> int:
        """Number of rate-setting steps ``N`` (nodes ``0..N``)."""
        return len(self.target_dfs)

    def short_rate(self, i: int, j: int) -> float:
        """Continuously-compounded short rate at node ``(i, j)`` = ``a_i + j*step``."""
        if not (0 <= i < self.steps) or not (0 <= j <= i):
            raise BondError("node_out_of_range", {"i": i, "j": j, "steps": self.steps})
        return self.drift[i] + j * self.rate_step

    def _calibrate(self, rate_step: float) -> tuple[tuple[float, ...], tuple[tuple[float, ...], ...]]:
        """Jamshidian forward induction: drift ``a_i`` + state prices ``Q(i, j)``."""
        dt = self.dt
        drift: list[float] = []
        state_prices: list[tuple[float, ...]] = [(1.0,)]  # Q(0, 0) = 1
        for i, target in enumerate(self.target_dfs):
            qi = state_prices[i]
            denom = sum(qi[j] * math.exp(-j * rate_step * dt) for j in range(i + 1))
            # exp(-a_i*dt) factors out of the one-step zero sum -> closed form.
            a_i = -(1.0 / dt) * math.log(target / denom)
            drift.append(a_i)
            # roll the state prices forward one step (up and down, prob 0.5 each).
            nxt = [0.0] * (i + 2)
            for j in range(i + 1):
                disc = 0.5 * qi[j] * math.exp(-(a_i + j * rate_step) * dt)
                nxt[j] += disc  # down move -> state j
                nxt[j + 1] += disc  # up move -> state j+1
            state_prices.append(tuple(nxt))
        return tuple(drift), tuple(state_prices)

    def zero_price(self, n: int) -> float:
        """Tree-computed zero-coupon DF to node ``n`` = ``sum_j Q(n, j)``.

        Equals ``target_dfs[n-1]`` by calibration (``1.0`` for ``n == 0``).
        """
        if not (0 <= n <= self.steps):
            raise BondError("node_out_of_range", {"n": n, "steps": self.steps})
        return sum(self._state_prices[n])

    def value(
        self,
        cashflows_by_node: Mapping[int, float],
        call_by_node: Mapping[int, float] | None = None,
        *,
        spread: float = 0.0,
    ) -> float:
        """Backward-induction value at the root, with optimal call exercise.

        ``cashflows_by_node[k]`` is the total cash paid at node ``k`` (coupon
        and/or redemption); ``call_by_node[k]`` is the clean call payment (a cash
        amount) if the bond is callable at node ``k``.  A constant ``spread`` is
        added to every short rate (the OAS knob).
        """
        calls = call_by_node or {}
        self._validate_nodes(cashflows_by_node, calls)
        n = self.steps
        dt = self.dt
        # terminal layer: value at maturity node = its cash flow (nothing after).
        values = [float(cashflows_by_node.get(n, 0.0))] * (n + 1)
        for k in range(n - 1, -1, -1):
            layer = [0.0] * (k + 1)
            for j in range(k + 1):
                rate = self.drift[k] + j * self.rate_step
                cont = math.exp(-(rate + spread) * dt) * RISK_NEUTRAL_PROB * (values[j + 1] + values[j])
                call_price = calls.get(k)
                redemption = min(cont, call_price) if call_price is not None else cont
                layer[j] = float(cashflows_by_node.get(k, 0.0)) + redemption
            values = layer
        return values[0]

    def _validate_nodes(self, cashflows_by_node: Mapping[int, float], calls: Mapping[int, float]) -> None:
        n = self.steps
        for k in cashflows_by_node:
            if not (0 <= k <= n):
                raise BondError("cashflow_node_out_of_range", {"node": k, "steps": n})
        for k in calls:
            if not (0 <= k <= n):
                raise BondError("call_node_out_of_range", {"node": k, "steps": n})


# --------------------------------------------------------------------------- #
# OAS solver (bisection reuses the Task-3 decreasing-function root finder).
# --------------------------------------------------------------------------- #
def solve_oas(
    lattice: HoLeeLattice,
    target_price: float,
    cashflows_by_node: Mapping[int, float],
    call_by_node: Mapping[int, float] | None = None,
) -> float:
    """Constant spread (continuously compounded) that reprices to ``target_price``.

    Bisection on the monotone-decreasing model value (reusing the same tested
    root finder as YTM / Z-spread).  Raises ``non_positive_price`` for a
    non-positive target, ``oas_out_of_bounds`` when the price is unbracketable by
    ``[OAS_SPREAD_FLOOR, OAS_SPREAD_CEILING]``, and ``oas_no_convergence`` on a
    stalled solve (defensive; bisection converges in << the iteration cap).
    """
    if target_price <= 0.0:
        raise BondError("non_positive_price", {"price": target_price})

    def model_value(spread: float) -> float:
        return lattice.value(cashflows_by_node, call_by_node, spread=spread)

    return _solve_decreasing(
        model_value, target_price, OAS_SPREAD_FLOOR, OAS_SPREAD_CEILING, err_prefix="oas"
    )


# --------------------------------------------------------------------------- #
# Bond adapter: Schedule + SpotCurve -> calibrated lattice + node-mapped flows.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class LatticeInputs:
    """A calibrated lattice plus its node-mapped cash flows and call payments."""

    lattice: HoLeeLattice
    cashflows_by_node: dict[int, float]
    call_by_node: dict[int, float]
    node_times: tuple[float, ...]  # ACT/365F time at each node 0..N


def _curve_discount_factor(curve: SpotCurve, tau: float, m: int) -> float:
    """Periodic curve DF at ``tau`` — IDENTICAL convention to ``curve_price`` at
    spread 0 (``(1 + z(tau)/m) ** (-m*tau)``); the option-free cross-check test
    proves the two agree, so this is not an independent reimplementation."""
    return (1.0 + curve.rate(tau) / m) ** (-m * tau)


def build_lattice(
    schedule: Schedule,
    settlement: date,
    curve: SpotCurve,
    *,
    sigma: float,
    steps_per_period: int,
) -> LatticeInputs:
    """Build a curve-calibrated lattice with the bond's cash flows placed on nodes.

    ``steps_per_period`` sub-divides each cash-flow period into that many uniform
    ACT/365F steps.  Every distinct future cash-flow date must land on a node to
    ``_NODE_ALIGN_TOL`` (uniform ``dt`` recombines the tree; unequal calendar
    periods therefore raise ``cashflow_off_lattice_node`` rather than silently
    approximating a placement).  Calls must sit on cash-flow (coupon) dates.
    """
    if not isinstance(steps_per_period, int) or steps_per_period < 1:
        raise BondError("invalid_steps", {"steps_per_period": steps_per_period})
    _validate_settlement(schedule.terms, settlement)

    terms = schedule.terms
    m = int(terms.frequency)
    future = tuple(cf for cf in schedule.cashflows if cf.pay_date > settlement)
    if not future:
        raise BondError("no_future_cashflows", {"settlement": settlement.isoformat()})

    # distinct future pay dates and their ACT/365F times (Task-3 curve basis).
    pay_dates = sorted({cf.pay_date for cf in future})
    times = {d: (d - settlement).days / _CURVE_DAYS_PER_YEAR for d in pay_dates}
    horizon = times[pay_dates[-1]]
    num_periods = len(pay_dates)
    n_steps = steps_per_period * num_periods
    dt = horizon / n_steps

    def _node_index(tau: float) -> int:
        raw = tau / dt
        node = round(raw)
        if node < 1 or node > n_steps or abs(raw - node) > _NODE_ALIGN_TOL:
            raise BondError(
                "cashflow_off_lattice_node",
                {"tau": tau, "dt": dt, "nearest_node": node, "steps": n_steps},
            )
        return node

    # aggregate cash flows onto their nodes.
    cashflows_by_node: dict[int, float] = {}
    for cf in future:
        node = _node_index(times[cf.pay_date])
        cashflows_by_node[node] = cashflows_by_node.get(node, 0.0) + cf.amount

    # calls (clean call payment = call.price * face / 100) on qualified nodes.
    call_by_node: dict[int, float] = {}
    for call in terms.call_schedule:
        if call.call_date <= settlement:
            continue
        if call.call_date not in times:
            raise BondError("call_not_on_coupon_grid", {"call_date": call.call_date.isoformat()})
        node = _node_index(times[call.call_date])
        call_by_node[node] = call.price * terms.face / 100.0

    node_times = tuple(k * dt for k in range(n_steps + 1))
    target_dfs = tuple(_curve_discount_factor(curve, node_times[k], m) for k in range(1, n_steps + 1))
    lattice = HoLeeLattice(dt=dt, sigma=sigma, target_dfs=target_dfs)
    return LatticeInputs(lattice, cashflows_by_node, call_by_node, node_times)


def oas_from_price(
    schedule: Schedule,
    settlement: date,
    curve: SpotCurve,
    price: float,
    *,
    sigma: float,
    steps_per_period: int,
    price_is_clean: bool = True,
) -> float:
    """OAS (continuously-compounded spread) that reprices the bond to ``price``.

    ``price`` is a clean price by default; the dirty target adds the Task-2
    accrued (0 on a coupon date).  Builds a curve-calibrated lattice, places the
    cash flows and calls on nodes, and bisects the spread.
    """
    if price <= 0.0:
        raise BondError("non_positive_price", {"price": price})
    inputs = build_lattice(schedule, settlement, curve, sigma=sigma, steps_per_period=steps_per_period)
    target = price if not price_is_clean else price + _accrued_amount(schedule.terms, settlement)
    return solve_oas(inputs.lattice, target, inputs.cashflows_by_node, inputs.call_by_node)


# re-export the cash-flow kind used by callers building node maps directly.
__all__ = [
    "MODEL_VALIDATION_STATUS",
    "RISK_NEUTRAL_PROB",
    "OAS_SPREAD_FLOOR",
    "OAS_SPREAD_CEILING",
    "HoLeeLattice",
    "LatticeInputs",
    "build_lattice",
    "solve_oas",
    "oas_from_price",
]
