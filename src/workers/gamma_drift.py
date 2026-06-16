"""gamma_drift — IPCA Gamma drift monitor (Tier 3, T3B-3).

Ported from quant_engine/ipca/drift_monitor.compute_gamma_drift. IPCA factor
loadings (Gamma) are identified only up to an orthogonal rotation / sign flip
(Kelly-Pruitt-Su 2019), so successive re-estimations may differ by a rotation
that carries NO economic drift. We align Gamma_new to Gamma_old by orthogonal
Procrustes (Schonemann 1966) before measuring the relative Frobenius-norm
change, and raise an alert when the aligned drift exceeds DRIFT_THRESHOLD.

The monitor reads the two latest gamma_loadings for a universe from
factor_model_fits (materialized by the factor_model worker) and persists the
drift back onto the newest fit row. DB-first: no fit math here. Unlike the
legacy module this returns the alert flag instead of logging via structlog
(structlog is not a worker dependency).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt

from src.db import LOCK_FACTOR_MODEL, advisory_lock, connect

DRIFT_THRESHOLD = 0.25
_MIN_GAMMA_NORM = 1e-12

ENGINE = "ipca"
ASSET_CLASS = "Equity"


def compute_gamma_drift(
    gamma_old: npt.NDArray[np.float64],
    gamma_new: npt.NDArray[np.float64],
) -> float:
    """Procrustes-aligned relative Frobenius drift between two Gamma matrices.

    Rotation/sign invariant: a pure rotation or sign flip yields 0.0. Raises
    ValueError on non-2D, shape mismatch, empty, or non-finite inputs, and on a
    near-zero baseline norm. Returns 1.0 when the new Gamma is near-zero.
    """
    gamma_old = np.asarray(gamma_old, dtype=np.float64)
    gamma_new = np.asarray(gamma_new, dtype=np.float64)

    if gamma_old.ndim != 2 or gamma_new.ndim != 2:
        raise ValueError(
            f"Gamma matrices must be 2D: gamma_old {gamma_old.shape}, "
            f"gamma_new {gamma_new.shape}"
        )
    if gamma_old.shape != gamma_new.shape:
        raise ValueError(
            f"Shape mismatch: gamma_old {gamma_old.shape} != gamma_new {gamma_new.shape}"
        )
    if gamma_old.size == 0:
        raise ValueError("Gamma matrices must be non-empty")
    if not np.isfinite(gamma_old).all() or not np.isfinite(gamma_new).all():
        raise ValueError("Gamma matrices must contain only finite values")

    norm_old = float(np.linalg.norm(gamma_old, ord="fro"))
    norm_new = float(np.linalg.norm(gamma_new, ord="fro"))
    if norm_old < _MIN_GAMMA_NORM:
        if norm_new < _MIN_GAMMA_NORM:
            return 0.0
        raise ValueError(
            "Cannot compute relative gamma drift from a near-zero baseline "
            f"(norm_old={norm_old:.3e}, norm_new={norm_new:.3e})"
        )
    if norm_new < _MIN_GAMMA_NORM:
        return 1.0

    # Orthogonal Procrustes: R = U V^T from SVD(gamma_old^T @ gamma_new),
    # minimizing ||gamma_new @ R^T - gamma_old||_F.
    U, _, Vt = np.linalg.svd(gamma_old.T @ gamma_new)
    R = U @ Vt
    gamma_new_aligned = gamma_new @ R.T

    diff = gamma_new_aligned - gamma_old
    return float(np.linalg.norm(diff, ord="fro") / norm_old)


def _fetch_latest_two_gammas(
    conn: Any, *, universe_hash: str, engine: str, asset_class: str
) -> list[np.ndarray]:
    """Latest two gamma_loadings (newest first) for one universe/engine/class."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT gamma_loadings
            FROM factor_model_fits
            WHERE engine = %s AND asset_class = %s AND universe_hash = %s
            ORDER BY fit_date DESC
            LIMIT 2
            """,
            (engine, asset_class, universe_hash),
        )
        rows = cur.fetchall()
    return [np.asarray(r[0], dtype=np.float64) for r in rows]


def monitor_gamma_drift(
    conn: Any,
    *,
    universe_hash: str,
    engine: str = ENGINE,
    asset_class: str = ASSET_CLASS,
) -> dict[str, Any] | None:
    """Compare the two latest Gamma fits for a universe; return drift + alert.

    Returns None when fewer than two fits exist (drift undefined). Otherwise
    returns {"drift": float, "alert": bool, "threshold": float}. Shape changes
    between fits (different K or L) surface as a ValueError from
    compute_gamma_drift — a fail-loud signal that the fit dimension moved.
    """
    gammas = _fetch_latest_two_gammas(
        conn, universe_hash=universe_hash, engine=engine, asset_class=asset_class
    )
    if len(gammas) < 2:
        return None
    gamma_new, gamma_old = gammas[0], gammas[1]
    drift = compute_gamma_drift(gamma_old, gamma_new)
    return {
        "drift": drift,
        "alert": drift > DRIFT_THRESHOLD,
        "threshold": DRIFT_THRESHOLD,
    }


def _persist_drift(
    conn: Any, *, universe_hash: str, engine: str, asset_class: str,
    drift: float, alert: bool,
) -> None:
    """Write the drift + alert onto the newest fit row for the universe."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE factor_model_fits
            SET gamma_drift_vs_prior = %s, drift_alert = %s
            WHERE fit_id = (
                SELECT fit_id FROM factor_model_fits
                WHERE engine = %s AND asset_class = %s AND universe_hash = %s
                ORDER BY fit_date DESC LIMIT 1
            )
            """,
            (drift, alert, engine, asset_class, universe_hash),
        )


def run(
    dsn: str,
    *,
    universe_hash: str | None = None,
    engine: str = ENGINE,
    asset_class: str = ASSET_CLASS,
) -> dict[str, Any]:
    """Compute + persist Gamma drift for one (or every) universe.

    When universe_hash is None, monitors every universe that has >= 2 fits.
    Reuses LOCK_FACTOR_MODEL so it serializes against the factor_model fit
    worker on the shared table.
    """
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FACTOR_MODEL) as got:
            if not got:
                return {"status": "skipped", "reason": "lock_held", "monitored": 0}

            if universe_hash is not None:
                hashes = [universe_hash]
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT universe_hash
                        FROM factor_model_fits
                        WHERE engine = %s AND asset_class = %s
                        GROUP BY universe_hash
                        HAVING count(*) >= 2
                        """,
                        (engine, asset_class),
                    )
                    hashes = [r[0] for r in cur.fetchall()]

            monitored = 0
            alerts = 0
            for uh in hashes:
                result = monitor_gamma_drift(
                    conn, universe_hash=uh, engine=engine, asset_class=asset_class
                )
                if result is None:
                    continue
                _persist_drift(
                    conn, universe_hash=uh, engine=engine, asset_class=asset_class,
                    drift=result["drift"], alert=result["alert"],
                )
                monitored += 1
                alerts += int(result["alert"])
            conn.commit()
            return {"status": "succeeded", "monitored": monitored, "alerts": alerts}
