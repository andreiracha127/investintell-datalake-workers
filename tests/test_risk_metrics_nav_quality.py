import datetime as dt

from src.workers.risk_metrics import nav_quality


def _rows(navs):
    return [(dt.date(2020, 1, 1) + dt.timedelta(days=i), v) for i, v in enumerate(navs)]


def test_clean_series_is_ok():
    ok, count = nav_quality(_rows([10, 10.1, 10.0, 10.2, 10.15]))
    assert ok is True and count == 0


def test_dead_series_not_ok():
    ok, _ = nav_quality(_rows([10, 9.5, 0.01, 0.01, 0.01, 0.01]))
    assert ok is False


def test_scale_step_not_ok():
    ok, _ = nav_quality(_rows([1, 1, 1, 71, 71, 71, 71]))
    assert ok is False


def test_repairable_glitch_is_ok_after_repair():
    ok, count = nav_quality(_rows([19.66, 0.02, 19.68, 0.01, 19.69]))
    assert ok is True and count == 0    # repaired -> no residual impossible print
