import datetime as dt

from src.workers._nav_sanitize import sanitize_nav_series


def _series(navs, start=dt.date(2020, 1, 1)):
    return [(start + dt.timedelta(days=i), v) for i, v in enumerate(navs)]


def test_single_round_trip_dip_is_repaired():
    res = sanitize_nav_series(_series([10.0, 10.1, 0.02, 10.2, 10.3]))
    assert res.repaired[2] is True
    assert 9.0 < res.nav[2] < 11.0       # interpolated back to the local level
    assert res.glitch_count == 1
    assert res.dead is False and res.scale_step is False


def test_alternating_glitches_paaa_style_all_repaired():
    res = sanitize_nav_series(_series([19.66, 0.02, 19.68, 0.01, 19.69]))
    assert res.repaired[1] is True and res.repaired[3] is True
    assert all(15.0 < v < 25.0 for v in res.nav)
    assert res.glitch_count == 2


def test_real_large_move_not_flagged():
    # a genuine -45% then sustained level (no round-trip) is NOT a glitch
    res = sanitize_nav_series(_series([100.0, 100.0, 55.0, 55.0, 55.0, 56.0]))
    assert not any(res.repaired)
    assert res.glitch_count == 0


def test_dead_fund_flagged_not_repaired():
    res = sanitize_nav_series(_series([10.0, 9.5, 0.01, 0.01, 0.01, 0.01]))
    assert res.dead is True
    assert not any(res.repaired)          # sustained near-zero is not interpolated


def test_scale_step_flagged_not_repaired():
    # a persistent ~70x level shift that never reverts = scale change
    res = sanitize_nav_series(_series([1.0, 1.0, 1.0, 71.0, 71.0, 71.0, 71.0]))
    assert res.scale_step is True
    assert not any(res.repaired)


def test_clean_series_unchanged():
    navs = [10.0, 10.1, 10.0, 10.2, 10.15]
    res = sanitize_nav_series(_series(navs))
    assert res.nav == navs
    assert res.glitch_count == 0 and not res.dead and not res.scale_step
