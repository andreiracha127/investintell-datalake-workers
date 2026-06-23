import datetime as dt

from scripts.reprocess_nav_glitches import plan_repairs


def test_plan_repairs_reports_per_fund_changes():
    rows_by_fund = {
        "f1": [(dt.date(2020, 1, 1), 19.66), (dt.date(2020, 1, 2), 0.02),
               (dt.date(2020, 1, 3), 19.68)],
        "f2": [(dt.date(2020, 1, 1), 10.0), (dt.date(2020, 1, 2), 10.1)],
    }
    plans = plan_repairs(rows_by_fund)
    assert plans["f1"].glitch_count >= 0 and any(plans["f1"].repaired)
    assert not any(plans["f2"].repaired)
