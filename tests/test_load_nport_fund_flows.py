from __future__ import annotations

from pathlib import Path

from scripts import load_nport_fund_flows as loader


def test_iter_reported_info_and_monthly_flows_normalize_nport_flows(tmp_path: Path):
    (tmp_path / "SUBMISSION.tsv").write_text(
        "\t".join(
            [
                "ACCESSION_NUMBER",
                "FILING_DATE",
                "FILE_NUM",
                "SUB_TYPE",
                "REPORT_ENDING_PERIOD",
                "REPORT_DATE",
                "IS_LAST_FILING",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "0000000000-24-000001",
                "15-APR-2024",
                "",
                "NPORT-P",
                "31-MAR-2024",
                "31-MAR-2024",
                "N",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "FUND_REPORTED_INFO.tsv").write_text(
        "\t".join(
            [
                "ACCESSION_NUMBER",
                "SERIES_NAME",
                "SERIES_ID",
                "SERIES_LEI",
                "TOTAL_ASSETS",
                "TOTAL_LIABILITIES",
                "NET_ASSETS",
                "SALES_FLOW_MON1",
                "REINVESTMENT_FLOW_MON1",
                "REDEMPTION_FLOW_MON1",
                "SALES_FLOW_MON2",
                "REINVESTMENT_FLOW_MON2",
                "REDEMPTION_FLOW_MON2",
                "SALES_FLOW_MON3",
                "REINVESTMENT_FLOW_MON3",
                "REDEMPTION_FLOW_MON3",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "0000000000-24-000001",
                "Sample Fund",
                "S000001234",
                "LEI",
                "1200",
                "200",
                "1000",
                "100",
                "10",
                "-30",
                "50",
                "5",
                "20",
                "0",
                "0",
                "0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    reported = list(loader.iter_reported_info(tmp_path))
    assert len(reported) == 1
    flows = list(loader.iter_monthly_flows(reported[0]))
    assert [row[5].isoformat() for row in flows] == [
        "2024-01-31",
        "2024-02-29",
        "2024-03-31",
    ]
    assert [row[6] for row in flows] == [1, 2, 3]
    assert flows[0][13] == 30
    assert flows[0][14] == 80
    assert flows[0][15] == loader.Decimal("0.08")
    assert flows[1][14] == 35
