"""The default-events export: confirmed D rows plus cure witnesses reproduce the full-publication windows."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import build_bond_panel_coupon_pit_returns as builder
from scripts import export_bond_market_implied_default_events as export

PUBLICATION = "c0172bf1-43e6-5175-be17-d54d708bf72a"
POLICY = {"policy_version": "bond_market_implied_rating_policy_v1", "policy_digest": "4b" * 32}


def _row(cusip: str, month: str, bucket: str, *, witnessed: bool = True, spell: int = 1, event: str | None = None) -> dict:
    return {
        "cusip_id": cusip, "month": dt.date.fromisoformat(month), "implied_bucket": bucket, "witnessed": witnessed,
        "spell_id": spell, "d_confirmed": bucket == "D",
        "d_event_month": dt.date.fromisoformat(event) if event else None, **POLICY,
    }


def _publication_rows() -> pd.DataFrame:
    """Every row of the defaulted CUSIPs, as ROWS_SQL returns them.

    - AAA000001: candidate never in D, then confirmed 2023-01 (event 2022-12), never cured.
    - DDD000004: D 2021-02..03, WITHDRAWN 2021-04 (keeps the window open), cured BB 2021-05,
      then a second episode D 2022-01 (event 2021-12), cured B 2022-03.
    """
    return pd.DataFrame([
        _row("AAA000001", "2022-11-01", "CCC"),
        _row("AAA000001", "2022-12-01", "CCC"),
        _row("AAA000001", "2023-01-01", "D", event="2022-12-01"),
        _row("AAA000001", "2023-02-01", "D", event="2022-12-01"),
        _row("AAA000001", "2023-03-01", "NOT_RATED", witnessed=False),
        _row("DDD000004", "2021-01-01", "CCC"),
        _row("DDD000004", "2021-02-01", "D", event="2021-02-01"),
        _row("DDD000004", "2021-03-01", "D", event="2021-02-01"),
        _row("DDD000004", "2021-04-01", "WITHDRAWN", witnessed=False),
        _row("DDD000004", "2021-05-01", "BB", spell=2),
        _row("DDD000004", "2021-06-01", "BB", spell=2),
        _row("DDD000004", "2022-01-01", "D", spell=2, event="2021-12-01"),
        _row("DDD000004", "2022-02-01", "CCC", witnessed=False, spell=3),
        _row("DDD000004", "2022-03-01", "B", spell=3),
        _row("DDD000004", "2022-04-01", "B", spell=3),
    ])


HEADER = {
    "publication_id": PUBLICATION, "publication_status": "validated", "rows_digest": "b3" * 32,
    "d_confirmed_count": 5, "d_candidate_count": 1, **POLICY,
}


def _write(path: Path, frame: pd.DataFrame) -> Path:
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
    return path


def test_confirmed_rows_plus_cure_witnesses_reproduce_the_full_publication_windows(tmp_path: Path) -> None:
    rows = _publication_rows()
    frame, counts = export.select_export_rows(rows, HEADER, cure_witnesses=True)

    assert counts["d_confirmed_rows"] == counts["header_d_confirmed_count"] == 5
    assert counts["episodes"] == 3 and counts["episodes_cured"] == 2 and counts["episodes_open"] == 1
    assert counts["cure_witness_rows"] == 2
    assert set(frame["export_role"]) == {"d_confirmed", "cure_witness"}
    witnesses = frame[frame["export_role"] == "cure_witness"]
    assert witnesses[["cusip_id", "month"]].values.tolist() == [
        ["DDD000004", dt.date(2021, 5, 1)], ["DDD000004", dt.date(2022, 3, 1)],
    ]
    # Only confirmed-D rows and the closing rows leave the database.
    assert not frame["implied_bucket"].isin(["NOT_RATED", "WITHDRAWN"]).any()

    full_windows, _ = builder.load_default_events(_write(tmp_path / "full.parquet", rows))
    export_windows, _ = builder.load_default_events(_write(tmp_path / "export.parquet", frame))
    pd.testing.assert_frame_equal(export_windows, full_windows)


def test_without_cure_witnesses_no_window_closes(tmp_path: Path) -> None:
    frame, counts = export.select_export_rows(_publication_rows(), HEADER, cure_witnesses=False)
    windows, _ = builder.load_default_events(_write(tmp_path / "d_only.parquet", frame))

    assert counts["cure_witness_rows"] == 0
    assert windows["cure_month"].isna().all() and len(windows) == 3


def test_the_d_row_count_must_equal_the_publication_header() -> None:
    with pytest.raises(export.ExportError, match="d_confirmed_count_mismatch:5!=6"):
        export.select_export_rows(_publication_rows(), {**HEADER, "d_confirmed_count": 6}, cure_witnesses=True)


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("publication_id", "ce46ae88-2363-58b7-8ca2-32cfe800aa12", "publication_mismatch"),
    ("publication_status", "superseded", "publication_not_validated"),
    ("policy_digest", "28" * 32, "policy_digest_mismatch"),
    ("rows_digest", "c7" * 32, "rows_digest_mismatch"),
])
def test_header_pins_are_refused_when_they_differ(field: str, value: str, reason: str) -> None:
    with pytest.raises(export.ExportError, match=reason):
        export.check_header({**HEADER, field: value}, publication_id=PUBLICATION,
                            policy_digest=POLICY["policy_digest"], rows_digest=HEADER["rows_digest"])


def test_the_builder_records_the_export_pins_and_refuses_a_disagreeing_file(tmp_path: Path) -> None:
    frame, counts = export.select_export_rows(_publication_rows(), HEADER, cure_witnesses=True)
    manifest = {
        "schema": export.EXPORT_SCHEMA, "purpose": "seed", "publication_id": PUBLICATION,
        "rows_digest": HEADER["rows_digest"], "code_revision": "d87e5808", "panel_publication_id": "aab1db6a",
        "as_of": "2026-09-01", "cure_witnesses": True, "counts": counts, **POLICY,
    }
    full = export.write_artifact(tmp_path / "out", frame, manifest)
    path = tmp_path / "out" / export.OUTPUT_NAME

    _, evidence = builder.load_default_events(path)

    assert evidence["sha256"] == full["artifact"]["sha256"]
    assert evidence["publication_id"] == PUBLICATION
    pins = evidence["export_manifest"]
    assert (pins["publication_id"], pins["rows_digest"], pins["code_revision"], pins["panel_publication_id"], pins["as_of"]) == (
        PUBLICATION, HEADER["rows_digest"], "d87e5808", "aab1db6a", "2026-09-01")
    assert json.loads((tmp_path / "out" / export.MANIFEST_NAME).read_text())["artifact"]["sha256"] == evidence["sha256"]

    table = pq.read_table(path)
    tampered = table.replace_schema_metadata({
        builder.EXPORT_MANIFEST_KEY:
        json.dumps({**manifest, "publication_id": "ce46ae88-2363-58b7-8ca2-32cfe800aa12"}).encode(),
    })
    pq.write_table(tampered, tmp_path / "tampered.parquet")
    with pytest.raises(builder.BuildError, match="export_manifest_publication_id_mismatch"):
        builder.load_default_events(tmp_path / "tampered.parquet")
