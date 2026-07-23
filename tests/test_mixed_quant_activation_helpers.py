from types import SimpleNamespace
from datetime import date, datetime, timezone
from uuid import UUID

from src.quant_data.contracts import IdentityObservation, resolve_identities
from src.workers import mixed_quant_publication as worker


def test_funds_v_identity_key_is_share_class_specific() -> None:
    lineage = {
        "source_surface": "funds_v",
        "instrument_id": "fdb5bfb0-3176-416b-96a5-cf1d4a6a55a3",
        "series_id": "S000002564",
    }
    assert worker._effective_deterministic_key("fund", lineage, "series:S000002564") == (
        "fund:fdb5bfb0-3176-416b-96a5-cf1d4a6a55a3"
    )


def test_non_fund_identity_key_is_unchanged() -> None:
    assert worker._effective_deterministic_key(
        "equity", {"source_surface": "sec_cusip_ticker_map"}, "cusip:037833100"
    ) == "cusip:037833100"


def test_series_maps_to_all_fund_share_class_instruments() -> None:
    first = UUID("11111111-1111-4111-8111-111111111111")
    second = UUID("22222222-2222-4222-8222-222222222222")
    resolved = [
        SimpleNamespace(
            instrument_id=first,
            instrument_type="fund",
            aliases=(SimpleNamespace(source_lineage={"series_id": "S1"}),),
        ),
        SimpleNamespace(
            instrument_id=second,
            instrument_type="fund",
            aliases=(SimpleNamespace(source_lineage={"series_id": "S1"}),),
        ),
        SimpleNamespace(
            instrument_id=UUID("33333333-3333-4333-8333-333333333333"),
            instrument_type="equity",
            aliases=(SimpleNamespace(source_lineage={}),),
        ),
    ]
    assert worker._fund_ids_by_series(resolved) == {"S1": [first, second]}


def test_fund_instrument_uses_public_app_uuid() -> None:
    public_id = UUID("44444444-4444-4444-8444-444444444444")
    resolved = resolve_identities(
        [
            IdentityObservation(
                observation_id=UUID("55555555-5555-4555-8555-555555555555"),
                instrument_type="fund",
                currency="USD",
                alias_type="ticker",
                alias_value="FUNDX",
                valid_from=date(2026, 7, 23),
                observed_at=datetime(2026, 7, 23, tzinfo=timezone.utc),
                source_lineage={
                    "source_surface": "funds_v",
                    "instrument_id": str(public_id),
                    "series_id": "S1",
                },
                deterministic_key=f"fund:{public_id}",
            )
        ]
    )[0]
    assert worker._with_public_fund_id(resolved).instrument_id == public_id
