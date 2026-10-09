-- Retain all bitemporal versions; consumers apply availability/retirement at D.
-- Run with psql -X -A -t -v ON_ERROR_STOP=1 as mcp_ro, read-only PGOPTIONS.
SELECT coalesce(json_agg(x ORDER BY cik, filed, adsh, ticker_key, available_on), '[]') FROM (
    SELECT cik, ticker, ticker_key, adsh, form, filed, period, ddate, accepted,
           available_on, source_available_on, retired_on, security_kind,
           security_title, exchange, class_key, segments,
           filing_equity_classes, filing_complete
    FROM public.sec_ticker_cik_observations
    WHERE form ~ '^(20-F|40-F|6-K)'
) x;
