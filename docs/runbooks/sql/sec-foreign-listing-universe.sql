-- Run with psql -X -A -t -v ON_ERROR_STOP=1 as mcp_ro, read-only PGOPTIONS.
SELECT coalesce(json_agg(x ORDER BY cik, symbol), '[]') FROM (
    SELECT cik, ticker_key AS symbol,
           array_agg(DISTINCT ticker ORDER BY ticker) AS source_symbols,
           array_agg(DISTINCT form ORDER BY form) AS forms,
           min(filed) AS first_filed, max(filed) AS last_filed,
           min(available_on) AS first_available_on,
           max(available_on) AS last_available_on,
           array_agg(DISTINCT security_kind ORDER BY security_kind) AS security_kinds,
           count(*) AS observation_count
    FROM public.sec_ticker_cik_observations
    WHERE retired_on IS NULL AND form ~ '^(20-F|40-F|6-K)'
    GROUP BY cik, ticker_key
) x;
