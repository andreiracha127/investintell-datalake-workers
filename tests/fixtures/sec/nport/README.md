# Fixtures N-PORT

Os testes criam pacotes mínimos em `tmp_path`: um metadata sintético com SHA
injetada no contrato, `nport_readme.htm` e somente os TSVs necessários ao caso.
Assim, os cenários exercitam o parser fechado sem copiar ou ler o
`FUND_REPORTED_HOLDING.tsv` real de 983 MB.
