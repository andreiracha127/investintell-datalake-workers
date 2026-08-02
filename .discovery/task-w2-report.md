# Task W2 — composer versionado para `fund_regulatory_serving_*` (Desenho A)

Worktree: `E:\investintell-datalake-workers-fi-serving-fix`, branch `fix/fi-serving-integrity`.
Base: `7592b2c`. Entregue em 2 commits: `0a9434b`, `76778be`.
Espelho usado nas provas: `postgresql://postgres@127.0.0.1:5432/market` (container `market-dev`, TimescaleDB 2.27.2/PG18).

**STATUS: COMPLETO.** Composer versionado + gate embutido + promoção/rollback + camada de verificação sec-api.io (report-only) + suíte no PG efêmero 65431 + wiring no `ci.yml` + v2 composta, gateada e **promovida no espelho**, provada pelas views reais do app.

---

## 1. O que foi entregue

| arquivo | papel |
|---|---|
| `src/sec_serving/app_composition.py` | lógica do composer: cascata de classe, fingerprint/identidade uuid5, escrita da publicação, gate, validate, promote/rollback |
| `scripts/compose_fund_regulatory_serving_mappings.py` | CLI (`--promote`, `--rollback-to`, thresholds do gate, `--json`) |
| `scripts/verify_class_mappings_secapi.py` | verificação sec-api.io **report-only**, nunca dependência do composer |
| `tests/test_fund_regulatory_serving_composition.py` | 8 testes no harness PG efêmero 65431 |
| `tests/fixtures/sec/app_regulatory_serving_v2.sql` | espelho verbatim do DDL app (o repo app não é checkout deste CI) |
| `.github/workflows/ci.yml` | suíte nos arrays `tests`/`db_tests` do job `workers-new-surfaces-postgres`; os 2 scripts no `compileall` dos dois jobs |
| `.discovery/task-w2-secapi-verification.json` | saída bruta da verificação sec-api |

### Desenho seguido (A da descoberta)

Uma variável muda: `class_id`/`mapping_state` dos mappings. Os 4 pins de worker da v1 `bcee45f9` são herdados byte a byte (`ncen_operating_profile_v1`, `regulatory_mandate`, `rr1_fee_profile_v1`, `sec_regulatory_serving_v1`, todos v1), lidos de `fund_regulatory_serving_artifacts`. Nenhum `UPDATE` na v1: a camada é append-only e os triggers só aceitam escrita com pai `prepared`.

### Cascata de classe

1. `instrument_identity.sec_class_id`
2. `sec_company_tickers_mf` por `(series_id, upper(ticker))`
3. `sec_series_class_catalog` por `(series_id, upper(class_ticker))`

Toda fonte é filtrada a `class_id LIKE 'C%'` **e** a grupos `(series, ticker)` de valor único — um ticker que nomeia mais de uma classe **não** resolve por aquela fonte (é o uso legítimo de `class_context_ambiguous`). `sec_fund_classes` / `fund_classes_latest_mv` nunca são consultadas; aparecem no código só como constante `POISONED_CLASS_SOURCES` para o teste de guarda.

**Sobre "confidence":** a tabela `fund_regulatory_serving_instrument_mappings` **não tem coluna de confiança** — seu vocabulário é exatamente `resolved` / `class_context_ambiguous` (CHECK do DDL). Não inventei coluna: `resolved` significa "toda fonte consultada concordou e o grupo `(series, ticker)` era único"; a cauda irresolúvel fica `class_context_ambiguous` com `class_id=''`. A proveniência por instrumento (qual fonte resolveu) sai no relatório do gate (`from_identity`/`from_tickers`/`from_catalog`), não numa coluna nova.

### Idempotência

`app_publication_id = uuid5(ns, "fund_regulatory_serving|{recipe_revision}|{pins}|{fingerprint}")`, onde `fingerprint` = sha256 do conteúdo exato dos mappings que serão escritos. Mesma disciplina do `sec_serving.materializer.publication_id_for`. Insumos iguais → mesmo id → **no-op explícito** (`created=False`); fonte mudou → id novo → nova publicação (nunca mutação in-place). `app_publication_version` = `max()+1`. Provado nos testes `..._is_an_explicit_no_op` e `..._mint_a_different_publication`.

---

## 2. Gate pré-flip (embutido, obrigatório)

Todas as métricas são lidas da publicação NOVA, **antes** de o ponteiro mover — por isso o gate não usa as views v2 (que leem o ponteiro corrente): ele replica a lógica de `fund_regulatory_serving_facts_v` (perna `rr1_fee` class-grain) e de `fund_regulatory_mandate_profiles_v` (arm `published`) ligada a um `app_publication_id` explícito.

Gate falha → `conn.rollback()`, nada é validado, ponteiro não move, `exit 1`, relatório impresso.

### Números medidos no espelho (publicação `2264190c-c766-553f-92b2-77539871f022`, v2)

| métrica | exigido | medido | v1 (`bcee45f9`) |
|---|---|---|---|
| `mappings` | = 7 375 | **7 375** | 7 375 |
| `resolved` | ≥ 7 374 | **7 374** | 0 |
| `class_id LIKE 'C%'` | ≥ 7 374 | **7 374** | 0 |
| `class_equals_series` (assinatura de fonte envenenada) | = 0 | **0** | 0 |
| instrumentos com ≥1 fato `rr1_fee` no worker pinado | ≥ 7 273 | **7 273** | 0 |
| mandate que publicaria | ≥ 7 370 | **7 374** | 0 |
| desacordos entre fontes da cascata | = 0 | **0** | — |
| `multi_class_tickers` | — | 0 | — |

Proveniência da cascata: `from_identity=4590`, `from_tickers=2783`, `from_catalog=1`, `unresolved=1` (`bedb24dd-6432-4e3b-9007-92d9ab51a94a`, `S000080439`/`TMET`), `identity_non_class_values=0`. Bate exatamente com a descoberta.

`mapping_fingerprint = sha256:d4f455d772c62005a0dc69a247336bc7790831b659b6f59fa690ad0eeb96bfdb`.

---

## 3. Prova no espelho — pelas views REAIS do app (pós-`--promote`)

```
pointer                                   = 2264190c-c766-553f-92b2-77539871f022  (era bcee45f9-…)
mappings / resolved / class-grain          = 7375 / 7374 / 7374
fund_regulatory_mandate_profiles_v         published = 7374 | class_context_ambiguous = 1
fund_regulatory_serving_facts_v (rr1_fee)  instrumentos published = 7273 | linhas published = 410 032
fund_regulatory_fee_profiles_v             published = 413 833 | source_filing_unavailable = 101 | ambiguous = 1
fund_regulatory_operating_profiles_v       published = 3 610 | source_filing_unavailable = 3 764 | ambiguous = 1
```

Sob a v1 essas quatro views entregavam **zero** linhas `published` por construção (todo mapping era `class_context_ambiguous`, e as views filtram/forçam por esse estado). A transição 0 → publicado é exercitada ponta a ponta no teste `test_promotion_moves_the_pointer_and_rollback_returns_it`, que mede (0,0) antes, (4,4) depois do flip e (0,0) de novo após o rollback.

Comandos executados (nesta ordem):

```
PYTHONPATH=. py -3.13 scripts/compose_fund_regulatory_serving_mappings.py \
    --dsn "postgresql+asyncpg://postgres@127.0.0.1:5432/market"            # compõe + gate + valida
PYTHONPATH=. py -3.13 scripts/compose_fund_regulatory_serving_mappings.py \
    --dsn "postgresql+asyncpg://postgres@127.0.0.1:5432/market" --promote  # 2ª execução: no-op + flip
```

A segunda execução confirmou a idempotência no espelho real: mesmo `app_publication_id`, mesmo fingerprint, `created=False`, e só o ponteiro mudou.

### Rollback

```sql
SELECT fund_set_current_regulatory_serving_publication_v2('bcee45f9-0b64-5903-8dcd-f3067ab2fb80'::uuid);
```

ou, versionado:

```
python scripts/compose_fund_regulatory_serving_mappings.py --dsn "$DATABASE_URL" \
    --rollback-to bcee45f9-0b64-5903-8dcd-f3067ab2fb80
```

Instantâneo, sem downtime, sem tocar em dado (a v1 continua `validated` e intacta). O script imprime essa linha em toda execução.

---

## 4. Verificação sec-api.io (report-only)

`scripts/verify_class_mappings_secapi.py` — **jamais** dependência do composer: o composer resolve classe só de relações do datalake e continua rodando sem rede e sem credencial de fornecedor. Credencial via `secapi_env.load_api_key()` (`secapi-api-key` em `backend/.env`); toda mensagem que poderia carregar URL passa por `scrub()` — nenhuma URL com token foi logada.

**O que N-CEN pode e não pode provar:** publica `seriesClass.reportSeriesClass.rptSeriesClassInfo[].classIds` (o conjunto de classes da série) mas **nunca o ticker da classe**. Logo é checagem de pertinência de conjunto, não de atribuição por ticker — pega uma classe estranha à série (exatamente o que `sec_fund_classes` produziria) mas não arbitra qual classe irmã um ticker significa. Está documentado no docstring para ninguém ler 100 % como "toda atribuição confirmada".

Resultado (amostra de 50 séries, seed 20260802, publicação `2264190c`):

| | |
|---|---|
| séries amostradas | 50 |
| comparáveis (N-CEN enumerou classes) | **30** |
| **concordam (nenhuma classe estranha)** | **30 → 100,00 %** |
| classe estranha à série | **0** |
| lista de classes omitida (`includeAllClassesFlag`) | 15 |
| sem filing N-CEN | 5 |

*Armadilha encontrada e corrigida:* a primeira rodada reportou 24/50 "sem filing". Falso — o filer pode responder a tabela série/classe com `includeAllClassesFlag: true` e não enumerar nada. Colapsar isso em "sem filing" subnotificaria a cobertura pela metade; os três desfechos agora são distintos e o probe varre os 5 filings mais recentes procurando um que enumere.

**Cauda.** `S000080439` / `TMET` — o único instrumento sem classe em qualquer relação do datalake — tem N-CEN (filed 2026-01-14) com **exatamente uma classe: `C000242839`**. Veredito `single_class_candidate`. **Reportado, não escrito**: não é uma fonte do datalake, e alimentar o composer com ela criaria uma dependência de rede num caminho que precisa ser determinístico. É item de ação para o dono (ver §6).

---

## 5. Testes

`tests/test_fund_regulatory_serving_composition.py` — **8 testes, todos passando** no PG efêmero docker `127.0.0.1:65431` (container `pg-bonds-test`, postgres:16), rodados como a W1 fez:

```
SEC_TEST_DATABASE_URL="host=127.0.0.1 port=65431 dbname=postgres user=postgres" PYTHONPATH=. \
  py -3.13 -m pytest tests/test_fund_regulatory_serving_composition.py -q
# 8 passed
```

Vizinhança (regressão): `test_fund_regulatory_serving_composition` + `test_sec_derived_publications` + `test_sec_regulatory_mandate` + `test_sec_regulatory_serving_materializer` + `test_sec_regulatory_serving_contract` → **29 passed**.

Cobertura:

1. `test_cascade_resolves_each_source_and_keeps_the_irreducible_tail` — cada um dos 3 níveis da cascata resolve o instrumento certo; a cauda fica ambígua; zero desacordos.
2. `test_composition_writes_class_grain_mappings_and_passes_the_gate` — mappings class-grain, gate verde, ponteiro **não** move sem `--promote`.
3. `test_poisoned_class_source_is_never_consulted` — estático (`sec_fund_classes`/`fund_classes_latest_mv` ausentes do SQL executado) **e** em runtime (a relação envenenada existe e está populada para todas as séries; nenhum mapping sai com `class_id = series_id`).
4. `test_gate_blocks_and_rolls_back_when_a_metric_regresses` — threshold de fee acima do disponível: gate falha, nada validado, nada promovido, publicação revertida (`count=1`), ponteiro intacto.
5. `test_promotion_moves_the_pointer_and_rollback_returns_it` — mede `fund_regulatory_mandate_profiles_v` e `fund_regulatory_serving_facts_v`: (0,0) → flip → (4,4) → rollback → (0,0).
6. `test_recomposition_with_unchanged_inputs_is_an_explicit_no_op`.
7. `test_changed_class_sources_mint_a_different_publication`.
8. `test_fixture_keeps_the_app_write_guards` — o fixture é espelho; um espelho enfraquecido não provaria nada.

**Fixtures class-grain, não `class_id=""`+`resolved`.** O hábito de `backend/tests/integration/test_sec_regulatory_vertical.py:108` (default `class_id=""` com `mapping_state="resolved"`) — a forma exata que deixou o incidente passar — **não** foi propagado: aqui a v1 de base é escrita como em produção (`class_id=''` **com** `class_context_ambiguous`) e tudo que o composer escreve nomeia um `C0000…` real.

**Limite honesto do harness:** o repo app não é checkout do CI deste repo, então `tests/fixtures/sec/app_regulatory_serving_v2.sql` é um espelho **verbatim** do subconjunto load-bearing do DDL app (`2026-07-18_sec_regulatory_serving_v2.sql` + `2026-07-21_..._families.sql`): todas as tabelas escritas, as 3 guardas de escrita, a guarda do ponteiro, `fund_validate_..._v2`, `fund_set_current_..._v2` e as 2 views cujo comportamento o fix muda. Omitidos deliberadamente (nada aqui os lê/escreve): o ponteiro legado `fund_regulatory_serving_current_pointers` e as views `fund_regulatory_operating_profiles_v` / `fund_regulatory_fee_profiles_v` (cujas tabelas-fonte o fixture não instala). Espelho pode derivar do original — por isso a prova contra o DDL **real** é a execução no espelho da §3, e o teste 8 trava o enfraquecimento do fixture.

### CI

`workers-new-surfaces-postgres`: suíte adicionada aos arrays `tests` e `db_tests` (padrão de `test_sec_derived_publications.py`). `scripts/compose_fund_regulatory_serving_mappings.py` e `scripts/verify_class_mappings_secapi.py` adicionados ao `compileall` dos dois jobs. `ruff check` limpo em todos os arquivos tocados.

---

## 6. Concerns / dívidas registradas

1. **O espelho está flipado, produção não.** A v2 só existe no espelho local. Rodar em produção exige o mesmo comando contra o `market` do GCloud (túnel IAP ou NLB+mTLS) e o E2E pela API do hub é a **Task W3**, não feita aqui.
2. **Continua pinado na `sec_regulatory_serving_v1` v1 (2026-07-23)** enquanto o worker está na v6 (2026-07-31). Desenho A por escolha: uma variável por flip. O repin é o Desenho B, como **v3**, com esta v2 de baseline. O composer já suporta (`--base-app-publication-id`), mas trocar o artifact exige um passo a mais — hoje ele **herda** os pins, não os re-resolve.
3. **A cauda `TMET` é resolvível pelo SEC, não pelo datalake.** N-CEN dá `C000242839`. O caminho correto é o dado entrar numa relação do datalake (o worker `sec_company_tickers_mf` roda desde 2026-07-22; `sec_series_class_catalog` desde 2026-07-17 — vale confirmar a cadência dos dois) e o composer recompor. Enquanto isso a linha fica honestamente `class_context_ambiguous`.
4. **`--promote` no espelho gastou ~4 min** (7 375 inserts sob trigger `FOR EACH ROW` que faz `SELECT … FOR UPDATE` no pai). Aceitável para uma operação de ops; se virar cron vale um `COPY` para staging + insert em bloco.
5. **`operating_v` publica só 3 610 de 7 375** — o resto é `source_filing_unavailable`, ou seja, falta de perfil N-CEN, não de mapping. Fora do escopo desta task, mas é o próximo teto do dossiê.
6. **A verificação sec-api só cobre 30 das 50 séries amostradas** (15 omitem a lista via `includeAllClassesFlag`, 5 sem filing). Concordância é 100 % sobre o que é comparável; ela não é, e não pode ser, prova de atribuição ticker→classe.
7. **`fund_classes_latest_mv` / `sec_fund_classes` seguem envenenadas** para qualquer outro consumidor (11 773 linhas com series id no campo `class_id`). Este composer as evita; um follow-up separado deveria auditar quem mais as lê.

---

## 7. Dívidas conhecidas

Registradas aqui como dívida escrita (decisão do fix round: documentar, não codar agora).

### D1 — o universo de instrumentos é re-projetado da publicação base, com INNER JOIN em `instrument_identity`

`CASCADE_SQL` parte de `fund_regulatory_serving_instrument_mappings WHERE app_publication_id = :base_app` e faz `JOIN instrument_identity ii ON ii.instrument_id = m.instrument_id`. Consequências:

- **(a) Não há caminho de bootstrap.** Instrumento que entrou no app depois de a publicação base ter sido composta **não** é incorporado: o composer herda o universo, não o descobre. Hoje isso é aceitável porque a base v1 cobre os 7 375 instrumentos do produto, mas na primeira vez que o app ganhar instrumentos novos será preciso um passo de descoberta (projetar de `instrument_identity` ∩ universo do app, em vez de da publicação anterior) — e esse passo muda o universo, então precisa vir com o `--expect-mappings` novo e explícito.
- **(b) Instrumento presente na base mas ausente de `instrument_identity` some silenciosamente.** O INNER JOIN o descarta sem erro.

**O que fecha isso hoje:** apenas o piso de **igualdade exata** da contagem de mappings no gate (`GateThresholds.mappings`, default 7 375). É a única métrica do gate que é `==` e não `>=`, exatamente por isso: uma queda silenciosa aparece como divergência de contagem e aborta a composição antes de qualquer validação. `--expect-mappings` desliga essa proteção — existe para uma mudança **deliberada** de universo, não para "fazer passar". O comentário correspondente está no próprio `CASCADE_SQL`, junto do JOIN.

**Trabalho futuro:** promover a descoberta de universo a passo próprio, com sua própria métrica de gate (delta de universo esperado vs. medido), em vez de depender de uma igualdade hardcoded.

### D2 — Desenho B: repin do worker para a v6, como app v3

O app segue pinado em `sec_regulatory_serving_v1` **v1** (2026-07-23) enquanto o worker está na **v6** (2026-07-31). O Desenho A resolveu só o `class_id` de propósito: uma variável por flip, para o A/B ser diagnóstico. A dívida do repin fica aberta:

- fazer como **v3**, com esta v2 de baseline;
- exige revalidar que existe artifact `regulatory_mandate` `validated` correspondente à v6;
- o composer hoje **herda** os pins de artifact da base — ele não os re-resolve. O repin precisa de um modo novo (ex.: `--repin product=publication_id`) ou de uma base preparada externamente;
- a v6 traz de volta as 8 famílias N-CEN aposentadas; o skip de `fund_regulatory_serving_families.py` cobre isso desde a Task 5, mas é código recém-mudado — mais uma razão para o repin ser um flip separado.

### D3 — `POISONED_CLASS_SOURCES` é constante documental, não é executada

A constante existe para nomear as relações envenenadas no código, e a guarda de teste inspeciona `CASCADE_SQL` diretamente (`"sec_fund_classes" not in ...`), não a constante. Ou seja: acrescentar um nome à constante hoje **não** cria proteção nova. Follow-up (minor, parked no ledger do orquestrador): fazer a guarda iterar sobre `POISONED_CLASS_SOURCES` e cobrir todo o SQL do módulo, não só `CASCADE_SQL`.

---

## 8. Fix round pós-revisão (2026-08-02)

O reviewer aprovou a substância (cascata, gate, idempotência recomputada, estado do espelho confirmado por SQL próprio). Três correções mecânicas aplicadas:

1. **`ci.yml` reescrito LF→CRLF.** O patch em Python tinha reescrito o arquivo inteiro com tradução de newline do Windows: 683+/677− para 6 linhas reais. Arquivo normalizado de volta para LF preservando as 6 linhas; `git diff --stat` agora mostra **6 insertions, 0 deletions** (os 2 scripts no `compileall` dos dois jobs + a suíte nos arrays `tests` e `db_tests`).
2. **Contrato de `compose()`.** A docstring prometia "rolls back and raises" enquanto o código devolvia `CompositionResult` com `gate.ok=False`. Escolhido o **raise**: nova exceção `GateFailed(CompositionError)` que carrega o `CompositionResult` completo. Um chamador programático não consegue mais ler gate reprovado como sucesso ignorando um flag; a CLI captura `GateFailed`, imprime o mesmo relatório que uma execução bem-sucedida imprime (para o operador ver QUAL métrica regrediu) e sai com 1. Teste `test_gate_blocks_and_rolls_back_when_a_metric_regresses` passou a usar `pytest.raises(GateFailed)` e a ler `excinfo.value.result`.
3. **Dívidas escritas** (§7 acima): D1 (bootstrap ausente + drop silencioso do INNER JOIN, e por que o piso de contagem é igualdade exata) com comentário correspondente no `CASCADE_SQL`, e D2 (Desenho B / repin v6 como v3). D3 registrado a partir do minor parkeado.

Reexecutado após o fix: suíte nova **8 passed** no PG efêmero 65431; `ruff check` limpo nos 4 arquivos tocados; `yaml.safe_load` do `ci.yml` OK (4 jobs). Smoke da CLI no espelho com `--min-fee-matched 99999`: gate reprova, relatório impresso, `exit 1`, ponteiro intacto.
