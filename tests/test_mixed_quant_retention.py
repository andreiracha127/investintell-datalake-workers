"""Retenção de publicações ``mixed_quant_v1``.

Cada publicação é um snapshot COMPLETO e imutável (~2,4 GB no universo atual) e
nada as podava, então o datalake crescia isso a cada publish — foi por esse medo
de disco que o cron ficou mensal em vez de semanal. Pior: o guard
`quant_reject_active_publication_write()` levantava `unknown publication` durante
o próprio cascade da publicação, o que tornava TODA publicação indeletável.

Rodam contra um Postgres isolado (nunca um DSN de produção). Defina
SEC_TEST_DATABASE_URL, ex.: "host=127.0.0.1 port=55432 dbname=postgres user=postgres".
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from src.quant_data import publication as pub  # noqa: E402

_BASE_DSN = os.getenv("SEC_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not _BASE_DSN, reason="SEC_TEST_DATABASE_URL not set")

_NOW = datetime(2026, 7, 30, 12, tzinfo=timezone.utc)


@pytest.fixture()
def env():
    schema = f"mixed_ret_{uuid.uuid4().hex}"
    with psycopg.connect(_BASE_DSN, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{schema}"')
        conn.execute(f'SET search_path TO "{schema}"')
        pub.install_schema(conn)
        try:
            yield conn
        finally:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _publication(conn, pid: str, as_of: date, *, age_days: int) -> uuid.UUID:
    """Nasce 'building' — único estado gravável — e recebe filhos, que é a ordem
    que o guard impõe na vida real."""
    conn.execute(
        "INSERT INTO quant_publication_v1(publication_id,product,as_of,status,"
        " code_revision,config_version,created_at)"
        " VALUES (%s,%s,%s,'building','dev','v1',%s)",
        (pid, pub.PRODUCT, as_of, _NOW - timedelta(days=age_days)),
    )
    for i in range(3):
        iid = uuid.uuid5(uuid.NAMESPACE_OID, f"{pid}-{i}")
        conn.execute(
            "INSERT INTO quant_instrument_v1(publication_id,instrument_id,"
            " instrument_type,currency) VALUES (%s,%s,'fund','USD')",
            (pid, iid),
        )
        conn.execute(
            "INSERT INTO quant_return_v1(publication_id,instrument_id,period_end,"
            " frequency,total_return,observed_at,source_lineage)"
            " VALUES (%s,%s,%s,'daily',0.01,now(),'{\"s\":\"t\"}')",
            (pid, iid, date(2026, 1, 1)),
        )
    return uuid.UUID(pid)


def _world(conn) -> dict[str, uuid.UUID]:
    """Quatro rolantes (nova -> velha) e uma histórica point-in-time de 2015."""
    ids = {
        "g1": _publication(conn, "00000000-0000-4000-8000-000000000001", date(2026, 7, 30), age_days=0),
        "g2": _publication(conn, "00000000-0000-4000-8000-000000000002", date(2026, 7, 23), age_days=1),
        "g3": _publication(conn, "00000000-0000-4000-8000-000000000003", date(2026, 6, 30), age_days=2),
        "g4": _publication(conn, "00000000-0000-4000-8000-000000000004", date(2026, 5, 30), age_days=3),
        "hist": _publication(conn, "00000000-0000-4000-8000-000000000005", date(2015, 9, 28), age_days=4),
    }
    conn.execute(
        "UPDATE quant_publication_v1 SET status='superseded' WHERE publication_id <> %s",
        (str(ids["g1"]),),
    )
    conn.execute(
        "UPDATE quant_publication_v1 SET status='ready' WHERE publication_id = %s",
        (str(ids["g1"]),),
    )
    pub.promote(conn, pub.PRODUCT, ids["g1"])
    return ids


def _surviving(conn) -> set[uuid.UUID]:
    return {
        row[0]
        for row in conn.execute("SELECT publication_id FROM quant_publication_v1").fetchall()
    }


def test_prune_keeps_the_requested_generations(env) -> None:
    conn = env
    ids = _world(conn)

    removed = pub.prune(conn, pub.PRODUCT, keep_generations=2)

    assert [r[0] for r in removed] == [ids["g3"], ids["g4"]] or set(
        r[0] for r in removed
    ) == {ids["g3"], ids["g4"]}
    assert _surviving(conn) == {ids["g1"], ids["g2"], ids["hist"]}


def test_prune_never_removes_the_active_publication(env) -> None:
    """A garantia vem do schema (RESTRICT), não da função: mesmo pedindo o mínimo
    de gerações, a ativa não pode sair — senão o builder ficaria sem universo."""
    conn = env
    ids = _world(conn)

    pub.prune(conn, pub.PRODUCT, keep_generations=1)

    assert ids["g1"] in _surviving(conn)
    assert pub.active_publication_id(conn, pub.PRODUCT) == ids["g1"]


def test_prune_leaves_historical_point_in_time_publications_alone(env) -> None:
    """Um as_of muito no passado é build deliberado para estudar universo de
    época; recência nunca deve podá-lo."""
    conn = env
    ids = _world(conn)

    pub.prune(conn, pub.PRODUCT, keep_generations=1)

    assert ids["hist"] in _surviving(conn)


def test_prune_cascades_children_and_leaves_no_orphans(env) -> None:
    """O bug que impedia isso: o guard levantava `unknown publication` durante o
    cascade, porque a linha-pai já tinha sido apagada quando ele consultava o
    status. Nenhuma publicação podia ser deletada."""
    conn = env
    _world(conn)

    pub.prune(conn, pub.PRODUCT, keep_generations=1)

    orphan_instruments = conn.execute(
        "SELECT count(*) FROM quant_instrument_v1 i WHERE NOT EXISTS ("
        " SELECT 1 FROM quant_publication_v1 p WHERE p.publication_id=i.publication_id)"
    ).fetchone()[0]
    orphan_returns = conn.execute(
        "SELECT count(*) FROM quant_return_v1 r WHERE NOT EXISTS ("
        " SELECT 1 FROM quant_publication_v1 p WHERE p.publication_id=r.publication_id)"
    ).fetchone()[0]
    assert (orphan_instruments, orphan_returns) == (0, 0)


def test_frozen_publications_are_still_write_protected(env) -> None:
    """O relaxamento do guard vale SÓ para DELETE. Escrever numa publicação
    congelada continua barrado — é a razão de o guard existir."""
    conn = env
    ids = _world(conn)
    iid = uuid.uuid5(uuid.NAMESPACE_OID, f"{ids['g2']}-0")

    with pytest.raises(psycopg.errors.RaiseException) as exc:
        conn.execute(
            "INSERT INTO quant_return_v1(publication_id,instrument_id,period_end,"
            " frequency,total_return,observed_at,source_lineage)"
            " VALUES (%s,%s,%s,'daily',0.5,now(),'{\"s\":\"t\"}')",
            (str(ids["g2"]), iid, date(2026, 2, 2)),
        )
    assert "no longer writable" in str(exc.value)


def test_prune_rejects_a_zero_generation_request(env) -> None:
    conn = env
    _world(conn)

    with pytest.raises(psycopg.errors.RaiseException) as exc:
        pub.prune(conn, pub.PRODUCT, keep_generations=0)
    assert "keep_generations" in str(exc.value)


def _wire_worker(monkeypatch, conn):
    """Faz o worker usar a conexão do teste em vez de abrir a sua."""
    from src.workers import mixed_quant_retention as worker

    class _Handle:
        def __enter__(self): return conn
        def __exit__(self, *a): return False

    monkeypatch.setattr(worker, "connect", lambda dsn: _Handle())
    monkeypatch.setattr(worker, "resolve_dsn", lambda dsn=None: "postgres://test")
    return worker


def test_worker_skips_when_no_pointer_is_published(env, monkeypatch) -> None:
    """Sem ponteiro ativo não há como medir gerações, e o RESTRICT não tem o que
    proteger — o worker se recusa em vez de adivinhar."""
    conn = env
    _publication(conn, "00000000-0000-4000-8000-0000000000aa", date(2026, 7, 30), age_days=0)
    worker = _wire_worker(monkeypatch, conn)

    result = worker.run()

    assert result["status"] == "skipped"
    assert result["reason"] == "no_active_publication"
    assert result["pruned"] == []
    # E nada foi apagado.
    assert len(_surviving(conn)) == 1


def test_worker_prunes_and_reports_what_it_removed(env, monkeypatch) -> None:
    conn = env
    ids = _world(conn)
    monkeypatch.setenv("MIXED_QUANT_KEEP_GENERATIONS", "2")
    worker = _wire_worker(monkeypatch, conn)

    result = worker.run()

    assert result["status"] == "ok"
    assert result["keep_generations"] == 2
    assert result["active_publication_id"] == str(ids["g1"])
    assert result["pruned_count"] == 2
    assert {entry["as_of"] for entry in result["pruned"]} == {"2026-06-30", "2026-05-30"}
    assert _surviving(conn) == {ids["g1"], ids["g2"], ids["hist"]}
