"""Tests for the livefeed worker (servidor WS de live price, fan-out Tiingo IEX).

Cobre as três camadas puras — parsing da mensagem Tiingo (exemplo literal da
doc oficial), Hub de subscribe/fan-out e simulador random-walk — mais um
end-to-end real: sobe o servidor em modo sim numa porta efêmera, conecta um
cliente websockets, assina TSLA e confirma ticks + /health. Roda em qualquer
lugar (não precisa de TIINGO_API_KEY nem de rede externa).
"""

from __future__ import annotations

import asyncio
import json
import socket

import pytest

from src.workers import livefeed as lf

# Exemplos LITERAIS da doc oficial (api.tiingo.com/documentation/websockets/iex)
TRADE_MSG = json.dumps({
    "messageType": "A", "service": "iex",
    "data": ["T", "2019-01-30T13:33:45.594808294-05:00", 1548873225594808294,
             "wes", None, None, None, None, None, 50.285, 200, None, 0, 0, 0, 0],
})
QUOTE_MSG = json.dumps({
    "messageType": "A", "service": "iex",
    "data": ["Q", "2019-01-30T13:33:45.383129126-05:00", 1548873225383129126,
             "vym", 100, 81.58, 81.585, 81.59, 100, None, None, 0, 0, None,
             None, None],
})
HEARTBEAT_MSG = json.dumps(
    {"messageType": "H", "response": {"code": 200, "message": "HeartBeat"}})
# Formato enxuto do thresholdLevel 6 — capturado ao vivo em 2026-06-12 (o tier
# atual da conta só aceita o nível 6; ver docstring do worker)
TRADE_MSG_L6 = ('{"service": "iex","messageType":"A",'
                '"data":["2026-06-12T13:13:20.100964571-04:00","tsla",401.32]}')


class StubClient:
    """Transporte fake para o Hub: só precisa de uma corrotina send(str)."""

    def __init__(self, fail: bool = False) -> None:
        self.sent: list[dict] = []
        self.fail = fail

    async def send(self, raw: str) -> None:
        if self.fail:
            raise ConnectionError("client gone")
        self.sent.append(json.loads(raw))


# ──────────────────────────────────────────────────────────────────────────────
# Parsing da mensagem Tiingo IEX
# ──────────────────────────────────────────────────────────────────────────────
def test_parse_trade_message_official_doc_example():
    tick = lf.parse_iex_message(TRADE_MSG)
    assert tick is not None
    assert tick["type"] == "tick"
    assert tick["symbol"] == "WES"          # data[3], normalizado p/ maiúsculo
    assert tick["price"] == 50.285          # data[9] lastPrice
    assert tick["size"] == 200              # data[10] lastSize
    # 13:33:45 em -05:00 → 18:33:45 UTC, ns truncados, sufixo Z
    assert tick["time"] == "2019-01-30T18:33:45Z"
    assert "source" not in tick             # tick real não leva marca de sim


def test_parse_trade_message_threshold6_trimmed_format():
    tick = lf.parse_iex_message(TRADE_MSG_L6)
    assert tick is not None
    assert tick["symbol"] == "TSLA"
    assert tick["price"] == 401.32
    assert tick["size"] == 0                # nível 6 não traz lastSize
    assert tick["time"] == "2026-06-12T17:13:20Z"


@pytest.mark.parametrize("raw", [
    QUOTE_MSG,                                # quote não vira tick
    HEARTBEAT_MSG,                            # heartbeat
    json.dumps({"messageType": "I", "data": {"subscriptionId": 1}}),
    json.dumps({"messageType": "A", "data": ["T", "2019-01-30", 1, "wes"]}),
    "not json {{{",
    json.dumps(["T", 1, 2]),
])
def test_parse_rejects_non_trade_frames(raw):
    assert lf.parse_iex_message(raw) is None


def test_parse_trade_without_price_is_rejected():
    msg = json.loads(TRADE_MSG)
    msg["data"][9] = None
    assert lf.parse_iex_message(json.dumps(msg)) is None


# ──────────────────────────────────────────────────────────────────────────────
# Hub — subscribe / unsubscribe / fan-out
# ──────────────────────────────────────────────────────────────────────────────
def test_hub_fanout_routes_only_subscribed_symbols():
    async def scenario():
        hub = lf.Hub()
        a, b = StubClient(), StubClient()
        hub.add(a), hub.add(b)
        hub.subscribe(a, ["TSLA", "AAPL"])
        hub.subscribe(b, ["AAPL"])
        assert hub.union() == {"TSLA", "AAPL"}

        tick = {"type": "tick", "symbol": "TSLA", "price": 388.42,
                "size": 1200, "time": "2026-06-12T14:32:08Z"}
        assert await hub.publish(tick) == 1          # só o cliente A
        assert a.sent == [tick] and b.sent == []

        hub.unsubscribe(a, ["TSLA"])
        assert await hub.publish(tick) == 0          # ninguém mais assina TSLA
        assert hub.union() == {"AAPL"}
    asyncio.run(scenario())


def test_hub_drops_dead_client_silently_and_keeps_fanout():
    async def scenario():
        hub = lf.Hub()
        dead, alive = StubClient(fail=True), StubClient()
        hub.add(dead), hub.add(alive)
        hub.subscribe(dead, ["TSLA"])
        hub.subscribe(alive, ["TSLA"])
        tick = {"type": "tick", "symbol": "TSLA", "price": 1.0,
                "size": 1, "time": "2026-06-12T00:00:00Z"}
        assert await hub.publish(tick) == 1          # vivo recebe, morto sai
        assert hub.client_count == 1
        assert alive.sent == [tick]
    asyncio.run(scenario())


def test_hub_union_changed_event_fires_on_real_changes_only():
    async def scenario():
        hub = lf.Hub()
        c = StubClient()
        hub.add(c)
        hub.subscribe(c, ["TSLA"])
        assert hub.union_changed.is_set()
        hub.union_changed.clear()
        hub.subscribe(c, ["TSLA"])                   # repetido → união igual
        assert not hub.union_changed.is_set()
        hub.drop(c)
        assert hub.union_changed.is_set()            # união esvaziou
    asyncio.run(scenario())


# ──────────────────────────────────────────────────────────────────────────────
# Simulador random-walk
# ──────────────────────────────────────────────────────────────────────────────
def test_simulator_random_walk_is_plausible_and_marked_as_sim():
    hub = lf.Hub()
    sim = lf.Simulator(hub)
    first = sim.step("TSLA")
    assert first["source"] == "sim"
    assert first["symbol"] == "TSLA"
    assert abs(first["price"] / lf.BASE_PRICES["TSLA"] - 1.0) < 0.01
    prev = first["price"]
    for _ in range(200):                             # passos ~0,09% (gauss)
        tick = sim.step("TSLA")
        assert tick["price"] > 0 and tick["size"] > 0
        assert abs(tick["price"] / prev - 1.0) < 0.01
        prev = tick["price"]


def test_simulator_starts_from_last_real_price_when_known():
    hub = lf.Hub()
    hub.last_prices["XYZW"] = 42.0
    sim = lf.Simulator(hub)
    assert abs(sim.step("XYZW")["price"] / 42.0 - 1.0) < 0.01
    # símbolo desconhecido sem preço real → âncora determinística plausível
    assert 20.0 <= lf.default_price("ZZZTOP") <= 520.0
    assert lf.default_price("ZZZTOP") == lf.default_price("ZZZTOP")


def test_sim_mode_activation_rules():
    srv = lf.LiveFeedServer(api_key=None)            # sem chave → forçado
    assert srv.sim_forced and srv.sim_active
    srv = lf.LiveFeedServer(api_key="k", sim_forced=True)   # SIM_MODE=1
    assert srv.sim_active
    srv = lf.LiveFeedServer(api_key="k")
    assert not srv.sim_active                        # acabou de subir
    srv.last_real_tick -= lf.SIM_AFTER_S + 1         # 60s sem tick real
    assert srv.sim_active
    import time
    srv.last_real_tick = time.monotonic()            # tick real voltou
    assert not srv.sim_active


# ──────────────────────────────────────────────────────────────────────────────
# End-to-end: servidor em modo sim + cliente websockets real
# ──────────────────────────────────────────────────────────────────────────────
def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_e2e_subscribe_stream_and_health():
    websockets = pytest.importorskip("websockets")
    httpx = pytest.importorskip("httpx")

    async def scenario():
        server = lf.LiveFeedServer(api_key=None)     # modo sim forçado
        port = _free_port()
        run = asyncio.create_task(server.run(port))
        await asyncio.sleep(0.3)
        try:
            async with websockets.connect(f"ws://127.0.0.1:{port}/stream") as ws:
                await ws.send(json.dumps(
                    {"action": "subscribe", "symbols": ["TSLA"]}))
                ticks = []
                while len(ticks) < 2:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), 10))
                    if msg.get("type") == "subscribed":
                        assert msg["symbols"] == ["TSLA"]
                    elif msg.get("type") == "tick":
                        ticks.append(msg)
                for t in ticks:
                    assert t["symbol"] == "TSLA" and t["source"] == "sim"
                    assert t["price"] > 0 and t["size"] > 0
                    assert t["time"].endswith("Z")
                async with httpx.AsyncClient() as client:
                    health = (await client.get(
                        f"http://127.0.0.1:{port}/health")).json()
                assert health["status"] == "ok"
                assert health["mode"] == "sim"
                assert health["clients"] == 1
                assert health["symbols"] == ["TSLA"]
        finally:
            run.cancel()
            try:
                await run
            except (asyncio.CancelledError, Exception):
                pass

    asyncio.run(scenario())
