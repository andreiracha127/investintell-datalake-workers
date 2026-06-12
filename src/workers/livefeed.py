"""livefeed worker — servidor WebSocket de live price (fan-out Tiingo IEX).

Serviço LONG-RUNNING (não cron, não usa src.run_worker): aceita clientes em
``/stream``, faz fan-out por símbolo e assina dinamicamente o upstream Tiingo
IEX (``wss://api.tiingo.com/iex``) com a UNIÃO dos símbolos pedidos pelos
clientes. Entry point próprio: ``python -m src.workers.livefeed``.

Protocolo (design/CHART_ARCHITECTURE.md §1 — o chart-engine.js só consome
``type == "tick"``; mensagens extras são ignoradas pelo cliente):

  cliente → servidor  {"action":"subscribe","symbols":["TSLA","AAPL"]}
                      {"action":"unsubscribe","symbols":["AAPL"]}
  servidor → cliente  {"type":"tick","symbol":"TSLA","price":388.42,
                       "size":1200,"time":"2026-06-12T14:32:08Z"}

Formatos de trade do Tiingo IEX (doc oficial,
api.tiingo.com/documentation/websockets/iex) — o parser aceita AMBOS:

* thresholdLevel <= 5 (formato completo; exemplo literal da doc:
  ``{"messageType":"A","service":"iex","data":["T","2019-01-30T13:33:45.59-05:00",
  1548873225594808294,"wes",null,null,null,null,null,50.285,200,...]}``):
    data[0]=tipo ('T' trade / 'Q' quote / 'B' break)   data[1]=data ISO c/ tz
    data[2]=epoch ns   data[3]=ticker (minúsculo)
    data[4..8]=bidSize,bidPrice,midPrice,askPrice,askSize (null em trades)
    data[9]=lastPrice   data[10]=lastSize
* thresholdLevel 6 (formato last-trade enxuto — o ÚNICO aceito pelo tier
  atual da conta; verificado ao vivo em 2026-06-12: nível 5 responde
  ``{"messageType":"E","response":{"code":400,"message":"thresholdLevel not
  valid for your subscription tier..."}}``):
    data = [dataISO, ticker, lastPrice]  (sem size → tick sai com size 0)

Nível enviado ao upstream: env TIINGO_THRESHOLD_LEVEL (default 6).

Fallback simulado (random walk ~0,09%/passo, intervalo 0,3–1,2s por símbolo,
campo extra ``"source":"sim"``): liga quando TIINGO_API_KEY falta, SIM_MODE=1,
ou nenhum tick real chega em SIM_AFTER_S (fora do pregão dos EUA a Tiingo não
emite trades); desliga sozinho quando ticks reais voltam. Upstream reconecta
com backoff exponencial.

HTTP ``GET /health`` → {"status":"ok","clients":N,"mode":"live"|"sim",...}
(healthcheck do Railway; porta via env PORT, default 8080).
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import hashlib
import json
import logging
import os
import random
import re
import time
from http import HTTPStatus
from typing import Any

log = logging.getLogger("livefeed")

TIINGO_WS_URL = "wss://api.tiingo.com/iex"
THRESHOLD_LEVEL = int(os.getenv("TIINGO_THRESHOLD_LEVEL", "6"))
SIM_AFTER_S = 60.0          # sem tick real por 60s → liga o simulador
SIM_STEP_PCT = 0.0009       # ~0,09% por passo do random walk
SIM_INTERVAL_S = (0.3, 1.2)
SIM_SCAN_S = 0.1            # granularidade do loop do simulador
BACKOFF_MAX_S = 60.0
PING_INTERVAL_S = 20.0

# Âncoras de preço plausíveis para o simulador (mesmas do protótipo do chart);
# símbolos fora da tabela ganham um preço determinístico via hash.
BASE_PRICES = {
    "TSLA": 388.27, "AAPL": 272.50, "MSFT": 510.30, "NVDA": 182.10,
    "AMZN": 244.60, "GOOGL": 318.40, "META": 640.20, "SPY": 685.40,
    "QQQ": 610.80, "HYG": 81.20, "IEF": 96.40,
}


def default_price(symbol: str) -> float:
    """Preço inicial plausível: âncora conhecida ou hash determinístico 20–520."""
    base = BASE_PRICES.get(symbol.upper())
    if base is not None:
        return base
    digest = hashlib.sha256(symbol.upper().encode()).digest()
    return 20.0 + (int.from_bytes(digest[:4], "big") % 50_000) / 100.0


def utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_FRACTION_RE = re.compile(r"(\.\d{6})\d+")  # nanossegundos → microssegundos


def _iso_to_utc(stamp: str) -> str | None:
    """ISO da Tiingo (precisão ns, tz local da bolsa) → 'YYYY-MM-DDTHH:MM:SSZ'."""
    try:
        dt = _dt.datetime.fromisoformat(_FRACTION_RE.sub(r"\1", stamp))
        if dt.tzinfo is None:
            return None
        return dt.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def parse_iex_message(raw: str | bytes) -> dict[str, Any] | None:
    """Frame Tiingo IEX → tick do protocolo do chart; None se não for trade.

    Aceita o formato completo (thresholdLevel <= 5, trade 'T') e o formato
    enxuto do thresholdLevel 6 ([dataISO, ticker, lastPrice], sem size).
    """
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(msg, dict) or msg.get("messageType") != "A":
        return None
    data = msg.get("data")
    if not isinstance(data, list):
        return None
    if len(data) == 3 and isinstance(data[1], str):     # thresholdLevel 6
        stamp, ticker, price, size = data[0], data[1], data[2], 0
    elif len(data) >= 11 and data[0] == "T":            # formato completo
        stamp, ticker, price, size = data[1], data[3], data[9], data[10]
    else:
        return None
    if not ticker or not isinstance(price, (int, float)):
        return None
    when = _iso_to_utc(str(stamp)) if stamp else None
    return {
        "type": "tick",
        "symbol": str(ticker).upper(),
        "price": float(price),
        "size": int(size) if size else 0,
        "time": when or utcnow_iso(),
    }


def _clean_symbols(symbols: Any) -> list[str]:
    if not isinstance(symbols, list):
        return []
    out = []
    for s in symbols:
        if isinstance(s, str) and 0 < len(s.strip()) <= 24:
            out.append(s.strip().upper())
    return out


class Hub:
    """Registro de clientes × símbolos com fan-out; agnóstico de transporte.

    ``client`` é qualquer objeto com corrotina ``send(str)`` (o ServerConnection
    do websockets nos handlers reais; stubs nos testes). Erros de envio derrubam
    só aquele cliente — nunca o loop de publicação.
    """

    def __init__(self) -> None:
        self._clients: dict[Any, set[str]] = {}
        self.union_changed = asyncio.Event()
        self.last_prices: dict[str, float] = {}

    # -- registro -----------------------------------------------------------
    def add(self, client: Any) -> None:
        self._clients.setdefault(client, set())

    def drop(self, client: Any) -> None:
        before = self.union()
        self._clients.pop(client, None)
        if self.union() != before:
            self.union_changed.set()

    def subscribe(self, client: Any, symbols: list[str]) -> set[str]:
        before = self.union()
        subs = self._clients.setdefault(client, set())
        subs.update(symbols)
        if self.union() != before:
            self.union_changed.set()
        return set(subs)

    def unsubscribe(self, client: Any, symbols: list[str]) -> set[str]:
        before = self.union()
        subs = self._clients.setdefault(client, set())
        subs.difference_update(symbols)
        if self.union() != before:
            self.union_changed.set()
        return set(subs)

    # -- consulta ------------------------------------------------------------
    def union(self) -> set[str]:
        out: set[str] = set()
        for subs in self._clients.values():
            out |= subs
        return out

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # -- fan-out -------------------------------------------------------------
    async def publish(self, tick: dict[str, Any]) -> int:
        """Envia o tick a todos os assinantes do símbolo; retorna nº entregue."""
        symbol = tick["symbol"]
        self.last_prices[symbol] = tick["price"]
        targets = [c for c, subs in self._clients.items() if symbol in subs]
        if not targets:
            return 0
        raw = json.dumps(tick)
        results = await asyncio.gather(
            *(c.send(raw) for c in targets), return_exceptions=True
        )
        delivered = 0
        for client, res in zip(targets, results):
            if isinstance(res, BaseException):
                self.drop(client)  # desconexão suja → remove em silêncio
            else:
                delivered += 1
        return delivered


class Simulator:
    """Random walk por símbolo (~0,09%/passo, 0,3–1,2s), partindo do último
    preço real conhecido ou de uma âncora plausível."""

    def __init__(self, hub: Hub, rng: random.Random | None = None) -> None:
        self.hub = hub
        self.rng = rng or random.Random()
        self.prices: dict[str, float] = {}
        self._due: dict[str, float] = {}

    def step(self, symbol: str) -> dict[str, Any]:
        price = self.prices.get(symbol)
        if price is None:
            price = self.hub.last_prices.get(symbol) or default_price(symbol)
        price = max(0.01, price * (1.0 + self.rng.gauss(0.0, SIM_STEP_PCT)))
        self.prices[symbol] = price
        return {
            "type": "tick",
            "symbol": symbol,
            "price": round(price, 2),
            "size": self.rng.choice((100, 200, 300, 500, 800, 1200, 2000)),
            "time": utcnow_iso(),
            "source": "sim",
        }

    def due_symbols(self, now: float) -> list[str]:
        out = []
        for symbol in self.hub.union():
            due = self._due.get(symbol, 0.0)
            if now >= due:
                out.append(symbol)
                self._due[symbol] = now + self.rng.uniform(*SIM_INTERVAL_S)
        return out


class LiveFeedServer:
    """Orquestra servidor WS de clientes, upstream Tiingo e simulador."""

    def __init__(self, api_key: str | None = None,
                 sim_forced: bool = False) -> None:
        self.hub = Hub()
        self.sim = Simulator(self.hub)
        self.api_key = api_key
        self.sim_forced = sim_forced or not api_key
        self.upstream_connected = False
        self.last_real_tick = time.monotonic()

    # -- estado ---------------------------------------------------------------
    @property
    def sim_active(self) -> bool:
        if self.sim_forced:
            return True
        return (time.monotonic() - self.last_real_tick) > SIM_AFTER_S

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "clients": self.hub.client_count,
            "symbols": sorted(self.hub.union()),
            "mode": "sim" if self.sim_active else "live",
            "upstream_connected": self.upstream_connected,
        }

    # -- HTTP (healthcheck) -----------------------------------------------------
    def process_request(self, connection: Any, request: Any):
        """Responde GET /health sem upgrade; rejeita paths != /stream."""
        path = request.path.split("?", 1)[0]
        if path == "/health":
            resp = connection.respond(HTTPStatus.OK, json.dumps(self.health()) + "\n")
            resp.headers["Content-Type"] = "application/json"
            return resp
        if path != "/stream":
            return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")
        return None  # segue com o handshake WebSocket

    # -- clientes ---------------------------------------------------------------
    async def handle_client(self, ws: Any) -> None:
        self.hub.add(ws)
        log.info("cliente conectado (%d ativos)", self.hub.client_count)
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if not isinstance(msg, dict):
                    continue
                symbols = _clean_symbols(msg.get("symbols"))
                action = msg.get("action")
                if action == "subscribe" and symbols:
                    subs = self.hub.subscribe(ws, symbols)
                    await ws.send(json.dumps(
                        {"type": "subscribed", "symbols": sorted(subs)}))
                elif action == "unsubscribe" and symbols:
                    subs = self.hub.unsubscribe(ws, symbols)
                    await ws.send(json.dumps(
                        {"type": "subscribed", "symbols": sorted(subs)}))
        except Exception:
            pass  # desconexão suja é rotina — não derruba o loop
        finally:
            self.hub.drop(ws)
            log.info("cliente saiu (%d ativos)", self.hub.client_count)

    # -- upstream Tiingo ----------------------------------------------------------
    def _sub_msg(self, event: str, tickers: set[str]) -> str:
        return json.dumps({
            "eventName": event,
            "authorization": self.api_key,
            "eventData": {
                "thresholdLevel": THRESHOLD_LEVEL,
                "tickers": sorted(t.lower() for t in tickers),
            },
        })

    async def _upstream_session(self, connect=None) -> None:
        """Uma sessão com a Tiingo: assina a união e repassa trades ao hub."""
        import websockets

        connect = connect or websockets.connect
        async with connect(TIINGO_WS_URL, ping_interval=PING_INTERVAL_S) as ws:
            self.upstream_connected = True
            subscribed = self.hub.union()
            await ws.send(self._sub_msg("subscribe", subscribed or {"spy"}))
            log.info("upstream Tiingo conectado (%d tickers)", len(subscribed))
            while True:
                # acorda com mensagem do upstream OU mudança na união de símbolos
                recv = asyncio.ensure_future(ws.recv())
                changed = asyncio.ensure_future(self.hub.union_changed.wait())
                done, pending = await asyncio.wait(
                    {recv, changed}, return_when=asyncio.FIRST_COMPLETED)
                for fut in pending:
                    fut.cancel()  # cancelar recv() é seguro (não perde mensagem)
                self.hub.union_changed.clear()
                if recv in done:
                    raw = recv.result()  # ConnectionClosed propaga → reconecta
                    tick = parse_iex_message(raw)
                    if tick is not None:
                        self.last_real_tick = time.monotonic()
                        await self.hub.publish(tick)
                # sincroniza a assinatura upstream com a união corrente
                current = self.hub.union()
                if current != subscribed:
                    added, removed = current - subscribed, subscribed - current
                    if added:
                        await ws.send(self._sub_msg("subscribe", added))
                    if removed:
                        await ws.send(self._sub_msg("unsubscribe", removed))
                    subscribed = current
                    log.info("upstream re-assinado: %s", sorted(current))

    async def upstream_task(self) -> None:
        if self.sim_forced:
            log.info("upstream desativado (%s)",
                     "SIM_MODE=1" if self.api_key else "TIINGO_API_KEY ausente")
            return
        backoff = 1.0
        while True:
            started = time.monotonic()
            try:
                await self._upstream_session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("upstream caiu: %s — reconectando em %.0fs",
                            type(exc).__name__, backoff)
            finally:
                self.upstream_connected = False
            if time.monotonic() - started > 120.0:
                backoff = 1.0  # sessão estável → zera o backoff
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, BACKOFF_MAX_S)

    # -- simulador -------------------------------------------------------------
    async def sim_task(self) -> None:
        announced = False
        while True:
            if self.sim_active:
                if not announced:
                    log.info("simulador LIGADO (%s)",
                             "forçado" if self.sim_forced else
                             f"sem tick real há {SIM_AFTER_S:.0f}s")
                    announced = True
                now = time.monotonic()
                for symbol in self.sim.due_symbols(now):
                    await self.hub.publish(self.sim.step(symbol))
            elif announced:
                log.info("simulador DESLIGADO — ticks reais voltaram")
                announced = False
                self.sim.prices.clear()  # próximo sim parte do último preço real
            await asyncio.sleep(SIM_SCAN_S)

    async def run(self, port: int) -> None:
        from websockets.asyncio.server import serve

        async with serve(self.handle_client, "0.0.0.0", port,
                         process_request=self.process_request,
                         ping_interval=PING_INTERVAL_S):
            log.info("livefeed ouvindo em :%d (/stream, /health) — modo %s",
                     port, "sim" if self.sim_active else "live")
            await asyncio.gather(self.upstream_task(), self.sim_task())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    server = LiveFeedServer(
        api_key=os.getenv("TIINGO_API_KEY"),
        sim_forced=os.getenv("SIM_MODE") == "1")
    asyncio.run(server.run(int(os.getenv("PORT", "8080"))))


if __name__ == "__main__":
    main()
