"""
╔═══════════════════════════════════════════════════════════════╗
║           NSE PAPER TRADING DATA SERVER  v5.0                 ║
║           Production-grade · Host anywhere · Zero auth        ║
╚═══════════════════════════════════════════════════════════════╝

Features:
  ✅ Live NSE option chain (Mon–Fri 9:15–15:30 IST)
  ✅ Smart mock data outside market hours (for dev/testing)
  ✅ Disk-persisted cache — survives restarts
  ✅ Auto-switches to real data the moment market opens
  ✅ Rate-limit safe — single NSE fetch, distributed to all clients
  ✅ CORS open — fetch from any frontend or Java backend
  ✅ WebSocket + REST — choose what suits your client
  ✅ Per-symbol subscriptions (NIFTY, BANKNIFTY, FINNIFTY, equities)
  ✅ Full OI, IV, LTP, bid/ask, volume, PCR, max-pain
  ✅ /health endpoint for uptime monitors

Setup:
    pip install fastapi uvicorn nsepython websockets

Run locally:
    python server.py

Deploy on Railway / Render / any VPS:
    Same command. Set PORT env var if needed.

Java client example:
    GET  http://your-host/snapshot?symbol=NIFTY
    GET  http://your-host/atm?symbol=BANKNIFTY
    WS   ws://your-host/ws?symbol=NIFTY

Docs:
    http://localhost:8080/docs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from nsepython import nse_optionchain_scrapper

# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════

FETCH_INTERVAL   = int(os.getenv("FETCH_INTERVAL",  "5"))    # seconds
DEFAULT_SYMBOL   = os.getenv("DEFAULT_SYMBOL", "NIFTY")
PORT             = int(os.getenv("PORT", "8080"))
CACHE_FILE       = os.getenv("CACHE_FILE", "cache.json")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")
IST              = timezone(timedelta(hours=5, minutes=30))

SUPPORTED_INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s │ %(levelname)-8s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("NSE")

# ══════════════════════════════════════════════════════════════
#  MARKET HOURS
# ══════════════════════════════════════════════════════════════

def ist_now() -> datetime:
    return datetime.now(IST)

def is_market_open() -> bool:
    now = ist_now()
    if now.weekday() >= 5:
        return False
    o = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    c = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return o <= now <= c

def next_market_open() -> str:
    now = ist_now()
    days_ahead = 0
    while True:
        candidate = now + timedelta(days=days_ahead)
        if candidate.weekday() < 5:
            open_time = candidate.replace(hour=9, minute=15, second=0, microsecond=0)
            if open_time > now:
                return open_time.strftime("%d-%b-%Y %H:%M IST")
        days_ahead += 1
        if days_ahead > 7:
            break
    return "Next weekday 09:15 IST"

# ══════════════════════════════════════════════════════════════
#  IN-MEMORY CACHE
# ══════════════════════════════════════════════════════════════

_cache:    dict[str, dict] = {}   # symbol → snapshot
_ws_stats: dict[str, int]  = {}   # symbol → subscriber count

def _save_cache() -> None:
    try:
        real = {k: v for k, v in _cache.items() if not v.get("mock")}
        if real:
            with open(CACHE_FILE, "w") as f:
                json.dump(real, f, indent=2)
            log.debug(f"[DISK] Saved {list(real.keys())} to {CACHE_FILE}")
    except Exception as e:
        log.warning(f"[DISK] Save failed: {e}")

def _load_cache() -> None:
    if not os.path.exists(CACHE_FILE):
        return
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        for sym, snap in data.items():
            snap["stale"]     = True
            snap["mock"]      = False
            snap["dataSource"] = "disk"
            snap["note"]      = f"Stale — last real data from {snap.get('fetchedAt', '?')}"
            _cache[sym] = snap
            log.info(f"[DISK] Restored {sym} (fetchedAt={snap.get('fetchedAt')})")
    except Exception as e:
        log.warning(f"[DISK] Load failed: {e}")

# ══════════════════════════════════════════════════════════════
#  MOCK DATA  (realistic, seed-stable)
# ══════════════════════════════════════════════════════════════

def _make_mock(symbol: str) -> dict:
    import random
    rng = random.Random(42)

    bases = {"NIFTY": 22450.0, "BANKNIFTY": 48200.0, "FINNIFTY": 23800.0}
    steps = {"NIFTY": 50,      "BANKNIFTY": 100,      "FINNIFTY": 50}
    base  = bases.get(symbol.upper(), 20000.0)
    step  = steps.get(symbol.upper(), 50)
    atm   = round(base / step) * step

    expiries = ["27-Mar-2026", "03-Apr-2026", "10-Apr-2026", "24-Apr-2026", "29-May-2026"]
    strikes  = range(int(atm - step * 15), int(atm + step * 16), step)
    chain    = []

    total_call_oi = 0
    total_put_oi  = 0

    for s in strikes:
        dist   = abs(s - atm) / step
        is_atm = s == atm
        itm_c  = s < atm
        itm_p  = s > atm

        call_ltp = round(max(0.05, (atm - s + rng.uniform(80, 120)) if itm_c else rng.uniform(2, 100) / (dist + 1)), 2)
        put_ltp  = round(max(0.05, (s - atm + rng.uniform(80, 120)) if itm_p else rng.uniform(2, 100) / (dist + 1)), 2)
        call_iv  = round(11 + dist * 1.3 + rng.uniform(-0.8, 0.8), 2)
        put_iv   = round(12 + dist * 1.3 + rng.uniform(-0.8, 0.8), 2)
        call_oi  = int(rng.uniform(300, 6000) / (dist + 0.8) * 100) * 25
        put_oi   = int(rng.uniform(300, 6000) / (dist + 0.8) * 100) * 25
        total_call_oi += call_oi
        total_put_oi  += put_oi

        chain.append({
            "strike": float(s),
            "isATM":  is_atm,
            "call": {
                "expiry":  expiries[0],
                "LTP":     call_ltp,
                "IV":      call_iv,
                "OI":      call_oi,
                "chgOI":   int(call_oi * rng.uniform(-0.12, 0.12)),
                "volume":  int(call_oi * rng.uniform(0.08, 0.35)),
                "change":  round(rng.uniform(-8, 8), 2),
                "bid":     round(call_ltp - rng.uniform(0.05, 0.5), 2),
                "bidQty":  rng.randint(25, 750),
                "ask":     round(call_ltp + rng.uniform(0.05, 0.5), 2),
                "askQty":  rng.randint(25, 750),
            },
            "put": {
                "expiry":  expiries[0],
                "LTP":     put_ltp,
                "IV":      put_iv,
                "OI":      put_oi,
                "chgOI":   int(put_oi * rng.uniform(-0.12, 0.12)),
                "volume":  int(put_oi * rng.uniform(0.08, 0.35)),
                "change":  round(rng.uniform(-8, 8), 2),
                "bid":     round(put_ltp - rng.uniform(0.05, 0.5), 2),
                "bidQty":  rng.randint(25, 750),
                "ask":     round(put_ltp + rng.uniform(0.05, 0.5), 2),
                "askQty":  rng.randint(25, 750),
            },
        })

    pcr = round(total_put_oi / total_call_oi, 4) if total_call_oi else 0

    # Max pain: strike where total loss of option buyers is maximum
    max_pain_strike = _calc_max_pain(chain)

    return {
        "symbol":         symbol.upper(),
        "underlying":     base,
        "atmStrike":      float(atm),
        "timestamp":      ist_now().strftime("%d-%b-%Y %H:%M:%S"),
        "fetchedAt":      datetime.utcnow().isoformat() + "Z",
        "expiryDates":    expiries,
        "strikes":        len(chain),
        "chain":          chain,
        "analytics": {
            "pcr":          pcr,
            "maxPain":      max_pain_strike,
            "totalCallOI":  total_call_oi,
            "totalPutOI":   total_put_oi,
        },
        "stale":      False,
        "mock":       True,
        "dataSource": "mock",
        "note":       f"⚠️ MOCK DATA — market closed. Live data from {next_market_open()}.",
    }

# ══════════════════════════════════════════════════════════════
#  ANALYTICS HELPERS
# ══════════════════════════════════════════════════════════════

def _calc_max_pain(chain: list[dict]) -> Optional[float]:
    """Max pain: strike where combined OI loss for buyers is maximised for writers."""
    if not chain:
        return None
    strikes = [r["strike"] for r in chain]
    best_strike, min_loss = None, float("inf")
    for target in strikes:
        total_loss = 0.0
        for row in chain:
            s = row["strike"]
            call_oi = (row.get("call") or {}).get("OI") or 0
            put_oi  = (row.get("put")  or {}).get("OI") or 0
            if target > s:
                total_loss += (target - s) * call_oi
            if target < s:
                total_loss += (s - target) * put_oi
        if total_loss < min_loss:
            min_loss     = total_loss
            best_strike  = target
    return best_strike

def _enrich_snapshot(snap: dict) -> dict:
    """Add PCR + max-pain to a live snapshot."""
    chain = snap.get("chain", [])
    total_c = sum((r.get("call") or {}).get("OI") or 0 for r in chain)
    total_p = sum((r.get("put")  or {}).get("OI") or 0 for r in chain)
    pcr     = round(total_p / total_c, 4) if total_c else 0
    snap["analytics"] = {
        "pcr":         pcr,
        "maxPain":     _calc_max_pain(chain),
        "totalCallOI": total_c,
        "totalPutOI":  total_p,
    }
    return snap

# ══════════════════════════════════════════════════════════════
#  NSE FETCH + PARSE
# ══════════════════════════════════════════════════════════════

def _fetch_and_parse(symbol: str) -> Optional[dict]:
    t0 = time.perf_counter()
    try:
        raw = nse_optionchain_scrapper(symbol.upper())
    except Exception as e:
        log.warning(f"[NSE] {symbol} fetch error: {e}")
        return None

    rec  = (raw or {}).get("records", {})
    rows = rec.get("data", [])
    if not rows:
        return None

    ce_map: dict[float, dict] = {}
    pe_map: dict[float, dict] = {}

    for row in rows:
        strike = row.get("strikePrice")
        if not strike:
            continue
        ce     = row.get("CE") or {}
        pe     = row.get("PE") or {}
        expiry = row.get("expiryDate")

        def _opt(d: dict) -> dict:
            return {
                "expiry":  expiry,
                "LTP":     d.get("lastPrice"),
                "IV":      d.get("impliedVolatility"),
                "OI":      d.get("openInterest"),
                "chgOI":   d.get("changeinOpenInterest"),
                "volume":  d.get("totalTradedVolume"),
                "change":  d.get("change"),
                "bid":     d.get("bidprice"),
                "bidQty":  d.get("bidQty"),
                "ask":     d.get("askPrice"),
                "askQty":  d.get("askQty"),
            }

        if ce: ce_map[float(strike)] = _opt(ce)
        if pe: pe_map[float(strike)] = _opt(pe)

    underlying  = float(rec.get("underlyingValue") or 0)
    all_strikes = sorted(set(list(ce_map) + list(pe_map)))
    atm = min(all_strikes, key=lambda s: abs(s - underlying)) if all_strikes else None

    chain = [
        {"strike": s, "isATM": s == atm, "call": ce_map.get(s), "put": pe_map.get(s)}
        for s in all_strikes
    ]

    snap = {
        "symbol":      symbol.upper(),
        "underlying":  underlying,
        "atmStrike":   atm,
        "timestamp":   rec.get("timestamp"),
        "fetchedAt":   datetime.utcnow().isoformat() + "Z",
        "fetchMs":     round((time.perf_counter() - t0) * 1000),
        "expiryDates": rec.get("expiryDates", []),
        "strikes":     len(chain),
        "chain":       chain,
        "stale":       False,
        "mock":        False,
        "dataSource":  "live",
    }
    return _enrich_snapshot(snap)

# ══════════════════════════════════════════════════════════════
#  WEBSOCKET MANAGER
# ══════════════════════════════════════════════════════════════

class WSManager:
    def __init__(self):
        self._clients: dict[WebSocket, str] = {}  # ws → symbol

    async def connect(self, ws: WebSocket, symbol: str) -> None:
        await ws.accept()
        self._clients[ws] = symbol.upper()
        _ws_stats[symbol.upper()] = _ws_stats.get(symbol.upper(), 0) + 1
        log.info(f"[WS +] {symbol.upper()} | total={len(self._clients)}")

    def disconnect(self, ws: WebSocket) -> None:
        sym = self._clients.pop(ws, None)
        if sym:
            _ws_stats[sym] = max(0, _ws_stats.get(sym, 1) - 1)
            log.info(f"[WS -] {sym} | total={len(self._clients)}")

    async def broadcast(self, symbol: str, payload: dict) -> None:
        dead = []
        msg  = json.dumps(payload)
        for ws, sym in self._clients.items():
            if sym == symbol.upper():
                try:
                    await ws.send_text(msg)
                except Exception:
                    dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def send(self, ws: WebSocket, payload: dict) -> None:
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            self.disconnect(ws)

    @property
    def count(self) -> int:
        return len(self._clients)

ws_manager = WSManager()

# ══════════════════════════════════════════════════════════════
#  BACKGROUND POLLER
# ══════════════════════════════════════════════════════════════

async def _poller() -> None:
    log.info(f"[POLLER] Started — interval={FETCH_INTERVAL}s")
    while True:
        symbols = set(_cache.keys()) | {DEFAULT_SYMBOL}

        if is_market_open():
            for sym in symbols:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, _fetch_and_parse, sym
                )
                if result:
                    _cache[sym] = result
                    _save_cache()
                    log.info(
                        f"[LIVE] {sym} | ₹{result['underlying']} | "
                        f"ATM={result['atmStrike']} | "
                        f"PCR={result['analytics']['pcr']} | "
                        f"{result['fetchMs']}ms"
                    )
                    # Broadcast to WebSocket subscribers
                    if ws_manager.count > 0:
                        await ws_manager.broadcast(sym, {**result, "type": "live"})
                else:
                    if sym in _cache and not _cache[sym].get("mock"):
                        _cache[sym]["stale"] = True
                        _cache[sym]["note"]  = f"Feed interrupted at {ist_now().strftime('%H:%M:%S IST')}"
                        log.warning(f"[STALE] {sym} — NSE returned empty during market hours")
        else:
            for sym in symbols:
                if sym not in _cache:
                    _cache[sym] = _make_mock(sym)
                    log.info(f"[MOCK] {sym} — injected mock data (market closed)")

        await asyncio.sleep(FETCH_INTERVAL)

# ══════════════════════════════════════════════════════════════
#  APP LIFESPAN
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_cache()
    task = asyncio.create_task(_poller())
    log.info(f"[SERVER] NSE Paper Trading Server v5.0 started on port {PORT}")
    log.info(f"[SERVER] Market open: {is_market_open()} | Next open: {next_market_open()}")
    yield
    task.cancel()
    log.info("[SERVER] Shutdown complete.")

# ══════════════════════════════════════════════════════════════
#  FASTAPI APP
# ══════════════════════════════════════════════════════════════

app = FastAPI(
    title="NSE Paper Trading Data Server",
    description="""
## NSE Live Option Chain API

Production-grade data server for paper trading platforms.

### Data Sources
- **Live** — NSE option chain during market hours (Mon–Fri 09:15–15:30 IST)
- **Stale** — Last real snapshot from disk after market close
- **Mock** — Realistic synthetic data for development/testing

### Connecting from Java
```java
// REST
HttpClient client = HttpClient.newHttpClient();
HttpRequest req = HttpRequest.newBuilder()
    .uri(URI.create("http://your-host/snapshot?symbol=NIFTY"))
    .build();

// WebSocket
WebSocket ws = client.newWebSocketBuilder()
    .buildAsync(URI.create("ws://your-host/ws?symbol=NIFTY"), listener)
    .join();
```

### Response flags
Every response includes:
- `mock: true/false` — is this real or synthetic data?
- `stale: true/false` — is this from a previous session?
- `dataSource: "live" | "stale" | "mock" | "disk"`
    """,
    version="5.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Data-Source", "X-Mock", "X-Stale"],
)

# ── Helper ────────────────────────────────────────────────────

def _get(symbol: str) -> dict:
    sym = symbol.upper()
    if sym not in _cache:
        if is_market_open():
            result = _fetch_and_parse(sym)
            if result:
                _cache[sym] = result
                _save_cache()
                return result
        _cache[sym] = _make_mock(sym)
    return _cache[sym]

def _resp(d: dict):
    """Attach data-source headers to response."""
    return JSONResponse(
        content=d,
        headers={
            "X-Data-Source": d.get("dataSource", "unknown"),
            "X-Mock":        str(d.get("mock", False)).lower(),
            "X-Stale":       str(d.get("stale", False)).lower(),
            "X-Symbol":      d.get("symbol", ""),
        }
    )

# ══════════════════════════════════════════════════════════════
#  REST ENDPOINTS
# ══════════════════════════════════════════════════════════════

@app.get("/", tags=["Meta"], summary="Server status")
def root():
    """Health + status overview. Safe to poll from load balancers."""
    return {
        "server":      "NSE Paper Trading Data Server v5.0",
        "status":      "running",
        "marketOpen":  is_market_open(),
        "timeIST":     ist_now().strftime("%d-%b-%Y %H:%M:%S IST"),
        "nextOpen":    next_market_open() if not is_market_open() else "Market is OPEN",
        "wsClients":   ws_manager.count,
        "cached": {
            sym: {
                "underlying":  d["underlying"],
                "dataSource":  d.get("dataSource", "?"),
                "stale":       d.get("stale", False),
                "mock":        d.get("mock", False),
                "fetchedAt":   d["fetchedAt"],
                "strikes":     d.get("strikes", 0),
            }
            for sym, d in _cache.items()
        },
        "endpoints": {
            "snapshot":     "/snapshot?symbol=NIFTY",
            "chain":        "/chain?symbol=NIFTY&expiry=27-Mar-2026",
            "atm":          "/atm?symbol=NIFTY",
            "underlying":   "/underlying?symbol=NIFTY",
            "strike":       "/strike?symbol=NIFTY&price=22450",
            "analytics":    "/analytics?symbol=NIFTY",
            "expiry_dates": "/expiry-dates?symbol=NIFTY",
            "websocket":    f"ws://localhost:{PORT}/ws?symbol=NIFTY",
            "health":       "/health",
        }
    }


@app.get("/health", tags=["Meta"], summary="Health check for uptime monitors")
def health():
    """Minimal health endpoint. Returns 200 if server is alive."""
    return {"status": "ok", "ts": datetime.utcnow().isoformat() + "Z"}


@app.get("/snapshot", tags=["Data"], summary="Full option chain snapshot")
def snapshot(symbol: str = Query(DEFAULT_SYMBOL, description="NSE symbol e.g. NIFTY, BANKNIFTY, RELIANCE")):
    """
    Complete option chain — all strikes, all expiries, call + put.
    Includes PCR and max-pain in `analytics`.
    """
    return _resp(_get(symbol))


@app.get("/chain", tags=["Data"], summary="Option chain filtered by expiry")
def chain(
    symbol: str           = Query(DEFAULT_SYMBOL),
    expiry: Optional[str] = Query(None, description="e.g. 27-Mar-2026. Omit for all."),
):
    """Option chain rows for a specific expiry date."""
    d    = _get(symbol)
    rows = d["chain"]
    if expiry:
        rows = [
            r for r in rows
            if (r.get("call") or {}).get("expiry") == expiry
            or (r.get("put")  or {}).get("expiry") == expiry
        ]
    return _resp({
        **{k: d[k] for k in ["symbol", "underlying", "atmStrike", "fetchedAt",
                               "mock", "stale", "dataSource", "analytics"] if k in d},
        "expiry":  expiry or "all",
        "strikes": len(rows),
        "chain":   rows,
    })


@app.get("/atm", tags=["Data"], summary="ATM strike — call + put")
def atm(symbol: str = Query(DEFAULT_SYMBOL)):
    """Returns only the At-The-Money strike with full call and put data."""
    d   = _get(symbol)
    row = next((r for r in d["chain"] if r.get("isATM")), None)
    if not row:
        raise HTTPException(404, "ATM strike not found")
    return _resp({
        **{k: d[k] for k in ["symbol", "underlying", "atmStrike", "fetchedAt",
                               "mock", "stale", "dataSource"] if k in d},
        "call": row.get("call"),
        "put":  row.get("put"),
    })


@app.get("/underlying", tags=["Data"], summary="Underlying price + ATM + analytics")
def underlying(symbol: str = Query(DEFAULT_SYMBOL)):
    """Current underlying value, ATM strike, PCR, and max pain."""
    d = _get(symbol)
    return _resp({k: d[k] for k in
                  ["symbol", "underlying", "atmStrike", "timestamp",
                   "fetchedAt", "mock", "stale", "dataSource", "analytics"] if k in d})


@app.get("/analytics", tags=["Data"], summary="PCR, max-pain, OI summary")
def analytics(symbol: str = Query(DEFAULT_SYMBOL)):
    """
    Key derived metrics:
    - **PCR** (Put-Call Ratio) — > 1 bearish, < 1 bullish
    - **Max Pain** — strike where option buyers lose the most
    - **Total Call OI / Put OI**
    """
    d = _get(symbol)
    return _resp({
        "symbol":     d["symbol"],
        "underlying": d["underlying"],
        "fetchedAt":  d["fetchedAt"],
        "mock":       d.get("mock", False),
        "stale":      d.get("stale", False),
        "analytics":  d.get("analytics", {}),
    })


@app.get("/strike", tags=["Data"], summary="Single strike — call + put")
def strike(
    symbol: str   = Query(DEFAULT_SYMBOL),
    price:  float = Query(..., description="Strike price e.g. 22450"),
):
    """Full call and put data for one specific strike price."""
    d   = _get(symbol)
    row = next((r for r in d["chain"] if r["strike"] == price), None)
    if not row:
        available = [r["strike"] for r in d["chain"]]
        raise HTTPException(404, {
            "detail":    f"Strike {price} not found for {symbol}",
            "available": available[:10],
            "hint":      "Use /snapshot to see all available strikes",
        })
    return _resp({
        **{k: d[k] for k in ["symbol", "underlying", "fetchedAt", "mock", "stale", "dataSource"] if k in d},
        **row,
    })


@app.get("/expiry-dates", tags=["Data"], summary="All available expiry dates")
def expiry_dates(symbol: str = Query(DEFAULT_SYMBOL)):
    d = _get(symbol)
    return {"symbol": d["symbol"], "expiryDates": d["expiryDates"], "mock": d.get("mock", False)}


@app.get("/oi-buildup", tags=["Analysis"], summary="OI buildup — top strikes by open interest")
def oi_buildup(
    symbol: str = Query(DEFAULT_SYMBOL),
    expiry: Optional[str] = Query(None),
    top:    int = Query(10, description="Number of top strikes to return"),
):
    """
    Returns top strikes ranked by OI for calls and puts separately.
    Useful for identifying support/resistance levels.
    """
    d    = _get(symbol)
    rows = d["chain"]
    if expiry:
        rows = [r for r in rows if
                (r.get("call") or {}).get("expiry") == expiry or
                (r.get("put")  or {}).get("expiry") == expiry]

    call_oi = sorted(
        [{"strike": r["strike"], "OI": (r.get("call") or {}).get("OI") or 0,
          "chgOI": (r.get("call") or {}).get("chgOI") or 0} for r in rows],
        key=lambda x: x["OI"], reverse=True
    )[:top]

    put_oi = sorted(
        [{"strike": r["strike"], "OI": (r.get("put") or {}).get("OI") or 0,
          "chgOI": (r.get("put") or {}).get("chgOI") or 0} for r in rows],
        key=lambda x: x["OI"], reverse=True
    )[:top]

    return _resp({
        "symbol":     d["symbol"],
        "underlying": d["underlying"],
        "fetchedAt":  d["fetchedAt"],
        "mock":       d.get("mock", False),
        "topCallOI":  call_oi,
        "topPutOI":   put_oi,
        "interpretation": {
            "callWall": call_oi[0]["strike"] if call_oi else None,
            "putWall":  put_oi[0]["strike"]  if put_oi  else None,
            "note":     "Call wall = likely resistance. Put wall = likely support.",
        }
    })


@app.get("/iv-skew", tags=["Analysis"], summary="IV skew across strikes")
def iv_skew(
    symbol: str           = Query(DEFAULT_SYMBOL),
    expiry: Optional[str] = Query(None),
):
    """
    Implied volatility across all strikes.
    Useful for identifying skew and pricing anomalies.
    """
    d    = _get(symbol)
    rows = d["chain"]
    if expiry:
        rows = [r for r in rows if
                (r.get("call") or {}).get("expiry") == expiry or
                (r.get("put")  or {}).get("expiry") == expiry]

    skew = []
    for r in rows:
        call_iv = (r.get("call") or {}).get("IV")
        put_iv  = (r.get("put")  or {}).get("IV")
        skew.append({
            "strike":  r["strike"],
            "isATM":   r.get("isATM"),
            "callIV":  call_iv,
            "putIV":   put_iv,
            "skew":    round(put_iv - call_iv, 4) if (call_iv and put_iv) else None,
        })

    return _resp({
        "symbol":     d["symbol"],
        "underlying": d["underlying"],
        "fetchedAt":  d["fetchedAt"],
        "mock":       d.get("mock", False),
        "ivSkew":     skew,
    })


# ══════════════════════════════════════════════════════════════
#  WEBSOCKET ENDPOINT
# ══════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, symbol: str = DEFAULT_SYMBOL):
    """
    WebSocket — receive live snapshots pushed every ~5s.

    Connect: ws://localhost:8080/ws?symbol=NIFTY

    Messages from server:
      { type: "live" | "snapshot_cached", symbol, underlying, chain, ... }
      { type: "keepalive", ts }

    Messages you can send:
      { type: "ping" }
      { type: "subscribe", symbol: "BANKNIFTY" }
    """
    await ws_manager.connect(ws, symbol)

    # Send cached snapshot immediately on connect
    cached = _cache.get(symbol.upper())
    if cached:
        await ws_manager.send(ws, {**cached, "type": "snapshot_cached"})
    else:
        await ws_manager.send(ws, {
            "type":    "info",
            "message": f"Subscribed to {symbol.upper()}. First snapshot in ~{FETCH_INTERVAL}s",
        })

    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=30)
                msg = json.loads(raw)

                if msg.get("type") == "ping":
                    await ws_manager.send(ws, {
                        "type":       "pong",
                        "ts":         datetime.utcnow().isoformat() + "Z",
                        "clients":    ws_manager.count,
                        "marketOpen": is_market_open(),
                    })

                elif msg.get("type") == "subscribe" and msg.get("symbol"):
                    new_sym = msg["symbol"].upper()
                    ws_manager._clients[ws] = new_sym
                    snap = _cache.get(new_sym) or _make_mock(new_sym)
                    await ws_manager.send(ws, {**snap, "type": "snapshot_cached"})

            except asyncio.TimeoutError:
                await ws_manager.send(ws, {
                    "type": "keepalive",
                    "ts":   datetime.utcnow().isoformat() + "Z",
                })

    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════════╗
║          NSE PAPER TRADING DATA SERVER  v5.0                  ║
╠═══════════════════════════════════════════════════════════════╣
║  REST   →  http://localhost:8080                              ║
║  Docs   →  http://localhost:8080/docs                         ║
║  WS     →  ws://localhost:8080/ws?symbol=NIFTY                ║
╠═══════════════════════════════════════════════════════════════╣
║  Endpoints:                                                   ║
║    /snapshot      full chain                                  ║
║    /atm           ATM strike only                             ║
║    /chain         filter by expiry                            ║
║    /underlying    price + PCR + max-pain                      ║
║    /analytics     PCR, max-pain, OI totals                    ║
║    /oi-buildup    top OI strikes (support/resistance)         ║
║    /iv-skew       IV across all strikes                       ║
║    /strike        single strike lookup                        ║
║    /expiry-dates  available expiries                          ║
║    /health        uptime monitor endpoint                     ║
╚═══════════════════════════════════════════════════════════════╝
""")
    uvicorn.run("server:app", host="0.0.0.0", port=PORT, reload=False)
