# NSE Paper Trading Data Server v5.0

Production-grade NSE option chain API. No auth. No signup. Just data.

## Quick Start

```bash
pip install -r requirements.txt
python server.py
```

Open: http://localhost:8080/docs

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /snapshot?symbol=NIFTY` | Full option chain |
| `GET /atm?symbol=NIFTY` | ATM strike only |
| `GET /chain?symbol=NIFTY&expiry=27-Mar-2026` | Filter by expiry |
| `GET /underlying?symbol=NIFTY` | Price + PCR + max pain |
| `GET /analytics?symbol=NIFTY` | PCR, max pain, OI totals |
| `GET /oi-buildup?symbol=NIFTY` | Top OI strikes (support/resistance) |
| `GET /iv-skew?symbol=NIFTY` | IV across strikes |
| `GET /strike?symbol=NIFTY&price=22450` | Single strike |
| `GET /expiry-dates?symbol=NIFTY` | Available expiries |
| `GET /health` | Uptime check |
| `WS  /ws?symbol=NIFTY` | Live WebSocket stream |

## Deploy on Railway

1. Push this folder to GitHub
2. Connect repo on railway.app
3. Set env vars from `.env.example`
4. Deploy — Railway auto-detects the Procfile

## Deploy on Render

Same steps. Use `python server.py` as start command.

## Java Backend Example

```java
// REST
HttpClient client = HttpClient.newHttpClient();
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("https://your-host.railway.app/atm?symbol=NIFTY"))
    .GET().build();
HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

// Parse JSON
ObjectMapper mapper = new ObjectMapper();
JsonNode data = mapper.readTree(response.body());
double underlying = data.get("underlying").asDouble();
double callLTP = data.get("call").get("LTP").asDouble();
```

## Response Flags

Every response includes:
- `mock: true/false` — synthetic or real data
- `stale: true/false` — from previous session
- `dataSource: "live" | "disk" | "mock"`

HTTP headers also set:
- `X-Data-Source`
- `X-Mock`
- `X-Stale`
