# LAES — Leader-Anchored Adverse Excursion Scalping

LAES is a paper-only research terminal for public Binance Futures Copy Trading portfolios. It does not log in to trader accounts, bypass hidden portfolios, place exchange orders, or transfer funds.

## Current data flow

- **Leader discovery and positions:** when `KOPYON_API_KEY` is configured, the app uses Kopyon's documented Binance Copy Trading v5 API. Without the key it falls back to Binance's logged-out public Copy Trading page JSON routes. Only leaders passing configurable quality gates and whose positions endpoint responds successfully are eligible for live discovery. Profile and position history are fetched on demand and cached for 24 hours.
- **Live prices:** Binance USDⓈ-M Futures mark-price WebSocket is the primary monitoring feed. REST price fallback is a single bulk request at most once per minute; the client reconnects with backoff.
- **Historical audits:** separate from live monitoring. Opening the strategy-replay page reads stored audit/candle data only and never rehydrates history automatically. Historical candle/audit requests remain explicitly user-triggered and subject to the REST pause and rate limiter.
- **Strategies:** RECOVERY and MOMENTUM have separate qualification scans, result caches, decision rows, and UI selection. They share the paper wallet limits but their signals are not mixed.

Kopyon is the preferred documented leader-data provider; Binance remains the official market-price/futures source. The Binance Copy Trading JSON fallback is undocumented, may change or be independently rate-limited, and is used only when Kopyon is not configured. LAES fails closed when response shape/disclosure is unavailable. Provider routes, cadence, quota budget, and references: `docs/KOPYON_INTEGRATION.md`.

## Safety and rate limits

Binance market-data REST calls pass through one process-wide rolling request-weight limiter, defaulting to 60 weight units per 60 seconds. When Kopyon is configured, Kopyon REST calls use its separate account quota and response headers; the default schedule is intentionally conservative. These local guards do not account for other applications sharing the same provider account or public IP; run one application worker/replica.

Keep the conservative budgets and do not disable the Binance circuit breaker. The app never retries Binance requests during an active exchange-ban window. Kopyon's account-level monthly and per-minute limits are documented in its API reference.

## Leader quality defaults

Public portfolios must meet all of the following by default: at least 30 trading days, positive 30-day ROI, 30-day win rate at least 55%, maximum drawdown at most 30%, Sharpe at least 0.5, and positive copier PnL. Thresholds can be tuned with `LAES_LEADER_MIN_TRADING_DAYS`, `LAES_LEADER_MIN_30D_ROI_PCT`, `LAES_LEADER_MIN_WIN_RATE_PCT`, `LAES_LEADER_MAX_30D_DRAWDOWN_PCT`, and `LAES_LEADER_MIN_SHARPE`. Profiles with missing current-position disclosure are not eligible.

## Run locally

Runtime: Python 3.12.8. Install dependencies and start the app:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app:app --reload
```

Open <http://127.0.0.1:8000>. Health: `/healthz`; API docs: `/api/docs`.

For production, set `DATABASE_URL` to persistent PostgreSQL and `KOPYON_API_KEY` in the service's secret environment (never commit the key). SQLite is for local development. No Binance account credential is used. Common environment settings are documented in `.env.example`.

## Runtime limitations

The monitor runs in-process. A sleeping/paused host pauses monitoring; use an always-on service/worker and persistent database for continuous unattended operation. REST telemetry and the WebSocket connection status are available from `/api/overview` and `/api/status`.

## Historical replay caveat

Existing paper-trade ledgers and historical provider observations are retained as historical records. New live qualification and paper entry require current Binance-source evidence; records from older sources are not silently mixed into current leader snapshots. Historical replay uses stored inputs and does not run or fetch audits by itself.
