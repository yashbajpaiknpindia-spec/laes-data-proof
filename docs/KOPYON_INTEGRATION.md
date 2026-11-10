# Kopyon leader-data integration

## Provider selection

When `KOPYON_API_KEY` is present, LAES uses Kopyon's documented Binance Copy Trading v5 API for leader discovery, current positions, profiles, performance, and position history. The secret is sent only in the server-side `X-API-KEY` header. It is never stored in the database or returned by the application. If the key is absent, the app retains its older logged-out Binance website-data fallback; a configured-but-invalid key does not silently fall back to Binance and risk repeating exchange-side REST bans.

Binance remains the source for public USDⓈ-M Futures market prices and the mark-price WebSocket. These are separate data paths: Kopyon provides public copy-trader data; Binance provides market prices and candles. No exchange account credentials are used and no real orders are placed.

## Routes used

- `GET /v5/binance-copy-trading/traders?period=30D&sortBy=pnl&order=DESC&type=PUBLIC&page=…&rows=20` discovers public leaders. LAES applies its existing tenure, ROI, win-rate, drawdown, Sharpe, and copier-PnL gates.
- `GET /v5/binance-copy-trading/openPositions?traderId=…` retrieves one leader's current positions. `side`, `size`, prices, leverage, margin mode, PnL, ROI, `hidden`, and `fetchedAt` are mapped into the existing snapshot schema. A hidden/failed response is never treated as evidence that a position closed.
- On a user opening a leader profile, `traderProfile`, `performance?period=30D`, and `positionHistory?rows=20&sortBy=opened` are requested when the existing 24-hour profile cache is stale.

## Default cadence and quota math

Kopyon documents a 10,000-request monthly free tier. The default LAES schedule is deliberately below that level:

| Operation | Default cadence | Approx. calls per 30 days |
|---|---:|---:|
| Leader discovery | One page (up to 20 leaders) every 60 minutes | 720 |
| Current positions | One rotating leader every 20 minutes | 2,160 |
| Baseline total | Excludes profile loads, retries, and manual requests | **2,880** |

This leaves roughly 7,120 calls in a 30-day period for profile/performance/history requests and occasional retries. Each stale profile view costs three calls, and profiles are cached for 24 hours. Kopyon returns account rate-limit headers; the application exposes the latest observed monthly limit/remaining/reset values in its provider status telemetry.

Kopyon says Binance updates its ranking about every 10 minutes and positions about every 5 minutes; LAES checks the ranking hourly and checks one rotating leader's positions every 20 minutes. A 20-leader rotation therefore takes about 6 hours 40 minutes. LAES retains a 15-minute freshness rule, so a checked leader can be considered stale for up to 5 minutes before its next refresh; this is a deliberate quota/freshness tradeoff, not full-market coverage. Checking 20 leaders every 6 minutes would require approximately 144,000 position requests per 30 days, before discovery/profile calls, far above the free allowance. Increase `KOPYON_POSITION_BATCH` or shorten `KOPYON_POSITION_REFRESH_INTERVAL_SECONDS` only if the account has sufficient remaining quota; increase `KOPYON_DISCOVERY_PAGES` only if the same is true.

The Kopyon docs say WebSocket streaming and webhooks require a paid plan; each streamed read also consumes account quota. LAES therefore uses REST by default rather than assuming a paid stream entitlement.

## Production setup and limitations

Set `KOPYON_API_KEY` as a Render secret/environment variable; do not put it in this repository, a URL, or a browser. `KOPYON_BASE_URL` defaults to `https://api.kopyon.com/v5`. Kopyon runs independently of the Binance REST cooldown, while Binance live prices continue over the official WebSocket.

The monitor is in-process. A Render Free web service can sleep when it has no inbound traffic, so this code-level polling cadence is the cadence while the web-service process is awake—not a 24/7 guarantee on a sleeping instance. For continuous monitoring, use an always-on service/worker plan or another supported always-running configuration.

## Official references

- [Kopyon API overview and limits](https://console.kopyon.com/docs/)
- [Kopyon Binance Copy Trading v5 reference](https://console.kopyon.com/docs/binance-copy-trading)
- [Kopyon product/plan page](https://kopyon.com/binance-copy-trading-api/)

## Update: batch of 20

`KOPYON_POSITION_BATCH` now defaults to 20: each position cycle (every `KOPYON_POSITION_REFRESH_INTERVAL_SECONDS`, default 1200s) makes 20 per-leader `openPositions` calls (no bulk endpoint is used). That is ~43,200 calls/30 days plus ~720 discovery calls, which exceeds the 10,000 free tier. Raise the interval or lower the batch if the account is on the free plan.

## Update: 120 leaders / 2 hours, prioritised position polling

Discovery now makes 6 calls (6 pages x 20 = 120 leaders) every 7200s (~2,160 calls/30d). Position polling runs every 600s with up to 20 calls per cycle, prioritised: leaders with open paper trades, then leaders holding live positions, then never-checked leaders. Leaders with no open positions are re-checked hourly and hidden-position leaders every 6 hours. Worst case is ~86,400 calls/30d; the real figure is lower when fewer than 20 leaders hold live positions.
