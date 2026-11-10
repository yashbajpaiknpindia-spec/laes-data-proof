# Binance Public Leader Data and Rate-Limit Notes

## Source and stability boundary

These Binance website routes are retained only as a compatibility fallback when `KOPYON_API_KEY` is not configured. With Kopyon configured, LAES uses Kopyon's documented Binance Copy Trading v5 API for public trader and position data. Neither provider logs in to trader accounts or requests non-public positions.

The page JSON routes below were observed in Binance's own logged-out marketplace/detail pages and are not part of the documented Futures REST API. They are therefore a website implementation detail, not a stability or availability guarantee. They may change, be region-dependent, or be rate-limited independently. The adapter validates Binance's JSON envelope and field shape; failed/unavailable data is not treated as an empty/closed portfolio. If Binance changes these routes, LAES should fail closed and disable leader-based entries until the adapter is updated. See `docs/KOPYON_INTEGRATION.md` for the preferred API source and polling budget.

Observed public routes used by LAES:

- `POST https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list` — public marketplace list; pagination uses `pageNumber`/`pageSize` and returns `data.list`/`data.total`.
- `GET https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-data/positions?portfolioId=...` — current positions shown by the leader page. `positionAmount`, `entryPrice`, `markPrice`, `unrealizedProfit`, `leverage`, `positionSide`, and `isolated` are used. Zero-amount rows are not treated as open positions.
- `GET https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/detail?portfolioId=...` — public detail/profile metrics, loaded only when a user opens a leader profile.
- `POST https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/position-history` — public history rows, loaded on demand and cached for 24 hours. Binance's `closingPnl` is retained as reported PnL; LAES does not label it net of fees/funding when those components are not specified.

The official Copy Trading developer API is not an anonymous public leaderboard API: documented trading/account operations use authentication/signatures. LAES does not use those operations for leader discovery.

## Leader quality gates

The public leaderboard is only a candidate source. By default, LAES requires a public portfolio, at least 30 trading days, positive 30-day ROI, 30-day win rate of at least 55%, maximum drawdown no higher than 30%, Sharpe ratio of at least 0.5, and positive copier PnL. Binance's marketplace page reports ROI, drawdown, and win rate in percentage units; LAES converts those to fractional ratios for the existing data model. A profile is surfaced only after a successful public Positions endpoint response verifies its position disclosure. These are screening rules, not claims of future performance.

## REST budget and bans

USD-M REST market-data requests and this fallback's public Copy Trading website calls share one in-process rolling budget, defaulting to 60 request-weight units per 60 seconds. Kopyon API requests are separate and governed by the account-level quota described in `docs/KOPYON_INTEGRATION.md`. Common higher-weight Futures routes are charged conservatively (for example, all-symbol `premiumIndex` and `ticker/24hr`, and order-book depth). The application observes `X-MBX-USED-WEIGHT-1M` headers when present. The process also persists an HTTP 403/429/418 cooldown and restores it after restart; it does not retry through an active cooldown. WebSocket prices are primary and REST mark-price fallback is bulk and at most once per minute.

Binance's official USD-M Futures documentation says HTTP 429 indicates a rate-limit violation, HTTP 418 indicates an IP ban after continued violations, the limits are IP-based, used-weight headers report current IP weight, bans can scale from 2 minutes to 3 days, and clients should back off after 429 and prefer WebSocket streams when possible. The app's 60-weight budget is an intentionally conservative local guard, not a guarantee against other applications sharing the same public IP or a Binance policy change.

## WebSocket routing

The app consumes mark-price updates from `wss://fstream.binance.com/market/stream` with dynamic subscriptions for active symbols. Binance's current official change notice classifies mark-price streams as regular market data and routes them through `/market`; legacy unrouted endpoints no longer provide market-data streams after the announced migration date. The client uses exponential reconnect backoff (2–30 seconds), resetting only after a connection has delivered stable prices, and REST is not used to keep the primary feed alive.

## Official references

- Binance USDⓈ-M Futures General Info and rate limits: <https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info>
- Binance USDⓈ-M Futures WebSocket Connect: <https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect>
- Binance WebSocket base-URL split and migration notice: <https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice>
- Binance public Copy Trading marketplace: <https://www.binance.com/en/copy-trading>
- Binance Copy Trading API catalog (documented developer API): <https://developers.binance.com/en/docs/catalog/advanced-trading-copy-trading/api/rest-api/future-copy-trading>
