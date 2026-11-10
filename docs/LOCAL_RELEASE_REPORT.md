# LAES local release report

**Release scope:** local source update only. No GitHub push, Render deploy, production database write, or live order placement was performed.

## What changed

- Replaced the live automatic fixed-stop exit with revised thesis-based management: MAE remains diagnostic only; confirmed leader close/reversal, a sustained buffered support/resistance break, and strategy-specific maximum hold are final exits.
- Both RECOVERY and MOMENTUM promptly capture cost-covered meaningful MFE. MOMENTUM uses a lower MFE/cost multiple than RECOVERY. After harvest, the system may re-enter on a new fresh leader snapshot while the exact leader position remains open and the setup requalifies; the former five-entry lifetime cap is removed, with duplicate-snapshot and 180-second cooldown safeguards retained.
- Added configurable live defaults: RECOVERY maximum hold 72 hours, MOMENTUM 24 hours, structure lookback 60 minutes, 0.20% break buffer, and three sampled confirmations. The former stop percentage is now a sizing reference, not a live exit trigger.
- Replaced live Kopyon dependency for leader discovery/current positions with Binance's logged-out public Copy Trading page JSON. Profiles are screened on performance/risk gates and current-position disclosure; position history/profile data are fetched only on demand and cached.
- Added one shared rolling Binance HTTP request-weight limiter, conservative budget configuration, server-used-weight reconciliation, persisted 403/429/418 cooldown handling, and operator telemetry. The limiter covers market-data and public Copy Trading website calls.
- Corrected the live mark-price WebSocket path and reconnect behavior; WebSocket is primary, with at most one bulk REST price fallback per minute. Only current Binance-source leader snapshots feed live scanning/subscriptions; legacy paper trades retain their recorded provenance.
- Kept RECOVERY and MOMENTUM qualification, caches, decisions, and scanner display separate while preserving their shared wallet constraints. Opportunity/page reads use cached/read-only results rather than starting extra Binance scans.
- Historical strategy replay is read-only and does not initiate candle rehydration. Recovery audits remain available only through the explicit audit endpoint; they are not part of startup or live monitoring.
- Optimized startup and slow views by deferring nonessential history/replay loads and using bounded latest-snapshot queries.
- Kept the old single-cycle exit replay and chronological $200 shared-wallet replay as clearly labelled legacy comparisons. The new replay is per-trade normalized sizing and is not yet a shared-wallet allocation of all re-entry cycles.
- Public Binance website JSON routes are undocumented and may change; no private endpoints or account credentials are used. No exchange orders are placed, no historical ledger PnL is rewritten, and this change did not push code or deploy the app.

## Existing production database snapshot checked read-only

The supplied archive's prior release report records a read-only Render database baseline dated 2026-11-09. That historical baseline was not re-queried as part of this code change and the production database was not modified.

| Measure | Recorded value |
|---|---:|
| Closed paper trades | 419 |
| Open paper trades | 5 |
| Closed-trade v9 candle audits stored | 0 |
| Existing-exit-engine replay rows stored | 0 |
| Old recorded closed net PnL | -$167,512.64 |
| Cumulative recorded margin across closed trades | $16,893,298.71 |
| Cumulative recorded notional across closed trades | $43,747,054.76 |
| Peak simultaneous recorded margin | $11,999,278.85 |
| Largest single recorded trade margin | $999,931.90 |
| Current open legacy positions | 5 |
| Current open legacy margin | $4,235,894.64 |
| Current open legacy notional | $8,471,789.27 |
| Largest current open legacy margin | $847,178.93 |

The historical ledger is not a $200-wallet result. Of the 419 closed trades, 17 had recorded margin above $200. Those 17 account for $16,854,049.26 of cumulative margin and -$167,442.78 of the old recorded net loss. The other 402 trades (margin at or below $200) account for $39,249.45 of cumulative margin and sum to -$69.86 net. The 17 oversized trades account for about 99.77% of cumulative recorded margin and 99.96% of the old recorded net loss. The old ledger therefore contains substantial oversized research positions; its total margin and PnL must not be represented as deployable $200 performance.

## Why no revised-policy PnL result is stated here

The supplied source archive does not contain the production ledger or its Binance 1m lifecycle paths. A valid revised-policy result requires rehydrating actual candles and time-aligned leader observations, then running the new replay. Those operations require the application database and public market endpoint and were not run against production here. Reporting revised-policy PnL now would be fabricated. After deployment or in a connected local database, use the new “Replay existing trades” panel; the endpoint rehydrates up to 20 eligible trade audits per request.

The current five open legacy positions are reported separately and excluded from the new wallet epoch. The historical closed-trade replay also reports those legacy open positions and their recorded margin as excluded rather than silently including their old oversized exposure.

## Validation

- `python -m py_compile app.py` — passed.
- `node --check static/app.js` — passed.
- `python -m unittest discover -s tests -v` — **44 tests passed**.
- Regression coverage includes shared-wallet sizing, legacy-wallet isolation, candidate decisions, MAE-independent live exits, MFE capture, repeated strategy re-entry, exact leader-state handling, historical candle replay, and UI/DOM checks.

## Endpoints added

- `GET /api/opportunity-decisions?limit=200&strategy=RECOVERY|MOMENTUM`
- `GET /api/paper/strategy-replay`
- `GET /api/paper/capital-replay`

Existing endpoints used for the closed-trade audit and legacy replay remain available; the paper UI exposes the batch audit control, revised multi-cycle replay, and legacy shared-wallet replay.
