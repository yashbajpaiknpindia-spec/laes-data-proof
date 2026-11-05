# LAES — Leader-Anchored Adverse Excursion Scalping

LAES is a public-data research terminal for Binance Futures copy-trading leaders. It monitors only portfolio and position information that the source provider can legitimately observe. It never logs into a trader's Binance account, bypasses hidden positions, places exchange orders, or transfers funds.

## Live data sources

- Kopyon Binance Copy Trading v5: public leaderboard discovery, trader profiles, performance, open positions when shared, and closed position history. Authentication is server-side with the \`X-API-KEY\` header.
- Binance USDⓈ-M Futures public endpoints: independent mark prices, funding rates, and historical candles.
- CoinDesk RSS: contextual crypto headlines. Headline timing is not treated as proof of causation.

Kopyon documents the Binance copy-trading source at https://kopyon.com/docs/binance-copy-trading/ and the API overview at https://kopyon.com/docs/.

## What the application does

- Automatically discovers public leaderboard profiles at startup and on a 30-minute cadence.
- Rotates open-position checks across five leaders per cycle to manage the provider's request quota.
- Stores every observed snapshot and retains earlier visible observations when a portfolio later becomes hidden.
- Loads profile, performance, and closed position history when a trader forensic profile is opened.
- Computes leader adverse excursion, current hold time, 7-day market return, 24-hour return, ATR, RSI, 5-minute momentum and relevant headline context.
- Calculates a recovery percentage only after at least ten comparable paths have been observed underwater by LAES and later matched to closed results. Otherwise it explicitly reports insufficient sample size.
- Runs a separate paper-only desk with adjustable paper capital, allocation, per-trade and total-portfolio margin caps, stops, targets, leverage cap, fees and slippage. It never submits orders.
- Reports actual database size and row/table counts where the database engine exposes them.

## Important evidence rules

- \`hidden=true\` is not a close event.
- An empty position array is not automatically interpreted as a closed position.
- When Kopyon does not disclose \`openedAt\`, LAES labels the first observation time as an estimate and does not present it as the true open time.
- Closed position history alone is not MAE/MFE path history. Recovery probability is withheld until LAES has recorded enough matched underwater-to-close paths.
- Market/news factors are contextual evidence, not a deterministic attribution.
- Binance public market endpoints do not reveal arbitrary individual lead-trader positions.

## Render setup

Runtime: Python 3.12.8  
Build command: \`pip install -r requirements.txt\`  
Start command: \`uvicorn app:app --host 0.0.0.0 --port $PORT\`

Required server environment:
- \`KOPYON_API_KEY\`
- \`DATABASE_URL\` pointing to persistent Render PostgreSQL

Recommended:
- \`KOPYON_BASE_URL=https://api.kopyon.com/v5\`
- \`BINANCE_BASE_URL=https://fapi.binance.com\`
- \`LAES_MODE=paper\`
- \`LAES_DISCOVERY_INTERVAL_SECONDS=1800\`
- \`LAES_POSITION_BATCH=20\`

There is no browser admin-token prompt. Source credentials remain server-side.

## Render Free limitations

A Render Free web service may spin down after 15 minutes without inbound traffic, and its local filesystem is ephemeral. The in-process monitor pauses while the instance is asleep. For continuous unattended monitoring, use an always-on paid service/worker and persistent PostgreSQL. The UI reports this runtime limitation.

## Local development

\`\`\`bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app:app --reload
\`\`\`

Open http://127.0.0.1:8000. Health endpoint: \`/healthz\`. API documentation: \`/api/docs\`.
