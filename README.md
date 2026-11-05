# LAES Data Proof of Concept

Fresh, independent, read-only research app. It does not connect a Binance account, request trading permissions, place orders, transfer funds, or access private trader data.

## Source boundaries

- **Kopyon** (`https://api.kopyon.com/v5/binance-copy-trading`): third-party provider for Binance Copy Trading leaderboards and publicly disclosed trader positions. Configure `KOPYON_API_KEY` in Render. A response with `hidden: true` is stored as hidden; it is never converted into a close event and previous observations are retained.
- **Binance official USDⓈ-M Futures public market data** (`https://fapi.binance.com`): used for public market-data health, mark price/funding and candles. It does not expose arbitrary individual copy-trader positions through a documented public endpoint. LAES does not attempt to bypass that boundary.
- **Direct Binance individual trader source**: documented as unavailable in this POC, rather than scraping undocumented/private endpoints. The dual-source design means Kopyon supplies public trader/position data while Binance supplies independently sourced market prices and candles.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
# Set KOPYON_API_KEY and ADMIN_TOKEN in your shell environment
uvicorn app:app --reload
```

Open `http://127.0.0.1:8000`. API docs: `/api/docs`. Health: `/healthz`.

## Render

The included `render.yaml` defines a small web service and PostgreSQL database in Singapore. Review current Render pricing and Blueprint validation before applying; free resources may have limits/expiry and are suitable only for a proof of concept. Never put API keys in Git. The Blueprint prompts for `KOPYON_API_KEY` and generates `ADMIN_TOKEN`.

## Initial verification sequence

1. Check `/healthz` and `/api/status`.
2. Enter the `ADMIN_TOKEN` in the dashboard and run **Test both sources**.
3. Confirm Kopyon authentication works and inspect actual response keys/counts.
4. Discover one page of `PUBLIC` traders.
5. Fetch positions for selected traders.
6. Verify public snapshots contain actual position rows and hidden snapshots have `hidden=true` with zero rows.
7. Fetch the same trader again later; compare stored observations and provider `fetchedAt` against our `observed_at`.
8. Confirm no hidden response is interpreted as a close.

## Known limits of this POC

- No automatic scheduler or stream worker yet; sync is manually triggered so provider request usage is controlled during verification.
- Kopyon profile/performance/history pagination is not yet expanded beyond leaderboard and open-position proof.
- Direct Binance market-data requests are read-only. No individual leader position endpoint is claimed.
- This POC stores raw provider JSON for auditability. Apply retention controls before scaling.
- Position `openedAt` may be absent in snapshots. LAES must not invent a true open timestamp; first observation time is only an observation timestamp.
- A public portfolio category and visibility of a particular position snapshot are tracked separately.
