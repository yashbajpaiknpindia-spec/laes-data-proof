# LAES trade qualification pipeline (current implementation)

This document describes the code in `app.py`. The two strategies remain separate at the signal/ledger level, while both draw from one shared paper wallet. Research discovery is not stopped merely because the wallet is full; a qualified candidate can be recorded as `ENTRY_BLOCKED_BY_SHARED_WALLET_OR_DUPLICATE`.

## 1. Shared rules

1. Candidate must come from a stored, visible Binance public Copy Trading leader-position observation. Hidden portfolios, failed requests and missing positions are not fabricated.
2. The observation must still be present in the leader's latest Binance-source visible snapshot and the snapshot must be no older than 15 minutes at qualification time.
3. Binance public USD-M mark/momentum data must be fresh where the strategy requires it. The live WebSocket is primary; REST is paced through the shared limiter.
4. Market quality checks are evaluated separately from signal direction. The scanner stores each measured field and each rejection reason in `opportunity_decisions`.
5. A qualifying signal is not automatically an executable paper trade. Shared-wallet capacity, duplicate-position and same-symbol/same-side exposure checks are applied at execution time.
6. The wallet is shared across RECOVERY and MOMENTUM. A persistent wallet epoch isolates the new $200 wallet from pre-release unconstrained paper trades and their historical PnL/margin. Legacy rows remain visible and are labeled as outside the current wallet; the separate historical replay still includes the full closed-trade period. Default initial capital is $200; per-trade margin ceiling is capped at 10% of current paper equity, total portfolio margin ceiling is capped at 40%, max notional per trade is capped at $50, leverage is capped at 3x, and only one simultaneous position per symbol/direction is allowed. The $200 starting capital is configurable, but these execution-risk ceilings cannot be raised by the settings form.
7. Sizing order is: compute available margin -> cap by per-trade and portfolio limits -> calculate notional at leverage -> apply the $50 notional ceiling -> recalculate margin as notional/leverage. This avoids inconsistent effective leverage after a notional cap.

Historical recovery audits are separate from the live monitoring worker. Page reads do not trigger historical candle rehydration; audits are explicitly requested.

## 2. RECOVERY strategy

A candidate is considered for market-data qualification only when the observed leader is underwater and the leader's adverse move is at least the configured `min_adverse_pct` (default 0.30%). Direction is the leader's direction.

Required gates:

- Leader is underwater by reported unrealized PnL or adverse mark-vs-entry movement.
- Adverse movement meets the configured threshold.
- Fresh 5-minute momentum is in the leader's direction.
- 24-hour quote volume is at least $5,000,000 and available.
- Spread is at most 0.10% and available.
- Order-book depth within 1% is at least $10,000 and available.
- The sampled order book covers the simulated notional within 1%; estimated VWAP slippage is at most 0.10%.
- Recent 6-hour volume is at least 30% of the prior 24-hour hourly baseline and available.
- 24-hour ATR/price is between 0.02% and 8.0%.
- Live mark price and 5-minute momentum are fresh.
- Funding must not be unusually adverse: positive funding above 0.0005 for LONG or negative funding below -0.0005 for SHORT is rejected.
- Latest leader snapshot must be visible/current and <=15 minutes old.

All failed gates are saved as rejection reasons. Missing required RECOVERY liquidity/market values are treated as unavailable and therefore do not qualify.

## 3. MOMENTUM strategy

This is a separate signal path and paper ledger:

- Leader must be profitable by at least `momentum_min_leader_profit_pct` (default 0.05%).
- Fresh 5-minute Binance momentum must point in the leader's direction and meet `momentum_min_5m_pct` (default 0.02%).
- Known bad market-quality values reject the candidate: quote volume below $5M, spread above 0.10%, depth below $10K, known order-book coverage failure, VWAP slippage above 0.10%, recent 6-hour volume below 30% baseline, or ATR/price outside 0.02%-8.0%.
- Unknown cached static fields remain explicitly unknown/pending rather than automatically rejecting a live momentum signal; live price and momentum freshness remain mandatory.
- Latest leader snapshot must be visible/current and <=15 minutes old.

## 4. Decision and execution ledger

`opportunity_decisions` stores a minute-bucketed record for each leader position and strategy, including:

- trader, provider snapshot ID, exact position key, symbol, side and timestamp;
- qualified/not-qualified status and human-readable status;
- every gate measurement and boolean check;
- rejection reasons;
- whether automation was disabled, the shared wallet blocked entry, or a paper trade was created;
- the created paper trade ID when available.

Read the latest decisions from `GET /api/opportunity-decisions?limit=200` or filter with `strategy=RECOVERY` / `strategy=MOMENTUM`. The worker prunes decision rows older than 90 days to prevent unbounded database growth.

## 5. Revised live exit policy and closed-trade forensics

Both strategies use the same lifecycle principle with strategy-specific MFE sensitivity and hold limits: enter on a qualified leader setup, capture cost-covered meaningful MFE, and allow a fresh qualified re-entry while the exact leader position remains open. MAE is retained as a diagnostic excursion and never closes a position by itself. The old fixed stop price remains a position-sizing reference only; it is not an automatic exit trigger.

Default final exits are: exact provider-confirmed leader closure, confirmed leader reversal, three sampled live marks beyond a buffered rolling 60-minute support/resistance boundary, or a strategy-specific maximum hold (RECOVERY 72 hours; MOMENTUM 24 hours). Structure defaults are a 0.20% buffer and 3 confirmations. These values can be changed in Paper Settings. The hold limits and structure signal are separate from MAE; a temporary deep drawdown does not trigger exit if the thesis remains intact. Meaningful MFE uses estimated round-trip costs as its minimum floor; MOMENTUM uses a lower 1.15× cost multiple and RECOVERY 1.35×. Positive net MFE is captured promptly, rather than waiting for the old fixed 0.70% margin threshold. Re-entry remains subject to fresh visible leader data, strategy qualification, duplicate/position rules, wallet capacity and a 180-second per-leader cooldown; the former five-entry lifetime cap is removed.

Version 9 closed-trade audit stores Binance USD-M mark-price 1-minute candles and computes separate 1h, 4h, 6h and 12h post-exit windows. Each window reports coverage, checkpoint net estimate, best/worst close-based estimate, intrabar favorable excursion, and first close-confirmed net-profit time. Incomplete windows are labeled partial; an intrabar wick is never represented as a realized fill. Recorded financial results are never rewritten.

`GET /api/paper/strategy-replay` and the “Replay existing trades” control re-evaluate the closed-trade cohort under the revised policy. It simulates repeated profit-harvest/re-entry cycles using stored Binance mark-price candles and time-aligned exact leader observations. Re-entry requires the leader snapshot to be visible and fresh, the same exact position to remain open, and the relevant RECOVERY/MOMENTUM setup to qualify again. The replay exits on leader close/reversal, close-confirmed structure break, or the strategy hold limit. It never assumes fills at 1-minute high/low wicks. Counterfactual per-trade cycle rows are persisted as `leader_following_replay_v1`; raw candle arrays are discarded after replay. If arrays were previously discarded and no current replay cache exists, the endpoint rehydrates up to 20 trade audits per request subject to Binance's REST circuit breaker. Remaining or incomplete paths are explicitly marked pending; no synthetic prices are used. The replay is per-trade normalized sizing, not a chronological cross-trade portfolio simulation.

The separate legacy exit-engine replay remains available as a comparison, but it is not the revised policy. Its per-trade counterfactual is persisted as `new_exit_engine_audit_v1` and retains the earlier single-cycle stop/target model.

## 6. Legacy $200 chronological shared-wallet replay

`GET /api/paper/capital-replay` combines RECOVERY and MOMENTUM trades, orders entries chronologically, applies shared margin and symbol-direction overlap constraints, and scales the legacy exit-engine replay PnL to the recalculated notional. It reports:

- initial and final equity estimate;
- counterfactual realized and still-open unrealized PnL;
- number of accepted trades and trades excluded for missing/incomplete candle evidence;
- skipped entries due to wallet capacity or same-symbol/direction overlap;
- cumulative margin/notional deployed across trades;
- peak simultaneous margin, peak utilization, realized-equity drawdown, margin-hours, average deployed margin and maximum concurrent positions;
- wins, losses and realized profit factor.

Legacy positions that were already open before the new wallet epoch are reported separately and excluded from this closed-trade historical replay; they do not consume the new $200 wallet. The replay is a research counterfactual, not exchange execution. Fee/slippage/funding estimates inherit the legacy candle replay assumptions; funding is not described as exchange-verified unless the underlying trade has verified funding history. Unknown candle paths are excluded, not filled with synthetic prices. This capital replay has not yet been converted to allocate each revised-policy re-entry cycle across a shared wallet.
