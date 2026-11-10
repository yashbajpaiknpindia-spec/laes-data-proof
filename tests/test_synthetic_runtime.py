import asyncio
import json
import os
import unittest
from datetime import datetime, timezone

os.environ["DATABASE_URL"] = "sqlite:///./synthetic_runtime_test.db"
os.environ["LAES_MODE"] = "test"

import app


class SyntheticRuntimeAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Fresh synthetic DB: no production data and no network.
        app.Base.metadata.drop_all(app.engine)
        app.Base.metadata.create_all(app.engine)

    @classmethod
    def tearDownClass(cls):
        app.Base.metadata.drop_all(app.engine)

    def _trade(self, source_key="T5|P5|BTCUSDT|LONG", net=-1.0):
        with app.SessionLocal() as db:
            trader = app.Trader(trader_id=source_key.split("|", 1)[0], name="Synthetic Leader")
            db.add(trader)
            db.commit()
            t = app.PaperTrade(
                source_key=source_key, trader_id=source_key.split("|", 1)[0], trader_name="Synthetic Leader",
                symbol="BTCUSDT", side="LONG", status="CLOSED",
                entry_price=100.0, mark_price=99.0, quantity=10.0,
                notional=1000.0, margin_used=100.0, leverage=2.0,
                stop_price=99.5, target_price=100.7, gross_pnl=net,
                estimated_costs=0.1, net_pnl=net, exit_price=99.0,
                exit_reason="TEST_EXIT", opened_at=datetime(2026, 11, 8, tzinfo=timezone.utc),
                closed_at=datetime(2026, 11, 8, 12, tzinfo=timezone.utc),
                raw_json=json.dumps({"funding_history_verified": True, "funding_cost_actual": 0.0})
            )
            db.add(t)
            db.commit()
            return t.id

    def test_kopyon_fractional_metrics_map_to_percentage_quality_gates(self):
        from datetime import timedelta
        started = datetime.now(timezone.utc) - timedelta(days=60)
        row = {
            "traderId": "KOPYON-LEADER", "name": "Kopyon Leader", "roi": 0.25,
            "pnl": 1200, "aum": 50000, "maxDrawdown": 0.12, "winRate": 0.80,
            "copierPnl": 800, "sharpeRatio": 1.2, "copiers": 30,
            "sourceDetails": {"type": "PUBLIC", "startedAt": int(started.timestamp() * 1000)},
        }
        normalized = app.normalize_kopyon_leader_row(row)
        self.assertAlmostEqual(normalized["roi30dPct"], 25.0)
        self.assertAlmostEqual(normalized["mdd30dPct"], 12.0)
        self.assertAlmostEqual(normalized["winRatePct"], 80.0)
        self.assertTrue(normalized["qualityQualified"])
        self.assertTrue(app.is_qualified_binance_leader(normalized))
        self.assertEqual(normalized["provider"], app.KOPYON_SOURCE)

    def test_kopyon_discovery_uses_documented_v5_route_without_network(self):
        from datetime import timedelta
        from unittest.mock import patch
        started = datetime.now(timezone.utc) - timedelta(days=90)
        row = {
            "traderId": "MOCK-KOPYON-DISCOVERY", "name": "Mock Leader",
            "roi": 0.2, "pnl": 500, "aum": 10000, "maxDrawdown": 0.1,
            "winRate": 0.75, "copierPnl": 300, "sharpeRatio": 1.0,
            "sourceDetails": {"type": "PUBLIC", "startedAt": int(started.timestamp() * 1000)},
        }
        calls = []

        async def fake_kopyon_get(route, params=None):
            calls.append((route, params))
            return {"data": {"traders": [row], "hasMore": False, "total": 1}}, 200

        original_active = app.KOPYON_ACTIVE
        try:
            app.KOPYON_ACTIVE = True
            with patch.object(app, "kopyon_get", new=fake_kopyon_get):
                count, ids = asyncio.run(app.discover_traders())
        finally:
            app.KOPYON_ACTIVE = original_active

        self.assertEqual(calls[0][0], "binance-copy-trading/traders")
        self.assertEqual(calls[0][1]["period"], "30D")
        self.assertEqual(calls[0][1]["sortBy"], "pnl")
        self.assertEqual(count, 1)
        self.assertEqual(ids, ["MOCK-KOPYON-DISCOVERY"])

    def test_kopyon_short_position_and_hidden_disclosure_are_preserved(self):
        payload = {
            "data": {
                "hidden": False,
                "fetchedAt": "2026-11-10T00:00:00Z",
                "positions": [{
                    "positionId": "p-1", "symbol": "ETHUSDT", "side": "SHORT",
                    "size": 2.5, "entryPrice": 3000, "markPrice": 2900,
                    "unrealizedPnl": 250, "leverage": 5, "marginMode": "ISOLATED",
                    "liquidationPrice": 3600, "roi": 0.2,
                }],
            }
        }
        normalized = app.normalize_binance_positions(payload)["data"]
        self.assertFalse(normalized["hidden"])
        self.assertEqual(normalized["fetchedAt"], "2026-11-10T00:00:00Z")
        self.assertEqual(normalized["positions"][0]["side"], "SHORT")
        self.assertEqual(normalized["positions"][0]["marginMode"], "ISOLATED")
        self.assertEqual(normalized["positions"][0]["roi"], 0.2)

        hidden = app.normalize_binance_positions({"data": {"hidden": True, "positions": []}})["data"]
        self.assertTrue(hidden["hidden"])


    def test_two_hundred_dollar_wallet_replay_caps_overlapping_margin(self):
        from datetime import timedelta
        opened = datetime(2026, 11, 1, tzinfo=timezone.utc)
        trades = []
        replay_rows = []
        for i in range(6):
            symbol = f"COIN{i}USDT"
            trade = app.PaperTrade(
                id=100+i, source_key=f"T{i}|P{i}|{symbol}|LONG", trader_id=f"T{i}",
                trader_name=f"Leader {i}", symbol=symbol, side="LONG", strategy="RECOVERY",
                status="CLOSED", entry_price=100.0, mark_price=100.0, quantity=1.0,
                notional=1000.0, margin_used=100.0, leverage=3.0,
                stop_price=99.0, target_price=101.0, net_pnl=1.0,
                opened_at=opened, closed_at=opened+timedelta(hours=1)
            )
            trades.append(trade)
            replay_rows.append({
                "trade_id": 100+i, "replay_status": "POLICY_EXIT_SIMULATED",
                "new_exit_at": (opened+timedelta(hours=1)).isoformat(),
                "new_policy_net_pnl_estimate": 20.0, "new_exit_reason": "EXISTING_ENGINE_REPLAY"
            })
        cfg = {"max_margin_pct": 10.0, "max_total_margin_pct": 40.0,
               "max_notional_usdt": 50.0, "max_leverage": 3.0,
               "max_positions_per_symbol_direction": 1}
        result = app.build_shared_wallet_new_engine_replay(trades, cfg, replay_rows, 200.0)
        summary = result["summary"]
        self.assertEqual(summary["accepted_trades"], 5)
        self.assertEqual(summary["skipped_insufficient_shared_wallet"], 1)
        self.assertLessEqual(summary["peak_simultaneous_margin"], 80.0 + 1e-8)
        self.assertLessEqual(summary["peak_wallet_utilization_pct"], 40.0 + 1e-8)
        self.assertAlmostEqual(summary["total_margin_deployed_across_trades"], 80.0, places=5)


    def test_legacy_unconstrained_trades_do_not_consume_new_shared_wallet(self):
        from datetime import timedelta
        epoch = app.now()
        old_time = epoch - timedelta(days=1)
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="NEWTRADER", name="New Leader"))
            snap = app.PositionSnapshot(trader_id="NEWTRADER", observed_at=epoch,
                hidden=False, position_count=1,
                raw_json=json.dumps({"data": {"source": app.BINANCE_COPY_SOURCE, "positions": [{}]}}, separators=(",", ":")))
            db.add(snap)
            db.flush()
            obs = app.PositionObservation(snapshot_id=snap.id, trader_id="NEWTRADER",
                position_key="NEWPOS|BTCUSDT|LONG", symbol="BTCUSDT", side="LONG",
                entry_price=100.0, mark_price=99.0, leverage=3.0, raw_json="{}")
            db.add(obs)
            db.add(app.PaperTrade(source_key="OLD|ETHUSDT", trader_id="OLD", symbol="ETHUSDT",
                side="LONG", strategy="RECOVERY", status="OPEN", entry_price=100, mark_price=100,
                quantity=10000, notional=1000000, margin_used=999999, leverage=1,
                stop_price=99, target_price=101, opened_at=old_time, raw_json="{}"))
            db.add(app.PaperTrade(source_key="OLD2|SOLUSDT", trader_id="OLD2", symbol="SOLUSDT",
                side="SHORT", strategy="MOMENTUM", status="CLOSED", entry_price=100, mark_price=100,
                quantity=10000, notional=1000000, margin_used=999999, leverage=1,
                stop_price=101, target_price=99, gross_pnl=-100000, net_pnl=-100000,
                opened_at=old_time, closed_at=old_time+timedelta(minutes=5), raw_json="{}"))
            db.commit()
            obs_id = obs.id
        app.set_setting("shared_wallet_epoch_at", epoch.isoformat())
        app.set_setting("paper_config_version", 2)
        cfg = app.paper_config()
        with app.SessionLocal() as db:
            obs = db.get(app.PositionObservation, obs_id)
        created = app.create_paper_trade(obs, "New Leader", 99.0, cfg, {}, "RECOVERY")
        self.assertEqual(created["reason"], "PAPER_ENTRY_CREATED")
        with app.SessionLocal() as db:
            new_trade = db.get(app.PaperTrade, created["trade_id"])
        self.assertLessEqual(new_trade.margin_used, 20.0 + 1e-8)
        self.assertLessEqual(new_trade.notional, 50.0 + 1e-8)

    def test_candidate_decision_persists_checks_and_execution_reason(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="PIPE", name="Pipeline Leader"))
            snap = app.PositionSnapshot(trader_id="PIPE", hidden=False, position_count=1, raw_json='{"positions":[{}]}')
            db.add(snap)
            db.flush()
            obs = app.PositionObservation(snapshot_id=snap.id, trader_id="PIPE",
                position_key="P|ETHUSDT|SHORT", symbol="ETHUSDT", side="SHORT",
                entry_price=3000, mark_price=3010, leverage=2, raw_json="{}")
            db.add(obs)
            db.commit()
        app.record_opportunity_decision(obs, "RECOVERY", False, "NOT UNDERWATER",
            ["adverse move below gate"], {"adverse_pct": 0.33, "adverse_gate_passed": False}, "NOT_QUALIFIED")
        with app.SessionLocal() as db:
            row = db.scalar(app.select(app.OpportunityDecision).where(app.OpportunityDecision.trader_id == "PIPE"))
        self.assertIsNotNone(row)
        self.assertEqual(app.parse_json(row.checks_json)["adverse_pct"], 0.33)
        self.assertIn("adverse move below gate", app.parse_json(row.reasons_json))


    def test_old_unconstrained_saved_config_is_migrated_once(self):
        app.set_setting("paper_config", {"capital_usdt": 1000000.0, "max_margin_pct": 100.0,
            "max_total_margin_pct": 1000.0, "research_unconstrained": True,
            "max_notional_usdt": 1000000.0, "max_leverage": 5.0})
        app.set_setting("paper_config_version", 1)
        cfg = app.paper_config()
        self.assertEqual(cfg["capital_usdt"], 200.0)
        self.assertEqual(cfg["max_margin_pct"], 10.0)
        self.assertEqual(cfg["max_total_margin_pct"], 40.0)
        self.assertEqual(cfg["max_notional_usdt"], 50.0)
        self.assertEqual(cfg["max_leverage"], 3.0)
        self.assertFalse(cfg["research_unconstrained"])
        self.assertEqual(app.setting("paper_config_version", 0), 2)

    def test_default_paper_config_is_a_shared_two_hundred_dollar_wallet(self):
        cfg = app.paper_config()
        self.assertEqual(cfg["capital_usdt"], 200.0)
        self.assertEqual(cfg["max_notional_usdt"], 50.0)
        self.assertEqual(cfg["max_total_margin_pct"], 40.0)
        self.assertFalse(cfg["research_unconstrained"])

    def test_exact_history_close_is_authoritative(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T1", name="Synthetic Leader"))
            db.commit()
            snap = app.PositionSnapshot(trader_id="T1", hidden=False, position_count=0, raw_json='{"positions":[]}')
            db.add(snap)
            db.flush()
            trade = app.PaperTrade(
                source_key="T1|P1|BTCUSDT|LONG", trader_id="T1", trader_name="Synthetic Leader",
                symbol="BTCUSDT", side="LONG", status="OPEN", entry_price=100, mark_price=99,
                quantity=10, notional=1000, margin_used=100, leverage=2, stop_price=99.5,
                target_price=100.7, raw_json='{"leader_missing_visible_snapshots":0}'
            )
            db.add(trade)
            db.add(app.HistoryPosition(
                trader_id="T1", position_id="P1", symbol="BTCUSDT", side="LONG",
                status="CLOSED", closed_at_ms=1794139200000
            ))
            db.commit()
            state, detail = app._leader_state_for_trade(db, trade)
            self.assertEqual(state, "CLOSED_CONFIRMED")
            self.assertIn("HistoryPosition", detail)

    def test_underwater_exact_open_leader_does_not_force_close(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T2", name="Open Leader"))
            snap = app.PositionSnapshot(trader_id="T2", hidden=False, position_count=1, raw_json="{}")
            db.add(snap)
            db.flush()
            db.add(app.PositionObservation(
                snapshot_id=snap.id, trader_id="T2", position_key="P2|BTCUSDT|LONG",
                symbol="BTCUSDT", side="LONG", entry_price=100, mark_price=95,
                unrealized_pnl=-50, raw_json="{}"
            ))
            trade = app.PaperTrade(
                source_key="T2|P2|BTCUSDT|LONG", trader_id="T2", symbol="BTCUSDT",
                side="LONG", status="OPEN", entry_price=100, mark_price=95, quantity=10,
                notional=1000, margin_used=100, leverage=2, stop_price=99.5,
                target_price=100.7, raw_json=json.dumps({"funding_history_verified": True, "funding_cost_actual": 0.0})
            )
            db.add(trade)
            db.commit()
            state, _ = app._leader_state_for_trade(db, trade)
            self.assertEqual(state, "OPEN_OBSERVED")

    def test_positive_net_pnl_is_harvested(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T3", name="Profit Leader"))
            db.commit()
        trade_id = self._open_trade("T3")
        asyncio.run(app.evaluate_open_paper_trades(prices={"BTCUSDT": 101.00}))
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            self.assertEqual(trade.status, "CLOSED")
            self.assertEqual(trade.exit_reason, "PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK")
            self.assertGreater(trade.net_pnl, 0)

    def test_extreme_mae_alone_does_not_close_live_trade(self):
        trade_id = self._open_trade("T12")
        asyncio.run(app.evaluate_open_paper_trades(prices={"BTCUSDT": 95.0}))
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            self.assertEqual(trade.status, "OPEN")
            self.assertLess(trade.net_pnl, 0)

    def _open_trade(self, trader_id):
        with app.SessionLocal() as db:
            trade = app.PaperTrade(
                source_key=f"{trader_id}|P3|BTCUSDT|LONG", trader_id=trader_id,
                symbol="BTCUSDT", side="LONG", status="OPEN", entry_price=100,
                mark_price=100, quantity=10, notional=1000, margin_used=100, leverage=2,
                stop_price=99.5, target_price=100.7, raw_json=json.dumps({"funding_history_verified": True, "funding_cost_actual": 0.0})
            )
            db.add(trade)
            db.commit()
            return trade.id

    def test_target_touch_without_net_profit_does_not_close(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T4", name="Cost Leader"))
            db.commit()
        trade_id = self._open_trade("T4")
        asyncio.run(app.evaluate_open_paper_trades(prices={"BTCUSDT": 100.05}))
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            self.assertEqual(trade.status, "OPEN")
            self.assertLessEqual(trade.net_pnl, 0)

    def test_post_exit_wick_is_not_reported_as_realized_recovery(self):
        trade_id = self._trade(source_key="T5|P5|BTCUSDT|LONG", net=-1.0)
        async def fake_binance_get(path, params=None):
            return ([[1794139200000, "99", "101.0", "99", "99.5", "0", "1794139259999"]], None)
        original = app.binance_get
        app.binance_get = fake_binance_get
        try:
            result = asyncio.run(app.run_post_exit_recovery_audit(max_trades=1))
        finally:
            app.binance_get = original
        self.assertEqual(result["audited"], 1)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            raw = json.loads(trade.raw_json)


    def test_replay_applies_existing_protective_stop_even_while_leader_is_open(self):
        trade_id = self._trade(source_key="T6|P6|BTCUSDT|LONG", net=-1.0)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            trade.opened_at = datetime.fromtimestamp(1794139140000/1000, timezone.utc)
            trade.closed_at = datetime.fromtimestamp(1794139200000/1000, timezone.utc)
            trade.raw_json = json.dumps({
                "funding_rate_at_entry": 0.0,
                "next_funding_time": 1794168000000,
                "post_exit_recovery_audit_version": 9,
                "post_exit_recovery_audit_updated_at": "2026-11-08T12:00:00+00:00",
                "mfe_net_pnl": 0.0,
                "closed_trade_candle_audit": {"status": "COMPLETE"},
                "entry_timing_mark_candles": [
                    [1794139140000, 100.1, 99.9, 100.0]
                ],
                "post_exit_mark_candles": [
                    [1794139260000, 100.0, 98.0, 99.0],
                    [1794139320000, 101.2, 100.5, 101.0]
                ]
            })
            snap = app.PositionSnapshot(trader_id="T6", observed_at=datetime.fromtimestamp(1794139260000/1000, timezone.utc),
                hidden=False, raw_json='{"positions":[{}]}')
            db.add(snap)
            db.flush()
            db.add(app.PositionObservation(snapshot_id=snap.id, trader_id="T6",
                position_key="P6|BTCUSDT|LONG", symbol="BTCUSDT", side="LONG",
                entry_price=100.0, mark_price=99.0, raw_json="{}"))
            db.commit()
            trade = db.get(app.PaperTrade, trade_id)
            replay = app.build_new_exit_policy_replay([trade], app.paper_config())
        row = replay["trades"][0]
        self.assertEqual(row["replay_status"], "POLICY_EXIT_SIMULATED")
        self.assertEqual(row["new_exit_reason"], "STOP_LOSS")
        self.assertEqual(row["leader_state_at_exit"], "OPEN")
        self.assertLess(row["new_policy_net_pnl_estimate"], 0)


    def test_existing_exit_engine_replay_starts_at_original_entry_not_old_close(self):
        from datetime import timedelta
        trade_id = self._trade(source_key="T10|P10|ETHUSDT|SHORT", net=-2.0)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            opened = datetime(2026, 11, 8, 0, 0, tzinfo=timezone.utc)
            old_exit = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)
            trade.opened_at = opened
            trade.closed_at = old_exit
            trade.side = "SHORT"
            trade.entry_price = 100.0
            trade.mark_price = 102.0
            trade.stop_price = 100.5
            trade.target_price = 99.0
            trade.quantity = 10.0
            trade.notional = 1000.0
            trade.leverage = 2.0
            trade.raw_json = json.dumps({
                "funding_history_verified": True, "funding_rate_at_entry": 0.0,
                "next_funding_time": 1794168000000, "post_exit_recovery_audit_version": 9,
                "post_exit_recovery_audit_updated_at": "2026-11-08T12:00:00+00:00",
                "closed_trade_candle_audit": {"status":"COMPLETE"},
                "entry_timing_mark_candles": [[int((opened+timedelta(minutes=1)).timestamp()*1000), 101.0, 100.0, 100.8]],
                "post_exit_mark_candles": [[int((old_exit+timedelta(minutes=1)).timestamp()*1000), 102.0, 101.0, 101.5]]
            })
            db.commit()
            trade = db.get(app.PaperTrade, trade_id)
            replay = app.build_new_exit_policy_replay([trade], app.paper_config())
        row = replay["trades"][0]
        self.assertEqual(row["replay_status"], "POLICY_EXIT_SIMULATED")
        self.assertEqual(row["new_exit_reason"], "STOP_LOSS")
        self.assertLess(row["new_exit_offset_from_old_exit_hours"], 0)
        self.assertAlmostEqual(row["replay_base_notional"], 40.0, places=6)

    def test_revised_replay_harvests_then_reenters_while_exact_leader_is_open(self):
        from datetime import timedelta
        opened = datetime(2026, 11, 8, 0, 0, tzinfo=timezone.utc)
        source_key = "T13|P13|BTCUSDT|LONG"
        closes = [100.0, 100.5, 100.4, 100.45, 100.55, 100.65, 100.7, 101.2, 101.1, 101.0]
        mark_candles = []
        for i, close in enumerate(closes, 1):
            at = int((opened + timedelta(minutes=i)).timestamp() * 1000)
            mark_candles.append([at, close + 0.01, close - 0.01, close])
        post_candles = []
        for i in range(11, 14):
            at = int((opened + timedelta(minutes=i)).timestamp() * 1000)
            post_candles.append([at, 101.1, 100.9, 101.0])
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T13", name="Re-entry Leader"))
            trade = app.PaperTrade(
                source_key=source_key, trader_id="T13", trader_name="Re-entry Leader",
                symbol="BTCUSDT", side="LONG", strategy="RECOVERY", status="CLOSED",
                entry_price=100.0, mark_price=100.0, quantity=10.0, notional=1000.0,
                margin_used=100.0, leverage=2.0, stop_price=99.5, target_price=100.7,
                net_pnl=1.0, exit_price=101.0, exit_reason="TEST_EXIT", opened_at=opened,
                closed_at=opened + timedelta(minutes=10),
                raw_json=json.dumps({
                    "post_exit_recovery_audit_version": 9,
                    "post_exit_recovery_audit_updated_at": "2026-11-08T12:00:00+00:00",
                    "closed_trade_candle_audit": {"status": "COMPLETE"},
                    "entry_timing_mark_candles": mark_candles,
                    "post_exit_mark_candles": post_candles,
                    "funding_history_replay_verified": True,
                    "funding_history_replay_points": [],
                })
            )
            db.add(trade)
            db.flush()
            for i in range(1, 14):
                snapshot = app.PositionSnapshot(
                    trader_id="T13", observed_at=opened + timedelta(minutes=i), hidden=False,
                    position_count=1, raw_json='{"positions":[{}]}'
                )
                db.add(snapshot)
                db.flush()
                db.add(app.PositionObservation(
                    snapshot_id=snapshot.id, trader_id="T13", position_key="P13|BTCUSDT|LONG",
                    symbol="BTCUSDT", side="LONG", entry_price=100.0, mark_price=99.5,
                    unrealized_pnl=-0.5, raw_json="{}"
                ))
            db.commit()
            trade = db.get(app.PaperTrade, trade.id)
            replay = app.build_leader_following_strategy_replay([trade], app.paper_config())
        row = replay["trades"][0]
        self.assertEqual(row["replay_status"], "OPEN_UNREALIZED_AT_LAST_CANDLE")
        self.assertEqual(row["completed_cycles"], 2)
        self.assertEqual(row["reentry_count"], 2)
        self.assertEqual(row["cycles"][0]["exit_reason"], "MEANINGFUL_MFE_PROFIT_CAPTURE")
        self.assertEqual(row["cycles"][1]["exit_reason"], "MEANINGFUL_MFE_PROFIT_CAPTURE")
        self.assertEqual(row["cycles"][2]["exit_reason"], "STILL_OPEN_AT_LAST_CANDLE")


    def test_ambiguous_stop_and_target_in_same_minute_is_not_fabricated(self):
        from datetime import timedelta
        trade_id = self._trade(source_key="T11|P11|BTCUSDT|LONG", net=-1.0)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            opened = datetime(2026, 11, 8, 0, 0, tzinfo=timezone.utc)
            old_exit = opened + timedelta(hours=1)
            trade.opened_at = opened
            trade.closed_at = old_exit
            trade.entry_price = 100.0
            trade.stop_price = 99.5
            trade.target_price = 100.7
            trade.quantity = 10.0
            trade.notional = 1000.0
            trade.leverage = 2.0
            trade.raw_json = json.dumps({
                "funding_history_verified": True, "funding_rate_at_entry": 0.0,
                "next_funding_time": 1794168000000, "post_exit_recovery_audit_version": 9,
                "post_exit_recovery_audit_updated_at": "2026-11-08T12:00:00+00:00",
                "closed_trade_candle_audit": {"status": "COMPLETE"},
                "entry_timing_mark_candles": [[int((opened+timedelta(minutes=1)).timestamp()*1000), 101.0, 98.0, 100.0]],
                "post_exit_mark_candles": [[int((old_exit+timedelta(minutes=1)).timestamp()*1000), 100.0, 99.9, 100.0]]
            })
            db.commit()
            trade = db.get(app.PaperTrade, trade_id)
            replay = app.build_new_exit_policy_replay([trade], app.paper_config())
        row = replay["trades"][0]
        self.assertEqual(row["replay_status"], "AMBIGUOUS_INTRABAR_ORDERING")
        self.assertIsNone(row["new_policy_net_pnl_estimate"])

    def test_missing_candle_path_is_not_replaced_with_synthetic_prices(self):
        trade_id = self._trade(source_key="T7|P7|BTCUSDT|LONG", net=-1.0)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            trade.raw_json = json.dumps({"post_exit_recovery_audit_version": 9,
                "post_exit_recovery_audit_updated_at": "2026-11-08T12:00:00+00:00",
                "post_exit_mark_candles": []})
            db.commit()
            trade = db.get(app.PaperTrade, trade_id)
            replay = app.build_new_exit_policy_replay([trade], app.paper_config())
        self.assertEqual(replay["trades"][0]["replay_status"], "MISSING_ENTRY_TO_EXIT_CANDLE_PATH")

    def test_two_visible_omissions_do_not_confirm_leader_closure(self):
        with app.SessionLocal() as db:
            db.add(app.Trader(trader_id="T8", name="Leader"))
            for i in range(2):
                db.add(app.PositionSnapshot(trader_id="T8", hidden=False, position_count=0,
                    raw_json='{"positions":[]}', observed_at=datetime(2026, 11, 8, 13+i, tzinfo=timezone.utc)))
            trade = app.PaperTrade(source_key="T8|P8|BTCUSDT|LONG", trader_id="T8", symbol="BTCUSDT",
                side="LONG", status="OPEN", entry_price=100, mark_price=99, quantity=1, notional=100,
                margin_used=10, leverage=2, stop_price=99, target_price=101,
                raw_json='{"leader_missing_visible_snapshots":2}')
            db.add(trade)
            db.commit()
            state, _ = app._leader_state_for_trade(db, trade)
        self.assertEqual(state, "STALE_OR_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
