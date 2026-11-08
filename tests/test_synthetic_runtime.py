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

    def _trade(self, source_key="P1|BTCUSDT|LONG", net=-1.0):
        with app.SessionLocal() as db:
            trader = app.Trader(trader_id="T1", name="Synthetic Leader")
            db.add(trader)
            db.commit()
            t = app.PaperTrade(
                source_key=source_key, trader_id="T1", trader_name="Synthetic Leader",
                symbol="BTCUSDT", side="LONG", status="CLOSED",
                entry_price=100.0, mark_price=99.0, quantity=10.0,
                notional=1000.0, margin_used=100.0, leverage=2.0,
                stop_price=99.5, target_price=100.7, gross_pnl=net,
                estimated_costs=0.1, net_pnl=net, exit_price=99.0,
                exit_reason="TEST_EXIT", opened_at=datetime(2026, 11, 8, tzinfo=timezone.utc),
                closed_at=datetime(2026, 11, 8, 12, tzinfo=timezone.utc),
                raw_json="{}"
            )
            db.add(t)
            db.commit()
            return t.id

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
                target_price=100.7, raw_json="{}"
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
        asyncio.run(app.evaluate_open_paper_trades(prices={"BTCUSDT": 100.20}))
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            self.assertEqual(trade.status, "CLOSED")
            self.assertEqual(trade.exit_reason, "PROFIT_HARVEST_NET_POSITIVE")
            self.assertGreater(trade.net_pnl, 0)

    def _open_trade(self, trader_id):
        with app.SessionLocal() as db:
            trade = app.PaperTrade(
                source_key=f"{trader_id}|P3|BTCUSDT|LONG", trader_id=trader_id,
                symbol="BTCUSDT", side="LONG", status="OPEN", entry_price=100,
                mark_price=100, quantity=10, notional=1000, margin_used=100, leverage=2,
                stop_price=99.5, target_price=100.7, raw_json="{}"
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
            result = asyncio.run(app.run_post_exit_recovery_audit(max_trades=10))
        finally:
            app.binance_get = original
        self.assertEqual(result["audited"], 1)
        with app.SessionLocal() as db:
            trade = db.get(app.PaperTrade, trade_id)
            raw = json.loads(trade.raw_json)
            self.assertEqual(raw["post_exit_recovery_status"], "INTRABAR_NET_PROFIT_ONLY")
            self.assertGreater(raw["post_exit_intrabar_potential_profit_usdt"], 0)
            self.assertLessEqual(raw["post_exit_max_net_pnl"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
