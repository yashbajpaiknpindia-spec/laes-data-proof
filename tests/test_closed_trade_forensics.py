from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text(encoding="utf-8")
HTML = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
JS = (ROOT / "static" / "app.js").read_text(encoding="utf-8")
CSS = (ROOT / "static" / "style.css").read_text(encoding="utf-8")


class ClosedTradeForensicsTests(unittest.TestCase):
    def test_closed_profit_and_loss_filters_and_latest_trades_exist(self):
        self.assertIn('data-closed-filter="profitable"', HTML)
        self.assertIn('data-closed-filter="loss"', HTML)
        self.assertIn('id="latestTradesBtn"', HTML)
        self.assertIn('function setClosedTradeFilter(filter)', JS)
        self.assertIn('function switchPaperTab(tab)', JS)
        self.assertIn('id="paperLatestTrades"', HTML)

    def test_historical_audit_is_versioned_resumable_and_rate_limit_aware(self):
        self.assertIn('"post_exit_recovery_audit_version": 8', APP)
        self.assertIn('pending = [t for t in closed if parse_json(t.raw_json).get("post_exit_recovery_audit_version", 0) != 8]', APP)
        self.assertIn('cooldown = binance_rest_status()', APP)
        self.assertIn('if cooldown.get("blocked"):', APP)
        self.assertIn('attempted >= max(1, min(20, int(max_trades)))', APP)
        self.assertIn('@app.post("/api/paper/audit/recovery")', APP)

    def test_audit_worker_does_not_report_failed_or_blocked_batches_as_success(self):
        self.assertIn('audit_ok = audit.get("failures", 0) == 0 and not audit.get("blocked", False)', APP)
        self.assertIn("pending=", APP)

    def test_replay_uses_binance_candle_paths_for_every_closed_trade(self):
        self.assertIn("post_exit_mark_candles", APP)
        self.assertIn("MISSING_VERIFIED_BINANCE_CANDLES", APP)
        self.assertIn("PROFIT_HARVEST_TARGET_OR_PROFIT_LOCK", APP)
        self.assertIn("PROTECTIVE_STOP_AFTER_LEADER_CLOSE_OR_UNKNOWN", APP)
        self.assertNotIn("EXCLUDED_NO_LEADER_OPEN_EVIDENCE_AT_OLD_EXIT", APP)
        self.assertIn("omissions do not prove closure", APP)

    def test_replay_never_counts_last_observation_open_position_as_realized(self):
        self.assertIn('"OPEN_UNREALIZED_AT_LAST_CANDLE"', APP)
        self.assertIn('"new_unrealized_net_pnl_estimate": p["net_estimate"]', APP)
        self.assertIn('"new_policy_net_pnl_estimate": p["net_estimate"]', APP)
        self.assertIn('"new_policy_open_unrealized_count"', APP)

    def test_per_attempt_financial_and_timing_fields_are_returned(self):
        for key in ("attempt_number", "gross_pnl", "estimated_costs", "net_pnl",
                    "hold_duration_seconds", "hold_duration_hours", "opened_at", "closed_at"):
            self.assertIn('"' + key + '"', APP)
        for label in ("ENTRY TIME", "EXIT TIME", "HOLD", "GROSS", "COSTS", "NET"):
            self.assertIn(label, JS)

    def test_leader_check_state_is_serialized_and_rendered(self):
        self.assertIn('"leader_status": leader_checks.get(trade.id', APP)
        self.assertIn('"leader_check_at": leader_checks.get(trade.id', APP)
        self.assertIn('function leaderCheck(t)', JS)
        self.assertIn('LATEST_VISIBLE_KOPYON_SNAPSHOT', APP)
        migration = APP[APP.index("def migrate_paper_audit_metadata"):APP.index("def _leader_state_for_trade")]
        paper_api = APP[APP.index('@app.get("/api/paper")'):APP.index('@app.get("/api/paper/research-replay")')]
        self.assertNotIn("_leader_state_for_trade(db, open_trade)", migration)
        self.assertIn("leader_checks = {}", paper_api)

    def test_new_vs_old_research_pnl_is_visible_and_labelled(self):
        self.assertIn('@app.get("/api/paper/research-replay")', APP)
        self.assertIn('"old_system_realized_net_pnl"', APP)
        self.assertIn('"new_policy_exit_net_pnl_estimate"', APP)
        self.assertIn('RESEARCH REALIZED ACCORDING TO OLD SYSTEM', JS)
        self.assertIn('COUNTERFACTUAL POLICY-EXIT NET ESTIMATE', JS)
        self.assertIn('Persisted Binance USD-M mark-price 1m OHLC candles', APP)

    def test_profit_and_loss_colours_are_bold_and_consistent(self):
        self.assertIn('.pnl-positive{color:var(--green)', CSS)
        self.assertIn('.pnl-negative{color:var(--red)', CSS)
        self.assertIn('.profile-metric b{font-size:13px;font-weight:900', CSS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
