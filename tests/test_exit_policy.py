import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text()
HTML = (ROOT / "templates" / "index.html").read_text()

class ExitPolicyRegressionTests(unittest.TestCase):
    # Revised live policy is distinct from the retained legacy comparison replay.
    def test_fixed_continuation_timeout_is_removed(self):
        self.assertNotIn("LEADER_CONTINUATION_TIMEOUT", APP)
        self.assertNotIn("continuation_timed_out", APP)
        self.assertNotIn("leader_continuation_max_minutes", APP)

    def test_fixed_adverse_price_ceiling_is_removed(self):
        self.assertNotIn("leader_continuation_hard_loss_price_pct", APP)
        self.assertNotIn("hard_loss_pct", APP)

    def test_live_exit_policy_ignores_fixed_stop_and_uses_thesis_invalidations(self):
        live = APP[APP.index("async def evaluate_open_paper_trades"):APP.index("def paper_exit_reason_display")]
        self.assertNotIn('exit_reason = "STOP_LOSS"', live)
        self.assertNotIn('elif net <= 0 and not gap_guard and hit_stop:', live)
        self.assertIn('"mae_is_exit_signal": False', live)
        self.assertIn('_paper_structure_break(', live)
        self.assertIn('exit_reason = "MARKET_STRUCTURE_BROKEN"', live)
        self.assertIn('exit_reason = "MAX_HOLD_TIME_EXCEEDED"', live)
        self.assertIn('leader_state == "REVERSED"', live)
        self.assertIn('leader_state == "CLOSED_CONFIRMED"', live)
        self.assertIn('OPEN_OBSERVED', APP)

    def test_mfe_capture_and_multi_cycle_replay_are_implemented(self):
        self.assertIn('"meaningful_mfe_observed": meaningful_mfe', APP)
        self.assertIn('meaningful_mfe_multiple = 1.15 if strategy == "MOMENTUM" else 1.35', APP)
        self.assertIn('def build_leader_following_strategy_replay', APP)
        self.assertIn('reentry_count', APP)
        self.assertIn('@app.get("/api/paper/strategy-replay")', APP)

    def test_confirmed_close_and_reversal_remain_exit_reasons(self):
        self.assertIn('exit_reason = "LEADER_POSITION_REVERSED"', APP)
        self.assertIn('exit_reason = "LEADER_POSITION_CLOSED"', APP)

    def test_history_position_is_used_as_exact_close_evidence(self):
        self.assertIn("Provider HistoryPosition is authoritative closure evidence", APP)
        self.assertIn("HistoryPosition.position_id == position_id", APP)
        self.assertIn("CLOSED_CONFIRMED", APP)

    def test_post_exit_recovery_requires_1m_close_for_realized_recovery(self):
        self.assertIn("post_exit_recovery_audit_version", APP)
        self.assertIn("post_exit_mark_candles", APP)
        self.assertIn('"post_exit_recovery_audit_version": 9', APP)
        self.assertIn("RECOVERED_TO_NET_PROFIT_CLOSE_CONFIRMED", APP)
        self.assertIn("INTRABAR_NET_PROFIT_ONLY", APP)
        self.assertIn("post_exit_intrabar_potential_profit_usdt", APP)

    def test_sqlite_timestamp_normalization_is_present(self):
        self.assertIn("last_path_at.tzinfo is None", APP)

    def test_ui_no_longer_exposes_removed_controls(self):
        self.assertNotIn("Leader-continuation hard-loss ceiling", HTML)
        self.assertNotIn("Maximum leader-continuation hold", HTML)

if __name__ == "__main__":
    unittest.main(verbosity=2)
