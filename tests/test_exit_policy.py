import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = (ROOT / "app.py").read_text()
HTML = (ROOT / "templates" / "index.html").read_text()

class ExitPolicyRegressionTests(unittest.TestCase):
    def test_fixed_continuation_timeout_is_removed(self):
        self.assertNotIn("LEADER_CONTINUATION_TIMEOUT", APP)
        self.assertNotIn("continuation_timed_out", APP)
        self.assertNotIn("leader_continuation_max_minutes", APP)

    def test_fixed_adverse_price_ceiling_is_removed(self):
        self.assertNotIn("leader_continuation_hard_loss_price_pct", APP)
        self.assertNotIn("hard_loss_pct", APP)

    def test_exact_open_leader_defers_paper_stop(self):
        self.assertIn('leader_state == "OPEN_OBSERVED"', APP)
        self.assertIn("LEADER_CONTINUATION_EVIDENCE_HOLD", APP)
        self.assertIn("LEADER_STATE_EVIDENCE_NO_FIXED_TIMEOUT_OR_PRICE_CEILING", APP)

    def test_confirmed_close_and_reversal_remain_exit_reasons(self):
        self.assertIn('exit_reason = "LEADER_POSITION_REVERSED"', APP)
        self.assertIn('exit_reason = "LEADER_POSITION_CLOSED"', APP)

    def test_history_position_is_used_as_exact_close_evidence(self):
        self.assertIn("Provider HistoryPosition is authoritative closure evidence", APP)
        self.assertIn("HistoryPosition.position_id == position_id", APP)
        self.assertIn("CLOSED_CONFIRMED", APP)

    def test_post_exit_recovery_requires_1m_close_for_realized_recovery(self):
        self.assertIn("post_exit_recovery_audit_version", APP)
        self.assertIn('"post_exit_recovery_audit_version": 5', APP)
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
