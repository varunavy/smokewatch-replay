"""Tests for the deterministic delay rules. Run: python src/kafka/test_delay_rules.py"""
import unittest

from config import DELAY_PROFILES
from replay_producer import build_messages, delay_hours_for


def fake_records(n_per_source=6000):
    recs = []
    for source in ("nasa_firms", "epa_pm25", "noaa_weather"):
        for i in range(n_per_source):
            recs.append({"event_id": f"{source}-{i:05d}", "source": source,
                         "county": f"County{i % 20}",
                         "event_hour": f"2021-06-{1 + i % 28:02d}T{i % 24:02d}:00:00Z"})
    return recs


class DelayRules(unittest.TestCase):
    def test_none_has_no_delay(self):
        self.assertTrue(all(m["delay_hours"] == 0 for m in
                            build_messages(fake_records(500), "s1", "none")))

    def test_repeatable(self):
        a = build_messages(fake_records(500), "s1", "moderate")
        b = build_messages(fake_records(500), "s1", "moderate")
        self.assertEqual(a, b)

    def test_scenario_changes_selection(self):
        a = {m["event_id"] for m in build_messages(fake_records(500), "s1", "moderate")
             if m["delay_hours"] > 0}
        b = {m["event_id"] for m in build_messages(fake_records(500), "s2", "moderate")
             if m["delay_hours"] > 0}
        self.assertNotEqual(a, b)

    def check_profile(self, name, frac, lo, hi):
        msgs = build_messages(fake_records(6000), "baseline", name)
        self.assertEqual(len(msgs), 18000)
        delayed = [m["delay_hours"] for m in msgs if m["delay_hours"] > 0]
        self.assertAlmostEqual(len(delayed) / len(msgs), frac, delta=0.015)
        self.assertTrue(all(lo <= d <= hi for d in delayed), f"{name}: delay outside range")
        self.assertTrue(all(m["delay_hours"] == 0 for m in msgs if m["delay_hours"] <= 0))

    def test_moderate(self):
        self.check_profile("moderate", 0.10, 1.0, 3.0)

    def test_severe(self):
        self.check_profile("severe", 0.25, 4.0, 12.0)

    def test_single_event_stable(self):
        self.assertEqual(delay_hours_for("x-1", "baseline", "severe"),
                         delay_hours_for("x-1", "baseline", "severe"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
