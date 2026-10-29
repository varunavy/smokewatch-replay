"""Tests for the deterministic delay rules.

Run from the repo root:  python3 -m pytest src/kafka/test_delay_rules.py -q
"""
import unittest

from config import DELAY_PROFILES
from replay_producer import build_messages, delay_hours_for

SOURCES = ("nasa_firms", "epa_pm25", "noaa_weather")
SPEEDS = ("slow", "medium", "fast")


def fake_records(n_per_source=6000):
    recs = []
    for source in SOURCES:
        for i in range(n_per_source):
            recs.append({"event_id": f"{source}-{i:05d}", "source": source,
                         "county": f"County{i % 20}",
                         "event_hour": f"2021-06-{1 + i % 28:02d}T{i % 24:02d}:00:00Z"})
    return recs


def delays_by_event(msgs):
    return {m["event_id"]: m["delay_hours"] for m in msgs}


class DelayRules(unittest.TestCase):
    def test_none_has_no_delay(self):
        self.assertTrue(all(m["delay_hours"] == 0 for m in
                            build_messages(fake_records(500), "s1", "none")))

    def test_repeatable(self):
        a = build_messages(fake_records(500), "s1", "moderate")
        b = build_messages(fake_records(500), "s1", "moderate")
        self.assertEqual(a, b)

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
        self.assertEqual(delay_hours_for("x-1", "severe"), delay_hours_for("x-1", "severe"))


class ScenarioIndependence(unittest.TestCase):
    """Replay speed must be the only changed factor: scenario/speed labels must not
    change which events are delayed or by how much."""

    def test_speeds_give_identical_delay_hours(self):
        records = fake_records(6000)
        for profile in ("moderate", "severe"):
            runs = {speed: delays_by_event(build_messages(records, f"{speed}__{profile}", profile))
                    for speed in SPEEDS}
            self.assertEqual(len(runs["slow"]), 18000)
            self.assertGreater(sum(1 for d in runs["slow"].values() if d > 0), 0)
            # same delay_hours for every single event across slow, medium, fast
            self.assertEqual(runs["slow"], runs["medium"], f"{profile}: slow != medium")
            self.assertEqual(runs["slow"], runs["fast"], f"{profile}: slow != fast")

    def test_scenario_label_does_not_change_delays(self):
        records = fake_records(1000)
        a = delays_by_event(build_messages(records, "baseline", "moderate"))
        b = delays_by_event(build_messages(records, "anything_else", "moderate"))
        self.assertEqual(a, b)

    def test_scenario_still_in_message(self):
        records = fake_records(50)
        for speed in SPEEDS:
            label = f"{speed}__moderate"
            msgs = build_messages(records, label, "moderate")
            self.assertTrue(all(m["replay_scenario"] == label for m in msgs))

    def test_arrival_times_identical_across_speeds(self):
        records = fake_records(1000)
        arr = {}
        for speed in SPEEDS:
            msgs = build_messages(records, f"{speed}__severe", "severe")
            arr[speed] = {m["event_id"]: m["scheduled_arrival_time"] for m in msgs}
        self.assertEqual(arr["slow"], arr["medium"])
        self.assertEqual(arr["slow"], arr["fast"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
