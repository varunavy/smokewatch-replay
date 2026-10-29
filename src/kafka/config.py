"""Shared configuration for the Kafka ingestion component."""

BOOTSTRAP_SERVERS = "localhost:9092"

# source name -> topic name
TOPICS = {
    "nasa_firms": "firms.events",
    "epa_pm25": "epa.pm25.events",
    "noaa_weather": "noaa.weather.events",
}

NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

# Replay speed: how many simulated event-hours pass per real second.
# Events are paced by their scheduled arrival time relative to the first event.
REPLAY_SPEEDS = {
    "slow": 1.0,     # 1 event-hour per real second
    "medium": 10.0,  # 10 event-hours per real second
    "fast": 100.0,   # 100 event-hours per real second
}

# Delay profiles (project methodology).
# A deterministic fraction of events is selected by a SHA-256 rule on
# event_id only (not the scenario or speed, so all speeds delay the same
# events); selected events are delayed by an amount in
# [min_hours, max_hours] (event-hours). All other events have zero delay.
DELAY_PROFILES = {
    "none":     {"fraction": 0.00, "min_hours": 0.0, "max_hours": 0.0},
    "moderate": {"fraction": 0.10, "min_hours": 1.0, "max_hours": 3.0},
    "severe":   {"fraction": 0.25, "min_hours": 4.0, "max_hours": 12.0},
}
