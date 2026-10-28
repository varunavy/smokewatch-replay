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
# Events are paced by their event_hour relative to the first event in the run.
REPLAY_SPEEDS = {
    "slow": 1.0,     # 1 event-hour per real second
    "medium": 10.0,  # 10 event-hours per real second
    "fast": 100.0,   # 100 event-hours per real second
}

# Arrival delay: scheduled_arrival_time = event_hour + delay (in hours),
# modeling late-arriving data. Delay is drawn uniformly from [min, max].
DELAY_PROFILES = {
    "none": (0.0, 0.0),
    "moderate": (1.0, 6.0),
    "severe": (12.0, 48.0),
}
