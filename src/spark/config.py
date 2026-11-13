"""Shared configuration for the Spark Structured Streaming job.

Topic names and replay speeds match src/kafka/config.py. Keep them in sync.
"""

BOOTSTRAP_SERVERS = "localhost:9092"

# source name -> topic name (same mapping as the Kafka producer)
TOPICS = {
    "nasa_firms": "firms.events",
    "epa_pm25": "epa.pm25.events",
    "noaa_weather": "noaa.weather.events",
}

TOPIC_TO_SOURCE = {topic: source for source, topic in TOPICS.items()}

# Severe delay is 4-12 event-hours. A 12-hour watermark would drop events at
# the 12-hour boundary; 13 hours retains the full delay window plus slack.
WATERMARK_DURATION = "13 hours"

CHECKPOINT_PATH = "data/interim/spark/checkpoint"
OUTPUT_PATH = "data/interim/spark/county_hour"
FILE_STAGING_PATH = "data/interim/spark/file_envelopes"

SPARK_VERSION = "3.5.1"
SCALA_BINARY = "2.12"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION}"

# Kafka producer speeds are event-hours per real second. PostgreSQL stores
# seconds per event-hour, which is the reciprocal.
REPLAY_SPEEDS = {
    "slow": 1.0,
    "medium": 10.0,
    "fast": 100.0,
}

SPEED_SECONDS_PER_EVENT_HOUR = {
    name: 1.0 / hours_per_second
    for name, hours_per_second in REPLAY_SPEEDS.items()
}

DELAY_LEVELS = ("none", "moderate", "severe")

APP_NAME = "smokewatch-county-hour"

# Kafka JSON envelope fields (message value). Spark parses these with from_json.
# run_code is optional (Priya PR #23): unique per execution so Mongo/Postgres
# can retain all 27 runs while deduping retries within one run.
ENVELOPE_FIELDS = (
    "source",
    "county",
    "event_hour",
    "event_id",
    "replay_scenario",
    "run_code",
    "scheduled_arrival_time",
    "delay_hours",
    "payload",
)

# Payload aliases: warehouse column -> accepted JSON keys in payload.
# Sample files use the second name; production county-hour events use the first.
# FIRMS sample rows have `frp` instead of `fire_detection_count`; presence of
# `frp` counts as one detection.
PAYLOAD_FIELD_ALIASES = {
    "fire_detection_count": ("fire_detection_count", "frp"),
    "pm25_ug_m3": ("pm25_ug_m3", "pm25_ugm3"),
    "temperature_c": ("temperature_c", "temp_c"),
    "humidity_pct": (
        "humidity_pct",
        "relative_humidity_percent",
        "relative_humidity_pct",
    ),
    "wind_speed_m_s": ("wind_speed_m_s", "wind_speed_ms"),
    "wind_direction_deg": (
        "wind_direction_deg",
        "wind_dir_deg",
        "wind_direction_degrees",
    ),
}
