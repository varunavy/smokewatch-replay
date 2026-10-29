"""Spark Structured Streaming county-hour job for SmokeWatch Replay.

Reads the three Kafka source topics (or staged Kafka-envelope files), parses
JSON with a defined envelope schema, retains the source label, unions the
streams, applies a 13-hour event-time watermark, and runs a stateful
aggregation by county, event hour, and replay scenario.

The output is a join-ready county-hour record that matches the PostgreSQL
fact_county_hour contract in src/storage/postgres_loader.py. Until that
loader is wired in, results go to the console and to JSON.

Usage (from the repository root):

    python src/spark/streaming_job.py --source kafka
    python src/spark/streaming_job.py --source file --input-dir data/samples --trigger once
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    APP_NAME,
    BOOTSTRAP_SERVERS,
    CHECKPOINT_PATH,
    DELAY_LEVELS,
    FILE_STAGING_PATH,
    KAFKA_PACKAGE,
    OUTPUT_PATH,
    PAYLOAD_FIELD_ALIASES,
    REPLAY_SPEEDS,
    SPARK_VERSION,
    SPEED_SECONDS_PER_EVENT_HOUR,
    TOPIC_TO_SOURCE,
    TOPICS,
    WATERMARK_DURATION,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_EVENT_FIELDS = ("event_id", "source", "county", "event_hour")

# Must match src/kafka/config.py. Used only when staging file envelopes.
DELAY_PROFILES = {
    "none": {"fraction": 0.00, "min_hours": 0.0, "max_hours": 0.0},
    "moderate": {"fraction": 0.10, "min_hours": 1.0, "max_hours": 3.0},
    "severe": {"fraction": 0.25, "min_hours": 4.0, "max_hours": 12.0},
}

# Production warehouse names plus sample-file aliases. Extra JSON keys are ignored.
PAYLOAD_SCHEMA = StructType(
    [
        StructField("fire_detection_count", LongType(), True),
        StructField("frp", DoubleType(), True),
        StructField("total_frp_mw", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("pm25_ug_m3", DoubleType(), True),
        StructField("pm25_ugm3", DoubleType(), True),
        StructField("pm25_available", BooleanType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("relative_humidity_percent", DoubleType(), True),
        StructField("relative_humidity_pct", DoubleType(), True),
        StructField("wind_speed_m_s", DoubleType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
        StructField("wind_direction_deg", DoubleType(), True),
        StructField("wind_dir_deg", DoubleType(), True),
        StructField("wind_direction_degrees", DoubleType(), True),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("source", StringType(), True),
        StructField("county", StringType(), True),
        StructField("event_hour", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("replay_scenario", StringType(), True),
        StructField("scheduled_arrival_time", StringType(), True),
        StructField("delay_hours", DoubleType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
)


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hash_unit(event_id: str, label: str) -> float:
    digest = hashlib.sha256(f"{event_id}:{label}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def delay_hours_for(event_id: str, delay_profile: str) -> float:
    """Deterministic delay; identical rule to src/kafka/replay_producer.py."""
    prof = DELAY_PROFILES[delay_profile]
    if prof["fraction"] <= 0:
        return 0.0
    if hash_unit(event_id, "select") >= prof["fraction"]:
        return 0.0
    v = hash_unit(event_id, "amount")
    return round(prof["min_hours"] + v * (prof["max_hours"] - prof["min_hours"]), 4)


def to_iso_timestamp_expr(column: str) -> F.Column:
    """Parse ISO-8601 UTC strings such as 2021-07-13T10:00:00Z."""
    cleaned = F.regexp_replace(F.col(column), "Z$", "+00:00")
    return F.to_timestamp(cleaned)


def payload_alias(warehouse_name: str) -> F.Column:
    """Coalesce payload keys listed in PAYLOAD_FIELD_ALIASES onto one column."""
    keys = PAYLOAD_FIELD_ALIASES[warehouse_name]
    return F.coalesce(*[F.col(f"payload.{key}") for key in keys])


def build_spark(app_name: str, source: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if source == "kafka":
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_topic(spark: SparkSession, topic: str, bootstrap: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("topic"),
            F.col("value").cast("string").alias("json_value"),
        )
    )


def parse_kafka_envelope(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        "kafka_timestamp",
        "topic",
        F.from_json(F.col("json_value"), ENVELOPE_SCHEMA).alias("event"),
    ).select("kafka_timestamp", "topic", "event.*")
    return parsed.withColumn(
        "source",
        F.coalesce(F.col("source"), F.col("topic").cast("string")),
    )


def read_file_source(spark: SparkSession, source_name: str, path: Path) -> DataFrame:
    topic = TOPICS[source_name]
    return (
        spark.readStream.schema(ENVELOPE_SCHEMA)
        .option("maxFilesPerTrigger", 100)
        .json(str(path))
        .withColumn("kafka_timestamp", F.current_timestamp())
        .withColumn("topic", F.lit(topic))
        .withColumn("source", F.coalesce(F.col("source"), F.lit(source_name)))
    )


def extract_measurements(events: DataFrame, default_scenario: str) -> DataFrame:
    """Retain the source label and flatten payload aliases onto warehouse names."""
    source_from_topic = F.col("topic").cast("string")
    for topic, source_name in TOPIC_TO_SOURCE.items():
        source_from_topic = F.when(F.col("topic") == topic, F.lit(source_name)).otherwise(
            source_from_topic
        )

    firms_count = F.coalesce(
        F.col("payload.fire_detection_count"),
        F.when(F.col("payload.frp").isNotNull(), F.lit(1)),
    )

    return (
        events.withColumn("source", F.coalesce(F.col("source"), source_from_topic))
        .withColumn(
            "replay_scenario",
            F.coalesce(F.col("replay_scenario"), F.lit(default_scenario)),
        )
        .withColumn("event_hour_ts", to_iso_timestamp_expr("event_hour"))
        .withColumn(
            "scheduled_arrival_ts",
            F.coalesce(
                to_iso_timestamp_expr("scheduled_arrival_time"),
                F.col("event_hour_ts"),
            ),
        )
        .withColumn(
            "delay_hours",
            F.coalesce(F.col("delay_hours").cast("double"), F.lit(0.0)),
        )
        .withColumn(
            "fire_detection_count",
            F.when(F.col("source") == "nasa_firms", firms_count),
        )
        .withColumn(
            "pm25_ug_m3",
            F.when(F.col("source") == "epa_pm25", payload_alias("pm25_ug_m3")),
        )
        .withColumn(
            "temperature_c",
            F.when(F.col("source") == "noaa_weather", payload_alias("temperature_c")),
        )
        .withColumn(
            "humidity_pct",
            F.when(F.col("source") == "noaa_weather", payload_alias("humidity_pct")),
        )
        .withColumn(
            "wind_speed_m_s",
            F.when(F.col("source") == "noaa_weather", payload_alias("wind_speed_m_s")),
        )
        .withColumn(
            "wind_direction_deg",
            F.when(F.col("source") == "noaa_weather", payload_alias("wind_direction_deg")),
        )
        .where(F.col("county").isNotNull() & F.col("event_hour_ts").isNotNull())
    )


def aggregate_county_hour(events: DataFrame, args: argparse.Namespace) -> DataFrame:
    """Stateful county-hour aggregation with a documented event-time watermark.

    Watermark: WATERMARK_DURATION (13 hours) on event_hour_ts. Severe replay
    delay is 4-12 event-hours, so 13 hours retains the full delay window plus
    one hour of slack. Records whose event_hour is older than
    (max observed event_hour - 13 hours) are dropped from state.
    """
    grouped = (
        events.withWatermark("event_hour_ts", WATERMARK_DURATION)
        .groupBy("county", "event_hour_ts", "replay_scenario")
        .agg(
            F.count(F.lit(1)).alias("input_event_count"),
            F.collect_set("source").alias("sources"),
            F.max("fire_detection_count").cast(IntegerType()).alias("fire_detection_count"),
            F.max("pm25_ug_m3").alias("pm25_ug_m3"),
            F.max("temperature_c").alias("temperature_c"),
            F.max("humidity_pct").alias("humidity_pct"),
            F.max("wind_speed_m_s").alias("wind_speed_m_s"),
            F.max("wind_direction_deg").alias("wind_direction_deg"),
            F.max("kafka_timestamp").alias("kafka_timestamp"),
            F.max("scheduled_arrival_ts").alias("scheduled_arrival_time"),
        )
    )

    distinct_count = F.size(F.col("sources"))
    firms = F.array_contains(F.col("sources"), F.lit("nasa_firms"))
    epa = F.array_contains(F.col("sources"), F.lit("epa_pm25"))
    noaa = F.array_contains(F.col("sources"), F.lit("noaa_weather"))

    return grouped.select(
        F.lit(args.run_code).alias("run_code"),
        F.col("replay_scenario").alias("scenario_code"),
        F.lit(SPEED_SECONDS_PER_EVENT_HOUR[args.speed]).alias(
            "speed_seconds_per_event_hour"
        ),
        F.lit(args.delay).alias("delay_level"),
        F.col("county"),
        F.col("event_hour_ts").alias("event_hour"),
        distinct_count.alias("distinct_source_count"),
        F.array_sort(F.col("sources")).alias("sources"),
        F.col("input_event_count"),
        F.col("fire_detection_count"),
        F.col("pm25_ug_m3"),
        F.col("temperature_c"),
        F.col("humidity_pct"),
        F.col("wind_speed_m_s"),
        F.col("wind_direction_deg"),
        firms.alias("firms_available"),
        epa.alias("epa_available"),
        noaa.alias("noaa_available"),
        (distinct_count == 3).alias("is_join_ready"),
        F.col("kafka_timestamp"),
        F.col("scheduled_arrival_time"),
    )


def write_batch(batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    """foreachBatch sink: completion timestamp, console, and JSON.

    PostgreSQL upsert_county_hour can replace the JSON write later without
    changing the aggregation. first_join_ready_at is set when the group first
    reaches three sources; the warehouse COALESCE keeps the earliest value.
    """
    if batch_df.isEmpty():
        print(f"batch {batch_id}: 0 county-hour groups", flush=True)
        return

    arrival = F.coalesce(F.col("kafka_timestamp"), F.col("scheduled_arrival_time"))
    result = (
        batch_df.withColumn("batch_id", F.lit(batch_id))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "first_join_ready_at",
            F.when(F.col("is_join_ready"), F.col("processed_at")),
        )
        .withColumn(
            "freshness_lag_event_hours",
            (
                F.col("processed_at").cast("double")
                - F.col("event_hour").cast("double")
            )
            / F.lit(3600.0),
        )
        .withColumn(
            "processing_latency_ms",
            (F.col("processed_at").cast("double") - arrival.cast("double")) * 1000.0,
        )
    )

    groups = result.count()
    records = result.agg(F.sum("input_event_count")).collect()[0][0] or 0
    join_ready = result.where(F.col("is_join_ready")).count()
    print(
        f"batch {batch_id}: input_records={int(records)} "
        f"county_hour_groups={groups} join_ready={join_ready}",
        flush=True,
    )
    result.orderBy("event_hour", "county").show(truncate=False)

    json_dir = REPO_ROOT / args.output
    result.write.mode("append").json(str(json_dir))
    print(f"batch {batch_id}: wrote json -> {json_dir}", flush=True)

    # Database integration (not required for this component):
    # from postgres_loader import get_connection, upsert_county_hour
    # conn = get_connection()
    # try:
    #     for record in result.toPandas().to_dict(orient="records"):
    #         upsert_county_hour(conn, record)
    # finally:
    #     conn.close()


def load_source_records(input_dir: Path) -> list[dict]:
    records = []
    for path in sorted(input_dir.glob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                missing = [field for field in REQUIRED_EVENT_FIELDS if not rec.get(field)]
                if missing:
                    raise ValueError(f"{path}:{line_no} missing {missing}")
                if rec["source"] not in TOPICS:
                    raise ValueError(f"{path}:{line_no} unknown source {rec['source']!r}")
                records.append(rec)
    return records


def stage_file_envelopes(args: argparse.Namespace) -> dict[str, Path]:
    """Write Kafka-envelope JSON files, one directory per source, then union them."""
    input_dir = Path(args.input_dir)
    if not input_dir.is_absolute():
        input_dir = REPO_ROOT / input_dir
    records = load_source_records(input_dir)
    if not records:
        raise FileNotFoundError(f"no *.jsonl records in {input_dir}")

    staging_root = REPO_ROOT / FILE_STAGING_PATH
    if staging_root.exists():
        shutil.rmtree(staging_root)

    paths: dict[str, Path] = {}
    counts = {source: 0 for source in TOPICS}
    delayed = 0
    scenario = args.scenario
    for rec in records:
        delay_h = delay_hours_for(str(rec["event_id"]), args.delay)
        if delay_h > 0:
            delayed += 1
        event_hour = parse_ts(rec["event_hour"])
        envelope = {
            "source": rec["source"],
            "county": rec["county"],
            "event_hour": fmt_ts(event_hour),
            "event_id": rec["event_id"],
            "replay_scenario": scenario,
            "scheduled_arrival_time": fmt_ts(event_hour + timedelta(hours=delay_h)),
            "delay_hours": delay_h,
            "payload": {k: v for k, v in rec.items() if k not in REQUIRED_EVENT_FIELDS},
        }
        source_dir = staging_root / rec["source"]
        source_dir.mkdir(parents=True, exist_ok=True)
        path = source_dir / "envelopes.json"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(envelope) + "\n")
        paths[rec["source"]] = source_dir
        counts[rec["source"]] += 1

    print(
        f"staged envelopes under {staging_root}: "
        f"{sum(counts.values())} records, {delayed} delayed "
        f"(profile={args.delay})",
        flush=True,
    )
    for source, count in counts.items():
        print(f"  {source}: {count} -> {paths.get(source)}", flush=True)
    missing = [source for source in TOPICS if source not in paths]
    if missing:
        raise FileNotFoundError(f"missing staged sources: {missing}")
    return paths


def union_source_streams(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    frames: list[DataFrame] = []
    if args.source == "kafka":
        for source_name, topic in TOPICS.items():
            raw = read_kafka_topic(spark, topic, args.bootstrap)
            parsed = parse_kafka_envelope(raw)
            print(f"subscribed to Kafka topic {topic} ({source_name})", flush=True)
            frames.append(extract_measurements(parsed, args.scenario))
    else:
        staged = stage_file_envelopes(args)
        for source_name, path in staged.items():
            parsed = read_file_source(spark, source_name, path)
            print(f"file stream {source_name} <- {path}", flush=True)
            frames.append(extract_measurements(parsed, args.scenario))

    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame, allowMissingColumns=True)
    return unioned


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", choices=("kafka", "file"), default="kafka")
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--input-dir", default="data/samples")
    parser.add_argument("--output", default=OUTPUT_PATH, help="JSON output directory")
    parser.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    parser.add_argument("--run-code", default="spark-local-001")
    parser.add_argument("--scenario", default="spark_smoke")
    parser.add_argument("--speed", choices=sorted(REPLAY_SPEEDS), default="fast")
    parser.add_argument("--delay", choices=list(DELAY_LEVELS), default="none")
    parser.add_argument(
        "--trigger",
        choices=("once", "processing-time"),
        default=None,
        help="once = availableNow (batch-and-stop). Default: once for file, processing-time for Kafka.",
    )
    parser.add_argument("--interval", default="5 seconds")
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--app-name", default=APP_NAME)
    return parser.parse_args(argv)


def print_banner(args: argparse.Namespace) -> None:
    trigger = args.trigger or ("once" if args.source == "file" else "processing-time")
    print("SmokeWatch Replay — Spark Structured Streaming", flush=True)
    print(f"  spark={SPARK_VERSION}  kafka_package={KAFKA_PACKAGE}", flush=True)
    print(f"  source={args.source}  trigger={trigger}  watermark={WATERMARK_DURATION}", flush=True)
    print(f"  topics={list(TOPICS.values())}", flush=True)
    print(
        f"  run_code={args.run_code}  scenario={args.scenario}  "
        f"speed={args.speed}  delay={args.delay}",
        flush=True,
    )
    print(
        "  aggregation=stateful groupBy(county, event_hour_ts, replay_scenario); "
        "collect_set(source); is_join_ready when distinct_source_count=3",
        flush=True,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.trigger is None:
        args.trigger = "once" if args.source == "file" else "processing-time"

    print_banner(args)

    checkpoint = REPO_ROOT / args.checkpoint
    if args.reset_checkpoint and checkpoint.exists():
        shutil.rmtree(checkpoint)
        print(f"removed checkpoint {checkpoint}", flush=True)
    checkpoint.mkdir(parents=True, exist_ok=True)

    output_root = REPO_ROOT / args.output
    if args.reset_checkpoint and output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    spark = build_spark(args.app_name, args.source)
    print(
        f"  runtime_spark={spark.version}  "
        f"java={spark.sparkContext._jvm.System.getProperty('java.version')}",
        flush=True,
    )

    events = union_source_streams(spark, args)
    aggregated = aggregate_county_hour(events, args)

    writer = (
        aggregated.writeStream.outputMode("update")
        .option("checkpointLocation", str(checkpoint))
        .foreachBatch(lambda df, batch_id: write_batch(df, batch_id, args))
    )
    if args.trigger == "once":
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=args.interval).start()

    query.awaitTermination()
    print(f"query stopped: status={query.status}", flush=True)
    total_input_rows = 0
    dropped_by_watermark = 0
    for progress in query.recentProgress or []:
        input_rows = sum(int(src.get("numInputRows") or 0) for src in progress.get("sources", []))
        total_input_rows += input_rows
        state_rows = 0
        batch_dropped = 0
        for operator in progress.get("stateOperators", []):
            if operator.get("operatorName") == "stateStoreSave":
                state_rows = int(operator.get("numRowsTotal") or 0)
                batch_dropped = int(operator.get("numRowsDroppedByWatermark") or 0)
        dropped_by_watermark += batch_dropped
        event_time = progress.get("eventTime") or {}
        print(
            f"progress batchId={progress.get('batchId')} numInputRows={input_rows} "
            f"stateRows={state_rows} droppedByWatermark={batch_dropped} "
            f"watermark={event_time.get('watermark')}",
            flush=True,
        )
    print(
        f"totals: numInputRows={total_input_rows} dropped_by_watermark={dropped_by_watermark}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
