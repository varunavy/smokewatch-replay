
import json
from uuid import uuid4

from kafka import KafkaConsumer
from mongo_loader import get_collection, insert_events

RUN_CODE = "smoke_test__20261113T070747Z__a40c61"

TOPICS = [
    "firms.events",
    "epa.pm25.events",
    "noaa.weather.events",
]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9092",
    group_id=f"smokewatch-mongo-smoke-{uuid4().hex[:8]}",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    consumer_timeout_ms=20000,
)

client, collection = get_collection()

matched = 0
inserted = 0

try:
    for message in consumer:
        event = message.value

        if event.get("run_code") != RUN_CODE:
            continue

        matched += 1
        inserted += insert_events(collection, [event])

        if matched >= 12:
            break

    stored = collection.count_documents({"run_code": RUN_CODE})

    print("Kafka events matched:", matched)
    print("New MongoDB documents inserted:", inserted)
    print("MongoDB documents for run:", stored)

    if matched == 12 and stored == 12:
        print("PASS: Kafka to MongoDB smoke test")
    else:
        print("INCOMPLETE: Check Kafka messages and MongoDB")

finally:
    consumer.close()
    client.close()
