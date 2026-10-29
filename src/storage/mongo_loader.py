
"""
SmokeWatch Replay - MongoDB Raw Event Storage

Stores raw wildfire, air-quality, and weather events
with source metadata and ingestion timestamps.

Required package:
    pip install pymongo
"""

import os
from copy import deepcopy
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/"
)

DATABASE_NAME = os.getenv(
    "MONGO_DATABASE",
    "smokewatch"
)

COLLECTION_NAME = "raw_events"


def get_collection():
    """Connect to MongoDB and return the raw-events collection."""

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000
    )

    client.admin.command("ping")

    database = client[DATABASE_NAME]
    collection = database[COLLECTION_NAME]

    # Prevent duplicate source events.
    collection.create_index(
        [
            ("source_name", ASCENDING),
            ("event_id", ASCENDING)
        ],
        unique=True
    )

    collection.create_index(
        [
            ("county", ASCENDING),
            ("event_hour", ASCENDING)
        ]
    )

    return client, collection


SOURCE_MAP = {
    "nasa_firms": "FIRMS",
    "epa_pm25": "EPA",
    "noaa_weather": "NOAA",
    "FIRMS": "FIRMS",
    "EPA": "EPA",
    "NOAA": "NOAA",
}


def prepare_event(event):
    """Normalize searchable fields while retaining the complete original event."""
    required = ["event_id", "county", "event_hour"]
    missing = [key for key in required if event.get(key) is None]
    if not event.get("source") and not event.get("source_name"):
        missing.append("source or source_name")
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    incoming_source = event.get("source", event.get("source_name"))
    source_name = SOURCE_MAP.get(str(incoming_source))
    if source_name is None:
        raise ValueError(f"Unsupported source: {incoming_source}")

    # Preserve the exact inbound Kafka envelope, including source, payload,
    # replay_scenario, scheduled_arrival_time and delay_hours. Do not mutate it.
    document = deepcopy(event)
    document["raw_message"] = deepcopy(event)
    document["source_name"] = source_name
    document["event_id"] = str(event["event_id"])

    event_hour = event["event_hour"]
    if isinstance(event_hour, str):
        event_hour = datetime.fromisoformat(event_hour.replace("Z", "+00:00"))
    if not isinstance(event_hour, datetime):
        raise ValueError("event_hour must be an ISO timestamp or datetime")
    if event_hour.tzinfo is None:
        event_hour = event_hour.replace(tzinfo=timezone.utc)
    document["event_hour"] = event_hour.astimezone(timezone.utc)
    document["ingestion_timestamp"] = datetime.now(timezone.utc)
    return document


def insert_events(collection, events):
    """Insert multiple events, skipping duplicate event IDs."""

    documents = [
        prepare_event(event)
        for event in events
    ]

    if not documents:
        return 0

    try:
        result = collection.insert_many(
            documents,
            ordered=False
        )
        return len(result.inserted_ids)

    except BulkWriteError as error:
        details = error.details or {}
        write_errors = details.get("writeErrors", [])

        # Ignore duplicate-key errors only.
        if any(
            item.get("code") != 11000
            for item in write_errors
        ):
            raise

        return details.get("nInserted", 0)


def retrieve_events(
    collection,
    source_name=None,
    county=None,
    limit=10
):
    """Retrieve stored records using optional filters."""

    query = {}

    if source_name:
        query["source_name"] = source_name.upper()

    if county:
        query["county"] = county

    return list(
        collection.find(
            query,
            {"_id": 0}
        ).limit(limit)
    )


def count_events(collection):
    """Count records grouped by source."""

    pipeline = [
        {
            "$group": {
                "_id": "$source_name",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    return list(collection.aggregate(pipeline))


def main():
    """Run a small MongoDB storage demonstration."""

    sample_events = [
        {
            "source": "nasa_firms", "event_id": "demo-kafka-firms-001",
            "county": "Plumas", "event_hour": "2021-07-13T10:00:00Z",
            "replay_scenario": "demo-normal",
            "scheduled_arrival_time": "2021-07-13T10:00:00Z",
            "delay_hours": 0,
            "payload": {"fire_detection_count": 3, "satellite": "VIIRS"},
        },
        {
            "source": "epa_pm25", "event_id": "demo-kafka-epa-001",
            "county": "Plumas", "event_hour": "2021-07-13T10:00:00Z",
            "replay_scenario": "demo-normal",
            "scheduled_arrival_time": "2021-07-13T10:00:00Z",
            "delay_hours": 0,
            "payload": {"pm25_ug_m3": 42.5, "parameter_code": 88101},
        },
        {
            "source": "noaa_weather", "event_id": "demo-kafka-noaa-001",
            "county": "Plumas", "event_hour": "2021-07-13T10:00:00Z",
            "replay_scenario": "demo-normal",
            "scheduled_arrival_time": "2021-07-13T10:00:00Z",
            "delay_hours": 0,
            "payload": {"temperature_c": 29.1, "humidity_pct": 24,
                        "wind_speed_m_s": 3.2},
        },
    ]

    client = None

    try:
        client, collection = get_collection()

        inserted = insert_events(
            collection,
            sample_events
        )

        print("MongoDB connection successful")
        print("New records inserted:", inserted)
        repeated = insert_events(collection, sample_events)
        print("New records on repeat insert:", repeated)
        print(
            "Total records:",
            collection.count_documents({})
        )

        print("\nRecords by source:")
        for result in count_events(collection):
            print(result)

        print("\nSample Plumas records:")
        for record in retrieve_events(
            collection,
            county="Plumas"
        ):
            print(record)

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()
