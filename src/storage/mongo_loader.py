
"""
SmokeWatch Replay - MongoDB Raw Event Storage

Stores raw wildfire, air-quality, and weather events
with source metadata and ingestion timestamps.

Required package:
    pip install pymongo
"""

import os
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


def prepare_event(event):
    """Validate and normalize required event metadata."""

    required = [
        "source_name",
        "event_id",
        "county",
        "event_hour"
    ]

    missing = [
        field for field in required
        if field not in event or event[field] is None
    ]

    if missing:
        raise ValueError(
            f"Missing required fields: {missing}"
        )

    document = event.copy()

    document["source_name"] = str(
        document["source_name"]
    ).upper()

    if document["source_name"] not in {
        "FIRMS", "EPA", "NOAA"
    }:
        raise ValueError("Unsupported source name")

    document["event_id"] = str(document["event_id"])

    # Accept ISO timestamps or Python datetime values.
    event_hour = document["event_hour"]

    if isinstance(event_hour, str):
        event_hour = datetime.fromisoformat(
            event_hour.replace("Z", "+00:00")
        )

    if event_hour.tzinfo is None:
        event_hour = event_hour.replace(
            tzinfo=timezone.utc
        )

    document["event_hour"] = event_hour.astimezone(
        timezone.utc
    )

    document["ingestion_timestamp"] = datetime.now(
        timezone.utc
    )

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
            "source_name": "FIRMS",
            "event_id": "demo-firms-001",
            "county": "Plumas",
            "event_hour": "2021-07-13T10:00:00Z",
            "fire_detection_count": 3,
            "satellite": "VIIRS"
        },
        {
            "source_name": "EPA",
            "event_id": "demo-epa-001",
            "county": "Plumas",
            "event_hour": "2021-07-13T10:00:00Z",
            "pm25_ug_m3": 42.5,
            "parameter_code": 88101
        },
        {
            "source_name": "NOAA",
            "event_id": "demo-noaa-001",
            "county": "Plumas",
            "event_hour": "2021-07-13T10:00:00Z",
            "temperature_c": 29.1,
            "humidity_pct": 24,
            "wind_speed_m_s": 3.2
        }
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
