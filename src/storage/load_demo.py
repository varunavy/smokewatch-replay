
from pymongo import MongoClient
from postgres_loader import get_connection, insert_source_event

mongo = MongoClient("mongodb://localhost:27017/")
records = mongo["smokewatch"]["raw_events"].find()

conn = get_connection()

try:
    for doc in records:
        record = {
            "run_code": "demo-run-001",
            "scenario_code": "demo-normal",
            "speed_seconds_per_event_hour": 1.0,
            "delay_level": "none",
            "source_name": doc["source_name"],
            "event_id": doc["event_id"],
            "county": doc["county"],
            "event_hour": doc["event_hour"],
            "ingestion_timestamp": doc["ingestion_timestamp"],
            # Limited audit/provenance metadata only; MongoDB holds the raw event.
            "source_payload": {
                "original_source": doc.get("source", doc["source_name"]),
                "raw_event_id": doc["event_id"],
            },
        }
        result = insert_source_event(conn, record)
        print(doc["source_name"], "inserted" if result else "already exists")
finally:
    conn.close()
    mongo.close()
