
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
            "source_payload": {
                k: v for k, v in doc.items()
                if k not in ("_id", "event_hour", "ingestion_timestamp")
            },
        }
        result = insert_source_event(conn, record)
        print(doc["source_name"], "inserted" if result else "already exists")
finally:
    conn.close()
    mongo.close()
