from collections import defaultdict
from datetime import datetime, UTC, timedelta
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"


CAPTURE_TIME_CYCLE_MINUTES = 10

def floor_ts_to_cycle(dt: datetime) -> datetime:
    minute = (dt.minute // CAPTURE_TIME_CYCLE_MINUTES) * CAPTURE_TIME_CYCLE_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)

def init_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    db.camera_hourly_summary.create_index(
        [("camera_id", 1), ("date", 1), ("hour", 1)],
        unique=True
    )
    return db

def load_active_cameras_from_mongo(db):
    return list(db.cameras.find({"status": "active"}, {"_id": 0}))

def group_key(camera_id: str, capture_ts: datetime):
    return (
        camera_id,
        capture_ts.strftime("%Y-%m-%d"),
        capture_ts.hour,
    )

def build_expected_per_hour(cameras, start_ts: datetime, end_ts: datetime):
    expected_per_hour = defaultdict(int)
    current = start_ts

    while current <= end_ts:
        for cam in cameras:
            expected_per_hour[group_key(cam["camera_id"], current)] += 1
        current += timedelta(minutes=CAPTURE_TIME_CYCLE_MINUTES)

    return expected_per_hour

def main():
    db = init_mongo()
    cameras = load_active_cameras_from_mongo(db)

    if not cameras:
        print("No active cameras found.")
        return

    audits = list(db.quality_audits.find({}))
    if not audits:
        print("No quality audits found.")
        return

    grouped = defaultdict(list)
    for doc in audits:
        grouped[group_key(doc["camera_id"], doc["capture_ts"])].append(doc)

    capture_times = [doc["capture_ts"] for doc in audits]
    start_ts = min(capture_times)
    end_ts = max(capture_times)

    expected_per_hour = build_expected_per_hour(cameras, start_ts, end_ts)

    for gk, expected_count in expected_per_hour.items():
        camera_id, date_str, hour = gk
        docs = grouped.get(gk, [])

        images_received = sum(
            1 for d in docs
            if (not d.get("is_missing_expected", False))
            and d.get("raw_capture_success", False)
        )

        duplicate_count = sum(1 for d in docs if d.get("is_duplicate", False))
        corrupted_count = sum(1 for d in docs if d.get("is_corrupted", False))
        delayed_count = sum(1 for d in docs if d.get("is_delayed", False))

        delay_values = [
            d["delay_seconds"]
            for d in docs
            if d.get("delay_seconds") is not None
        ]
        avg_delay_sec = (
            sum(delay_values) / len(delay_values)
            if delay_values else None
        )

        completeness_rate = (
            images_received / expected_count if expected_count > 0 else None
        )

        summary_doc = {
            "camera_id": camera_id,
            "date": date_str,
            "hour": hour,
            "images_expected": expected_count,
            "images_received": images_received,
            "completeness_rate": completeness_rate,
            "duplicate_count": duplicate_count,
            "corrupted_count": corrupted_count,
            "delayed_count": delayed_count,
            "avg_delay_sec": avg_delay_sec,
            "last_updated_ts": datetime.now(UTC),
        }

        db.camera_hourly_summary.update_one(
            {
                "camera_id": camera_id,
                "date": date_str,
                "hour": hour,
            },
            {"$set": summary_doc},
            upsert=True,
        )

        print(
            f"[SUMMARY] {camera_id} {date_str} hour={hour} "
            f"expected={expected_count} received={images_received} "
            f"dup={duplicate_count} corrupt={corrupted_count} delayed={delayed_count}"
        )

    print("curate completed.")

if __name__ == "__main__":
    main()