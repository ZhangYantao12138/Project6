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


NORMAL_CYCLE_MINUTES = 10
PRIORITY_CYCLE_MINUTES = 2
LOOKBACK_HOURS = 2

def init_mongo():
    client = MongoClient(MONGO_URI,tz_aware=True)
    db = client[DB_NAME]

    db.camera_hourly_summary.create_index(
        [("camera_id", 1), ("date", 1), ("hour", 1)],
        unique=True
    )
    return db

def load_cameras_from_mongo(db):
    return list(db.cameras.find({"status": {"$in": ["active", "no_feed"]}}, {"_id": 0}))

def group_key(camera_id: str, capture_ts: datetime):
    return (
        camera_id,
        capture_ts.strftime("%Y-%m-%d"),
        capture_ts.hour,
    )

def build_expected_per_hour(cameras, start_ts: datetime, end_ts: datetime):
    expected_per_hour = defaultdict(int)
    for cam in cameras:
        cycle = PRIORITY_CYCLE_MINUTES if cam.get("priority") else NORMAL_CYCLE_MINUTES
        current = start_ts
        while current <= end_ts:
            expected_per_hour[group_key(cam["camera_id"], current)] += 1
            current += timedelta(minutes=cycle)
    return expected_per_hour

def main():
    db = init_mongo()
    cameras = load_cameras_from_mongo(db)

    if not cameras:
        print("No active or no_feed cameras found.")
        return

    active_cameras = [c for c in cameras if c["status"] == "active"]
    no_feed_cameras = [c for c in cameras if c["status"] == "no_feed"]

    cutoff = datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)
    audits = list(db.quality_audits.find({"capture_ts": {"$gte": cutoff}}))

    if audits:
        grouped = defaultdict(list)
        for doc in audits:
            grouped[group_key(doc["camera_id"], doc["capture_ts"])].append(doc)

        capture_times = [doc["capture_ts"] for doc in audits]
        start_ts = min(capture_times)
        end_ts = max(capture_times)

        camera_ids_in_audits = {doc["camera_id"] for doc in audits}
        recent_cameras = [c for c in active_cameras if c["camera_id"] in camera_ids_in_audits]
        expected_per_hour = build_expected_per_hour(recent_cameras, start_ts, end_ts)

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
                "camera_status": "active",
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
                {"camera_id": camera_id, "date": date_str, "hour": hour},
                {"$set": summary_doc},
                upsert=True,
            )

            print(
                f"[SUMMARY] {camera_id} {date_str} hour={hour} "
                f"expected={expected_count} received={images_received} "
                f"dup={duplicate_count} corrupt={corrupted_count} delayed={delayed_count}"
            )
    else:
        print("No recent quality audits found.")

    # Write no_feed summary entries for the current hour
    now = datetime.now(UTC)
    date_str = now.strftime("%Y-%m-%d")
    hour = now.hour
    for cam in no_feed_cameras:
        summary_doc = {
            "camera_id": cam["camera_id"],
            "date": date_str,
            "hour": hour,
            "camera_status": "no_feed",
            "images_expected": 0,
            "images_received": 0,
            "completeness_rate": None,
            "duplicate_count": 0,
            "corrupted_count": 0,
            "delayed_count": 0,
            "avg_delay_sec": None,
            "last_updated_ts": now,
        }
        db.camera_hourly_summary.update_one(
            {"camera_id": cam["camera_id"], "date": date_str, "hour": hour},
            {"$set": summary_doc},
            upsert=True,
        )
        print(f"[NO_FEED] {cam['camera_id']} {date_str} hour={hour}")

    print("curate completed.")

    # feature extraction backlog monitoring
    cutoff = datetime.now(UTC) - timedelta(hours=2)
    total = db.raw_captures.count_documents({"success": True, "capture_ts": {"$gte": cutoff}})
    done = db.derived_features.count_documents({"capture_ts": {"$gte": cutoff}}) if "derived_features" in db.list_collection_names() else 0
    backlog = total - done
    level = "[WARN]" if backlog > 150 else "[OK]"
    print(f"{level} feature_extract backlog (last 2h): {backlog}/{total}")

if __name__ == "__main__":
    main()