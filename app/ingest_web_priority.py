import hashlib
import time
from datetime import datetime, UTC
from io import BytesIO
import requests
from minio import Minio
from pymongo import MongoClient

from app.utils import sha256_bytes, build_object_key

NO_FEED_MD5 = "9c2d059e65a23b43a5b481c16918fed4"

import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"

SLEEP_BETWEEN_CAMERAS = 1
CAPTURE_TIME_CYCLE_MINUTES = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def floor_ts_to_cycle(dt: datetime) -> datetime:
    minute = (dt.minute // CAPTURE_TIME_CYCLE_MINUTES) * CAPTURE_TIME_CYCLE_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)


def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def init_mongo():
    client = MongoClient(MONGO_URI, tz_aware=True)
    db = client[DB_NAME]
    db.raw_captures.create_index(
        [("camera_id", 1), ("capture_ts", 1)],
        unique=True,
    )
    return db


def fetch_priority_cameras(db):
    return list(
        db.cameras.find(
            {"status": "active", "priority": True, "image_url": {"$ne": None}},
            {"_id": 0},
        )
    )


def ingest_one(camera_doc, db, minio_client: Minio):
    camera_id = camera_doc["camera_id"]
    image_url = camera_doc["image_url"]

    actual_ingest_start_ts = datetime.now(UTC)
    capture_dt = floor_ts_to_cycle(actual_ingest_start_ts)
    ts_str = capture_dt.strftime("%Y%m%d_%H%M")

    fetch_url = f"{image_url}?t={int(time.time())}"

    try:
        response = requests.get(fetch_url, headers=HEADERS, timeout=20)
        ingest_dt = datetime.now(UTC)

        if response.status_code != 200:
            db.raw_captures.update_one(
                {"camera_id": camera_id, "capture_ts": capture_dt},
                {"$set": {
                    "camera_id": camera_id,
                    "capture_ts": capture_dt,
                    "ingest_ts": ingest_dt,
                    "source_url": fetch_url,
                    "success": False,
                    "http_status": response.status_code,
                    "error_message": response.text[:500],
                    "roadway": camera_doc.get("roadway"),
                    "direction": camera_doc.get("direction"),
                    "location": camera_doc.get("location"),
                    "view_id": camera_doc.get("view_id"),
                    "pipeline": "priority",
                }},
                upsert=True,
            )
            print(f"[WARN] {camera_id} cycle={ts_str} -> {response.status_code}")
            return

        content_type = response.headers.get("Content-Type", "")
        if "image" not in content_type:
            db.raw_captures.update_one(
                {"camera_id": camera_id, "capture_ts": capture_dt},
                {"$set": {
                    "camera_id": camera_id,
                    "capture_ts": capture_dt,
                    "ingest_ts": ingest_dt,
                    "source_url": fetch_url,
                    "success": False,
                    "http_status": response.status_code,
                    "error_message": f"Unexpected content-type: {content_type}",
                    "roadway": camera_doc.get("roadway"),
                    "direction": camera_doc.get("direction"),
                    "location": camera_doc.get("location"),
                    "view_id": camera_doc.get("view_id"),
                    "pipeline": "priority",
                }},
                upsert=True,
            )
            print(f"[WARN] {camera_id} cycle={ts_str} non-image: {content_type}")
            return

        image_bytes = response.content
        if hashlib.md5(image_bytes).hexdigest() == NO_FEED_MD5:
            db.raw_captures.update_one(
                {"camera_id": camera_id, "capture_ts": capture_dt},
                {"$set": {
                    "camera_id": camera_id,
                    "capture_ts": capture_dt,
                    "ingest_ts": ingest_dt,
                    "source_url": fetch_url,
                    "success": False,
                    "http_status": response.status_code,
                    "error_message": "no_feed_placeholder",
                    "roadway": camera_doc.get("roadway"),
                    "direction": camera_doc.get("direction"),
                    "location": camera_doc.get("location"),
                    "view_id": camera_doc.get("view_id"),
                    "pipeline": "priority",
                }},
                upsert=True,
            )
            db.cameras.update_one(
                {"camera_id": camera_id},
                {"$set": {"status": "no_feed"}},
            )
            print(f"[SKIP][PRIORITY] {camera_id} cycle={ts_str} -> no live feed")
            return

        checksum = sha256_bytes(image_bytes)
        object_key = build_object_key(camera_id, ts_str)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_key,
            data=BytesIO(image_bytes),
            length=len(image_bytes),
            content_type=content_type,
        )

        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {"$set": {
                "camera_id": camera_id,
                "capture_ts": capture_dt,
                "ingest_ts": ingest_dt,
                "source_url": fetch_url,
                "object_key": object_key,
                "http_status": response.status_code,
                "content_type": content_type,
                "file_size": len(image_bytes),
                "checksum": checksum,
                "success": True,
                "error_message": None,
                "roadway": camera_doc.get("roadway"),
                "direction": camera_doc.get("direction"),
                "location": camera_doc.get("location"),
                "view_id": camera_doc.get("view_id"),
                "view_description": camera_doc.get("view_description"),
                "source": camera_doc.get("source", "511NY"),
                "pipeline": "priority",
            }},
            upsert=True,
        )

        db.cameras.update_one(
            {"camera_id": camera_id},
            {"$set": {"status": "active", "last_ingested_at": ingest_dt}},
        )
        print(f"[OK][PRIORITY] {camera_id} cycle={ts_str} stored as {object_key}")

    except Exception as e:
        ingest_dt = datetime.now(UTC)
        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {"$set": {
                "camera_id": camera_id,
                "capture_ts": capture_dt,
                "ingest_ts": ingest_dt,
                "source_url": fetch_url,
                "success": False,
                "http_status": None,
                "error_message": str(e),
                "roadway": camera_doc.get("roadway"),
                "direction": camera_doc.get("direction"),
                "location": camera_doc.get("location"),
                "view_id": camera_doc.get("view_id"),
                "pipeline": "priority",
            }},
            upsert=True,
        )
        print(f"[ERROR][PRIORITY] {camera_id} cycle={ts_str}: {e}")


def main():
    db = init_mongo()

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client, MINIO_BUCKET)

    cameras = fetch_priority_cameras(db)
    print(f"Priority cameras: {len(cameras)}")

    for cam in cameras:
        ingest_one(cam, db, minio_client)
        time.sleep(SLEEP_BETWEEN_CAMERAS)


if __name__ == "__main__":
    main()
