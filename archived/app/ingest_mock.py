
from datetime import datetime, UTC
from io import BytesIO
import os
import requests
from minio import Minio
from pymongo import MongoClient

from app.utils import sha256_bytes, build_object_key

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
DB_NAME = "traffic_monitoring"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")

CAPTURE_TIME_CYCLE_MINUTES = 10

def floor_ts_to_cycle(dt: datetime) -> datetime:
    minute = (dt.minute // CAPTURE_TIME_CYCLE_MINUTES) * CAPTURE_TIME_CYCLE_MINUTES
    return dt.replace(minute=minute, second=0, microsecond=0)

def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

def init_mongo():
    client = MongoClient(MONGO_URI,tz_aware=True)
    db = client[DB_NAME]
    db.raw_captures.create_index(
        [("camera_id", 1), ("capture_ts", 1)],
        unique=True
    )
    return db

def fetch_active_cameras(db):
    return list(
        db.cameras.find(
            {"status": "active", "image_url": {"$ne": None}},
            {"_id": 0}
        )
    )

def ingest_one(camera_doc, db, minio_client: Minio):
    camera_id = camera_doc["camera_id"]
    image_url = camera_doc["image_url"]

    actual_ingest_start_ts = datetime.now(UTC)
    capture_dt = floor_ts_to_cycle(actual_ingest_start_ts)
    ts_str = capture_dt.strftime("%Y%m%d_%H%M")

    try:
        response = requests.get(image_url, params={"ts": ts_str}, timeout=20)
        ingest_dt = datetime.now(UTC)

        if response.status_code != 200:
            db.raw_captures.update_one(
                {"camera_id": camera_id, "capture_ts": capture_dt},
                {"$set": {
                    "camera_id": camera_id,
                    "capture_ts": capture_dt,
                    "ingest_ts": ingest_dt,
                    "source_url": str(response.url),
                    "success": False,
                    "http_status": response.status_code,
                    "error_message": response.text[:500],
                    "roadway": camera_doc.get("roadway"),
                    "direction": camera_doc.get("direction"),
                    "location": camera_doc.get("location"),
                    "view_id": camera_doc.get("view_id"),
                }},
                upsert=True,
            )
            print(f"[WARN] {camera_id} cycle={ts_str} -> {response.status_code}")
            return

        image_bytes = response.content
        checksum = sha256_bytes(image_bytes)
        object_key = build_object_key(camera_id, ts_str)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_key,
            data=BytesIO(image_bytes),
            length=len(image_bytes),
            content_type=response.headers.get("Content-Type", "image/jpeg"),
        )

        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {"$set": {
                "camera_id": camera_id,
                "capture_ts": capture_dt,
                "ingest_ts": ingest_dt,
                "source_url": str(response.url),
                "object_key": object_key,
                "http_status": response.status_code,
                "content_type": response.headers.get("Content-Type"),
                "file_size": len(image_bytes),
                "checksum": checksum,
                "success": True,
                "error_message": None,
                "roadway": camera_doc.get("roadway"),
                "direction": camera_doc.get("direction"),
                "location": camera_doc.get("location"),
                "view_id": camera_doc.get("view_id"),
                "view_description": camera_doc.get("view_description"),
                "source": camera_doc.get("source", "mock"),
            }},
            upsert=True,
        )

        print(f"[OK] {camera_id} cycle={ts_str} stored as {object_key}")

    except Exception as e:
        ingest_dt = datetime.now(UTC)
        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {"$set": {
                "camera_id": camera_id,
                "capture_ts": capture_dt,
                "ingest_ts": ingest_dt,
                "source_url": image_url,
                "success": False,
                "http_status": None,
                "error_message": str(e),
                "roadway": camera_doc.get("roadway"),
                "direction": camera_doc.get("direction"),
                "location": camera_doc.get("location"),
                "view_id": camera_doc.get("view_id"),
                "view_description": camera_doc.get("view_description"),
                "source": camera_doc.get("source", "mock"),
            }},
            upsert=True,
        )
        print(f"[ERROR] {camera_id} cycle={ts_str}: {e}")

def main():
    db = init_mongo()

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client, MINIO_BUCKET)

    cameras = fetch_active_cameras(db)

    for cam in cameras:
        ingest_one(cam, db, minio_client)

if __name__ == "__main__":
    main()