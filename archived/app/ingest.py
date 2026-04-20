import json
from datetime import datetime, UTC
from pathlib import Path

import requests
from minio import Minio
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from app.utils import sha256_bytes, build_object_key

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "cameras.json"

import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"
MOCK_API_BASE = "http://127.0.0.1:8000"

TEST_TIMESTAMPS = [
    "20260324_1400",
    "20260324_1410",
    "20260324_1420",
    "20260414_1400",
]


def ensure_bucket(client: Minio, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def load_cameras():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def init_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    db.raw_captures.create_index(
        [("camera_id", 1), ("capture_ts", 1)],
        unique=True
    )
    db.cameras.create_index("camera_id", unique=True)

    return db


def seed_cameras(db, cameras):
    for cam in cameras:
        db.cameras.update_one(
            {"camera_id": cam["camera_id"]},
            {"$set": cam},
            upsert=True
        )


def ingest_one(camera_id: str, ts_str: str, db, minio_client: Minio):
    capture_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M")
    ingest_dt = datetime.now(UTC)

    url = f"{MOCK_API_BASE}/camera/{camera_id}"
    params = {"ts": ts_str}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code != 200:
            db.raw_captures.update_one(
                {"camera_id": camera_id, "capture_ts": capture_dt},
                {
                    "$set": {
                        "camera_id": camera_id,
                        "capture_ts": capture_dt,
                        "ingest_ts": ingest_dt,
                        "success": False,
                        "http_status": response.status_code,
                        "error_message": response.text,
                    }
                },
                upsert=True,
            )
            print(f"[WARN] {camera_id} {ts_str} -> {response.status_code}")
            return

        image_bytes = response.content
        checksum = sha256_bytes(image_bytes)
        object_key = build_object_key(camera_id, ts_str)

        from io import BytesIO
        data_stream = BytesIO(image_bytes)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_key,
            data=data_stream,
            length=len(image_bytes),
            content_type="image/jpeg",
        )

        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {
                "$set": {
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
                }
            },
            upsert=True,
        )

        print(f"[OK] {camera_id} {ts_str} stored as {object_key}")

    except Exception as e:
        db.raw_captures.update_one(
            {"camera_id": camera_id, "capture_ts": capture_dt},
            {
                "$set": {
                    "camera_id": camera_id,
                    "capture_ts": capture_dt,
                    "ingest_ts": ingest_dt,
                    "success": False,
                    "http_status": None,
                    "error_message": str(e),
                }
            },
            upsert=True,
        )
        print(f"[ERROR] {camera_id} {ts_str}: {e}")


def main():
    db = init_mongo()
    cameras = load_cameras()
    seed_cameras(db, cameras)

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(minio_client, MINIO_BUCKET)

    for cam in cameras:
        if cam.get("status") != "active":
            continue

        camera_id = cam["camera_id"]
        for ts_str in TEST_TIMESTAMPS:
            ingest_one(camera_id, ts_str, db, minio_client)


if __name__ == "__main__":
    main()