from datetime import datetime, UTC
from pymongo import MongoClient
import requests
import os
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"
API_KEY = os.getenv("NY511_API_KEY")
CAMERAS_API_URL = "https://511ny.org/api/v2/get/cameras"

def init_mongo():
    client = MongoClient(MONGO_URI,tz_aware=True)
    db = client[DB_NAME]
    db.cameras.create_index("camera_id", unique=True)
    return db

def fetch_cameras():
    if not API_KEY:
        raise RuntimeError("NY511_API_KEY is not set")

    resp = requests.get(
        CAMERAS_API_URL,
        params={"key": API_KEY, "format": "json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def normalize_camera(cam):
    views = cam.get("Views") or []

    first_enabled_view = None
    for v in views:
        if (v.get("Status") or "").lower() == "enabled":
            first_enabled_view = v
            break

    return {
        "camera_id": f"NY511_{cam['Id']}",
        "source": cam.get("Source", "511NY"),
        "source_camera_id": str(cam.get("Id")),
        "source_id": cam.get("SourceId"),
        "roadway": cam.get("Roadway"),
        "direction": cam.get("Direction"),
        "latitude": cam.get("Latitude"),
        "longitude": cam.get("Longitude"),
        "location": cam.get("Location"),
        "status": "active" if first_enabled_view else "inactive",
        "image_url": first_enabled_view.get("Url") if first_enabled_view else None,
        "view_id": first_enabled_view.get("Id") if first_enabled_view else None,
        "view_description": first_enabled_view.get("Description") if first_enabled_view else None,
        "views": views,
        "last_catalog_refresh_ts": datetime.now(UTC),
    }

def main():
    db = init_mongo()
    cameras = fetch_cameras()

    count = 0
    for cam in cameras:
        doc = normalize_camera(cam)
        db.cameras.update_one(
            {"camera_id": doc["camera_id"]},
            {"$set": doc},
            upsert=True,
        )
        count += 1

    print(f"Synced {count} cameras into MongoDB.")

if __name__ == "__main__":
    main()