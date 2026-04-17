import json
import os
from datetime import datetime, UTC
from pathlib import Path
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "cameras.json"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
DB_NAME = "traffic_monitoring"

def init_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    db.cameras.create_index("camera_id", unique=True)
    return db

def load_mock_cameras():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    db = init_mongo()
    cameras = load_mock_cameras()

    count = 0
    for cam in cameras:
        doc = {
            **cam,
            "last_catalog_refresh_ts": datetime.now(UTC),
        }
        db.cameras.update_one(
            {"camera_id": doc["camera_id"]},
            {"$set": doc},
            upsert=True,
        )
        count += 1

    print(f"Synced {count} mock cameras into MongoDB.")

if __name__ == "__main__":
    main()