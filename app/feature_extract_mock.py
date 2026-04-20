import random
import time
from datetime import datetime, UTC, timedelta
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
DB_NAME = "traffic_monitoring"

MODEL_ID = "mock_extractor"
MODEL_VERSION = "1.0"
PROCESSING_WINDOW_HOURS = 2
BATCH_SIZE = 200


def init_mongo():
    client = MongoClient(MONGO_URI, tz_aware=True)
    db = client[DB_NAME]
    db.derived_features.create_index(
        [("camera_id", 1), ("capture_ts", 1), ("model_id", 1)],
        unique=True,
    )
    db.derived_features.create_index([("camera_id", 1), ("capture_ts", 1)])
    db.derived_features.create_index([("model_id", 1), ("processed_at", 1)])
    return db


def get_priority_camera_ids(db) -> set:
    return {
        doc["camera_id"]
        for doc in db.cameras.find({"priority": True}, {"camera_id": 1})
    }


def get_pending_captures(db, cutoff: datetime, batch_size: int) -> list:
    """Return unprocessed successful captures within the processing window,
    priority cameras first, then oldest-first."""
    processed = {
        (d["camera_id"], d["capture_ts"])
        for d in db.derived_features.find(
            {"capture_ts": {"$gte": cutoff}, "model_id": MODEL_ID},
            {"camera_id": 1, "capture_ts": 1},
        )
    }

    candidates = [
        doc for doc in db.raw_captures.find(
            {"success": True, "object_key": {"$ne": None},
             "capture_ts": {"$gte": cutoff}},
        )
        if (doc["camera_id"], doc["capture_ts"]) not in processed
    ]

    priority_ids = get_priority_camera_ids(db)
    candidates.sort(
        key=lambda d: (0 if d["camera_id"] in priority_ids else 1, d["capture_ts"])
    )
    return candidates[:batch_size]


def get_prev_checksum(db, camera_id: str, capture_ts: datetime):
    prev = db.raw_captures.find_one(
        {"camera_id": camera_id, "capture_ts": {"$lt": capture_ts}, "success": True},
        sort=[("capture_ts", -1)],
    )
    return prev.get("checksum") if prev else None


def compute_mock_features(raw_doc: dict, prev_checksum: str | None) -> dict:
    """Derive realistic mock CV features from image metadata (deterministic via checksum seed)."""
    file_size = raw_doc.get("file_size", 0)
    checksum = raw_doc.get("checksum", "")

    seed = int(checksum[:8], 16) if checksum else 0
    rng = random.Random(seed)

    # larger file → more road content → more vehicles (rough heuristic)
    size_factor = min(file_size / 80_000, 1.0)
    vehicle_count = max(0, int(size_factor * 30) + rng.randint(-3, 3))

    traffic_density = round(min(vehicle_count / 25.0, 1.0), 3)

    if traffic_density < 0.2:
        congestion_level = "free"
    elif traffic_density < 0.5:
        congestion_level = "moderate"
    elif traffic_density < 0.8:
        congestion_level = "heavy"
    else:
        congestion_level = "standstill"

    if prev_checksum is None:
        scene_change_score = round(rng.uniform(0.3, 0.7), 3)
    elif prev_checksum == checksum:
        scene_change_score = 0.0  # duplicate frame
    else:
        scene_change_score = round(rng.uniform(0.1, 0.9), 3)

    return {
        "vehicle_count": vehicle_count,
        "traffic_density": traffic_density,
        "congestion_level": congestion_level,
        "scene_change_score": scene_change_score,
        "confidence": round(rng.uniform(0.75, 0.95), 3),
    }


def process_one(raw_doc: dict, db) -> None:
    camera_id = raw_doc["camera_id"]
    capture_ts = raw_doc["capture_ts"]
    start = time.time()

    try:
        prev_checksum = get_prev_checksum(db, camera_id, capture_ts)
        features = compute_mock_features(raw_doc, prev_checksum)
        processing_ms = int((time.time() - start) * 1000)

        doc = {
            "camera_id": camera_id,
            "capture_ts": capture_ts,
            "object_key": raw_doc.get("object_key"),
            "model_id": MODEL_ID,
            "model_version": MODEL_VERSION,
            "processed_at": datetime.now(UTC),
            "processing_ms": processing_ms,
            "success": True,
            "error_message": None,
            **features,
        }

        db.derived_features.update_one(
            {"camera_id": camera_id, "capture_ts": capture_ts, "model_id": MODEL_ID},
            {"$set": doc},
            upsert=True,
        )
        print(
            f"[OK] {camera_id} {capture_ts} "
            f"vehicles={features['vehicle_count']} "
            f"density={features['traffic_density']} "
            f"congestion={features['congestion_level']} "
            f"scene_change={features['scene_change_score']}"
        )

    except Exception as e:
        db.derived_features.update_one(
            {"camera_id": camera_id, "capture_ts": capture_ts, "model_id": MODEL_ID},
            {"$set": {
                "camera_id": camera_id,
                "capture_ts": capture_ts,
                "model_id": MODEL_ID,
                "model_version": MODEL_VERSION,
                "processed_at": datetime.now(UTC),
                "success": False,
                "error_message": str(e),
            }},
            upsert=True,
        )
        print(f"[ERROR] {camera_id} {capture_ts}: {e}")


def main():
    db = init_mongo()
    cutoff = datetime.now(UTC) - timedelta(hours=PROCESSING_WINDOW_HOURS)

    pending = get_pending_captures(db, cutoff, BATCH_SIZE)
    total_in_window = db.raw_captures.count_documents(
        {"success": True, "capture_ts": {"$gte": cutoff}}
    )
    already_done = db.derived_features.count_documents(
        {"capture_ts": {"$gte": cutoff}, "model_id": MODEL_ID}
    )
    backlog = total_in_window - already_done

    print(f"Window: last {PROCESSING_WINDOW_HOURS}h | backlog={backlog} | processing={len(pending)}")

    if backlog > BATCH_SIZE * 3:
        print(f"[WARN] backlog={backlog} exceeds threshold — consider scaling up workers")

    for doc in pending:
        process_one(doc, db)

    print(f"feature_extract completed. processed={len(pending)}")


if __name__ == "__main__":
    main()
