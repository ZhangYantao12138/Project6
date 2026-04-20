import json
from collections import defaultdict
from datetime import datetime, UTC, timedelta
from io import BytesIO
from pathlib import Path

from minio import Minio
from PIL import Image, UnidentifiedImageError
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "cameras.json"

import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"
#TEST_TIMESTAMPS = [
#    "20260324_1400",
#    "20260324_1410",
#    "20260324_1420",
#]

DELAY_THRESHOLD_SECONDS = 120
NORMAL_CYCLE_MINUTES = 10
PRIORITY_CYCLE_MINUTES = 2

def floor_ts_to_cycle(dt: datetime, cycle_minutes: int) -> datetime:
    minute = (dt.minute // cycle_minutes) * cycle_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)

def get_expected_records(cameras, cycle_minutes: int):
    now = floor_ts_to_cycle(datetime.now(UTC), cycle_minutes)
    start = now - timedelta(minutes=cycle_minutes)

    expected = []
    current = start
    while current <= now:
        for cam in cameras:
            if cam.get("status") != "active":
                continue
            # Priority cycle only checks priority cameras;
            # normal cycle checks all active cameras (incl. priority at 10-min slots)
            if cycle_minutes == PRIORITY_CYCLE_MINUTES and not cam.get("priority"):
                continue
            expected.append({
                "camera_id": cam["camera_id"],
                "capture_ts": current,
            })
        current += timedelta(minutes=cycle_minutes)

    return expected

def load_cameras(): # local test uses static config, in prod we would load from db
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)
def load_cameras_from_mongo(db):
    return list(db.cameras.find({"status": "active"}, {"_id": 0}))

def init_mongo():
    client = MongoClient(MONGO_URI,tz_aware=True)
    db = client[DB_NAME]

    db.quality_audits.create_index(
        [("camera_id", 1), ("capture_ts", 1)],
        unique=True
    )
    return db


def init_minio():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y%m%d_%H%M")


#def get_expected_records(cameras):
#    expected = []
#    for cam in cameras:
#        if cam.get("status") != "active":
#            continue
#        for ts_str in TEST_TIMESTAMPS:
#            expected.append({
#                "camera_id": cam["camera_id"],
#                "capture_ts": parse_ts(ts_str),
#            })
#    return expected

def ensure_utc_aware(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)

def load_raw_capture_map(db,start_ts=None, end_ts=None):
    query = {}
    if start_ts:
        query["capture_ts"] = {"$gte": start_ts}
    if end_ts:
        query.setdefault("capture_ts", {})["$lte"] = end_ts
    raw_docs = list(db.raw_captures.find(query))
    raw_map = {}
    for doc in raw_docs:
        doc["capture_ts"] = ensure_utc_aware(doc.get("capture_ts"))
        if doc.get("ingest_ts"):
            doc["ingest_ts"] = ensure_utc_aware(doc.get("ingest_ts"))
        raw_map[(doc["camera_id"], doc["capture_ts"])] = doc
    return raw_map, raw_docs
def build_duplicate_lookup(raw_docs):
    """
    For each camera, compare each successful capture only with the
    immediately previous successful capture. If checksums match,
    mark current capture as duplicate/stale.
    """
    by_camera = defaultdict(list)

    for doc in raw_docs:
        if not doc.get("success"):
            continue
        checksum = doc.get("checksum")
        if not checksum:
            continue
        by_camera[doc["camera_id"]].append(doc)

    duplicate_lookup = {}

    for camera_id, docs in by_camera.items():
        docs_sorted = sorted(docs, key=lambda x: x["capture_ts"])

        prev_checksum = None
        for doc in docs_sorted:
            key = (doc["camera_id"], doc["capture_ts"])
            curr_checksum = doc.get("checksum")

            is_dup = (
                prev_checksum is not None and curr_checksum == prev_checksum
            )
            duplicate_lookup[key] = is_dup
            prev_checksum = curr_checksum

    return duplicate_lookup

#def build_duplicate_lookup(raw_docs):
#    """
#    Mark duplicates when same camera_id has same checksum across multiple capture_ts.
#    We keep the earliest one as non-duplicate, later ones as duplicates.
#    """
#    groups = defaultdict(list)
#
#    for doc in raw_docs:
#        if not doc.get("success"):
#            continue
#        checksum = doc.get("checksum")
#        if not checksum:
#            continue
#        groups[(doc["camera_id"], checksum)].append(doc)
#
#    duplicate_lookup = {}
#
#    for (_, _), docs in groups.items():
#        docs_sorted = sorted(docs, key=lambda x: x["capture_ts"])
#        for i, doc in enumerate(docs_sorted):
#            key = (doc["camera_id"], doc["capture_ts"])
#            duplicate_lookup[key] = (i > 0)
#
#    return duplicate_lookup


def check_corrupted(minio_client, object_key: str):
    """
    Returns:
      (is_corrupted: bool, corruption_reason: str | None)
    """
    try:
        response = minio_client.get_object(MINIO_BUCKET, object_key)
        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()

        if not data:
            return True, "empty_file"

        try:
            img = Image.open(BytesIO(data))
            img.verify()
            return False, None
        except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as e:
            return True, f"invalid_image:{type(e).__name__}"

    except Exception as e:
        return True, f"minio_read_error:{type(e).__name__}"


def compute_delay_seconds(capture_ts, ingest_ts):
    if not capture_ts or not ingest_ts:
        return None
    delta = ingest_ts - capture_ts
    return int(delta.total_seconds())


def upsert_quality_audit(db, audit_doc):
    db.quality_audits.update_one(
        {
            "camera_id": audit_doc["camera_id"],
            "capture_ts": audit_doc["capture_ts"],
        },
        {"$set": audit_doc},
        upsert=True,
    )


def main(cycle_minutes: int = NORMAL_CYCLE_MINUTES):
    db = init_mongo()
    minio_client = init_minio()
    cameras = load_cameras_from_mongo(db)

    expected_records = get_expected_records(cameras, cycle_minutes)

    if not expected_records:
        print("No expected records for this cycle — nothing to audit.")
        return

    window_start = min(item["capture_ts"] for item in expected_records) - timedelta(minutes=cycle_minutes)
    window_end = max(item["capture_ts"] for item in expected_records)

    raw_map, raw_docs = load_raw_capture_map(
        db,
        start_ts=window_start,
        end_ts=window_end,
    )
    duplicate_lookup = build_duplicate_lookup(raw_docs)

    for item in expected_records:
        camera_id = item["camera_id"]
        capture_ts = item["capture_ts"]
        key = (camera_id, capture_ts)

        raw_doc = raw_map.get(key)

        audit_doc = {
            "camera_id": camera_id,
            "capture_ts": capture_ts,
            "audit_ts": datetime.now(UTC),
            "is_missing_expected": False,
            "is_duplicate": False,
            "is_corrupted": False,
            "is_delayed": False,
            "delay_seconds": None,
            "raw_capture_found": raw_doc is not None,
            "raw_capture_success": raw_doc.get("success") if raw_doc else False,
            "object_key": raw_doc.get("object_key") if raw_doc else None,
            "corruption_reason": None,
            "notes": [],
        }

        # 1) missing
        if raw_doc is None:
            audit_doc["is_missing_expected"] = True
            audit_doc["notes"].append("expected_capture_missing")
            upsert_quality_audit(db, audit_doc)
            print(f"[MISSING] {camera_id} {capture_ts}")
            continue

        # raw record exists but failed to ingest successfully
        if not raw_doc.get("success", False):
            audit_doc["is_missing_expected"] = True
            audit_doc["notes"].append("capture_record_exists_but_ingest_failed")
            upsert_quality_audit(db, audit_doc)
            print(f"[FAILED->MISSING] {camera_id} {capture_ts}")
            continue

        # 2) duplicate
        is_duplicate = duplicate_lookup.get(key, False)
        audit_doc["is_duplicate"] = is_duplicate
        if is_duplicate:
            audit_doc["notes"].append("duplicate_checksum_for_same_camera")

        # 3) corrupted
        object_key = raw_doc.get("object_key")
        if object_key:
            is_corrupted, corruption_reason = check_corrupted(minio_client, object_key)
            audit_doc["is_corrupted"] = is_corrupted
            audit_doc["corruption_reason"] = corruption_reason
            if is_corrupted:
                audit_doc["notes"].append("image_corrupted_or_unreadable")
        else:
            audit_doc["is_corrupted"] = True
            audit_doc["corruption_reason"] = "missing_object_key"
            audit_doc["notes"].append("missing_object_key")

        # 4) delayed
        delay_seconds = compute_delay_seconds(
            raw_doc.get("capture_ts"),
            raw_doc.get("ingest_ts"),
        )
        audit_doc["delay_seconds"] = delay_seconds
        if delay_seconds is not None and delay_seconds > DELAY_THRESHOLD_SECONDS:
            audit_doc["is_delayed"] = True
            audit_doc["notes"].append("ingestion_delay_exceeded_threshold")

        upsert_quality_audit(db, audit_doc)

        flags = []
        if audit_doc["is_duplicate"]:
            flags.append("UNCHANGED")
        if audit_doc["is_corrupted"]:
            flags.append("CORRUPTED")
        if audit_doc["is_delayed"]:
            flags.append("DELAYED")

        flag_str = ",".join(flags) if flags else "OK"
        print(f"[{flag_str}] {camera_id} {capture_ts}")

    print("quality_check completed.")


if __name__ == "__main__":
    main()