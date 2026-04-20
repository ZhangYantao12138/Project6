import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import StreamingResponse
from minio import Minio
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-raw")
DB_NAME = "traffic_monitoring"

app = FastAPI(
    title="Traffic Monitoring API",
    description="Query traffic camera images, quality metrics, and CV features from the 511NY pipeline.",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/docs", include_in_schema=False)
def custom_swagger():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Traffic Monitoring API - Swagger",
        swagger_js_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css",
    )


def get_db():
    client = MongoClient(MONGO_URI, tz_aware=True)
    return client[DB_NAME]


def get_minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def clean(doc: dict) -> dict:
    doc.pop("_id", None)
    return doc


# ── Cameras ──────────────────────────────────────────────────────────────────

@app.get("/cameras", summary="List cameras", tags=["Cameras"])
def list_cameras(
    region:   Optional[str] = Query(None, description="Filter by region"),
    county:   Optional[str] = Query(None, description="Filter by county"),
    state:    Optional[str] = Query(None, description="Filter by state"),
    roadway:  Optional[str] = Query(None, description="Partial match on roadway"),
    priority: Optional[bool] = Query(None, description="Filter priority cameras"),
    status:   Optional[str] = Query("active", description="active / inactive"),
    limit:    int = Query(20, ge=1, le=200),
    offset:   int = Query(0, ge=0),
):
    db = get_db()
    query = {}
    if region:   query["region"]   = region
    if county:   query["county"]   = county
    if state:    query["state"]    = state
    if roadway:  query["roadway"]  = {"$regex": roadway, "$options": "i"}
    if priority is not None: query["priority"] = priority
    if status:   query["status"]   = status

    total = db.cameras.count_documents(query)
    cameras = list(
        db.cameras.find(query, {"_id": 0, "images": 0})
        .skip(offset).limit(limit)
    )
    return {"total": total, "offset": offset, "limit": limit, "data": cameras}


@app.get("/cameras/{camera_id}", summary="Camera detail", tags=["Cameras"])
def get_camera(camera_id: str):
    db = get_db()
    doc = db.cameras.find_one({"camera_id": camera_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Camera not found")
    return doc


# ── Captures ─────────────────────────────────────────────────────────────────

@app.get("/cameras/{camera_id}/captures", summary="Image captures", tags=["Captures"])
def list_captures(
    camera_id: str,
    start: Optional[datetime] = Query(None, description="Start time (ISO 8601)"),
    end:   Optional[datetime] = Query(None, description="End time (ISO 8601)"),
    success_only: bool = Query(True),
    limit:  int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    db = get_db()
    query: dict = {"camera_id": camera_id}
    if success_only:
        query["success"] = True
    ts_filter = {}
    if start: ts_filter["$gte"] = start
    if end:   ts_filter["$lte"] = end
    if ts_filter:
        query["capture_ts"] = ts_filter

    total = db.raw_captures.count_documents(query)
    docs = list(
        db.raw_captures.find(query, {"_id": 0})
        .sort("capture_ts", -1).skip(offset).limit(limit)
    )
    return {"total": total, "offset": offset, "limit": limit, "data": docs}


# ── Quality Summary ───────────────────────────────────────────────────────────

@app.get("/cameras/{camera_id}/summary", summary="Hourly quality summary", tags=["Quality"])
def get_summary(
    camera_id: str,
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit:  int = Query(24, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    db = get_db()
    query: dict = {"camera_id": camera_id}
    if date:
        query["date"] = date

    total = db.camera_hourly_summary.count_documents(query)
    docs = list(
        db.camera_hourly_summary.find(query, {"_id": 0})
        .sort([("date", -1), ("hour", -1)]).skip(offset).limit(limit)
    )
    return {"total": total, "offset": offset, "limit": limit, "data": docs}


# ── CV Features ───────────────────────────────────────────────────────────────

@app.get("/cameras/{camera_id}/features", summary="CV derived features", tags=["Features"])
def get_features(
    camera_id: str,
    start: Optional[datetime] = Query(None),
    end:   Optional[datetime] = Query(None),
    model_id: Optional[str] = Query(None),
    limit:  int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    db = get_db()
    query: dict = {"camera_id": camera_id, "success": True}
    if model_id:
        query["model_id"] = model_id
    ts_filter = {}
    if start: ts_filter["$gte"] = start
    if end:   ts_filter["$lte"] = end
    if ts_filter:
        query["capture_ts"] = ts_filter

    total = db.derived_features.count_documents(query)
    docs = list(
        db.derived_features.find(query, {"_id": 0})
        .sort("capture_ts", -1).skip(offset).limit(limit)
    )
    return {"total": total, "offset": offset, "limit": limit, "data": docs}


# ── Image Proxy ───────────────────────────────────────────────────────────────

@app.get("/images/{object_key:path}", summary="Fetch raw image from MinIO", tags=["Images"])
def get_image(object_key: str):
    minio = get_minio()
    try:
        response = minio.get_object(MINIO_BUCKET, object_key)
        return StreamingResponse(response, media_type="image/jpeg")
    except Exception:
        raise HTTPException(status_code=404, detail="Image not found")
