import json
import re
import time
from datetime import datetime, UTC
from pathlib import Path
import requests
from pymongo import MongoClient
import os

# FIPS county lookup: code -> (state, county)
FIPS_LOOKUP = {
    # New York
    "36001":("New York","Albany"),"36003":("New York","Allegany"),"36005":("New York","Bronx"),
    "36007":("New York","Broome"),"36009":("New York","Cattaraugus"),"36011":("New York","Cayuga"),
    "36013":("New York","Chautauqua"),"36015":("New York","Chemung"),"36017":("New York","Chenango"),
    "36019":("New York","Clinton"),"36021":("New York","Columbia"),"36023":("New York","Cortland"),
    "36025":("New York","Delaware"),"36027":("New York","Dutchess"),"36029":("New York","Erie"),
    "36031":("New York","Essex"),"36033":("New York","Franklin"),"36035":("New York","Fulton"),
    "36037":("New York","Genesee"),"36039":("New York","Greene"),"36041":("New York","Hamilton"),
    "36043":("New York","Herkimer"),"36045":("New York","Jefferson"),"36047":("New York","Kings"),
    "36049":("New York","Lewis"),"36051":("New York","Livingston"),"36053":("New York","Madison"),
    "36055":("New York","Monroe"),"36057":("New York","Montgomery"),"36059":("New York","Nassau"),
    "36061":("New York","New York"),"36063":("New York","Niagara"),"36065":("New York","Oneida"),
    "36067":("New York","Onondaga"),"36069":("New York","Ontario"),"36071":("New York","Orange"),
    "36073":("New York","Orleans"),"36075":("New York","Oswego"),"36077":("New York","Otsego"),
    "36079":("New York","Putnam"),"36081":("New York","Queens"),"36083":("New York","Rensselaer"),
    "36085":("New York","Richmond"),"36087":("New York","Rockland"),"36089":("New York","St. Lawrence"),
    "36091":("New York","Saratoga"),"36093":("New York","Schenectady"),"36095":("New York","Schoharie"),
    "36097":("New York","Schuyler"),"36099":("New York","Seneca"),"36101":("New York","Steuben"),
    "36103":("New York","Suffolk"),"36105":("New York","Sullivan"),"36107":("New York","Tioga"),
    "36109":("New York","Tompkins"),"36111":("New York","Ulster"),"36113":("New York","Warren"),
    "36115":("New York","Washington"),"36117":("New York","Wayne"),"36119":("New York","Westchester"),
    "36121":("New York","Wyoming"),"36123":("New York","Yates"),
    # Connecticut
    "09001":("Connecticut","Fairfield"),"09003":("Connecticut","Hartford"),
    "09005":("Connecticut","Litchfield"),"09007":("Connecticut","Middlesex"),
    "09009":("Connecticut","New Haven"),"09011":("Connecticut","New London"),
    "09013":("Connecticut","Tolland"),"09015":("Connecticut","Windham"),
    # Massachusetts
    "25001":("Massachusetts","Barnstable"),"25003":("Massachusetts","Berkshire"),
    "25005":("Massachusetts","Bristol"),"25009":("Massachusetts","Essex"),
    "25011":("Massachusetts","Franklin"),"25013":("Massachusetts","Hampden"),
    "25015":("Massachusetts","Hampshire"),"25017":("Massachusetts","Middlesex"),
    "25021":("Massachusetts","Norfolk"),"25023":("Massachusetts","Plymouth"),
    "25025":("Massachusetts","Suffolk"),"25027":("Massachusetts","Worcester"),
    # New Jersey
    "34003":("New Jersey","Bergen"),"34013":("New Jersey","Essex"),
    "34017":("New Jersey","Hudson"),"34021":("New Jersey","Mercer"),
    "34023":("New Jersey","Middlesex"),"34025":("New Jersey","Monmouth"),
    "34027":("New Jersey","Morris"),"34031":("New Jersey","Passaic"),
    "34035":("New Jersey","Somerset"),"34039":("New Jersey","Union"),
    # Pennsylvania
    "42011":("Pennsylvania","Berks"),"42017":("Pennsylvania","Bucks"),
    "42029":("Pennsylvania","Chester"),"42045":("Pennsylvania","Delaware"),
    "42069":("Pennsylvania","Lackawanna"),"42071":("Pennsylvania","Lancaster"),
    "42077":("Pennsylvania","Lehigh"),"42079":("Pennsylvania","Luzerne"),
    "42091":("Pennsylvania","Montgomery"),"42095":("Pennsylvania","Northampton"),
    "42101":("Pennsylvania","Philadelphia"),"42103":("Pennsylvania","Pike"),
    "42115":("Pennsylvania","Susquehanna"),"42127":("Pennsylvania","Wayne"),
    # Vermont
    "50007":("Vermont","Chittenden"),"50021":("Vermont","Rutland"),
    "50025":("Vermont","Windham"),"50027":("Vermont","Windsor"),
}


def fips_lookup(area_id: str):
    """Return (state, county) from 5-digit FIPS code, or (None, None)."""
    if not area_id:
        return None, None
    return FIPS_LOOKUP.get(str(area_id).zfill(5), (None, None))


def reverse_geocode(lat: float, lon: float) -> dict:
    """Call Nominatim to get state/county/country from coordinates."""
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "json"},
            headers={"User-Agent": "511NY-Traffic-Pipeline/1.0"},
            timeout=10,
        )
        if resp.status_code == 200:
            addr = resp.json().get("address", {})
            county = addr.get("county") or addr.get("city_district") or ""
            county = county.removesuffix(" County").removesuffix(" Parish")
            return {
                "state": addr.get("state"),
                "county": county or None,
                "country": addr.get("country"),
            }
    except Exception:
        pass
    return {}


def enrich_null_geo_cameras(db):
    """Reverse geocode cameras still missing state/county, cache result in MongoDB.

    Cameras with (0, 0) coordinates have invalid source data — skip geocoding
    and flag them for manual review instead of guessing the state.
    """
    null_cameras = list(db.cameras.find(
        {"state": None},
        {"camera_id": 1, "latitude": 1, "longitude": 1},
    ))
    if not null_cameras:
        print("No cameras need geocoding.")
        return

    geocode_needed = []
    for cam in null_cameras:
        lat = cam.get("latitude") or 0
        lon = cam.get("longitude") or 0
        if lat == 0 and lon == 0:
            db.cameras.update_one(
                {"camera_id": cam["camera_id"]},
                {"$set": {"geo_quality": "invalid_coordinates"}},
            )
            print(f"  [FLAG] {cam['camera_id']} -> invalid coords (0,0), flagged for manual review")
        else:
            geocode_needed.append(cam)

    if not geocode_needed:
        return

    print(f"Geocoding {len(geocode_needed)} cameras...")
    for i, cam in enumerate(geocode_needed, 1):
        geo = reverse_geocode(cam["latitude"], cam["longitude"])
        if geo.get("state"):
            db.cameras.update_one(
                {"camera_id": cam["camera_id"]},
                {"$set": {**geo, "geo_quality": "geocoded"}},
            )
            print(f"  [{i}/{len(geocode_needed)}] {cam['camera_id']} -> {geo['state']}, {geo.get('county')}")
        else:
            db.cameras.update_one(
                {"camera_id": cam["camera_id"]},
                {"$set": {"geo_quality": "geocode_failed"}},
            )
            print(f"  [{i}/{len(geocode_needed)}] {cam['camera_id']} -> geocode failed, flagged for manual review")
        time.sleep(1.0)  # Nominatim rate limit: 1 req/sec

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
DB_NAME = "traffic_monitoring"

BASE_DIR = Path(__file__).resolve().parent.parent
PRIORITY_CONFIG_PATH = BASE_DIR / "config" / "priority_cameras.json"

BASE_URL = "https://511ny.org/List/GetData/Cameras"
PAGE_SIZE = 100
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

QUERY_TEMPLATE = {
    "columns": [
        {"data": None, "name": ""},
        {"name": "sortOrder", "s": True},
        {"name": "state", "s": True},
        {"name": "region", "s": True},
        {"name": "county", "s": True},
        {"name": "roadway", "s": True},
        {"data": 6, "name": ""},
    ],
    "order": [{"column": 1, "dir": "asc"}, {"column": 4, "dir": "asc"}],
    "search": {"value": ""},
}


def load_priority_config():
    with open(PRIORITY_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return set(cfg.get("camera_ids", [])), set(cfg.get("roadways", []))


def init_mongo():
    client = MongoClient(MONGO_URI, tz_aware=True)
    db = client[DB_NAME]
    db.cameras.create_index("camera_id", unique=True)
    return db


def fetch_page(start: int) -> dict:
    query = {**QUERY_TEMPLATE, "start": start, "length": PAGE_SIZE}
    resp = requests.get(
        BASE_URL,
        params={"query": json.dumps(query), "lang": "en"},
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def parse_coords(wkt: str):
    m = re.search(r"POINT \(([^ ]+) ([^)]+)\)", wkt or "")
    if m:
        return float(m.group(2)), float(m.group(1))  # lat, lon
    return None, None


def normalize_camera(raw: dict, priority_ids: set, priority_roadways: set) -> dict:
    images = raw.get("images") or []
    first_img = next((img for img in images if not img.get("disabled") and not img.get("blocked")), None)

    lat, lon = parse_coords((raw.get("latLng") or {}).get("geography", {}).get("wellKnownText"))

    image_url = None
    view_id = None
    view_description = None
    if first_img:
        rel_url = first_img.get("imageUrl", "")
        image_url = f"https://511ny.org{rel_url}" if rel_url else None
        view_id = first_img.get("id")
        view_description = first_img.get("description")

    status = "active" if first_img else "inactive"
    camera_id = f"NY511_{raw['id']}"
    roadway = raw.get("roadway") or ""
    priority = camera_id in priority_ids or any(r in roadway for r in priority_roadways)

    area_id = raw.get("areaId")
    state = raw.get("state")
    county = raw.get("county")
    region = raw.get("region")

    # fill state/county from FIPS when 511ny doesn't provide them
    # geo_quality=None means "no new geo info from this sync" → preserve existing DB value
    geo_quality = None
    if state or county:
        geo_quality = "source"
    elif area_id:
        fips_state, fips_county = fips_lookup(area_id)
        if fips_state or fips_county:
            state = state or fips_state
            county = county or fips_county
            geo_quality = "fips"

    return {
        "camera_id": camera_id,
        "source": raw.get("source", "511NY"),
        "source_camera_id": str(raw.get("id")),
        "source_id": raw.get("sourceId"),
        "area_id": area_id,
        "roadway": roadway or None,
        "direction": raw.get("direction"),
        "latitude": lat,
        "longitude": lon,
        "location": raw.get("location"),
        "state": state,
        "region": region,
        "county": county,
        "country": raw.get("country"),
        "geo_quality": geo_quality,
        "status": status,
        "priority": priority,
        "image_url": image_url,
        "view_id": view_id,
        "view_description": view_description,
        "images": images,
        "last_catalog_refresh_ts": datetime.now(UTC),
    }


def main():
    db = init_mongo()

    first_page = fetch_page(0)
    total = first_page["recordsTotal"]
    print(f"Total cameras: {total}")

    all_cameras = first_page["data"]
    start = PAGE_SIZE
    while start < total:
        page = fetch_page(start)
        all_cameras.extend(page["data"])
        start += PAGE_SIZE
        time.sleep(0.5)

    GEO_FIELDS = {"state", "county", "region", "country", "geo_quality", "area_id"}

    priority_ids, priority_roadways = load_priority_config()
    count = 0
    for raw in all_cameras:
        doc = normalize_camera(raw, priority_ids, priority_roadways)

        new_status = doc["status"]
        # always overwrite operational fields except status (handled below)
        always_set = {k: v for k, v in doc.items() if k not in GEO_FIELDS and k != "status"}
        # only overwrite geo fields when source provides a non-null value
        geo_set = {k: v for k, v in doc.items() if k in GEO_FIELDS and v is not None}

        if new_status == "inactive":
            # Camera removed from 511ny — always force inactive
            db.cameras.update_one(
                {"camera_id": doc["camera_id"]},
                {"$set": {**always_set, **geo_set, "status": "inactive"}},
                upsert=True,
            )
        else:
            # Camera has an image_url — preserve no_feed if already detected by ingest
            db.cameras.update_one(
                {"camera_id": doc["camera_id"]},
                [{"$set": {
                    **always_set,
                    **geo_set,
                    "status": {"$cond": {
                        "if": {"$eq": ["$status", "no_feed"]},
                        "then": "no_feed",
                        "else": "active",
                    }},
                }}],
                upsert=True,
            )
        count += 1

    print(f"Synced {count} cameras into MongoDB.")

    enrich_null_geo_cameras(db)

    filled = db.cameras.count_documents({"state": {"$ne": None}})
    null_remaining = db.cameras.count_documents({"state": None})
    print(f"State filled: {filled} | Still null: {null_remaining}")


if __name__ == "__main__":
    main()
