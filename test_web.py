"""
Quick smoke test — no Docker needed.
Run: python test_web.py
"""
import json
import time
import requests

BASE_URL = "https://511ny.org/List/GetData/Cameras"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

def test_camera_list():
    print("=== Test 1: Camera list fetch ===")
    query = {
        "columns": [{"data": None, "name": ""}],
        "order": [{"column": 0, "dir": "asc"}],
        "start": 0, "length": 5,
        "search": {"value": ""},
    }
    resp = requests.get(BASE_URL, params={"query": json.dumps(query), "lang": "en"},
                        headers=HEADERS, timeout=15)
    assert resp.status_code == 200, f"HTTP {resp.status_code}"
    data = resp.json()
    total = data["recordsTotal"]
    cameras = data["data"]
    print(f"  Total cameras: {total}")
    print(f"  First camera: id={cameras[0]['id']} roadway={cameras[0]['roadway']}")
    print(f"  Image URL: https://511ny.org{cameras[0]['images'][0]['imageUrl']}")
    print("  PASS\n")
    return cameras

def test_image_fetch(cameras):
    print("=== Test 2: Image fetch ===")
    cam = cameras[0]
    img_path = cam["images"][0]["imageUrl"]
    url = f"https://511ny.org{img_path}?t={int(time.time())}"
    print(f"  Fetching: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=15)
    print(f"  HTTP status: {resp.status_code}")
    print(f"  Content-Type: {resp.headers.get('Content-Type')}")
    print(f"  Size: {len(resp.content)} bytes")
    assert resp.status_code == 200, f"HTTP {resp.status_code}"
    assert "image" in resp.headers.get("Content-Type", ""), "Not an image response"
    with open("sample_download.jpg", "wb") as f:
        f.write(resp.content)
    print("  Saved to sample_download.jpg")
    print("  PASS\n")

def test_priority_config():
    print("=== Test 3: Priority config parsing ===")
    import json, re
    from pathlib import Path
    cfg_path = Path(__file__).parent / "config" / "priority_cameras.json"
    with open(cfg_path) as f:
        cfg = json.load(f)
    priority_roadways = set(cfg.get("roadways", []))
    print(f"  Priority roadways: {priority_roadways}")
    test_roadway = "I-87 - NYS Thruway"
    matched = any(r in test_roadway for r in priority_roadways)
    assert matched, f"{test_roadway} should be priority"
    print(f"  '{test_roadway}' correctly marked as priority")
    print("  PASS\n")

if __name__ == "__main__":
    cameras = test_camera_list()
    test_image_fetch(cameras)
    test_priority_config()
    print("All tests passed.")
