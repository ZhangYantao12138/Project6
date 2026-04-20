from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent.parent
MOCK_SOURCE_DIR = BASE_DIR / "mock_source"

@app.get("/camera/{camera_id}")
def get_camera_image(camera_id: str, ts: str):
    """
    Example:
    /camera/CAM001?ts=20260324_1400
    """
    image_path = MOCK_SOURCE_DIR / camera_id / f"{ts}.jpg"

    if not image_path.exists():
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(image_path, media_type="image/jpeg")