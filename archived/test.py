from minio import Minio

client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

client.fget_object(
    "traffic-raw",
    "camera_id=CAM001/date=2026-03-24/hour=14/CAM001_20260324T140000.jpg",
    "downloaded.jpg"
)

print("done")