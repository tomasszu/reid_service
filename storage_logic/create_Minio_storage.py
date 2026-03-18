
from storage_logic.StorageBackend import StorageBackend
from storage_logic.MinioBackend import MinioBackend
import os

from dotenv import load_dotenv

# ---------------- ENV ----------------
load_dotenv()


def create_storage_from_env() -> StorageBackend:
    return MinioBackend(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        bucket=os.getenv("MINIO_BUCKET"),
        secure=False,
    )