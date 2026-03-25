import os
from dotenv import load_dotenv

load_dotenv()  # does nothing if no .env file exists

class Config:
    def __init__(self):
        # --- MinIO ---
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        self.minio_bucket = os.getenv("MINIO_BUCKET")
        self.minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

        # --- OpenSearch ---
        self.os_host = os.getenv("OS_HOST", "opensearch-node1")
        self.os_port = int(os.getenv("OS_PORT", 9200))
        self.os_index = os.getenv("OS_INDEX", "reid_features")
        self.os_user = os.getenv("OS_USER")
        self.os_password = os.getenv("OS_PASSWORD")
        self.os_use_ssl = os.getenv("OS_USE_SSL", "true").lower() == "true"

config = Config()