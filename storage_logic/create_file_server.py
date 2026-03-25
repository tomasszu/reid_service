
from storage_logic.FileServer import StorageBackend
from storage_logic.MinioLogic import MinioBackend
from credentials_config import Config

def create_storage(config: Config) -> StorageBackend:
    return MinioBackend(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        bucket=config.minio_bucket,
        secure=config.minio_secure,
    )