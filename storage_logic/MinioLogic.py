from typing import Optional

from minio import Minio
from minio.error import S3Error

from io import BytesIO


class MinioBackend:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
    ):
        self.bucket = bucket
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def list_objects(self, prefix: str = "", recursive: bool = True, max_keys: Optional[int] = None):
        """
        Returns a generator of object names. Max returnable objects can be limited with max_keys.
        """
        count = 0
        for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
            yield obj.object_name
            count += 1
            if max_keys is not None and count >= max_keys:
                break

    def get_object(self, key: str) -> bytes:
        response = self.client.get_object(self.bucket, key)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    
    def put_object(self, key: str, data: bytes, content_type="application/octet-stream") -> None:
        """
        Upload or overwrite object at given key.
        """
        data_stream = BytesIO(data)
        data_length = len(data)

        self.client.put_object(
            bucket_name=self.bucket,
            object_name=key,
            data=data_stream,
            length=data_length,
            content_type=content_type,
        )

    def bucket_exists(self) -> bool:
        return self.client.bucket_exists(self.bucket)