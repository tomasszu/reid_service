import os
from opensearchpy import OpenSearch
from minio import Minio
import sys


def test_opensearch():
    print("[TEST] OpenSearch...")

    client = OpenSearch(
        hosts=[{"host": os.getenv("OS_HOST"), "port": int(os.getenv("OS_PORT", 9200))}],
        http_auth=(os.getenv("OS_USER"), os.getenv("OS_PASSWORD")),
        use_ssl=os.getenv("OS_USE_SSL", "true").lower() == "true",
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )

    info = client.info()
    print("[OK] OpenSearch:", info["cluster_name"])


def test_minio():
    print("[TEST] MinIO...")

    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )

    buckets = client.list_buckets()
    print("[OK] MinIO buckets:", [b.name for b in buckets])


if __name__ == "__main__":
    try:
        test_opensearch()
        test_minio()
    except Exception as e:
        print("[FAIL]", e)
        sys.exit(1)