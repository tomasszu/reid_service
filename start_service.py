#!/usr/bin/env python3
import os
import signal
import sys
import time

import numpy as np
from opensearchpy import OpenSearch
from minio import Minio

# ---------------- CONFIG ----------------
from credentials_config import config
from ReIDService import ReIDService
from inputs_logic.JSONLFileReceiver import JSONLFileReceiver
from storage_logic.VectorDatabase import Database
from storage_logic.create_file_server import create_storage
from storage_logic.DatalakeUploader import MinioReIDUploader


INDEX_NAME = os.getenv("OS_INDEX", config.os_index)


# ---------------- CONNECTIONS ----------------
def check_opensearch():
    print("[TEST] OpenSearch...")
    client = OpenSearch(
        hosts=[{"host": config.os_host, "port": config.os_port}],
        http_auth=(config.os_user, config.os_password),
        use_ssl=config.os_use_ssl,
        verify_certs=False,
    )
    info = client.info()
    print("[OK] OpenSearch:", info["cluster_name"])
    return client


def check_minio():
    print("[TEST] MinIO...")
    client = Minio(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        secure=config.minio_secure,
    )
    buckets = [b.name for b in client.list_buckets()]
    print("[OK] MinIO buckets:", buckets)
    return client


# ---------------- INDEX MANAGEMENT ----------------
def create_index(client: OpenSearch):
    index_body = {
        "settings": {"index.knn": True},
        "mappings": {
            "properties": {
                "object_key": {"type": "keyword"},
                "camera_id": {"type": "keyword"},
                "track_id": {"type": "integer"},
                "feature_vector": {
                    "type": "knn_vector",
                    "dimension": 256
                },
                "bbox": {"type": "integer"},
                "vehicle_id": {"type": "keyword"},
                "timestamp_ms": {"type": "date", "format": "epoch_millis"},
            }
        },
    }
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(index=INDEX_NAME, body=index_body)
        print(f"[OK] Index '{INDEX_NAME}' created")
    else:
        print(f"[INFO] Index '{INDEX_NAME}' already exists")


def delete_index(client: OpenSearch):
    if client.indices.exists(index=INDEX_NAME):
        client.indices.delete(index=INDEX_NAME)
        print(f"[OK] Index '{INDEX_NAME}' deleted")
    else:
        print(f"[INFO] Index '{INDEX_NAME}' does not exist")


# ---------------- SIGNAL HANDLER ----------------
def handle_exit(sig, frame):
    print("[INFO] Container shutting down, deleting OpenSearch index...")
    try:
        delete_index(os_client)
    except Exception as e:
        print("[WARN] Failed to delete index:", e)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)


# ---------------- MAIN SERVICE ----------------
def run_service():
    receiver = JSONLFileReceiver(
        path="test/MinIO_toJSONL/2026/02/12.jsonl",
        mps=4
    )

    db = Database(config)
    datalake_storage = create_storage(config)
    datalake_uploader = MinioReIDUploader(
        storage=datalake_storage,
        model_name="sp4_ep6_ft_noCEL_070126_26ep.engine"
    )

    service = ReIDService(receiver, db, datalake_uploader)
    service.run()


# ---------------- RUN ----------------
if __name__ == "__main__":
    # 1 Test connections
    os_client = check_opensearch()
    minio_client = check_minio()

    # 2 Create index
    create_index(os_client)

    # 3 Start main service
    print("[INFO] Starting ReIDService...")
    run_service()

    # 4 Keep running until container shutdown
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        handle_exit(None, None)