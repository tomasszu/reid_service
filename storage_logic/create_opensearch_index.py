from opensearchpy import OpenSearch
import os

def create_index(index_name="reid_features"):
    client = OpenSearch(
        hosts=[{"host": os.getenv("OS_HOST", "localhost"), "port": int(os.getenv("OS_PORT", 9200))}],
        http_auth=(os.getenv("OS_USER", "admin"), os.getenv("OS_PASSWORD", "admin")),
        use_ssl=True,
        verify_certs=False,
    )

    index_body = {
        "settings": {"index.knn": True},
        "mappings": {
            "properties": {
                "object_key": {"type": "keyword"},
                "camera_id": {"type": "keyword"},
                "track_id": {"type": "integer"},
                "feature_vector": {"type": "knn_vector", "dimension": 256, "space_type": "cosinesimil"},
                "bbox": {"type": "integer"},
                "vehicle_id": {"type": "keyword"},
                "timestamp_ms": {"type": "date", "format": "epoch_millis"},
            }
        },
    }

    if not client.indices.exists(index_name):
        client.indices.create(index=index_name, body=index_body)
        print(f"[OK] Index '{index_name}' created")
    else:
        print(f"[INFO] Index '{index_name}' already exists")

if __name__ == "__main__":
    create_index()