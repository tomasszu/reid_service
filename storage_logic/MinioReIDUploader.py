import json
import time
import numpy as np
import io
from datetime import datetime, timezone
from uuid6 import uuid7
import cv2




class MinioReIDUploader:
    def __init__(self, storage, model_name: str):
        self.storage = storage
        self.model_name = model_name

        assert self.storage.bucket_exists(), "MinIO bucket does not exist"

    def upload_sighting(self, sighting, object_key: str):
        """
        Uploads a single sighting:
        - image (PNG)
        - embedding (.npy)
        - metadata (JSON)
        """

        ts_ns = sighting.timestamp
        ts_iso = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).isoformat(timespec="milliseconds")

        base = object_key  # already in YYYY/MM/DD/uuid format

        # -------- IMAGE --------
        success, buffer = cv2.imencode(".png", sighting.image)
        if not success:
            raise RuntimeError("Failed to encode image")

        img_bytes = buffer.tobytes()
        img_path = f"images/{base}.png"

        self.storage.put_object(img_path, img_bytes)

        # -------- EMBEDDING --------
        emb_buf = io.BytesIO()
        np.save(emb_buf, sighting.embedding)
        emb_bytes = emb_buf.getvalue()

        emb_path = f"embeddings/{self.model_name}/{base}.npy"
        self.storage.put_object(emb_path, emb_bytes)

        # -------- METADATA --------
        metadata = {
            "sighting_id": base.split("/")[-1],  # uuid
            "timestamp_utc": ts_iso,
            "timestamp_ns": ts_ns,
            "camera_id": sighting.camera_id,
            "track_id": sighting.track_id,
            "vehicle_id": sighting.vehicle_id,
            "image_path": img_path,
            "embeddings": {
                self.model_name: {
                    "path": emb_path,
                    "dim": int(sighting.embedding.shape[0]),
                    "normalized": True,
                }
            },
        }

        meta_bytes = json.dumps(metadata).encode("utf-8")
        meta_path = f"sightings/{base}.json"

        self.storage.put_object(meta_path, meta_bytes)

        return meta_path