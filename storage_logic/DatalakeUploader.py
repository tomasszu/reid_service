import json
import time
import numpy as np
import io
from datetime import datetime, timezone
from uuid6 import uuid7
from PIL import Image
from misc.daytime_check import DaylightFilter

# -------- IMAGE --------
def encode_image(image_np):
    # if your pipeline is still BGR (from old cv2 usage), convert:
    # image_np = image_np[:, :, ::-1]  # BGR → RGB

    image = Image.fromarray(image_np)

    buffer = io.BytesIO()
    image.save(buffer, format="PNG")

    img_bytes = buffer.getvalue()
    return img_bytes

class MinioReIDUploader:
    def __init__(self, storage, model_name: str):
        self.storage = storage
        self.model_name = model_name

        #initialize daylight filter (assumes all cameras in same city)
        self.daylight_filter = DaylightFilter(
            latitude=56.98,
            longitude=24.19,
            timezone="Europe/Riga"
        )

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

        # -------- DAYTIME CHECK --------
        daytime = self.daylight_filter.is_daytime(ts_ns)

        # -------- IMAGE --------
        # WARNING!! TE ARI AIZVIETOJU CV2 AR PIL !!!
        img_bytes = encode_image(sighting.image)
        img_path = f"images/{base}.png"

        self.storage.put_object(img_path, img_bytes)

        # -------- EMBEDDING --------
        emb_buf = io.BytesIO()
        np.save(emb_buf, sighting.embedding)
        emb_bytes = emb_buf.getvalue()

        emb_path = f"sightings_embeddings/{self.model_name}/{base}.npy"
        self.storage.put_object(emb_path, emb_bytes)

        # -------- METADATA --------
        metadata = {
            "sighting_id": base.split("/")[-1],  # uuid
            "timestamp_utc": ts_iso,
            "timestamp_ns": ts_ns,
            "camera_id": sighting.camera_id,
            "track_id": sighting.track_id,
            "vehicle_id": None,
            "bbox": sighting.bbox,
            "daytime": daytime,
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
    
    def upload_vehicle_event(
        self,
        vehicle_id: str,
        reid_score: float,
        object_key: str,
        camera_id: str,
        track_id: int,
        representative_key: str,
        sighting_keys: list,
        centroid: np.ndarray = None
    ):
        """
        Upload aggregated vehicle event.

        Stores:
        - metadata JSON
        - optional centroid embedding
        """
        ts_iso = datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds")

        base = object_key  # YYYY/MM/DD/uuid

        # -------- LOAD SIGHTINGS METADATA --------
        timestamps_ns = []

        for key in sighting_keys:
            meta_path = f"sightings/{key}.json"  # PREFIX NEEDED

            try:
                meta_bytes = self.storage.get_object(meta_path)
                meta = json.loads(meta_bytes)

                timestamps_ns.append(meta["timestamp_ns"])
            except Exception as e:
                print(f"[VehicleEvent] Failed to load {meta_path}: {e}")

        if not timestamps_ns:
            raise RuntimeError("No valid sightings found for event")

        # -------- COMPUTE TIME RANGE --------
        start_ns = min(timestamps_ns)
        end_ns = max(timestamps_ns)

        start_iso = datetime.fromtimestamp(start_ns / 1e9, tz=timezone.utc).isoformat(timespec="milliseconds")
        end_iso = datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc).isoformat(timespec="milliseconds")

        # -------- DAYTIME (use midpoint) --------
        mid_ns = (start_ns + end_ns) // 2
        daytime = self.daylight_filter.is_daytime(mid_ns)

        # -------- CENTROID EMBEDDING --------
        emb_path = None

        if centroid is not None:
            emb_buf = io.BytesIO()
            np.save(emb_buf, centroid)
            emb_bytes = emb_buf.getvalue()

            emb_path = f"event_embeddings/{self.model_name}/{base}.npy"
            self.storage.put_object(emb_path, emb_bytes)


        # -------- METADATA --------
        metadata = {
            "vehicle_event_id": base.split("/")[-1],
            "vehicle_id": vehicle_id,
            "reid_score": reid_score,

            "start_timestamp_utc": start_iso,
            "end_timestamp_utc": end_iso,
            "start_timestamp_ns": start_ns,
            "end_timestamp_ns": end_ns,
            "daytime": daytime,

            "camera_id": camera_id,
            "track_id": track_id,

            "representative": {
                "sighting_key": representative_key,
                "image_path": f"images/{representative_key}.png"
            },

            "sightings": sighting_keys,
            "num_sightings": len(sighting_keys)

        }

        if centroid is not None:
            metadata["embedding"] = {
                self.model_name: {
                    "path": emb_path,
                    "dim": int(len(centroid)),
                    "normalized": True,
                }
            }

        meta_bytes = json.dumps(metadata).encode("utf-8")
        meta_path = f"vehicle_events/{base}.json"

        self.storage.put_object(meta_path, meta_bytes)

        return meta_path