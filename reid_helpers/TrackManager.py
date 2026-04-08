import threading
import time
import numpy as np
from reid_helpers.VehicleEvent import VehicleEvent


class TrackManager:

    def __init__(self, reid_service, timeout_ns: int):
        self.tracks = {}  # (camera_id, track_id) → VehicleEvent
        self.timeout_ns = timeout_ns
        self.reid_service = reid_service  # callback access
        self.lock = threading.Lock()

    def update(self, sighting, object_key):
        key = (sighting.camera_id, sighting.track_id)

        if key not in self.tracks:
            self.tracks[key] = VehicleEvent(
                sighting.camera_id,
                sighting.track_id
            )

        event = self.tracks[key]
        event.add_sighting(sighting, object_key)

    def finalize_expired(self):
        now_ns = time.time_ns()
        timeout_ns = self.timeout_ns

        to_finalize = []

        with self.lock:
            for key, event in self.tracks.items():
                if now_ns - event.last_seen_ns > timeout_ns:
                    to_finalize.append({
                        "camera_id": event.camera_id,
                        "track_id": event.track_id,
                        "embeddings": list(event.embeddings),
                        "object_keys": list(event.object_keys)
                    })

            for key in [k for k, e in self.tracks.items()
                        if now_ns - e.last_seen_ns > timeout_ns]:
                del self.tracks[key]

        return to_finalize