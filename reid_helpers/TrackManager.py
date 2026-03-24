import time
import numpy as np
from reid_helpers.VehicleEvent import VehicleEvent


class TrackManager:

    def __init__(self, reid_service, timeout=10.0):
        self.tracks = {}  # (camera_id, track_id) → VehicleEvent
        self.timeout = timeout
        self.reid_service = reid_service  # callback access

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
        now = time.time()
        to_remove = []

        for key, event in self.tracks.items():
            if now - event.last_seen > self.timeout:
                self.reid_service.finalize_event(event)
                to_remove.append(key)

        for key in to_remove:
            del self.tracks[key]