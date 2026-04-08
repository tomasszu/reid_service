import time

class VehicleEvent:
    def __init__(self, camera_id, track_id):
        self.camera_id = camera_id
        self.track_id = track_id

        self.embeddings = []
        self.object_keys = []  # references to MinIO sightings

        self.start_time_ns = None
        self.last_seen_ns = None

    def add_sighting(self, sighting, object_key):
        if self.start_time_ns is None:
            self.start_time_ns = sighting.timestamp_ns

        if self.last_seen_ns is None:
            self.last_seen_ns = sighting.timestamp_ns
        else:
            self.last_seen_ns = max(self.last_seen_ns, sighting.timestamp_ns)

        self.embeddings.append(sighting.embedding)
        self.object_keys.append(object_key)

    def is_empty(self):
        return len(self.embeddings) == 0