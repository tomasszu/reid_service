import time

class VehicleEvent:
    def __init__(self, camera_id, track_id):
        self.camera_id = camera_id
        self.track_id = track_id

        self.embeddings = []
        self.object_keys = []  # references to MinIO sightings

        # business timestamp tracking (from sightings) - not sure if needed anymore
        self.start_time_ns = None
        self.last_seen_ns = None

        # system runtime timestamp tracking (for expiration)
        self.last_seen_runtime_ns = None

    def add_sighting(self, sighting, object_key):
        now_runtime_ns = time.time_ns()

        if self.start_time_ns is None:
            self.start_time_ns = sighting.timestamp_ns

        # business timestamp still tracked if needed
        self.last_seen_ns = sighting.timestamp_ns

        # system runtime timeout tracking
        self.last_seen_runtime_ns = now_runtime_ns

        self.embeddings.append(sighting.embedding)
        self.object_keys.append(object_key)

    def is_empty(self):
        return len(self.embeddings) == 0