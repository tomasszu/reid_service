import time

class VehicleEvent:
    def __init__(self, camera_id, track_id):
        self.camera_id = camera_id
        self.track_id = track_id

        self.embeddings = []
        self.object_keys = []  # references to MinIO sightings

        self.start_time = None
        self.last_seen = None

    def add_sighting(self, sighting, object_key):
        if self.start_time is None:
            self.start_time = sighting.timestamp

        self.last_seen = time.time()

        self.embeddings.append(sighting.embedding)
        self.object_keys.append(object_key)

    def is_empty(self):
        return len(self.embeddings) == 0