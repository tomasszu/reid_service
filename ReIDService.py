import time
from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting
from reid_helpers.TrackCache import TrackCache

from utils import generate_object_key
import uuid

class ReIDService:
    def __init__(self, receiver: BaseSightingReceiver, database, datalake):
        self.receiver = receiver
        self.database = database
        self.datalake = datalake
        self.total_processed = 0

        # Threshold for cosine similarity (future improvement)
        self.threshold = 0.75

        # Track cache: prevents repeated DB queries for same (cam, track)
        self.track_cache = TrackCache(max_size=2000, ttl_sec=120)

    def _generate_vehicle_id(self):
        return str(uuid.uuid4())

    def _match_vehicle(self, sighting):
        """
        Match a vehicle embedding against DB or cache.

        Returns:
            vehicle_id (str), is_new (bool)
        """
        # --- first check track cache ---
        cached_vid = self.track_cache.get(sighting.camera_id, sighting.track_id)
        if cached_vid:
            return cached_vid, False

        # --- query DB if not in cache ---
        try:
            results = self.database.query(sighting.embedding.tolist(), k=1)
        except Exception as e:
            print(f"[ReID] query failed: {e}")
            vid = self._generate_vehicle_id()
            self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
            return vid, True

        if not results:
            vid = self._generate_vehicle_id()
            self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
            return vid, True

        best = results[0]
        score = best["score"]

        if score >= self.threshold:
            vid = best["vehicle_id"]
            is_new = False
            print(f"\n REID'd from {best['track_id']}")
        else:
            vid = self._generate_vehicle_id()
            is_new = True

        # --- store in cache for this track ---
        self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
        return vid, is_new

    def process(self, sighting):
        object_key = generate_object_key(sighting.timestamp)

        # --- match vehicle (cache + DB) ---
        vehicle_id, is_new = self._match_vehicle(sighting)
        sighting.vehicle_id = vehicle_id

        # --- insert into vector DB ---
        self.database.insert(
            object_key=object_key,
            vehicle_id=vehicle_id,
            camera_id=sighting.camera_id,
            track_id=sighting.track_id,
            vector=sighting.embedding.tolist()
        )

        # --- store in datalake / MinIO ---
        self.datalake.upload_sighting(sighting, object_key)

        print(
            f"[ReID] New: {is_new} vid={vehicle_id} "
            f"cam={sighting.camera_id} "
            f"track={sighting.track_id}"
        )

    def run(self):
        print("[ReID] Service started")

        while True:
            batch = self.receiver.poll()
            if not batch:
                time.sleep(0.01)
                continue

            for sighting in batch:
                self.process(sighting)
                self.total_processed += 1

            if self.total_processed % 50 == 0:
                print(f"[ReID] Processed {self.total_processed}")