import time
import numpy as np
from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting
from reid_helpers.TrackManager import TrackManager

from utils import generate_object_key
import uuid

class ReIDService:
    def __init__(self, receiver: BaseSightingReceiver, database, datalake):
        self.receiver = receiver
        self.database = database
        self.datalake = datalake
        self.total_processed = 0

        # Threshold for cosine similarity (future improvement)
        self.threshold = 0.675

        # Track cache: prevents repeated DB queries for same (cam, track)
        self.track_manager = TrackManager(self, timeout=10.0)

    def _generate_vehicle_id(self):
        return str(uuid.uuid4())
    
    def finalize_event(self, event):
        print(f"\n[ReID] Finalizing track {event.track_id} cam={event.camera_id}")

        embeddings = np.stack(event.embeddings)

        # --- normalize + centroid ---
        embs = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        centroid = embs.mean(axis=0)
        centroid /= np.linalg.norm(centroid)

        # --- match ---
        vehicle_id, is_new = self._match_vehicle_event(centroid, event.camera_id)

        # --- representative ---
        mid_idx = len(event.object_keys) // 2
        rep_key = event.object_keys[mid_idx]

        # --- insert into DB ---
        object_key = generate_object_key(int(time.time() * 1000))

        self.database.insert(
            object_key=object_key,
            vehicle_id=vehicle_id,
            camera_id=event.camera_id,
            track_id=event.track_id,
            vector=centroid.tolist()
        )

        # --- store event in MinIO ---
        self.datalake.upload_vehicle_event(
            vehicle_id=vehicle_id,
            object_key=object_key,
            representative_key=rep_key,
            sighting_keys=event.object_keys,
            centroid=centroid
        )

        print(
            f"[ReID] Finalized: vid={vehicle_id} "
            f"cam={event.camera_id} track={event.track_id} "
            f"sightings={len(event.object_keys)} new={is_new}"
        )

    def _match_vehicle_event(self, embedding, camera_id):
        try:
            results = self.database.query_cross_camera(
                embedding.tolist(),
                camera_id,
                k=3
            )
        except Exception as e:
            print(f"[ReID] query failed: {e}")
            return self._generate_vehicle_id(), True

        if not results:
            return self._generate_vehicle_id(), True

        best = results[0]
        score = best["score"]

        for i, r in enumerate(results):
            print(f"[ReID] {i}. score={r['score']:.4f} track={r['track_id']} from cam={r['camera_id']}")

        if score >= self.threshold:
            print(f"[ReID] REID from cam={best['camera_id']} track={best['track_id']}")
            return best["vehicle_id"], False
        else:
            return self._generate_vehicle_id(), True

    def _match_vehicle(self, sighting):
        # --- cache ---
        cached_vid = self.track_cache.get(sighting.camera_id, sighting.track_id)
        if cached_vid:
            return cached_vid, False

        # --- cross-camera query ---
        try:
            results = self.database.query_cross_camera(
                sighting.embedding.tolist(),
                sighting.camera_id,
                k=3
            )
        except Exception as e:
            print(f"[ReID] query failed: {e}")
            vid = self._generate_vehicle_id()
            self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
            return vid, True

        if not results:
            vid = self._generate_vehicle_id()
            self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
            print(f"[ReID] No results returned.")
            return vid, True            

        best = results[0]
        score = best["score"]

        for i, r in enumerate(results):
            print(f"[ReID] {i}. score={r['score']:.4f} track={r['track_id']} from cam={r['camera_id']}")

        if score >= self.threshold:
            vid = best["vehicle_id"]
            is_new = False
            print(f"[ReID] REID from cam={best['camera_id']} track={best['track_id']}")
        else:
            vid = self._generate_vehicle_id()
            is_new = True

        self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
        return vid, is_new

    def process(self, sighting):
        object_key = generate_object_key(sighting.timestamp)

        # --- save sighting immediately to datalake ---
        self.datalake.upload_sighting(sighting, object_key)

        # --- including the sighting in track aggregation dict ---
        self.track_manager.update(sighting, object_key)


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

            self.track_manager.finalize_expired()

            if self.total_processed % 50 == 0:
                print(f"[ReID] Processed {self.total_processed}")