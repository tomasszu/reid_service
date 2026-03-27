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
        self.threshold = 0.77

        # Track cache: prevents repeated DB queries for same (cam, track)
        self.track_manager = TrackManager(self, timeout=10.0)

        # --- cleanup config ---
        self.cleanup_interval = 30        # seconds between cleanup runs
        self.ttl_ms = 5 * 60 * 1000       # lifespan of vectors in vector DB = minutes * seconds * in milliseconds

        # initialize so first cleanup happens AFTER interval
        self.last_cleanup = time.time()


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
        vehicle_id, score, is_new = self._match_vehicle_event(centroid, event.camera_id)

        # --- representative ---
        mid_idx = len(event.object_keys) // 2
        rep_key = event.object_keys[mid_idx]

        # --- insert into DB ---

        now_time = int(time.time() * 1000)

        object_key = generate_object_key(now_time)

        self.database.insert(
            object_key=object_key,
            vehicle_id=vehicle_id,
            camera_id=event.camera_id,
            track_id=event.track_id,
            vector=centroid.tolist(),
            timestamp_ms=now_time
        )

        # --- store event in MinIO ---
        self.datalake.upload_vehicle_event(
            vehicle_id=vehicle_id,
            reid_score=score,
            object_key=object_key,
            camera_id=event.camera_id,
            track_id=event.track_id,
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
            return self._generate_vehicle_id(), 0.00, True

        if not results:
            return self._generate_vehicle_id(), 0.00, True

        best = results[0]
        score = best["score"]

        for i, r in enumerate(results):
            print(f"[ReID] {i}. score={r['score']:.4f} track={r['track_id']} from cam={r['camera_id']}")

        if score >= self.threshold:
            print(f"[ReID] REID from cam={best['camera_id']} track={best['track_id']}")
            return best["vehicle_id"], score, False
        else:
            return self._generate_vehicle_id(), 0.00, True

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

            now = time.time()

            # Vector DB cleanup
            if now - self.last_cleanup > self.cleanup_interval:
                cutoff = int(now * 1000) - self.ttl_ms

                try:
                    self.database.delete_older_than(cutoff)
                    print(f"[ReID] Cleanup done (cutoff={cutoff})")
                except Exception as e:
                    print(f"[ReID] Cleanup failed: {e}")

                self.last_cleanup = now

            for sighting in batch:
                self.process(sighting)
                self.total_processed += 1

            self.track_manager.finalize_expired()

            if self.total_processed % 50 == 0:
                print(f"[ReID] Processed {self.total_processed}")