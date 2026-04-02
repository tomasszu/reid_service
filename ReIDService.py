import time
import numpy as np
import os
from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting
from reid_helpers.TrackManager import TrackManager

from collections import defaultdict


from utils import generate_object_key
import uuid

INDEX_CLEANUP_TTL_MINUTES = int(os.getenv("INDEX_CLEANUP_TTL", 5))

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
        self.ttl_ms = INDEX_CLEANUP_TTL_MINUTES * 60 * 1000       # lifespan of vectors in vector DB = minutes * seconds * in milliseconds

        # initialize so first cleanup happens AFTER interval
        self.last_cleanup = time.time()


    def _generate_vehicle_id(self):
        return str(uuid.uuid4())
    
    def finalize_event(self, event):
        print(f"\n[ReID] Finalizing track {event.track_id} cam={event.camera_id}")

        embeddings = np.stack(event.embeddings)

        # --- normalize + centroid ---
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1e-8

        embs = embeddings / norms

        centroid = embs.mean(axis=0)
        centroid /= (np.linalg.norm(centroid) + 1e-8)

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

    def compute_ambiguity_margin(self, best_score):
        if best_score > 0.9:
            return 0.02
        elif best_score > 0.85:
            return 0.03
        else:
            return 0.06

    def _match_vehicle_event(self, embedding, camera_id):
        print("\n[ReID] --- MATCH VEHICLE EVENT START ---")

        try:
            results = self.database.query_cross_camera(
                embedding.tolist(),
                camera_id,
                k=10
            )
        except Exception as e:
            print(f"[ReID] query failed: {e}")
            return self._generate_vehicle_id(), None, True

        if not results:
            print("[ReID] No results returned from DB")
            return self._generate_vehicle_id(), None, True

        # --- RAW ---
        print(f"[ReID] Raw results (k={len(results)}):")
        for i, r in enumerate(results):
            print(
                f"  {i}: vid={r['vehicle_id']} "
                f"score={r['score']:.4f} "
                f"cam={r['camera_id']} "
                f"track={r['track_id']}"
            )

        # =========================
        # STEP 1: GROUP
        # =========================
        scores = defaultdict(list)

        for r in results:
            scores[r["vehicle_id"]].append(r["score"])

        # =========================
        # STEP 2: HYBRID AGGREGATION
        # =========================
        vehicle_scores = {}

        print("\n[ReID] Aggregated scores:")
        for vid, vals in scores.items():
            max_score = max(vals)
            mean_score = sum(vals) / len(vals)
            n = len(vals)

            support_bonus = min(0.01 * (n - 1), 0.03)  # capped boost

            combined = 0.5 * max_score + 0.5 * mean_score + support_bonus

            vehicle_scores[vid] = {
                "score": combined,
                "max": max_score,
                "mean": mean_score,
                "support": n
            }

            print(
                f"  {vid}: max={max_score:.4f} "
                f"mean={mean_score:.4f} "
                f"n={n} "
                f"final={combined:.4f}"
            )

        # =========================
        # STEP 3: THRESHOLD FILTER
        # =========================
        THRESHOLD = self.threshold

        candidates = [
            (vid, data)
            for vid, data in vehicle_scores.items()
            if data["score"] >= THRESHOLD
        ]

        print(f"\n[ReID] Candidates after threshold ({THRESHOLD}): {len(candidates)}")

        if not candidates:
            print("[ReID] No candidates passed threshold → NEW VEHICLE")
            return self._generate_vehicle_id(), None, True

        # =========================
        # STEP 4: SORT + LIMIT
        # =========================
        candidates.sort(key=lambda x: x[1]["score"], reverse=True)

        unique_cams = len(set(r["camera_id"] for r in results))
        MAX_CANDIDATES = max(3, unique_cams)

        candidates = candidates[:MAX_CANDIDATES]

        print(f"[ReID] Top candidates (limited to {MAX_CANDIDATES}):")
        for vid, data in candidates:
            print(f"  {vid}: {data['score']:.4f}")

        # =========================
        # STEP 5: MARGIN FILTER
        # =========================
        best_score = candidates[0][1]["score"]

        MARGIN = self.compute_ambiguity_margin(best_score)

        final_candidates = [
            (vid, data)
            for vid, data in candidates
            if best_score - data["score"] <= MARGIN
        ]

        print(f"\n[ReID] Final candidates after margin ({MARGIN}):")
        for vid, data in final_candidates:
            print(f"  {vid}: {data['score']:.4f}")

        if not final_candidates:
            print("[ReID] No candidates survived margin → NEW VEHICLE")
            return self._generate_vehicle_id(), None, True

        # =========================
        # FINAL DECISION
        # =========================
        OVERRULE_THRESHOLD = 0.92

        num_final = len(final_candidates)

        print(f"\n[ReID] Final candidate count: {num_final}")

        best_vid, best_data = final_candidates[0]
        best_score = best_data["score"]

        if num_final == 1:
            print(
                f"[ReID] CLEAR MATCH: {best_vid} "
                f"score={best_score:.4f}"
            )
            return best_vid, best_score, False

        # --- ambiguous case ---
        print("[ReID] Ambiguous match detected")

        if best_score >= OVERRULE_THRESHOLD:
            print(
                f"[ReID] OVERRULE: accepting best despite ambiguity "
                f"(score={best_score:.4f} >= {OVERRULE_THRESHOLD})"
            )
            return best_vid, best_score, False
        else:
            print(
                f"[ReID] REJECTED: ambiguous and below overrule threshold "
                f"(score={best_score:.4f} < {OVERRULE_THRESHOLD})"
            )
            return self._generate_vehicle_id(), None, True

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

        print(f"[CONFIG] Index cleanup TTL: {INDEX_CLEANUP_TTL_MINUTES * 60}s")

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