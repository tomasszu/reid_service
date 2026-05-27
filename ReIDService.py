import time
import numpy as np
import os
from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from reid_helpers.TrackManager import TrackManager

import threading

from collections import defaultdict


from utils import generate_object_key
import uuid

# ============================================================
# ENV CONFIG
# ============================================================

REID_THRESHOLD = float(
    os.getenv("REID_THRESHOLD", 0.77)
)

REID_OVERRULE_THRESHOLD = float(
    os.getenv("REID_OVERRULE_THRESHOLD", 0.92)
)

TRACK_TIMEOUT_SECONDS = int(
    os.getenv("TRACK_TIMEOUT_SECONDS", 30)
)

DB_CLEANUP_INTERVAL_SECONDS = int(
    os.getenv("DB_CLEANUP_INTERVAL_SECONDS", 30)
)

VECTOR_RETENTION_MINUTES = os.getenv("VECTOR_RETENTION_MINUTES")

VECTOR_RETENTION_MINUTES = (
    int(VECTOR_RETENTION_MINUTES)
    if VECTOR_RETENTION_MINUTES not in (None, "", "None")
    else None
)

class ReIDService:
    def __init__(self, receiver: BaseSightingReceiver, database, datalake):
        self.receiver = receiver
        self.database = database
        self.datalake = datalake
        self.total_processed = 0

        # Threshold for cosine similarity to consider a match (tunable)
        self.threshold = REID_THRESHOLD
        # Threshold above which we accept the best candidate even if ambiguous (tunable)
        self.overrule_threshold = REID_OVERRULE_THRESHOLD

        print(f"[CONFIG] ReID threshold: {self.threshold}")
        print(f"[CONFIG] ReID overrule threshold: {self.overrule_threshold}")

        # Aggregates sightings into finalized track events
        self.track_manager = TrackManager(self, timeout_ns=int(TRACK_TIMEOUT_SECONDS * 1e9))

        # --- cleanup config ---
        self.db_cleanup_interval_ns = (
            DB_CLEANUP_INTERVAL_SECONDS * 1_000_000_000
        )
        self.vector_retention_ns = (
            VECTOR_RETENTION_MINUTES * 60 * 1_000_000_000  # lifespan of vectors in vector DB = minutes * seconds * in nanoseconds
            if VECTOR_RETENTION_MINUTES is not None
            else None
        )      

        self.last_cleanup = int(time.time() * 1e9)
        self.last_finalize = int(time.time() * 1e9)
        self.finalize_interval = 1.0  # seconds

        #shutdown event for maintenance thread
        self.shutdown_event = threading.Event()


    def _generate_vehicle_id(self):
        return str(uuid.uuid4())
    
    def _maintenance_loop(self):
        print("[ReID] Maintenance thread started")

        while not self.shutdown_event.is_set():
            now_ns = time.time_ns()

            # FINALIZE
            if now_ns - self.last_finalize > int(self.finalize_interval * 1e9):
                try:
                    events = self.track_manager.finalize_expired()
                    for event_data in events:
                        self.finalize_event(event_data)
                except Exception as e:
                    print(f"[ReID] Finalize failed: {e}")

                self.last_finalize += int(self.finalize_interval * 1e9)  # FIXED DRIFT

            # CLEANUP
            if (
                self.vector_retention_ns is not None
                and now_ns - self.last_cleanup > self.db_cleanup_interval_ns
            ):
                cutoff_ns = time.time_ns() - self.vector_retention_ns

                try:
                    self.database.delete_older_than(cutoff_ns)

                    print(
                        f"[ReID] Cleanup done "
                        f"(retention={VECTOR_RETENTION_MINUTES}min)"
                    )

                except Exception as e:
                    print(f"[ReID] Cleanup failed: {e}")

                self.last_cleanup += self.db_cleanup_interval_ns

            # responsive sleep
            if self.shutdown_event.wait(timeout=0.2):
                break
    
    def finalize_event(self, event):
        print(f"\n[ReID] Finalizing track {event['track_id']} cam={event['camera_id']}")

        embeddings = np.stack(event["embeddings"])
        object_keys = event["object_keys"]

        # --- normalize + centroid ---
        norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
        norms[norms == 0] = 1e-8

        embs = embeddings / norms

        centroid = embs.mean(axis=0)
        centroid /= (np.linalg.norm(centroid) + 1e-8)

        # --- match ---
        vehicle_id, score, is_new = self._match_vehicle_event(
            centroid,
            event["camera_id"]
        )

        # --- representative ---
        mid_idx = len(object_keys) // 2
        rep_key = object_keys[mid_idx]

        now_ns = time.time_ns()
        object_key = generate_object_key(now_ns)

        self.database.insert(
            object_key=object_key,
            vehicle_id=vehicle_id,
            camera_id=event["camera_id"],
            track_id=event["track_id"],
            vector=centroid.tolist(),
            timestamp_ns=now_ns
        )

        self.datalake.upload_vehicle_event(
            vehicle_id=vehicle_id,
            reid_score=score,
            object_key=object_key,
            camera_id=event["camera_id"],
            track_id=event["track_id"],
            representative_key=rep_key,
            sighting_keys=object_keys,
            centroid=centroid
        )

        print(
            f"[ReID] Finalized: vid={vehicle_id} "
            f"cam={event['camera_id']} track={event['track_id']} "
            f"sightings={len(object_keys)} new={is_new}"
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
        OVERRULE_THRESHOLD = self.overrule_threshold

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

    # def _match_vehicle(self, sighting):
    #     # --- cache ---
    #     cached_vid = self.track_cache.get(sighting.camera_id, sighting.track_id)
    #     if cached_vid:
    #         return cached_vid, False

    #     # --- cross-camera query ---
    #     try:
    #         results = self.database.query_cross_camera(
    #             sighting.embedding.tolist(),
    #             sighting.camera_id,
    #             k=3
    #         )
    #     except Exception as e:
    #         print(f"[ReID] query failed: {e}")
    #         vid = self._generate_vehicle_id()
    #         self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
    #         return vid, True

    #     if not results:
    #         vid = self._generate_vehicle_id()
    #         self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
    #         print(f"[ReID] No results returned.")
    #         return vid, True            

    #     best = results[0]
    #     score = best["score"]

    #     for i, r in enumerate(results):
    #         print(f"[ReID] {i}. score={r['score']:.4f} track={r['track_id']} from cam={r['camera_id']}")

    #     if score >= self.threshold:
    #         vid = best["vehicle_id"]
    #         is_new = False
    #         print(f"[ReID] REID from cam={best['camera_id']} track={best['track_id']}")
    #     else:
    #         vid = self._generate_vehicle_id()
    #         is_new = True

    #     self.track_cache.set(sighting.camera_id, sighting.track_id, vid)
    #     return vid, is_new

    def process(self, sighting):
        object_key = generate_object_key(sighting.timestamp_ns)

        # --- save sighting immediately to datalake ---
        self.datalake.upload_sighting(sighting, object_key)

        # --- including the sighting in track aggregation dict ---
        self.track_manager.update(sighting, object_key)

    def stop(self):
        print("[ReID] Shutdown requested")
        self.shutdown_event.set()

        if hasattr(self, "maintenance_thread"):
            self.maintenance_thread.join(timeout=2)

        print("[ReID] Shutdown complete")


    def run(self):
        print("[ReID] Service started")
        if VECTOR_RETENTION_MINUTES is None:
            print("[CONFIG] Vector retention cleanup: DISABLED")
            print("[WARNING] Vector retention disabled — DB growth is unbounded")
        else:
            print(
                f"[CONFIG] Vector retention: "
                f"{VECTOR_RETENTION_MINUTES} minutes"
            )

        print(
            f"[CONFIG] DB cleanup interval: "
            f"{DB_CLEANUP_INTERVAL_SECONDS}s"
        )

        # start maintenance thread
        self.maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="maintenance_thread",
            daemon=True
        )
        self.maintenance_thread.start()

        while not self.shutdown_event.is_set():
            batch = self.receiver.poll()

            if not batch:
                if self.shutdown_event.wait(timeout=0.05):
                    break
                continue

            for sighting in batch:
                self.process(sighting)
                self.total_processed += 1

            if self.total_processed % 50 == 0:
                print(f"[ReID] Processed {self.total_processed}")