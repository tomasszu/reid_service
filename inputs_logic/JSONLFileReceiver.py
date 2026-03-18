import json
import time
import cv2
import numpy as np

from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting

class JSONLFileReceiver(BaseSightingReceiver):
    def __init__(self, path: str, mps: float = 4.0):
        self.path = path
        self.mps = mps
        self.interval = 1.0 / mps

        self.file = open(path, "r")
        self.last_emit_time = 0.0

    def _decode_image(self, encoded_crop):
        crop_bytes = bytes.fromhex(encoded_crop)
        np_arr = np.frombuffer(crop_bytes, dtype=np.uint8)
        image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError("Failed to decode image")
        return image

    def _parse_line(self, line: str) -> ReIDSighting:
        payload = json.loads(line)

        image = self._decode_image(payload["cropped_image"])

        embedding = np.array(payload["embedding"], dtype=np.float32)

        return ReIDSighting(
            camera_id=payload["camera_id"],
            track_id=payload["track_id"],
            timestamp=payload["timestamp"],
            embedding=embedding,
            image=image,
            bbox=payload.get("bbox"),
        )

    def poll(self):
        now = time.time()

        if now - self.last_emit_time < self.interval:
            return []

        line = self.file.readline()
        if not line:
            return []  # EOF → just stop emitting

        self.last_emit_time = now

        try:
            sighting = self._parse_line(line)
            return [sighting]
        except Exception as e:
            print(f"[ERROR] Failed to parse line: {e}")
            return []