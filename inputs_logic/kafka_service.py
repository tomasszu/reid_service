import json
import time
import uuid
import io

import numpy as np
from PIL import Image
from confluent_kafka import Consumer


class KafkaService:
    def __init__(self, bootstrap_servers, topic, cafile, certfile, keyfile, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.queue = []

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',

            'security.protocol': 'SSL',
            'ssl.ca.location': cafile,
            'ssl.certificate.location': certfile,
            'ssl.key.location': keyfile,
        })

        self.running = False

    # ---------- lifecycle ----------

    def start(self):
        print(f"[KAFKA] Connecting to {self.bootstrap_servers}")
        self.consumer.subscribe([self.topic])
        self.running = True

    # ---------- polling loop ----------

    def poll(self, timeout=0.1, max_messages=50):
        """
        Non-blocking-ish poll to fill queue
        """
        count = 0

        while count < max_messages:
            msg = self.consumer.poll(timeout)

            if msg is None:
                break

            if msg.error():
                print(f"[KAFKA ERROR] {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))

                image = self._decode_crop_np(payload["image"])
                embedding = np.array(payload["features"], dtype=np.float32)

                self.queue.append({
                    "camera_id": payload.get("cam_id", payload.get("camera_id")),
                    "track_id": payload["track_id"],
                    "timestamp_ns": payload.get("timestamp_ns", time.time_ns()),
                    "embedding": embedding,
                    "image": image,
                    "bbox": payload.get("bbox"),
                })

                count += 1

            except Exception as e:
                print(f"[KAFKA PARSE ERROR] {e}")

    # ---------- utils ----------

    def _decode_crop_np(self, encoded_crop):
        crop_bytes = bytes.fromhex(encoded_crop)

        try:
            image = Image.open(io.BytesIO(crop_bytes)).convert("RGB")
        except Exception as e:
            raise ValueError(f"Failed to decode image: {e}")

        return np.array(image)

    # ---------- external API ----------

    def get_pending_messages(self):
        # fill queue first
        self.poll()

        data = self.queue[:]
        self.queue.clear()
        return data