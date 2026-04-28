import numpy as np

from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting


class KafkaReceiverWrapper(BaseSightingReceiver):
    def __init__(self, kafka_service):
        self.kafka_service = kafka_service

    def poll(self):
        raw_msgs = self.kafka_service.get_pending_messages()

        results = []
        for msg in raw_msgs:
            try:
                embedding = np.array(msg["embedding"], dtype=np.float32)

                results.append(
                    ReIDSighting(
                        camera_id=msg.get("camera_id", msg.get("cam_id")),
                        track_id=msg["track_id"],
                        timestamp_ns=msg["timestamp_ns"],
                        embedding=embedding,
                        image=msg["image"],
                        bbox=msg.get("bbox"),
                    )
                )
            except Exception as e:
                print(f"[ERROR] Kafka parse failed: {e}")

        return results