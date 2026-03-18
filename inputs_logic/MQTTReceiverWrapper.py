import numpy as np

from inputs_logic.BaseSightingReceiver import BaseSightingReceiver
from inputs_logic.ReIDSighting import ReIDSighting

class MQTTReceiverWrapper(BaseSightingReceiver):
    def __init__(self, mqtt_service):
        self.mqtt_service = mqtt_service

    def poll(self):
        raw_msgs = self.mqtt_service.get_pending_images()

        results = []
        for msg in raw_msgs:
            try:
                embedding = np.array(msg["embedding"], dtype=np.float32)

                results.append(
                    ReIDSighting(
                        camera_id=msg["camera_id"],
                        track_id=msg["track_id"],
                        timestamp=msg["timestamp"],
                        embedding=embedding,
                        image=msg["image"],
                        bbox=msg.get("bbox"),
                    )
                )
            except Exception as e:
                print(f"[ERROR] MQTT parse failed: {e}")

        return results