import json
import time
import uuid
from PIL import Image
import io
import numpy as np
import paho.mqtt.client as mqtt


class MQTTService:
    def __init__(self, host, port, topic, cafile=None, certfile=None, keyfile=None):
        self.host = host
        self.port = port
        self.topic = topic

        self.queue = []
        self.connected = False

        client_id = f"reid-{uuid.uuid4()}"
        self.client = mqtt.Client(client_id=client_id)

        # TLS (if provided)
        if cafile:
            self.client.tls_set(
                ca_certs=cafile,
                certfile=certfile,
                keyfile=keyfile
            )

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    # ---------- lifecycle ----------

    def start(self):
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()

        # wait for connection
        timeout = time.time() + 5
        while not self.connected:
            if time.time() > timeout:
                raise TimeoutError("MQTT connection timed out")
            time.sleep(0.1)

    # ---------- callbacks ----------

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] Connected to {self.host}:{self.port}")
            client.subscribe(self.topic)
            self.connected = True
        else:
            print(f"[MQTT] Connection failed: rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        print(f"[MQTT] Disconnected (rc={rc})")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))

            # --- decode image ---
            image = self._decode_crop_np(payload["image"])

            # --- parse embedding ---
            embedding = np.array(payload["features"], dtype=np.float32)

            self.queue.append({
                "camera_id": payload["cam_id"],
                "track_id": payload["track_id"],
                "timestamp_ns": payload.get("timestamp_ns", time.time_ns()),  # fallback timestamp
                "embedding": embedding,
                "image": image,
                "bbox": payload.get("bbox"),
            })

        except Exception as e:
            print(f"[MQTT ERROR] {e}")

    # ---------- utils ----------

    #with CV2
    # def _decode_crop_np(self, encoded_crop):
    #     crop_bytes = bytes.fromhex(encoded_crop)
    #     np_arr = np.frombuffer(crop_bytes, dtype=np.uint8)
    #     image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

    #     if image is None:
    #         raise ValueError("Failed to decode image")

    #     return image

    # With PIL
    def _decode_crop_np(self, encoded_crop):
        crop_bytes = bytes.fromhex(encoded_crop)

        try:
            image = Image.open(io.BytesIO(crop_bytes)).convert("RGB")
        except Exception as e:
            raise ValueError(f"Failed to decode image: {e}")

        # convert to numpy (same as cv2 output shape, but RGB instead of BGR)
        image_np = np.array(image)
        # ŠO VAJAG PARBAUDIT KA IZSKATAS ATTELI!!!!!! mAINIJU NO CV2 UZ PIL
        #image_np = image_np[:, :, ::-1]  # RGB → BGR

        return image_np

    # ---------- polling ----------

    def get_pending_images(self):
        data = self.queue[:]
        self.queue.clear()
        return data