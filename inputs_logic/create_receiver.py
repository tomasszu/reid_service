import os

from inputs_logic.JSONLFileReceiver import JSONLFileReceiver
from inputs_logic.MQTTReceiverWrapper import MQTTReceiverWrapper

# assuming you have some MQTT service class already
from inputs_logic.mqtt_service import MQTTService  # adjust import


def create_receiver():
    mode = os.getenv("INPUT_MODE", "json").lower()

    if mode == "json":
        path = os.getenv("INPUT_PATH", "test/MinIO_toJSONL/2026/02/12.jsonl")
        mps = int(os.getenv("JSON_MPS", 4))

        print(f"[INPUT] Using JSON receiver: {path}")

        return JSONLFileReceiver(path=path, mps=mps)

    elif mode == "mqtt":
        print("[INPUT] Using MQTT receiver")

        mqtt_service = MQTTService(
            host=os.getenv("MQTT_HOST"),
            port=int(os.getenv("MQTT_PORT", 8884)),
            topic=os.getenv("MQTT_TOPIC"),
            cafile=os.getenv("MQTT_CA_CERT"),
            certfile=os.getenv("MQTT_CERT"),
            keyfile=os.getenv("MQTT_KEY"),
        )

        mqtt_service.start()  # important

        return MQTTReceiverWrapper(mqtt_service)

    else:
        raise ValueError(f"Unknown INPUT_MODE: {mode}")