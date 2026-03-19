from ReIDService import ReIDService
from inputs_logic.JSONLFileReceiver import JSONLFileReceiver

from storage_logic.VectorDatabase import Database
from storage_logic.create_Minio_storage import create_storage_from_env
from storage_logic.DatalakeUploader import MinioReIDUploader

def main():
    receiver = JSONLFileReceiver(
        path="test/MinIO_toJSONL/2026/02/12.jsonl",  # your generated file
        mps=4                     # simulate 4 messages per second
    )

    db = Database()

    datalake_storage = create_storage_from_env()

    datalake_uploader = MinioReIDUploader(
            storage=datalake_storage,
            model_name="sp4_ep6_ft_noCEL_070126_26ep.engine"
        )

    service = ReIDService(receiver, db, datalake_uploader)
    service.run()


if __name__ == "__main__":
    main()