from storage_logic.OpensearchLogic import Opensearch_db


class Database:
    def __init__(self, config):
        self.db = Opensearch_db(config)

    def insert(self, object_key, vehicle_id, camera_id, track_id, vector, timestamp_ms):
        self.db.insert(object_key, vehicle_id, camera_id, track_id, vector, timestamp_ms)

    def query(self, vector, k=5):
        return self.db.query_vector(vector, k=k)
    
    def query_cross_camera(self, query_vector, camera_id, k=5):
        return self.db.query_vector_cross_camera(query_vector, camera_id, k=k)
    
    def delete_older_than(self, cutoff_ms):
        self.db.delete_older_than(cutoff_ms)