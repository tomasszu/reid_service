from storage_logic.OpensearchLogic import Opensearch_db


class Database:
    def __init__(self):
        self.db = Opensearch_db("localhost", 9200, ("admin", "admin"))

    def insert(self, object_key, vehicle_id, camera_id, track_id, vector):
        self.db.insert(object_key, vehicle_id, camera_id, track_id, vector)

    def query(self, vector, k=5):
        return self.db.query_vector(vector, k=k)
    
    def query_cross_camera(self, query_vector, camera_id, k=5):
        return self.db.query_vector_cross_camera(query_vector, camera_id, k=k)