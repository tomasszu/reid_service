from opensearchpy import OpenSearch

class Opensearch_db:
    def __init__(self, host, port, auth, index_name="reid_features"):
        self.host = host
        self.port = port
        self.auth = auth
        self.index_name = index_name

        try:
            # Connect to the OpenSearch instance
            self.client = OpenSearch(
                hosts=[{'host': self.host, 'port': self.port}],
                http_auth=self.auth,  # If authentication is enabled
                use_ssl=False,  # Set to True if you're using HTTPS
                verify_certs=False,  # Set to True if you have valid SSL certificates
            )
            
            # Test the connection
            response = self.client.info()
            print("Connected to OpenSearch:", response)
        
        except ConnectionError as e:
            # Handle connection errors, such as network issues or OpenSearch being down
            print(f"Error: Unable to connect to OpenSearch at {self.host}:{self.port}.")
            print(f"Exception: {e}")
        
        except Exception as e:
            # Catch any other exceptions
            print(f"An unexpected error occurred: {e}")

    def insert(self, object_key, vehicle_id, camera_id, track_id, feature_vector):
        """
        Inserts a vector document into OpenSearch.
        """
        if self.client is None:
            print("Not connected to OpenSearch.")
            return

        document = {
            "object_key": object_key,
            "vehicle_id": vehicle_id,
            "camera_id": camera_id,
            "track_id": track_id,
            "feature_vector": feature_vector
        }

        try:
            response = self.client.index(
                index=self.index_name,
                id=object_key,  # <-- IMPORTANT: deterministic ID
                body=document,
                refresh=False   # <-- DO NOT keep True in production
            )
        except Exception as e:
            print(f"Error inserting document {object_key}: {e}")


    def query_vector(self, query_vector, k=5):
        if self.client is None:
            print("Not connected to OpenSearch.")
            return []

        query = {
            "size": k,
            "query": {
                "knn": {
                    "feature_vector": {
                        "vector": query_vector,
                        "k": k
                    }
                }
            }
        }

        try:
            response = self.client.search(index=self.index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])

            return [
                {
                    "object_key": hit["_source"]["object_key"],
                    "vehicle_id": hit["_source"]["vehicle_id"],
                    "camera_id": hit["_source"]["camera_id"],
                    "track_id": hit["_source"]["track_id"],
                    "score": hit["_score"],
                }
                for hit in hits
            ]

        except Exception as e:
            print(f"Error querying vector: {e}")
            return []
        

    def query_vector_cross_camera(self, query_vector, camera_id, k=5):
        if self.client is None:
            print("Not connected to OpenSearch.")
            return []

        query = {
            "size": k,
            "query": {
                "knn": {
                    "feature_vector": {
                        "vector": query_vector,
                        "k": k,
                        "filter": {   # <-- THIS is the key change
                            "bool": {
                                "must_not": [
                                    {
                                        "term": {
                                            "camera_id": camera_id
                                        }
                                    }
                                ]
                            }
                        },
                        "method_parameters": {
                            "ef_search": 100
                        }
                    }
                }
            }
        }

        try:
            response = self.client.search(index=self.index_name, body=query)
            hits = response.get("hits", {}).get("hits", [])

            return [
                {
                    "object_key": hit["_source"]["object_key"],
                    "vehicle_id": hit["_source"]["vehicle_id"],
                    "camera_id": hit["_source"]["camera_id"],
                    "track_id": hit["_source"]["track_id"],
                    "score": hit["_score"],
                }
                for hit in hits
            ]

        except Exception as e:
            print(f"Error querying vector: {e}")
            return []
