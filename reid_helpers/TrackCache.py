import time
from collections import OrderedDict

class TrackCache:
    """
    Cache mapping (camera_id, track_id) -> vehicle_id with expiration.
    Supports either time-based or size-based eviction.
    """

    def __init__(self, max_size=1000, ttl_sec=120):
        """
        Args:
            max_size: Maximum number of entries in cache
            ttl_sec: Time-to-live in seconds per entry
        """
        self.cache = OrderedDict()  # preserves insertion order
        self.max_size = max_size
        self.ttl_sec = ttl_sec

    def _current_time(self):
        return time.time()

    def get(self, camera_id, track_id):
        key = (camera_id, track_id)
        entry = self.cache.get(key)
        if entry:
            vehicle_id, timestamp = entry
            if self.ttl_sec is None or self._current_time() - timestamp <= self.ttl_sec:
                return vehicle_id
            else:
                # expired
                del self.cache[key]
        return None

    def set(self, camera_id, track_id, vehicle_id):
        key = (camera_id, track_id)

        # evict oldest if cache too big
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)  # FIFO eviction

        self.cache[key] = (vehicle_id, self._current_time())

    def cleanup(self):
        """Optional: manually purge expired entries"""
        now = self._current_time()
        keys_to_delete = [
            key for key, (_, ts) in self.cache.items()
            if self.ttl_sec is not None and now - ts > self.ttl_sec
        ]
        for key in keys_to_delete:
            del self.cache[key]