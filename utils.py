import time
from uuid6 import uuid7  # pip install uuid6

def generate_object_key(ts_ns: int):
    """
    Generates object key from nanosecond timestamp.

    Args:
        ts_ns: int timestamp in nanoseconds

    Returns:
        str: "YYYY/MM/DD/uuid7"
    """
    ts_sec = ts_ns / 1e9

    year, month, day = time.gmtime(ts_sec)[:3]

    uid = uuid7()
    uid_str = str(uid)

    return f"{year}/{month:02d}/{day:02d}/{uid_str}"