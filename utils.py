import time
from uuid6 import uuid7  # pip install uuid6

def generate_object_key(ts):
    """
    Generates a deterministic object key from timestamp.

    Args:
        ts: int timestamp, can be ns or ms

    Returns:
        str: "YYYY/MM/DD/uuid7"
    """
    # auto-detect scale
    if ts > 1e15:
        ts_sec = ts / 1e9  # ns → s
    elif ts > 1e12:
        ts_sec = ts / 1e3  # ms → s
    else:
        ts_sec = ts        # assume seconds

    year, month, day = time.gmtime(ts_sec)[:3]

    uid = uuid7()
    uid_str = str(uid)

    return f"{year}/{month:02d}/{day:02d}/{uid_str}"