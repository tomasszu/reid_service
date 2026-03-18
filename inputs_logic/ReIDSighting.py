from dataclasses import dataclass
import numpy as np
from typing import Optional, List


@dataclass
class ReIDSighting:
    camera_id: str
    track_id: int
    timestamp: int
    embedding: np.ndarray  # (D,)
    image: np.ndarray      # decoded BGR image
    bbox: Optional[List[int]] = None