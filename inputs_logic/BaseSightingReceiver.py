from abc import ABC, abstractmethod
from typing import List

from inputs_logic.ReIDSighting import ReIDSighting


class BaseSightingReceiver(ABC):
    @abstractmethod
    def poll(self) -> List[ReIDSighting]:
        """
        Returns a batch of new sightings (can be empty).
        Must be NON-blocking.
        """
        pass