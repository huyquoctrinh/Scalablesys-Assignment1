
"""
Stateful load shedding strategies that can inspect and prune partial matches.
"""
from abc import ABC, abstractmethod
from typing import List
from base.PatternMatch import PatternMatch

class StatefulLoadSheddingStrategy(ABC):
    """
    Abstract base class for stateful load shedding strategies.
    These strategies can inspect the current set of partial matches and
    decide which ones to prune.
    """
    def __init__(self, name: str = "BaseStatefulStrategy"):
        self.name = name

    @abstractmethod
    def prune_partial_matches(self, partial_matches: List[PatternMatch]) -> List[PatternMatch]:
        """
        Given a list of partial matches, this method returns a list of
        matches to keep. The rest will be pruned.
        """
        pass
