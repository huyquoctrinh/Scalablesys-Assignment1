"""
Implementation of a load shedding strategy that prioritizes partial matches based on a score.
"""
from typing import List
from base.PatternMatch import PatternMatch
from loadshedding.StatefulLoadSheddingStrategy import StatefulLoadSheddingStrategy

class PriorityBasedLoadShedding(StatefulLoadSheddingStrategy):
    """
    This strategy assigns a priority score to each partial match and prunes the ones
    with the lowest scores when the system is under load.
    """
    def __init__(self, max_partial_matches: int = 1000):
        super().__init__("PriorityBasedLoadShedding")
        self.max_partial_matches = max_partial_matches

    def prune_partial_matches(self, partial_matches: List[PatternMatch]) -> List[PatternMatch]:
        """
        Prunes the list of partial matches to be under the configured limit.
        The pruning is based on a calculated priority score.
        """
        if len(partial_matches) <= self.max_partial_matches:
            return partial_matches

        # Sort partial matches by priority (higher is better)
        partial_matches.sort(key=self._calculate_priority, reverse=True)

        # Prune the lowest priority matches
        return partial_matches[:self.max_partial_matches]

    def _calculate_priority(self, partial_match: PatternMatch) -> float:
        """
        Calculates the priority of a partial match based on its length and age.
        """
        # Priority is a combination of the number of events and the time window utilization.
        # Longer chains that have been developing for longer get higher priority.
        num_events = len(partial_match.events)
        if num_events == 0:
            return 0.0

        time_window_utilization = (partial_match.last_timestamp - partial_match.first_timestamp).total_seconds()

        # Simple scoring: combine length and time utilization.
        # We can tune the weights later.
        score = num_events * 0.7 + time_window_utilization * 0.3
        return score
