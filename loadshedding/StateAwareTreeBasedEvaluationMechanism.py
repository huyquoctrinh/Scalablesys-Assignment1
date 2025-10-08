"""
State-Aware Tree-Based Evaluation Mechanism for CEP with Load Shedding.
This module extends the standard tree-based evaluation mechanism to work seamlessly
with stateful load shedding strategies, maintaining pattern state across evaluation cycles.
"""
import logging
from collections import deque
from typing import Dict, List
from base.Event import Event
from base.Pattern import Pattern
from evaluation.EvaluationMechanismFactory import EvaluationMechanismParameters
from plan.TreePlan import TreePlan
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter
from tree.evaluation.TreeBasedEvaluationMechanism import TreeBasedEvaluationMechanism
from tree.PatternMatchStorage import TreeStorageParameters
from loadshedding.StatefulLoadSheddingStrategy import StatefulLoadSheddingStrategy
from loadshedding.PriorityBasedLoadShedding import PriorityBasedLoadShedding
from plan.LeftDeepTreeBuilders import TrivialLeftDeepTreeBuilder
from plan.TreeCostModels import TreeCostModels
from plan.negation.NegationAlgorithmTypes import NegationAlgorithmTypes
from adaptive.statistics.StatisticsTypes import StatisticsTypes
logger = logging.getLogger(__name__)
class StateAwareTreeBasedEvaluationMechanism(TreeBasedEvaluationMechanism):
    """
    Enhanced tree-based evaluation mechanism that integrates with stateful load shedding.
    """
    def __init__(self, patterns: List[Pattern], storage_params: TreeStorageParameters,
                 stateful_strategy: StatefulLoadSheddingStrategy = None):
        tree_builder = TrivialLeftDeepTreeBuilder(TreeCostModels.INTERMEDIATE_RESULTS_TREE_COST_MODEL,
                                                  NegationAlgorithmTypes.NAIVE_NEGATION_ALGORITHM)
        dummy_statistics = {
            StatisticsTypes.ARRIVAL_RATES: [0.1, 0.1],
            StatisticsTypes.SELECTIVITY_MATRIX: [[1, 0.001], [0.001, 1]]
        }
        pattern_to_tree_plan_map = {p: tree_builder.build_tree_plan(p, dummy_statistics) for p in patterns}
        super().__init__(pattern_to_tree_plan_map, storage_params)
        self.stateful_strategy = stateful_strategy
        if self.stateful_strategy is None:
            self.stateful_strategy = PriorityBasedLoadShedding()
        self.evaluation_metrics = {
            'total_events': 0,
            'events_processed': 0,
            'events_dropped': 0,
            'matches_found': 0,
        }
    def _play_new_event_on_tree(self, event: Event, matches: OutputStream):
        """
        Processes a new event and then applies load shedding.
        """
        # 1. Process event using parent's logic
        self._play_new_event(event, self._event_types_listeners)
        # 2. Apply load shedding
        self._apply_load_shedding()
    def _get_all_nodes(self):
        """
        Returns a list of all nodes in the evaluation tree(s).
        """
        all_nodes = []
        if not self._tree:
            return all_nodes
        q = deque([self._tree.get_root()])
        visited = {id(self._tree.get_root())}
        while len(q) > 0:
            curr_node = q.popleft()
            all_nodes.append(curr_node)
            children = []
            if hasattr(curr_node, '_left_subtree') and curr_node._left_subtree:
                children.append(curr_node._left_subtree)
            if hasattr(curr_node, '_right_subtree') and curr_node._right_subtree:
                children.append(curr_node._right_subtree)
            if hasattr(curr_node, '_child') and curr_node._child:
                children.append(curr_node._child)
            for child in children:
                if id(child) not in visited:
                    q.append(child)
                    visited.add(id(child))
        return all_nodes
    def _apply_load_shedding(self):
        """
        Collects all partial matches from the tree, asks the load shedding strategy to prune them,
        and updates the tree.
        """
        if not self.stateful_strategy:
            return
        # 1. Collect all partial matches
        all_partial_matches = []
        nodes = self._get_all_nodes()
        for node in nodes:
            storage = node.get_storage_unit()
            if storage:
                all_partial_matches.extend(storage.get_internal_buffer())
        if not all_partial_matches:
            return
        # 2. Prune using the strategy
        original_count = len(all_partial_matches)
        matches_to_keep = self.stateful_strategy.prune_partial_matches(all_partial_matches)
        pruned_count = original_count - len(matches_to_keep)
        if pruned_count == 0:
            return
        self.evaluation_metrics['events_dropped'] += pruned_count
        logger.debug(f"Load shedding pruned {pruned_count} partial matches.")
        # 3. Update node storages
        kept_matches_set = set(matches_to_keep)
        for node in nodes:
            storage = node.get_storage_unit()
            if not storage:
                continue
            new_storage_buffer = deque([pm for pm in storage.get_internal_buffer() if pm in kept_matches_set])
            storage._partial_matches = new_storage_buffer