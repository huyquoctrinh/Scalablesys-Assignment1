# loadshedding/proxy.py
"""
Proxy that intercepts event processing without modifying original code.
"""
import time
from datetime import datetime
from base.Event import Event
from loadshedding.manager import LoadSheddingManager


class EvaluationMechanismProxy:
    """
    Proxy for evaluation mechanism that intercepts events for load shedding.
    Uses delegation pattern - wraps the original evaluation mechanism.
    """
    
    def __init__(self, wrapped_eval_mechanism):
        """
        Args:
            wrapped_eval_mechanism: Original TreeBasedEvaluationMechanism instance
        """
        self._wrapped = wrapped_eval_mechanism
        self._manager = LoadSheddingManager.get_instance()
        self._total_start_time = None
        
        # Monkey patch the tree's nodes to intercept partial matches
        if self._manager.is_enabled():
            self._patch_tree_nodes()
    
    def _patch_tree_nodes(self):
        """
        Monkey patch nodes to intercept partial match updates.
        This is done once during initialization.
        """
        tree = self._wrapped._tree
        
        for leaf in tree.get_leaves():
            self._patch_node(leaf)
            # Also patch parent nodes recursively
            self._patch_node_hierarchy(leaf.parent)
    
    def _patch_node_hierarchy(self, node):
        """Recursively patch all nodes in the hierarchy"""
        if node is None:
            return
        
        self._patch_node(node)
        
        if hasattr(node, 'parent'):
            self._patch_node_hierarchy(node.parent)
    
    def _patch_node(self, node):
        """
        Patch a single node to intercept partial match updates.
        Wraps the node's storage update methods.
        """
        if node is None or hasattr(node, '_load_shedding_patched'):
            return
        
        node._load_shedding_patched = True
        node_id = id(node)  # Use memory address as unique ID
        self._manager.register_node(node_id)
        
        # Find the storage unit (where partial matches are stored)
        if hasattr(node, 'get_storage_unit'):
            storage = node.get_storage_unit()
            if storage:
                self._patch_storage(storage, node_id)
    
    def _patch_storage(self, storage, node_id):
        """
        Patch the storage unit to intercept partial match additions.
        """
        # Save original methods
        original_add = storage.add_item if hasattr(storage, 'add_item') else None
        original_update = storage.update if hasattr(storage, 'update') else None
        
        def intercepted_add(item):
            """Intercept add_item to apply load shedding after addition"""
            if original_add:
                result = original_add(item)
            else:
                # Fallback if add_item doesn't exist
                result = None
            
            # Apply load shedding after adding
            if self._manager.is_enabled():
                current_matches = list(storage) if hasattr(storage, '__iter__') else []
                filtered = self._manager.intercept_partial_matches(
                    node_id,
                    current_matches,
                    datetime.now()
                )
                # Replace storage contents if items were dropped
                if len(filtered) < len(current_matches):
                    self._replace_storage_contents(storage, filtered)
            
            return result
        
        # Replace methods
        if original_add:
            storage.add_item = intercepted_add
    
    def _replace_storage_contents(self, storage, new_items):
        """Replace storage contents with filtered items"""
        if hasattr(storage, 'clear') and hasattr(storage, 'add_item'):
            storage.clear()
            for item in new_items:
                storage.add_item(item)
    
    def eval(self, events, matches, data_formatter):
        """
        Proxy eval method that adds monitoring around original eval.
        """
        if not self._manager.is_enabled():
            # No load shedding - just delegate
            return self._wrapped.eval(events, matches, data_formatter)
        
        # Wrap the evaluation with monitoring
        self._total_start_time = time.time()
        
        # Create an event interceptor
        monitored_events = self._create_monitored_stream(events)
        
        # Call original eval
        self._wrapped.eval(monitored_events, matches, data_formatter)
        
        # Calculate duration
        duration = time.time() - self._total_start_time
        return duration
    
    def _create_monitored_stream(self, original_stream):
        """
        Create a wrapper around the event stream that monitors each event.
        """
        class MonitoredStream:
            def __init__(self, stream, manager):
                self.stream = stream
                self.manager = manager
                self.event_count = 0
            
            def __iter__(self):
                for raw_event in self.stream:
                    event_start = time.time()
                    
                    yield raw_event
                    
                    # Record metrics after event is processed
                    processing_time = time.time() - event_start
                    queue_size = (self.stream.qsize() if hasattr(self.stream, 'qsize') 
                                 else 0)
                    pm_count = 0  # Will be updated by actual node processing
                    
                    self.manager.record_event_processing(
                        datetime.now(),
                        processing_time,
                        queue_size,
                        pm_count
                    )
                    
                    self.manager.check_overload()
                    self.event_count += 1
            
            def __getattr__(self, name):
                # Delegate all other attributes to original stream
                return getattr(self.stream, name)
        
        return MonitoredStream(original_stream, self._manager)
    
    def get_structure_summary(self):
        """Delegate to wrapped mechanism"""
        return self._wrapped.get_structure_summary()
    
    def __getattr__(self, name):
        """Delegate all other method calls to wrapped object"""
        return getattr(self._wrapped, name)