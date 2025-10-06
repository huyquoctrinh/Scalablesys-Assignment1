"""CEP wrapper that adds load shedding via monkey patching"""
from CEP import CEP as OriginalCEP
from loadshedding.manager import LoadSheddingManager
from loadshedding.shedder import HotPathLoadShedder
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class LoadSheddingCEP:
    """Wrapper that adds load shedding by patching node storage"""
    
    def __init__(self, patterns, eval_mechanism_params=None, load_shedding_config=None):
        self._cep = OriginalCEP(patterns, eval_mechanism_params)
        self._load_shedding_config = load_shedding_config
        self._manager = LoadSheddingManager.get_instance()
        
        if load_shedding_config and load_shedding_config.enabled:
            self._manager.configure(load_shedding_config)
            self._shedder = HotPathLoadShedder(load_shedding_config)
            self._patch_applied = False
        else:
            self._shedder = None
    
    def run(self, input_stream, output_stream, data_formatter):
        """Run CEP with load shedding"""
        import time
        
        if self._shedder and not self._patch_applied:
            self._patch_tree_storage()
            self._patch_applied = True
        
        start = time.time()
        self._cep.run(input_stream, output_stream, data_formatter)
        duration = time.time() - start
        
        if self._shedder:
            stats = self._shedder.get_statistics()
            self._manager._stats = stats
            self._manager._stats['events_processed'] = input_stream.count
        
        return duration
    
    def _patch_tree_storage(self):
        """Patch the tree's node storage to intercept partial matches"""
        try:
            eval_manager = self._cep._CEP__evaluation_manager
            eval_mechanism = eval_manager._SequentialEvaluationManager__eval_mechanism
            tree = eval_mechanism._tree
            
            logger.info("Patching tree nodes for load shedding...")
            
            patched_count = 0
            for leaf in tree.get_leaves():
                if self._patch_node_storage(leaf):
                    patched_count += 1
            
            logger.info(f"Patched {patched_count} leaf nodes")
            
        except Exception as e:
            logger.error(f"Failed to patch tree storage: {e}", exc_info=True)
    
    def _patch_node_storage(self, node):
        """Patch a single node's storage unit"""
        try:
            storage = node.get_storage_unit()
            if storage is None:
                return False
            
            # CRITICAL: Store the ORIGINAL add method before patching
            original_add_method = storage.add_item
            
            # Store reference to shedder and this instance
            shedder = self._shedder
            parent_self = self
            
            # Flag to prevent recursion during replacement
            _in_replacement = {'flag': False}
            
            def intercepted_add(item):
                """Intercept add_item to apply load shedding"""
                
                # CRITICAL: Skip load shedding if we're replacing storage
                if _in_replacement['flag']:
                    return original_add_method(item)
                
                # Add item using ORIGINAL method
                result = original_add_method(item)
                
                # After adding, check if we should shed
                try:
                    current_items = list(storage)
                    
                    if shedder.should_shed_load(current_items):
                        filtered_items = shedder.shed_partial_matches(
                            current_items,
                            datetime.now()
                        )
                        
                        if len(filtered_items) < len(current_items):
                            # Set flag to prevent recursion
                            _in_replacement['flag'] = True
                            try:
                                parent_self._replace_storage_contents(
                                    storage, 
                                    filtered_items, 
                                    original_add_method
                                )
                            finally:
                                _in_replacement['flag'] = False
                
                except Exception as e:
                    logger.debug(f"Error during load shedding: {e}")
                
                return result
            
            # Replace the method
            storage.add_item = intercepted_add
            logger.debug(f"Patched storage for node: {node}")
            return True
            
        except Exception as e:
            logger.debug(f"Could not patch node storage: {e}")
            return False
    
    def _replace_storage_contents(self, storage, new_items, original_add):
        """Replace storage contents with filtered items"""
        try:
            # Direct access to clear the storage
            if hasattr(storage, '_PatternMatchStorage__storage'):
                storage._PatternMatchStorage__storage.clear()
                
                # Re-add items using the ORIGINAL add method (not the patched one)
                for item in new_items:
                    original_add(item)
                
                logger.debug(f"Replaced storage: {len(new_items)} items kept")
            else:
                logger.warning("Cannot access storage internals for replacement")
                
        except Exception as e:
            logger.error(f"Could not replace storage: {e}", exc_info=True)
    
    def is_load_shedding_enabled(self):
        return self._manager.is_enabled()
    
    def get_load_shedding_statistics(self):
        return self._manager.get_statistics()
    
    def reset_load_shedding(self):
        self._manager.reset()
        if self._shedder:
            self._shedder.dropped_partial_matches = 0
            self._shedder.total_partial_matches = 0
        if self._load_shedding_config:
            self._manager.configure(self._load_shedding_config)
        # Reset patch flag so it can be reapplied
        self._patch_applied = False
    
    def __getattr__(self, name):
        return getattr(self._cep, name)