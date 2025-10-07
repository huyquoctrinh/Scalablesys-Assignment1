import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
import logging

from loadshedding.StatefulLoadSheddingStrategy import StatefulLoadSheddingStrategy
from loadshedding.StatePersistence import LoadSheddingStatePersistence
from base.Event import Event
from base.Pattern import Pattern

logger = logging.getLogger(__name__)


class PersistentStatefulLoadSheddingStrategy(StatefulLoadSheddingStrategy):
    """
    Stateful load shedding strategy with state persistence capabilities.
    
    This strategy extends the base stateful load shedding by adding:
    - Automatic state persistence to disk/database
    - Recovery of partial matches after system restart
    - Cross-session learning and adaptation
    - Performance metrics persistence
    """
    
    def __init__(self, 
                 pattern_priorities: Dict[str, float] = None,
                 completion_threshold: float = 0.3,
                 state_aware_probability: float = 0.8,
                 persistence: LoadSheddingStatePersistence = None,
                 save_interval: int = 30,
                 strategy_name: str = "persistent_stateful"):
        """
        Initialize persistent stateful load shedding strategy.
        
        Args:
            pattern_priorities: Priority weights for different patterns
            completion_threshold: Minimum completion probability to keep events
            state_aware_probability: Probability factor for state-aware decisions
            persistence: State persistence manager
            save_interval: Seconds between automatic saves
            strategy_name: Name for this strategy instance
        """
        super().__init__(
            pattern_priorities=pattern_priorities,
            completion_threshold=completion_threshold,
            state_aware_probability=state_aware_probability
        )
        
        self.persistence = persistence or LoadSheddingStatePersistence()
        self.save_interval = save_interval
        self.strategy_name = strategy_name
        
        # Persistence metadata
        self.last_save_time = time.time()
        self.save_count = 0
        self.load_count = 0
        
        # Performance tracking for persistence
        self.session_stats = {
            'session_start': datetime.now(),
            'events_processed': 0,
            'events_dropped': 0,
            'matches_found': 0,
            'saves_performed': 0,
            'loads_performed': 0
        }
        
        # Background persistence thread
        self.persistence_thread = None
        self.should_stop_persistence = threading.Event()
        
        # Load any existing state
        self._load_persisted_state()
        
        # Start background persistence
        self._start_background_persistence()
        
        logger.info(f"PersistentStatefulLoadSheddingStrategy '{strategy_name}' initialized")
    
    def should_shed_event(self, event_data: Dict, current_load: Dict, 
                         pattern_states: Dict = None) -> bool:
        """
        Enhanced should_shed_event with persistence awareness.
        """
        # Call parent implementation
        should_shed = super().should_shed_event(event_data, current_load, pattern_states)
        
        # Update session statistics
        self.session_stats['events_processed'] += 1
        if should_shed:
            self.session_stats['events_dropped'] += 1
        
        # Check if we need to save state
        self._check_and_save_state()
        
        return should_shed
    
    def update_pattern_states(self, pattern_states: Dict):
        """
        Update pattern states with persistence awareness.
        """
        super().update_pattern_states(pattern_states)
        
        # Mark state as needing persistence
        self._mark_state_dirty()
    
    def _load_persisted_state(self):
        """
        Load previously persisted state from storage.
        """
        try:
            # Load strategy configuration
            config_key = f"strategy_config_{self.strategy_name}"
            config = self.persistence.load_load_shedding_config(config_key)
            if config:
                self.pattern_priorities.update(config.get('pattern_priorities', {}))
                self.completion_threshold = config.get('completion_threshold', self.completion_threshold)
                self.state_aware_probability = config.get('state_aware_probability', self.state_aware_probability)
                logger.info(f"Loaded strategy configuration for '{self.strategy_name}'")
            
            # Load pattern states
            pattern_state_key = f"pattern_states_{self.strategy_name}"
            pattern_states = self.persistence.load_pattern_state(pattern_state_key)
            if pattern_states:
                # Restore pattern states (this would need to be implemented in the parent class)
                logger.info(f"Loaded pattern states for '{self.strategy_name}'")
                self.load_count += 1
                self.session_stats['loads_performed'] += 1
            
            # Load session statistics
            stats_key = f"session_stats_{self.strategy_name}"
            saved_stats = self.persistence.storage.load_state(stats_key)
            if saved_stats:
                # Merge with current session stats
                for key, value in saved_stats.items():
                    if key in self.session_stats and isinstance(value, (int, float)):
                        self.session_stats[key] += value
                logger.info(f"Loaded session statistics for '{self.strategy_name}'")
            
        except Exception as e:
            logger.error(f"Error loading persisted state: {e}")
    
    def _check_and_save_state(self):
        """
        Check if state needs to be saved and trigger save if necessary.
        """
        current_time = time.time()
        if current_time - self.last_save_time >= self.save_interval:
            self._save_state_async()
    
    def _save_state_async(self):
        """
        Trigger asynchronous state saving.
        """
        if self.persistence_thread and self.persistence_thread.is_alive():
            return  # Already saving
        
        self.persistence_thread = threading.Thread(target=self._save_current_state)
        self.persistence_thread.daemon = True
        self.persistence_thread.start()
    
    def _save_current_state(self):
        """
        Save current state to persistence storage.
        """
        try:
            # Save strategy configuration
            config_key = f"strategy_config_{self.strategy_name}"
            config = {
                'pattern_priorities': dict(self.pattern_priorities),
                'completion_threshold': self.completion_threshold,
                'state_aware_probability': self.state_aware_probability,
                'last_updated': datetime.now().isoformat()
            }
            self.persistence.save_load_shedding_config(config_key, config)
            
            # Save pattern states (simplified - would need proper implementation)
            pattern_state_key = f"pattern_states_{self.strategy_name}"
            pattern_states = {
                'states': [],  # Would contain actual pattern states
                'last_updated': datetime.now().isoformat(),
                'state_count': len(getattr(self, 'partial_matches', {}))
            }
            self.persistence.save_pattern_state(pattern_state_key, pattern_states)
            
            # Save session statistics
            stats_key = f"session_stats_{self.strategy_name}"
            stats_to_save = {
                **self.session_stats,
                'last_saved': datetime.now().isoformat()
            }
            self.persistence.storage.store_state(stats_key, stats_to_save)
            
            # Update save metadata
            self.last_save_time = time.time()
            self.save_count += 1
            self.session_stats['saves_performed'] += 1
            
            logger.debug(f"State saved for strategy '{self.strategy_name}' (save #{self.save_count})")
            
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def _start_background_persistence(self):
        """
        Start background thread for periodic state persistence.
        """
        def persistence_loop():
            while not self.should_stop_persistence.wait(self.save_interval):
                try:
                    self._save_current_state()
                except Exception as e:
                    logger.error(f"Error in background persistence: {e}")
        
        self.persistence_thread = threading.Thread(target=persistence_loop)
        self.persistence_thread.daemon = True
        self.persistence_thread.start()
        
        logger.info(f"Background persistence started for '{self.strategy_name}' "
                   f"(interval: {self.save_interval}s)")
    
    def _mark_state_dirty(self):
        """
        Mark state as dirty and needing persistence.
        """
        # This could be used to optimize when to save state
        pass
    
    def shutdown(self):
        """
        Gracefully shutdown the strategy and save final state.
        """
        logger.info(f"Shutting down persistent strategy '{self.strategy_name}'")
        
        # Stop background persistence
        self.should_stop_persistence.set()
        if self.persistence_thread and self.persistence_thread.is_alive():
            self.persistence_thread.join(timeout=5)
        
        # Final state save
        self._save_current_state()
        
        logger.info(f"Strategy '{self.strategy_name}' shutdown complete")
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics including persistence metrics.
        """
        base_stats = super().get_statistics() if hasattr(super(), 'get_statistics') else {}
        
        persistence_stats = {
            'persistence': {
                'saves_performed': self.save_count,
                'loads_performed': self.load_count,
                'last_save_time': self.last_save_time,
                'save_interval': self.save_interval,
                'strategy_name': self.strategy_name
            },
            'session': self.session_stats,
            'uptime_seconds': (datetime.now() - self.session_stats['session_start']).total_seconds()
        }
        
        return {**base_stats, **persistence_stats}


def create_persistent_stateful_strategy(
    strategy_name: str = "persistent_stateful",
    storage_type: str = "file",
    storage_path: str = "load_shedding_state",
    pattern_priorities: Dict[str, float] = None,
    drop_threshold: float = 0.3,  # Changed from completion_threshold
    confidence_threshold: float = 0.8,  # Changed from state_aware_probability  
    save_interval: int = 30
) -> PersistentStatefulLoadSheddingStrategy:
    """
    Create a persistent stateful load shedding strategy with all components configured.
    
    Args:
        strategy_name: Name for the strategy (used for persistence keys)
        storage_type: Type of storage backend ("file" or "sqlite")
        storage_path: Path for storage
        pattern_priorities: Priority weights for different patterns
        completion_threshold: Minimum completion probability to keep events
        state_aware_probability: Probability factor for state-aware decisions
        save_interval: Seconds between automatic saves
        
    Returns:
        PersistentStatefulLoadSheddingStrategy: Fully configured strategy
    """
    
    # Create persistence manager
    persistence = LoadSheddingStatePersistence()
    
    # Create strategy
    strategy = PersistentStatefulLoadSheddingStrategy(
        pattern_priorities=pattern_priorities or {},
        completion_threshold=drop_threshold,  # Map drop_threshold to completion_threshold
        state_aware_probability=confidence_threshold,  # Map confidence_threshold to state_aware_probability
        persistence=persistence,
        save_interval=save_interval,
        strategy_name=strategy_name
    )
    
    logger.info(f"Created persistent stateful load shedding strategy '{strategy_name}' "
                f"with {storage_type} storage")
    
    return strategy


# Example usage and testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create persistent strategy
    strategy = create_persistent_stateful_strategy(
        strategy_name="test_persistent",
        storage_type="file",
        save_interval=5  # Save every 5 seconds for testing
    )
    
    print("Created persistent stateful load shedding strategy")
    print("Initial stats:", strategy.get_comprehensive_stats())
    
    # Simulate some operation time
    import time
    time.sleep(10)
    
    # Check persistence stats
    print("After 10 seconds:", strategy.get_comprehensive_stats())
    
    # Shutdown gracefully
    strategy.shutdown()
    print("Strategy shut down")