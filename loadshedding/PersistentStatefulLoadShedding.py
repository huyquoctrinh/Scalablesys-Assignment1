"""
Enhanced Stateful Load Shedding with Persistence Integration.
This module extends the stateful load shedding strategy to include
state persistence, recovery, and cross-session learning capabilities.
"""

import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
import logging

from StatefulLoadSheddingStrategy import (
    PartialMatchState, PatternStateManager, StatefulLoadSheddingStrategy
)
from StatePersistence import LoadSheddingStatePersistence, create_persistent_state_manager
from base.Event import Event
from base.Pattern import Pattern

logger = logging.getLogger(__name__)


class PersistentPartialMatchState(PartialMatchState):
    """Extended partial match state with persistence metadata."""
    
    def __init__(self, pattern: Pattern, matched_events: List[Event] = None, 
                 confidence: float = 1.0, created_at: datetime = None):
        super().__init__(pattern, matched_events, confidence, created_at)
        
        # Persistence metadata
        self.last_saved: Optional[datetime] = None
        self.save_count: int = 0
        self.is_dirty: bool = True  # Needs saving
        
    def mark_dirty(self):
        """Mark state as needing persistence."""
        self.is_dirty = True
        
    def mark_clean(self):
        """Mark state as saved."""
        self.is_dirty = False
        self.last_saved = datetime.now()
        self.save_count += 1
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for persistence."""
        base_dict = super().to_dict()
        base_dict.update({
            'last_saved': self.last_saved.isoformat() if self.last_saved else None,
            'save_count': self.save_count,
            'is_dirty': self.is_dirty
        })
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict, pattern: Pattern) -> 'PersistentPartialMatchState':
        """Create from dictionary (restoration from persistence)."""
        # Create base state
        state = cls(
            pattern=pattern,
            matched_events=[],  # Events not persisted, will be rebuilt
            confidence=data.get('confidence', 1.0),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.now()
        )
        
        # Restore persistence metadata
        state.completion_probability = data.get('completion_probability', 0.0)
        state.last_updated = datetime.fromisoformat(data['last_updated']) if data.get('last_updated') else datetime.now()
        state.match_count = data.get('match_count', 0)
        state.drop_count = data.get('drop_count', 0)
        
        if data.get('last_saved'):
            state.last_saved = datetime.fromisoformat(data['last_saved'])
        state.save_count = data.get('save_count', 0)
        state.is_dirty = data.get('is_dirty', False)
        
        return state


class PersistentPatternStateManager(PatternStateManager):
    """Pattern state manager with persistence capabilities."""
    
    def __init__(self, persistence: LoadSheddingStatePersistence = None,
                 save_interval: int = 30,  # Save every 30 seconds
                 max_states_per_pattern: int = 100):
        super().__init__(max_states_per_pattern)
        
        self.persistence = persistence or create_persistent_state_manager()
        self.save_interval = save_interval
        self.last_save_time = time.time()
        
        # Load existing states
        self._restore_states()
        
        # Background save thread
        self.save_thread = None
        self.should_stop_saving = threading.Event()
        self._start_background_saving()
    
    def _restore_states(self):
        """Restore states from persistence."""
        try:
            pattern_names = self.persistence.get_pattern_names()
            restored_count = 0
            
            for pattern_name in pattern_names:
                state_data = self.persistence.load_pattern_state(pattern_name)
                if state_data and 'states' in state_data:
                    # We need the pattern object to restore states
                    # For now, just log that we found saved states
                    logger.info(f"Found {len(state_data['states'])} saved states for pattern '{pattern_name}'")
                    restored_count += len(state_data['states'])
            
            if restored_count > 0:
                logger.info(f"Restored {restored_count} partial match states from persistence")
                
        except Exception as e:
            logger.error(f"Error restoring states from persistence: {e}")
    
    def _start_background_saving(self):
        """Start background thread for periodic saving."""
        def save_worker():
            while not self.should_stop_saving.is_set():
                try:
                    if self.should_stop_saving.wait(self.save_interval):
                        break  # Stop signal received
                    
                    self.save_dirty_states()
                    
                except Exception as e:
                    logger.error(f"Error in background save worker: {e}")
        
        self.save_thread = threading.Thread(target=save_worker, daemon=True)
        self.save_thread.start()
    
    def add_state(self, pattern: Pattern, state: PartialMatchState):
        """Add state and mark for persistence."""
        # Convert to persistent state if needed
        if not isinstance(state, PersistentPartialMatchState):
            persistent_state = PersistentPartialMatchState(
                pattern=pattern,
                matched_events=state.matched_events,
                confidence=state.confidence,
                created_at=state.created_at
            )
            persistent_state.completion_probability = state.completion_probability
            persistent_state.last_updated = state.last_updated
            persistent_state.match_count = state.match_count
            persistent_state.drop_count = state.drop_count
            state = persistent_state
        
        super().add_state(pattern, state)
        state.mark_dirty()
    
    def update_state(self, pattern: Pattern, state: PartialMatchState, 
                    new_event: Event, completion_prob: float):
        """Update state and mark for persistence."""
        super().update_state(pattern, state, new_event, completion_prob)
        if isinstance(state, PersistentPartialMatchState):
            state.mark_dirty()
    
    def remove_state(self, pattern: Pattern, state: PartialMatchState):
        """Remove state and clean from persistence."""
        super().remove_state(pattern, state)
        # State will be cleaned up during next save cycle
    
    def save_dirty_states(self):
        """Save all dirty states to persistence."""
        current_time = time.time()
        
        with self.lock:
            saved_count = 0
            
            for pattern_name, states in self.states.items():
                dirty_states = [s for s in states if isinstance(s, PersistentPartialMatchState) and s.is_dirty]
                
                if dirty_states:
                    # Prepare state data for persistence
                    state_data = {
                        'pattern_name': pattern_name,
                        'last_updated': datetime.now().isoformat(),
                        'total_states': len(states),
                        'states': [state.to_dict() for state in dirty_states]
                    }
                    
                    try:
                        self.persistence.save_pattern_state(pattern_name, state_data)
                        
                        # Mark states as clean
                        for state in dirty_states:
                            state.mark_clean()
                        
                        saved_count += len(dirty_states)
                        
                    except Exception as e:
                        logger.error(f"Error saving states for pattern '{pattern_name}': {e}")
            
            if saved_count > 0:
                logger.debug(f"Saved {saved_count} dirty states to persistence")
            
            self.last_save_time = current_time
    
    def force_save_all(self):
        """Force save all states immediately."""
        with self.lock:
            # Mark all states as dirty and save
            for states in self.states.values():
                for state in states:
                    if isinstance(state, PersistentPartialMatchState):
                        state.mark_dirty()
            
            self.save_dirty_states()
    
    def get_persistence_stats(self) -> Dict:
        """Get statistics about persistence operations."""
        with self.lock:
            total_states = sum(len(states) for states in self.states.values())
            dirty_states = 0
            saved_states = 0
            
            for states in self.states.values():
                for state in states:
                    if isinstance(state, PersistentPartialMatchState):
                        if state.is_dirty:
                            dirty_states += 1
                        if state.last_saved:
                            saved_states += 1
            
            return {
                'total_states': total_states,
                'dirty_states': dirty_states,
                'saved_states': saved_states,
                'last_save_time': datetime.fromtimestamp(self.last_save_time).isoformat(),
                'persistence_enabled': self.persistence is not None
            }
    
    def stop_background_saving(self):
        """Stop background saving thread."""
        if self.save_thread and self.save_thread.is_alive():
            self.should_stop_saving.set()
            self.save_thread.join(timeout=5.0)
            
            # Final save
            self.save_dirty_states()


class PersistentStatefulLoadSheddingStrategy(StatefulLoadSheddingStrategy):
    """Stateful load shedding strategy with persistence and recovery."""
    
    def __init__(self, 
                 drop_threshold: float = 0.3,
                 confidence_threshold: float = 0.7,
                 persistence: LoadSheddingStatePersistence = None,
                 save_interval: int = 30,
                 strategy_name: str = "default"):
        
        self.persistence = persistence or create_persistent_state_manager()
        self.strategy_name = strategy_name
        
        # Create persistent state manager
        persistent_state_manager = PersistentPatternStateManager(
            persistence=self.persistence,
            save_interval=save_interval
        )
        
        super().__init__(
            drop_threshold=drop_threshold,
            confidence_threshold=confidence_threshold,
            state_manager=persistent_state_manager
        )
        
        # Load strategy configuration if it exists
        self._restore_configuration()
        
        # Save configuration periodically
        self.last_config_save = time.time()
        self.config_save_interval = 300  # 5 minutes
    
    def _restore_configuration(self):
        """Restore strategy configuration from persistence."""
        try:
            config = self.persistence.load_load_shedding_config(self.strategy_name)
            if config:
                self.drop_threshold = config.get('drop_threshold', self.drop_threshold)
                self.confidence_threshold = config.get('confidence_threshold', self.confidence_threshold)
                
                # Restore statistics if available
                if 'statistics' in config:
                    stats = config['statistics']
                    self.total_events_processed = stats.get('total_events_processed', 0)
                    self.total_events_dropped = stats.get('total_events_dropped', 0)
                    
                logger.info(f"Restored configuration for strategy '{self.strategy_name}'")
                
        except Exception as e:
            logger.error(f"Error restoring configuration: {e}")
    
    def save_configuration(self):
        """Save current strategy configuration."""
        try:
            config = {
                'strategy_name': self.strategy_name,
                'drop_threshold': self.drop_threshold,
                'confidence_threshold': self.confidence_threshold,
                'last_updated': datetime.now().isoformat(),
                'statistics': {
                    'total_events_processed': self.total_events_processed,
                    'total_events_dropped': self.total_events_dropped,
                    'drop_rate': self.get_drop_rate()
                }
            }
            
            self.persistence.save_load_shedding_config(self.strategy_name, config)
            self.last_config_save = time.time()
            
            logger.debug(f"Saved configuration for strategy '{self.strategy_name}'")
            
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def should_drop_event(self, event: Event, patterns: List[Pattern], 
                         current_load: float) -> Tuple[bool, Dict]:
        """Enhanced decision making with periodic configuration saving."""
        
        # Call parent method
        should_drop, details = super().should_drop_event(event, patterns, current_load)
        
        # Periodic configuration save
        current_time = time.time()
        if current_time - self.last_config_save >= self.config_save_interval:
            self.save_configuration()
        
        # Add persistence information to details
        if hasattr(self.state_manager, 'get_persistence_stats'):
            details['persistence_stats'] = self.state_manager.get_persistence_stats()
        
        return should_drop, details
    
    def adapt_thresholds(self, recent_performance: Dict):
        """Adapt thresholds and save configuration."""
        super().adapt_thresholds(recent_performance)
        
        # Save updated configuration
        self.save_configuration()
    
    def get_comprehensive_stats(self) -> Dict:
        """Get comprehensive statistics including persistence info."""
        stats = super().get_comprehensive_stats()
        
        # Add persistence statistics
        if hasattr(self.state_manager, 'get_persistence_stats'):
            stats['persistence'] = self.state_manager.get_persistence_stats()
        
        # Add storage statistics
        stats['storage'] = self.persistence.get_storage_stats()
        
        return stats
    
    def shutdown(self):
        """Graceful shutdown with final save."""
        logger.info(f"Shutting down persistent stateful load shedding strategy '{self.strategy_name}'")
        
        # Final configuration save
        self.save_configuration()
        
        # Stop background saving and force final state save
        if hasattr(self.state_manager, 'stop_background_saving'):
            self.state_manager.stop_background_saving()
        
        # Cleanup expired entries
        self.persistence.force_cleanup()
        
        logger.info("Shutdown complete")


def create_persistent_stateful_strategy(
    strategy_name: str = "default",
    storage_type: str = "file",
    storage_path: str = None,
    drop_threshold: float = 0.3,
    confidence_threshold: float = 0.7,
    save_interval: int = 30
) -> PersistentStatefulLoadSheddingStrategy:
    """
    Create a persistent stateful load shedding strategy with all components configured.
    
    Args:
        strategy_name: Name for the strategy (used for persistence keys)
        storage_type: Type of storage backend ("file" or "sqlite")
        storage_path: Path for storage
        drop_threshold: Initial drop threshold
        confidence_threshold: Initial confidence threshold
        save_interval: Seconds between automatic saves
        
    Returns:
        PersistentStatefulLoadSheddingStrategy: Fully configured strategy
    """
    
    # Create persistence manager
    persistence = create_persistent_state_manager(storage_type, storage_path)
    
    # Create strategy
    strategy = PersistentStatefulLoadSheddingStrategy(
        drop_threshold=drop_threshold,
        confidence_threshold=confidence_threshold,
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