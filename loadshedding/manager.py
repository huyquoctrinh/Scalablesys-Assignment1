# loadshedding/manager.py
"""
Singleton manager for load shedding across the entire CEP system.
"""
import threading
from datetime import datetime
from typing import Dict, Any
from loadshedding.config import LoadSheddingConfig
from loadshedding.shedder import HotPathLoadShedder
from loadshedding.monitor import OverloadMonitor


class LoadSheddingManager:
    """
    Singleton that manages load shedding state across all nodes.
    Thread-safe implementation.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Only initialize once
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._config = None
        self._shedder = None
        self._monitor = None
        self._enabled = False
        
        # Track partial matches per node (using node id as key)
        self._node_partial_matches: Dict[int, list] = {}
        self._node_locks: Dict[int, threading.Lock] = {}
        
    def configure(self, config: LoadSheddingConfig):
        """Configure the load shedding manager"""
        self._config = config
        self._enabled = config.enabled if config else False
        
        if self._enabled:
            self._shedder = HotPathLoadShedder(config)
            self._monitor = OverloadMonitor(
                window_size=100,
                latency_threshold=0.1,
                queue_threshold=1000
            )
    
    def reset(self):
        """Reset manager state (useful for testing)"""
        self._config = None
        self._shedder = None
        self._monitor = None
        self._enabled = False
        self._node_partial_matches.clear()
        self._node_locks.clear()
    
    def is_enabled(self):
        """Check if load shedding is enabled"""
        return self._enabled
    
    def register_node(self, node_id: int):
        """Register a node for load shedding tracking"""
        if node_id not in self._node_locks:
            self._node_locks[node_id] = threading.Lock()
    
    def intercept_partial_matches(self, node_id: int, partial_matches: list, 
                                  current_time: datetime) -> list:
        """
        Intercept partial matches from a node and apply load shedding.
        This is called after a node processes events.
        
        Args:
            node_id: Unique identifier for the node
            partial_matches: Current partial matches in the node
            current_time: Current timestamp
            
        Returns:
            Filtered partial matches after load shedding
        """
        if not self._enabled or not self._shedder:
            return partial_matches
        
        # Get or create lock for this node
        if node_id not in self._node_locks:
            self.register_node(node_id)
        
        with self._node_locks[node_id]:
            if self._shedder.should_shed_load(partial_matches):
                return self._shedder.shed_partial_matches(
                    partial_matches,
                    current_time
                )
        
        return partial_matches
    
    def record_event_processing(self, arrival_time, processing_time, 
                               queue_size, partial_match_count):
        """Record event processing metrics"""
        if self._monitor:
            self._monitor.record_event(
                arrival_time,
                processing_time,
                queue_size,
                partial_match_count
            )
    
    def check_overload(self):
        """Check if system is overloaded"""
        if self._monitor:
            return self._monitor.check_overload()
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive load shedding statistics"""
        if not self._enabled:
            return None
        
        stats = {}
        
        if self._shedder:
            stats.update(self._shedder.get_statistics())
        
        if self._monitor:
            monitor_stats = self._monitor.get_statistics()
            stats['avg_throughput_eps'] = monitor_stats.get('throughput_eps', 0)
            stats['avg_latency'] = monitor_stats.get('avg_latency', 0)
            stats['p95_latency'] = monitor_stats.get('p95_latency', 0)
            stats['current_load_level'] = ('overload' if monitor_stats.get('overload_detected') 
                                          else 'normal')
        
        return stats
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        return cls()