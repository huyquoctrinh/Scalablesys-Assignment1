"""Singleton manager for load shedding"""
import threading

class LoadSheddingManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._config = None
        self._enabled = False
        self._stats = {
            'partial_matches_dropped': 0,
            'total_partial_matches': 0,
            'events_processed': 0,
            'drop_rate': 0.0,
            'strategy': 'none',
            'memory_threshold': 0.0
        }
    
    def configure(self, config):
        self._config = config
        self._enabled = config.enabled if config else False
    
    def reset(self):
        self._stats = {
            'partial_matches_dropped': 0,
            'total_partial_matches': 0,
            'events_processed': 0,
            'drop_rate': 0.0,
            'strategy': self._config.strategy_name if self._config else 'none',
            'memory_threshold': self._config.memory_threshold if self._config else 0.0
        }
    
    def is_enabled(self):
        return self._enabled
    
    def get_statistics(self):
        if not self._enabled:
            return None
        return self._stats.copy()
    
    @classmethod
    def get_instance(cls):
        return cls()