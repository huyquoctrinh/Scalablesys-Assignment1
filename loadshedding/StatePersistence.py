"""
Load Shedding State Persistence Layer.
This module provides persistent state management for load shedding strategies,
allowing state to be maintained across system restarts and shared across instances.
"""

import json
import pickle
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class StateSerializer(ABC):
    """Abstract base class for state serialization."""
    
    @abstractmethod
    def serialize(self, state: Dict) -> bytes:
        """Serialize state to bytes."""
        pass
    
    @abstractmethod
    def deserialize(self, data: bytes) -> Dict:
        """Deserialize bytes to state."""
        pass


class JSONStateSerializer(StateSerializer):
    """JSON-based state serializer."""
    
    def serialize(self, state: Dict) -> bytes:
        """Serialize state to JSON bytes."""
        # Handle datetime objects and other non-JSON serializable types
        serializable_state = self._make_serializable(state)
        return json.dumps(serializable_state, separators=(',', ':')).encode('utf-8')
    
    def deserialize(self, data: bytes) -> Dict:
        """Deserialize JSON bytes to state."""
        state = json.loads(data.decode('utf-8'))
        return self._restore_types(state)
    
    def _make_serializable(self, obj):
        """Convert object to JSON-serializable format."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return {'__datetime__': obj.isoformat()}
        elif isinstance(obj, timedelta):
            return {'__timedelta__': obj.total_seconds()}
        elif hasattr(obj, '__dict__'):
            return {'__object__': obj.__class__.__name__, 'data': self._make_serializable(obj.__dict__)}
        else:
            return obj
    
    def _restore_types(self, obj):
        """Restore original types from serialized format."""
        if isinstance(obj, dict):
            if '__datetime__' in obj:
                return datetime.fromisoformat(obj['__datetime__'])
            elif '__timedelta__' in obj:
                return timedelta(seconds=obj['__timedelta__'])
            elif '__object__' in obj:
                # For now, just return the data dict
                return self._restore_types(obj['data'])
            else:
                return {k: self._restore_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._restore_types(item) for item in obj]
        else:
            return obj


class PickleStateSerializer(StateSerializer):
    """Pickle-based state serializer (more efficient but less portable)."""
    
    def serialize(self, state: Dict) -> bytes:
        """Serialize state using pickle."""
        return pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
    
    def deserialize(self, data: bytes) -> Dict:
        """Deserialize pickle data to state."""
        return pickle.loads(data)


class StateStorage(ABC):
    """Abstract base class for state storage backends."""
    
    @abstractmethod
    def store_state(self, key: str, state: Dict, expiry_time: Optional[datetime] = None):
        """Store state with optional expiry."""
        pass
    
    @abstractmethod
    def load_state(self, key: str) -> Optional[Dict]:
        """Load state by key."""
        pass
    
    @abstractmethod
    def delete_state(self, key: str):
        """Delete state by key."""
        pass
    
    @abstractmethod
    def list_keys(self, pattern: Optional[str] = None) -> List[str]:
        """List all state keys matching optional pattern."""
        pass
    
    @abstractmethod
    def cleanup_expired(self):
        """Remove expired state entries."""
        pass


class FileStateStorage(StateStorage):
    """File-based state storage."""
    
    def __init__(self, storage_dir: str = "load_shedding_state", serializer: StateSerializer = None):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.serializer = serializer or JSONStateSerializer()
        self.lock = threading.RLock()
    
    def store_state(self, key: str, state: Dict, expiry_time: Optional[datetime] = None):
        """Store state to file."""
        with self.lock:
            state_file = self.storage_dir / f"{key}.state"
            metadata_file = self.storage_dir / f"{key}.meta"
            
            # Serialize state
            state_data = self.serializer.serialize(state)
            
            # Create metadata
            metadata = {
                'created_at': datetime.now().isoformat(),
                'expiry_time': expiry_time.isoformat() if expiry_time else None,
                'size_bytes': len(state_data)
            }
            
            # Write files atomically
            temp_state_file = state_file.with_suffix('.tmp')
            temp_metadata_file = metadata_file.with_suffix('.tmp')
            
            try:
                with open(temp_state_file, 'wb') as f:
                    f.write(state_data)
                
                with open(temp_metadata_file, 'w') as f:
                    json.dump(metadata, f)
                
                # Atomic rename
                temp_state_file.rename(state_file)
                temp_metadata_file.rename(metadata_file)
                
                logger.debug(f"Stored state for key '{key}' ({len(state_data)} bytes)")
            
            except Exception as e:
                # Cleanup temp files on error
                temp_state_file.unlink(missing_ok=True)
                temp_metadata_file.unlink(missing_ok=True)
                raise e
    
    def load_state(self, key: str) -> Optional[Dict]:
        """Load state from file."""
        with self.lock:
            state_file = self.storage_dir / f"{key}.state"
            metadata_file = self.storage_dir / f"{key}.meta"
            
            if not state_file.exists():
                return None
            
            try:
                # Check expiry
                if metadata_file.exists():
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    if metadata.get('expiry_time'):
                        expiry_time = datetime.fromisoformat(metadata['expiry_time'])
                        if datetime.now() > expiry_time:
                            # State has expired
                            self.delete_state(key)
                            return None
                
                # Load state
                with open(state_file, 'rb') as f:
                    state_data = f.read()
                
                state = self.serializer.deserialize(state_data)
                logger.debug(f"Loaded state for key '{key}' ({len(state_data)} bytes)")
                return state
            
            except Exception as e:
                logger.error(f"Error loading state for key '{key}': {e}")
                return None
    
    def delete_state(self, key: str):
        """Delete state files."""
        with self.lock:
            state_file = self.storage_dir / f"{key}.state"
            metadata_file = self.storage_dir / f"{key}.meta"
            
            state_file.unlink(missing_ok=True)
            metadata_file.unlink(missing_ok=True)
            logger.debug(f"Deleted state for key '{key}'")
    
    def list_keys(self, pattern: Optional[str] = None) -> List[str]:
        """List all state keys."""
        with self.lock:
            state_files = list(self.storage_dir.glob("*.state"))
            keys = [f.stem for f in state_files]
            
            if pattern:
                import fnmatch
                keys = [k for k in keys if fnmatch.fnmatch(k, pattern)]
            
            return keys
    
    def cleanup_expired(self):
        """Remove expired state entries."""
        with self.lock:
            keys = self.list_keys()
            expired_keys = []
            
            for key in keys:
                metadata_file = self.storage_dir / f"{key}.meta"
                if metadata_file.exists():
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        
                        if metadata.get('expiry_time'):
                            expiry_time = datetime.fromisoformat(metadata['expiry_time'])
                            if datetime.now() > expiry_time:
                                expired_keys.append(key)
                    except Exception as e:
                        logger.warning(f"Error checking expiry for key '{key}': {e}")
            
            for key in expired_keys:
                self.delete_state(key)
            
            logger.info(f"Cleaned up {len(expired_keys)} expired state entries")


class SQLiteStateStorage(StateStorage):
    """SQLite-based state storage for better concurrency and querying."""
    
    def __init__(self, db_path: str = "load_shedding_state.db", serializer: StateSerializer = None):
        self.db_path = db_path
        self.serializer = serializer or PickleStateSerializer()
        self.lock = threading.RLock()
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize SQLite database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS state_storage (
                    key TEXT PRIMARY KEY,
                    data BLOB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expiry_time TIMESTAMP,
                    size_bytes INTEGER
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_expiry_time ON state_storage(expiry_time)")
            conn.commit()
    
    def store_state(self, key: str, state: Dict, expiry_time: Optional[datetime] = None):
        """Store state in SQLite."""
        with self.lock:
            state_data = self.serializer.serialize(state)
            
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO state_storage 
                    (key, data, created_at, expiry_time, size_bytes)
                    VALUES (?, ?, ?, ?, ?)
                """, (key, state_data, datetime.now(), expiry_time, len(state_data)))
                conn.commit()
            
            logger.debug(f"Stored state for key '{key}' in SQLite ({len(state_data)} bytes)")
    
    def load_state(self, key: str) -> Optional[Dict]:
        """Load state from SQLite."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT data, expiry_time FROM state_storage 
                    WHERE key = ?
                """, (key,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                # Check expiry
                if row['expiry_time']:
                    expiry_time = datetime.fromisoformat(row['expiry_time'])
                    if datetime.now() > expiry_time:
                        self.delete_state(key)
                        return None
                
                # Deserialize state
                try:
                    state = self.serializer.deserialize(row['data'])
                    logger.debug(f"Loaded state for key '{key}' from SQLite")
                    return state
                except Exception as e:
                    logger.error(f"Error deserializing state for key '{key}': {e}")
                    return None
    
    def delete_state(self, key: str):
        """Delete state from SQLite."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM state_storage WHERE key = ?", (key,))
                conn.commit()
            logger.debug(f"Deleted state for key '{key}' from SQLite")
    
    def list_keys(self, pattern: Optional[str] = None) -> List[str]:
        """List all state keys from SQLite."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                if pattern:
                    cursor = conn.execute("SELECT key FROM state_storage WHERE key LIKE ?", 
                                        (pattern.replace('*', '%'),))
                else:
                    cursor = conn.execute("SELECT key FROM state_storage")
                
                return [row[0] for row in cursor.fetchall()]
    
    def cleanup_expired(self):
        """Remove expired state entries from SQLite."""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    DELETE FROM state_storage 
                    WHERE expiry_time IS NOT NULL AND expiry_time < ?
                """, (datetime.now(),))
                
                deleted_count = cursor.rowcount
                conn.commit()
            
            logger.info(f"Cleaned up {deleted_count} expired state entries from SQLite")


class LoadSheddingStatePersistence:
    """Main interface for load shedding state persistence."""
    
    def __init__(self, 
                 storage: StateStorage = None,
                 auto_cleanup_interval: int = 3600,  # 1 hour
                 default_expiry_hours: int = 24):
        """
        Initialize state persistence.
        
        Args:
            storage: Storage backend to use
            auto_cleanup_interval: Seconds between automatic cleanup runs
            default_expiry_hours: Default expiry time for states in hours
        """
        self.storage = storage or FileStateStorage()
        self.auto_cleanup_interval = auto_cleanup_interval
        self.default_expiry_hours = default_expiry_hours
        
        self.last_cleanup = time.time()
        self.lock = threading.RLock()
    
    def save_pattern_state(self, pattern_name: str, state: Dict, 
                          expiry_hours: Optional[int] = None):
        """Save pattern state with automatic expiry."""
        key = f"pattern_{pattern_name}"
        expiry_time = None
        
        if expiry_hours or self.default_expiry_hours:
            hours = expiry_hours or self.default_expiry_hours
            expiry_time = datetime.now() + timedelta(hours=hours)
        
        self.storage.store_state(key, state, expiry_time)
    
    def load_pattern_state(self, pattern_name: str) -> Optional[Dict]:
        """Load pattern state."""
        key = f"pattern_{pattern_name}"
        return self.storage.load_state(key)
    
    def save_load_shedding_config(self, config_name: str, config: Dict):
        """Save load shedding configuration."""
        key = f"config_{config_name}"
        # Configs don't expire by default
        self.storage.store_state(key, config)
    
    def load_load_shedding_config(self, config_name: str) -> Optional[Dict]:
        """Load load shedding configuration."""
        key = f"config_{config_name}"
        return self.storage.load_state(key)
    
    def save_system_state(self, state: Dict, expiry_hours: int = 1):
        """Save overall system state (short-lived)."""
        key = "system_state"
        expiry_time = datetime.now() + timedelta(hours=expiry_hours)
        self.storage.store_state(key, state, expiry_time)
    
    def load_system_state(self) -> Optional[Dict]:
        """Load overall system state."""
        return self.storage.load_state("system_state")
    
    def get_pattern_names(self) -> List[str]:
        """Get all pattern names with saved state."""
        keys = self.storage.list_keys("pattern_*")
        return [key[8:] for key in keys]  # Remove "pattern_" prefix
    
    def get_config_names(self) -> List[str]:
        """Get all saved configuration names."""
        keys = self.storage.list_keys("config_*")
        return [key[7:] for key in keys]  # Remove "config_" prefix
    
    def cleanup_if_needed(self):
        """Perform cleanup if enough time has passed."""
        current_time = time.time()
        if current_time - self.last_cleanup >= self.auto_cleanup_interval:
            self.storage.cleanup_expired()
            self.last_cleanup = current_time
    
    def force_cleanup(self):
        """Force immediate cleanup of expired entries."""
        self.storage.cleanup_expired()
        self.last_cleanup = time.time()
    
    def get_storage_stats(self) -> Dict:
        """Get statistics about stored state."""
        all_keys = self.storage.list_keys()
        pattern_keys = self.storage.list_keys("pattern_*")
        config_keys = self.storage.list_keys("config_*")
        
        return {
            'total_keys': len(all_keys),
            'pattern_states': len(pattern_keys),
            'configurations': len(config_keys),
            'storage_type': type(self.storage).__name__
        }


# Example usage and integration helpers
def create_persistent_state_manager(storage_type: str = "file", 
                                   storage_path: str = None) -> LoadSheddingStatePersistence:
    """
    Create a configured persistent state manager.
    
    Args:
        storage_type: Type of storage ("file" or "sqlite")
        storage_path: Path for storage (directory for file, db file for sqlite)
        
    Returns:
        LoadSheddingStatePersistence: Configured persistence manager
    """
    if storage_type.lower() == "sqlite":
        storage_path = storage_path or "load_shedding_state.db"
        storage = SQLiteStateStorage(storage_path)
    else:
        storage_path = storage_path or "load_shedding_state"
        storage = FileStateStorage(storage_path)
    
    return LoadSheddingStatePersistence(storage)


def restore_stateful_load_shedding(persistence: LoadSheddingStatePersistence,
                                   strategy_name: str = "default") -> Optional[Dict]:
    """
    Restore stateful load shedding configuration and state.
    
    Args:
        persistence: Persistence manager
        strategy_name: Name of the strategy configuration to restore
        
    Returns:
        Dict: Restored state or None if not found
    """
    # Try to restore configuration
    config = persistence.load_load_shedding_config(strategy_name)
    if not config:
        logger.warning(f"No saved configuration found for strategy '{strategy_name}'")
        return None
    
    # Restore pattern states
    pattern_names = persistence.get_pattern_names()
    pattern_states = {}
    
    for pattern_name in pattern_names:
        state = persistence.load_pattern_state(pattern_name)
        if state:
            pattern_states[pattern_name] = state
    
    # Restore system state
    system_state = persistence.load_system_state()
    
    return {
        'config': config,
        'pattern_states': pattern_states,
        'system_state': system_state
    }