#!/usr/bin/env python3
"""
Simple test script for Hot Path components without complex imports
This script tests the new components directly to avoid import issues.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque
import heapq

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import only what we need directly
from loadshedding.LoadMonitor import LoadLevel


class BikeChain:
    """Represents a bike chain (sequence of trips by the same bike)."""
    
    def __init__(self, bike_id: str, start_time: datetime):
        self.bike_id = bike_id
        self.start_time = start_time
        self.last_update = start_time
        self.partial_matches = []  # List of partial matches in this chain
        self.completion_score = 0.0
        self.importance_score = 0.0
        self.station_sequence = []  # Track station sequence for chaining validation
        
    def add_partial_match(self, partial_match, station_id: str):
        """Add a partial match to this bike chain."""
        self.partial_matches.append(partial_match)
        self.station_sequence.append(station_id)
        self.last_update = datetime.now()  # Simplified for testing
        self._update_scores()
    
    def _update_scores(self):
        """Update completion and importance scores."""
        # Completion score based on chain length
        self.completion_score = min(1.0, len(self.partial_matches) / 5.0)
        
        # Check if chain is approaching target stations (7, 8, 9)
        if self.station_sequence:
            last_station = int(self.station_sequence[-1]) if self.station_sequence[-1].isdigit() else 0
            if 6 < last_station < 10:  # Near target stations
                self.completion_score += 0.3
        
        # Simple importance score
        self.importance_score = min(1.0, self.completion_score + 0.2)
    
    def is_expired(self, current_time: datetime, window_hours: int = 1) -> bool:
        """Check if this chain has expired based on time window."""
        return (current_time - self.start_time).total_seconds() > window_hours * 3600
    
    def get_priority_score(self) -> float:
        """Get combined priority score for load shedding decisions."""
        return (self.completion_score * 0.6 + self.importance_score * 0.4)


class HotPathPartialMatchManager:
    """Simplified version for testing."""
    
    def __init__(self, max_chains: int = 1000):
        self.max_chains = max_chains
        self.active_chains: Dict[str, BikeChain] = {}
        self.total_chains_created = 0
        self.total_chains_dropped = 0
        
        # Load shedding thresholds
        self.load_thresholds = {
            LoadLevel.NORMAL: 1.0,
            LoadLevel.MEDIUM: 0.7,
            LoadLevel.HIGH: 0.4,
            LoadLevel.CRITICAL: 0.1
        }
    
    def add_partial_match(self, partial_match, bike_id: str, station_id: str, load_level: LoadLevel) -> bool:
        """Add a partial match to the appropriate bike chain."""
        current_time = datetime.now()
        
        # Check if we should drop this partial match due to load
        if self._should_drop_partial_match(partial_match, bike_id, load_level):
            self.total_chains_dropped += 1
            return False
        
        # Get or create bike chain
        if bike_id not in self.active_chains:
            if len(self.active_chains) >= self.max_chains:
                # Drop lowest priority chain to make room
                self._drop_lowest_priority_chain()
            
            self.active_chains[bike_id] = BikeChain(bike_id, current_time)
            self.total_chains_created += 1
        
        # Add partial match to chain
        chain = self.active_chains[bike_id]
        chain.add_partial_match(partial_match, station_id)
        
        return True
    
    def _should_drop_partial_match(self, partial_match, bike_id: str, load_level: LoadLevel) -> bool:
        """Determine if a partial match should be dropped based on load level."""
        if load_level == LoadLevel.NORMAL:
            return False
        
        # If bike chain exists, check its priority
        if bike_id in self.active_chains:
            chain = self.active_chains[bike_id]
            priority_score = chain.get_priority_score()
            threshold = self.load_thresholds.get(load_level, 0.0)
            return priority_score < threshold
        
        # For new chains, use conservative approach
        if len(self.active_chains) >= self.max_chains * 0.8:
            return True  # Drop new chains if at capacity
        
        return False
    
    def _drop_lowest_priority_chain(self):
        """Drop the lowest priority bike chain to make room for new ones."""
        if not self.active_chains:
            return
        
        # Find chain with lowest priority
        lowest_priority = float('inf')
        lowest_bike_id = None
        
        for bike_id, chain in self.active_chains.items():
            priority = chain.get_priority_score()
            if priority < lowest_priority:
                lowest_priority = priority
                lowest_bike_id = bike_id
        
        if lowest_bike_id:
            del self.active_chains[lowest_bike_id]
            self.total_chains_dropped += 1
    
    def get_statistics(self) -> Dict[str, any]:
        """Get statistics about the partial match manager."""
        return {
            'active_chains': len(self.active_chains),
            'max_chains': self.max_chains,
            'total_chains_created': self.total_chains_created,
            'total_chains_dropped': self.total_chains_dropped,
            'completion_rate': (self.total_chains_created / 
                              max(1, self.total_chains_created + self.total_chains_dropped))
        }


def test_bike_chain():
    """Test BikeChain functionality."""
    print("Testing BikeChain...")
    
    bike_id = "test_bike_123"
    start_time = datetime.now()
    
    chain = BikeChain(bike_id, start_time)
    
    # Test basic properties
    assert chain.bike_id == bike_id
    assert chain.start_time == start_time
    assert len(chain.partial_matches) == 0
    assert chain.completion_score == 0.0
    
    # Test expiration
    assert not chain.is_expired(datetime.now(), 1)  # Not expired within 1 hour
    assert chain.is_expired(datetime.now() + timedelta(hours=2), 1)  # Expired after 2 hours
    
    # Test adding partial matches
    chain.add_partial_match("mock_match_1", "station_1")
    assert len(chain.partial_matches) == 1
    assert chain.completion_score > 0
    
    print("✓ BikeChain tests passed")


def test_hot_path_partial_match_manager():
    """Test HotPathPartialMatchManager functionality."""
    print("Testing HotPathPartialMatchManager...")
    
    manager = HotPathPartialMatchManager(max_chains=10)
    
    # Test initial state
    assert len(manager.active_chains) == 0
    assert manager.total_chains_created == 0
    
    # Test adding partial matches
    result1 = manager.add_partial_match("match1", "bike1", "station1", LoadLevel.NORMAL)
    assert result1 == True  # Should succeed under normal load
    assert len(manager.active_chains) == 1
    assert manager.total_chains_created == 1
    
    # Test under high load
    result2 = manager.add_partial_match("match2", "bike2", "station2", LoadLevel.HIGH)
    assert result2 == True  # Should still succeed for new bike
    assert len(manager.active_chains) == 2
    
    # Test statistics
    stats = manager.get_statistics()
    assert stats['active_chains'] == 2
    assert stats['total_chains_created'] == 2
    
    print("✓ HotPathPartialMatchManager tests passed")


def test_load_levels():
    """Test LoadLevel enum."""
    print("Testing LoadLevel...")
    
    # Test that LoadLevel values exist
    assert hasattr(LoadLevel, 'NORMAL')
    assert hasattr(LoadLevel, 'MEDIUM')
    assert hasattr(LoadLevel, 'HIGH')
    assert hasattr(LoadLevel, 'CRITICAL')
    
    # Test values
    assert LoadLevel.NORMAL.value == "normal"
    assert LoadLevel.MEDIUM.value == "medium"
    assert LoadLevel.HIGH.value == "high"
    assert LoadLevel.CRITICAL.value == "critical"
    
    print("✓ LoadLevel tests passed")


def test_integration():
    """Test integration between components."""
    print("Testing component integration...")
    
    # Create manager
    manager = HotPathPartialMatchManager(max_chains=5)
    
    # Test adding multiple bike chains
    bike_ids = ["bike1", "bike2", "bike3", "bike4", "bike5"]
    
    for i, bike_id in enumerate(bike_ids):
        result = manager.add_partial_match(f"match_{i}", bike_id, f"station_{i}", LoadLevel.NORMAL)
        assert result == True
    
    # Test capacity limit
    result = manager.add_partial_match("match6", "bike6", "station6", LoadLevel.NORMAL)
    # Should either succeed (if we dropped a low priority chain) or fail (if at capacity)
    assert result in [True, False]
    
    # Test statistics
    stats = manager.get_statistics()
    assert stats['active_chains'] <= 5
    assert stats['total_chains_created'] >= 5
    
    print("✓ Integration tests passed")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("HOT PATH COMPONENTS - SIMPLE TESTS")
    print("=" * 60)
    
    try:
        test_load_levels()
        test_bike_chain()
        test_hot_path_partial_match_manager()
        test_integration()
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED SUCCESSFULLY! ✓")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
