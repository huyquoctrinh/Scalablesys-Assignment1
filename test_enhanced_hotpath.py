#!/usr/bin/env python3
"""
Test script for Enhanced Hot Path Load Shedding Implementation
This script tests the new components to ensure they work correctly.
"""

import os
import sys
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loadshedding import (
    HotPathPartialMatchManager, BikeChain, HotPathLoadSheddingStrategy,
    LatencyAwareHotPathLoadShedding, HotPathBenchmark, LoadLevel
)
from base.Event import Event
from base.PatternMatch import PatternMatch


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
    
    print("✓ BikeChain tests passed")


def test_hot_path_partial_match_manager():
    """Test HotPathPartialMatchManager functionality."""
    print("Testing HotPathPartialMatchManager...")
    
    manager = HotPathPartialMatchManager(max_chains=100, cleanup_interval_sec=1)
    
    # Test initial state
    assert len(manager.active_chains) == 0
    assert manager.total_chains_created == 0
    
    # Test statistics
    stats = manager.get_statistics()
    assert stats['active_chains'] == 0
    assert stats['max_chains'] == 100
    
    print("✓ HotPathPartialMatchManager tests passed")


def test_hot_path_load_shedding_strategy():
    """Test HotPathLoadSheddingStrategy functionality."""
    print("Testing HotPathLoadSheddingStrategy...")
    
    strategy = HotPathLoadSheddingStrategy(
        pattern_priorities={
            'RushHourHotPath': 10.0,
            'RegularHotPath': 5.0
        },
        importance_attributes=['importance', 'priority']
    )
    
    # Test initial state
    assert strategy.name == "HotPathLoadSheddingStrategy"
    assert strategy.total_events_seen == 0
    assert strategy.events_dropped == 0
    
    # Test drop rate calculation
    assert strategy.get_drop_rate() == 0.0
    
    # Test reset statistics
    strategy.reset_statistics()
    assert strategy.total_events_seen == 0
    assert strategy.events_dropped == 0
    
    print("✓ HotPathLoadSheddingStrategy tests passed")


def test_latency_aware_strategy():
    """Test LatencyAwareHotPathLoadShedding functionality."""
    print("Testing LatencyAwareHotPathLoadShedding...")
    
    strategy = LatencyAwareHotPathLoadShedding(
        target_latency_bounds=[0.1, 0.3, 0.5, 0.7, 0.9],
        base_latency_ms=100.0
    )
    
    # Test initial state
    assert strategy.name == "LatencyAwareHotPathLoadShedding"
    assert strategy.base_latency_ms == 100.0
    assert len(strategy.target_latency_bounds) == 5
    
    # Test latency feedback
    strategy.update_latency_feedback(150.0)  # 150% of base latency
    assert strategy.current_latency_ms == 150.0
    assert len(strategy.latency_history) == 1
    
    print("✓ LatencyAwareHotPathLoadShedding tests passed")


def test_hot_path_benchmark():
    """Test HotPathBenchmark functionality."""
    print("Testing HotPathBenchmark...")
    
    benchmark = HotPathBenchmark("test_benchmark_output")
    
    # Test initial state
    assert benchmark.output_dir == "test_benchmark_output"
    assert len(benchmark.latency_bounds) == 5
    assert len(benchmark.load_levels) == 4
    assert benchmark.total_benchmarks == 0
    
    # Test configuration creation
    configs = benchmark._create_test_configurations()
    assert len(configs) > 0
    assert 'No Load Shedding' in configs
    assert 'Hot Path Semantic' in configs
    
    print("✓ HotPathBenchmark tests passed")


def test_integration():
    """Test integration between components."""
    print("Testing component integration...")
    
    # Create strategy with partial match manager
    strategy = HotPathLoadSheddingStrategy()
    
    # Test that partial match manager is initialized
    assert hasattr(strategy, 'partial_match_manager')
    assert strategy.partial_match_manager is not None
    
    # Test statistics integration
    stats = strategy.get_partial_match_statistics()
    assert isinstance(stats, dict)
    assert 'active_chains' in stats
    
    print("✓ Integration tests passed")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("ENHANCED HOT PATH LOAD SHEDDING - COMPONENT TESTS")
    print("=" * 60)
    
    try:
        test_bike_chain()
        test_hot_path_partial_match_manager()
        test_hot_path_load_shedding_strategy()
        test_latency_aware_strategy()
        test_hot_path_benchmark()
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
