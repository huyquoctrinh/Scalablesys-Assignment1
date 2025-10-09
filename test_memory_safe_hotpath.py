#!/usr/bin/env python3
"""
Memory-safe test for hot path detection to prevent system kills.
"""

import os
import sys
import time
import psutil
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_hotpath_demo import run_enhanced_hotpath_demo

def monitor_memory_usage():
    """Monitor memory usage during test."""
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    return memory_mb

def test_memory_safe_hotpath():
    """Test hot path detection with memory safety."""
    print("=" * 60)
    print("MEMORY-SAFE HOT PATH DETECTION TEST")
    print("=" * 60)
    
    # Monitor initial memory
    initial_memory = monitor_memory_usage()
    print(f"Initial memory usage: {initial_memory:.1f} MB")
    
    # Test with very small dataset
    csv_pattern = "citybike_dataset/201306*.csv"
    
    print(f"Testing with pattern: {csv_pattern}")
    print("Max events: 100 (very conservative)")
    print("Threads: 1 (single-threaded)")
    print("Benchmark: Disabled")
    
    try:
        start_time = time.time()
        
        results = run_enhanced_hotpath_demo(
            csv_files=csv_pattern,
            max_events=100,  # Very small for safety
            run_benchmark=False,
            num_threads=1
        )
        
        end_time = time.time()
        final_memory = monitor_memory_usage()
        
        print(f"\n{'=' * 60}")
        print("TEST RESULTS")
        print(f"{'=' * 60}")
        print(f"✅ Test completed successfully!")
        print(f"⏱️  Duration: {end_time - start_time:.2f} seconds")
        print(f"💾 Initial memory: {initial_memory:.1f} MB")
        print(f"💾 Final memory: {final_memory:.1f} MB")
        print(f"📈 Memory increase: {final_memory - initial_memory:.1f} MB")
        
        if results:
            for config_name, result in results.items():
                print(f"\n{config_name}:")
                print(f"  Events processed: {result['events_processed']}")
                print(f"  Matches found: {result['analysis']['total_matches']}")
        
        # Memory safety check
        if final_memory > initial_memory + 500:
            print(f"⚠️  WARNING: High memory usage increase!")
        else:
            print(f"✅ Memory usage is under control")
            
        return True
        
    except Exception as e:
        final_memory = monitor_memory_usage()
        print(f"\n❌ Test failed with error: {e}")
        print(f"💾 Memory at failure: {final_memory:.1f} MB")
        return False

def test_incremental_sizes():
    """Test with incrementally larger datasets to find safe limits."""
    print(f"\n{'=' * 60}")
    print("INCREMENTAL SIZE TESTING")
    print(f"{'=' * 60}")
    
    test_sizes = [50, 100, 200, 500]
    
    for size in test_sizes:
        print(f"\nTesting with {size} events...")
        
        initial_memory = monitor_memory_usage()
        
        try:
            start_time = time.time()
            
            results = run_enhanced_hotpath_demo(
                csv_files="citybike_dataset/201306*.csv",
                max_events=size,
                run_benchmark=False,
                num_threads=1
            )
            
            end_time = time.time()
            final_memory = monitor_memory_usage()
            memory_increase = final_memory - initial_memory
            
            print(f"  ✅ Size {size}: {end_time - start_time:.1f}s, +{memory_increase:.1f}MB")
            
            # Stop if memory increase is too high
            if memory_increase > 300:
                print(f"  ⚠️  Memory increase too high, stopping incremental test")
                break
                
        except Exception as e:
            final_memory = monitor_memory_usage()
            print(f"  ❌ Size {size} failed: {e}")
            print(f"  💾 Memory at failure: {final_memory:.1f}MB")
            break

if __name__ == "__main__":
    print("Memory-Safe Hot Path Detection Test")
    print(f"Python version: {sys.version}")
    print(f"Available memory: {psutil.virtual_memory().available / 1024 / 1024 / 1024:.1f} GB")
    
    # Run basic test
    success = test_memory_safe_hotpath()
    
    if success:
        # Run incremental tests if basic test passes
        test_incremental_sizes()
    else:
        print("Basic test failed, skipping incremental tests")
    
    print("\nTest completed!")
