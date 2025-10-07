#!/usr/bin/env python3
"""
Test script for memory-optimized CitiBike processing with 20K events
"""

import sys
import os
import time
import psutil
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_memory_scaling():
    """Test memory scaling with different event counts."""
    print("Testing Memory Scaling for CitiBike Processing")
    print("=" * 60)
    
    # Import the enhanced demo (after path setup)
    from enhanced_citybike_demo_hot_path import run_enhanced_demo, get_memory_usage, force_garbage_collection
    
    # Test different event counts
    test_sizes = [1000, 5000, 10000, 20000]
    
    # Find test data file
    test_files = [
        'test_data/201306-citibike-tripdata.csv',
        'test_data_split/201306-citibike-tripdata_part1.csv',
        'test_data/*.csv'
    ]
    
    csv_file = None
    for test_file in test_files:
        if '*' in test_file:
            import glob
            matches = glob.glob(test_file)
            if matches:
                csv_file = test_file
                break
        elif os.path.exists(test_file):
            csv_file = test_file
            break
    
    if not csv_file:
        print("ERROR: No test data found. Please ensure CSV files are available.")
        print("Looked for:", test_files)
        return
    
    print(f"Using data file(s): {csv_file}")
    print()
    
    results = {}
    
    for event_count in test_sizes:
        print(f"Testing with {event_count:,} events...")
        print("-" * 40)
        
        # Force garbage collection before test
        collected = force_garbage_collection()
        print(f"Pre-test cleanup: {collected} objects collected")
        
        # Get initial memory
        initial_memory = get_memory_usage()
        print(f"Initial memory: {initial_memory['rss_mb']:.1f} MB ({initial_memory['percent']:.1f}%)")
        
        # Run the demo
        start_time = time.time()
        
        try:
            demo_results = run_enhanced_demo(
                csv_files=csv_file,
                max_events=event_count,
                force_async=False,
                num_threads=3  # Reduced for memory testing
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Get final memory
            final_memory = get_memory_usage()
            memory_delta = final_memory['rss_mb'] - initial_memory['rss_mb']
            
            print(f"\nResults for {event_count:,} events:")
            print(f"  Duration: {duration:.2f} seconds")
            print(f"  Final memory: {final_memory['rss_mb']:.1f} MB ({final_memory['percent']:.1f}%)")
            print(f"  Memory increase: {memory_delta:+.1f} MB")
            print(f"  Memory per event: {memory_delta*1024/event_count:.2f} KB/event")
            
            # Calculate throughput
            if demo_results:
                config_name = list(demo_results.keys())[0]
                events_processed = demo_results[config_name]['events_processed']
                throughput = events_processed / duration
                print(f"  Events processed: {events_processed:,}")
                print(f"  Throughput: {throughput:.1f} events/sec")
            
            results[event_count] = {
                'duration': duration,
                'initial_memory_mb': initial_memory['rss_mb'],
                'final_memory_mb': final_memory['rss_mb'],
                'memory_delta_mb': memory_delta,
                'memory_per_event_kb': memory_delta*1024/event_count,
                'throughput': throughput if demo_results else 0
            }
            
        except Exception as e:
            print(f"ERROR processing {event_count:,} events: {e}")
            import traceback
            traceback.print_exc()
            results[event_count] = {'error': str(e)}
        
        # Cleanup after each test
        collected = force_garbage_collection()
        print(f"Post-test cleanup: {collected} objects collected")
        
        print()
    
    # Summary
    print("=" * 60)
    print("MEMORY SCALING SUMMARY")
    print("=" * 60)
    
    print(f"{'Events':<10} {'Duration':<10} {'Memory':<12} {'Per Event':<12} {'Throughput':<12}")
    print(f"{'Count':<10} {'(sec)':<10} {'Delta (MB)':<12} {'(KB)':<12} {'(ev/s)':<12}")
    print("-" * 60)
    
    for event_count, result in results.items():
        if 'error' in result:
            print(f"{event_count:<10,} {'ERROR':<10} {'N/A':<12} {'N/A':<12} {'N/A':<12}")
        else:
            print(f"{event_count:<10,} {result['duration']:<10.1f} {result['memory_delta_mb']:<12.1f} "
                  f"{result['memory_per_event_kb']:<12.2f} {result['throughput']:<12.1f}")
    
    # Memory efficiency analysis
    print("\nMemory Efficiency Analysis:")
    successful_results = {k: v for k, v in results.items() if 'error' not in v}
    
    if len(successful_results) >= 2:
        sizes = sorted(successful_results.keys())
        memory_per_event = [successful_results[size]['memory_per_event_kb'] for size in sizes]
        
        print(f"  Memory per event range: {min(memory_per_event):.2f} - {max(memory_per_event):.2f} KB")
        
        # Check if memory usage is scaling linearly or exponentially
        if len(sizes) >= 3:
            small = successful_results[sizes[0]]
            medium = successful_results[sizes[len(sizes)//2]]
            large = successful_results[sizes[-1]]
            
            linear_expected = (large['memory_delta_mb'] / small['memory_delta_mb']) 
            actual_ratio = sizes[-1] / sizes[0]
            
            if linear_expected <= actual_ratio * 1.5:
                print("  ✅ Memory scaling appears LINEAR (good)")
            else:
                print("  ⚠️  Memory scaling may be EXPONENTIAL (needs optimization)")
    
    # Check for 20K event feasibility
    if 20000 in successful_results:
        result_20k = successful_results[20000]
        if result_20k['final_memory_mb'] < 8000:  # Less than 8GB
            print("  ✅ 20K events feasible within reasonable memory limits")
        else:
            print("  ⚠️  20K events uses high memory - consider further optimization")
    
    print(f"\nTesting completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        test_memory_scaling()
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()