#!/usr/bin/env python3
"""
Test script for memory-optimized hot path detection.
This script runs a minimal test to validate the memory safety features.
"""

import os
import sys
import psutil
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_memory_optimized_hotpath():
    """Test with very small dataset and detailed logging."""
    
    from memory_optimized_hotpath_demo import (
        MemoryOptimizedHotPathStream, 
        create_memory_optimized_hot_path_patterns,
        HotPathOutputStream,
        HotPathDataFormatter,
        MemoryMonitor
    )
    from CEP import CEP
    
    print("=" * 60)
    print("TESTING MEMORY-OPTIMIZED HOT PATH DETECTION")
    print("=" * 60)
    
    # Create memory monitor
    memory_monitor = MemoryMonitor(memory_limit_percent=95)  # Very lenient
    memory_monitor.start_monitoring()
    
    # Create patterns with debugging
    patterns, kc_condition = create_memory_optimized_hot_path_patterns(memory_monitor)
    print(f"✓ Created {len(patterns)} patterns")
    
    # Create small input stream
    csv_files = ["citybike_dataset/201306-citibike-tripdata.csv"]
    if not os.path.exists(csv_files[0]):
        print(f"❌ CSV file not found: {csv_files[0]}")
        return
        
    input_stream = MemoryOptimizedHotPathStream(csv_files, max_events=200, num_threads=1)
    print(f"✓ Created input stream with {input_stream.count} events")
    
    # Show sample events
    print("\n📊 SAMPLE EVENTS:")
    sample_count = 0
    stream_copy = []
    while not input_stream._stream.empty():
        event_data = input_stream._stream.get()
        stream_copy.append(event_data)
        if sample_count < 5:
            print(f"  Event {sample_count + 1}: bikeid={event_data.get('bikeid')}, "
                  f"start={event_data.get('start_station_id')}, "
                  f"end={event_data.get('end_station_id')}, "
                  f"time={event_data.get('ts')}")
        sample_count += 1
    
    # Restore stream
    for event_data in stream_copy:
        input_stream._stream.put(event_data)
    
    # Create CEP and output
    cep = CEP(patterns)
    output_stream = HotPathOutputStream("test_output.txt", is_async=False)
    data_formatter = HotPathDataFormatter()
    
    print(f"\n🚀 RUNNING CEP PROCESSING...")
    print(f"   Max events: 200")
    print(f"   Pattern time window: 45 minutes")
    print(f"   Max chain length: 15")
    
    # Run processing
    duration = cep.run(input_stream, output_stream, data_formatter)
    
    # Results
    output_stream.close()
    memory_monitor.stop_monitoring()
    
    print(f"\n📈 RESULTS:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Events processed: {input_stream.count}")
    print(f"   Pattern matches: {len(output_stream.matches)}")
    
    # KC condition statistics
    if hasattr(kc_condition, 'get_statistics'):
        kc_stats = kc_condition.get_statistics()
        print(f"\n🔍 KC CONDITION STATISTICS:")
        print(f"   Chains processed: {kc_stats['chains_processed']}")
        print(f"   Chains truncated: {kc_stats['chains_truncated']}")
        print(f"   Memory safe calls: {kc_stats['memory_safe_calls']}")
        print(f"   Truncation rate: {kc_stats['truncation_rate']:.2%}")
    
    # Memory statistics
    memory_status = memory_monitor.get_memory_status()
    print(f"\n💾 MEMORY STATUS:")
    print(f"   System memory: {memory_status['system_memory_percent']:.1f}%")
    print(f"   Process memory: {memory_status['process_memory_mb']:.1f}MB")
    print(f"   Memory alerts: {memory_status['alert_count']}")
    
    if len(output_stream.matches) == 0:
        print(f"\n⚠️  NO MATCHES FOUND - ANALYZING DATA...")
        
        # Analyze the data for clues
        bike_ids = set()
        station_ids = set()
        target_endings = 0
        
        for event_data in stream_copy:
            if isinstance(event_data, dict):
                bike_ids.add(event_data.get('bikeid'))
                station_ids.add(event_data.get('start_station_id'))
                station_ids.add(event_data.get('end_station_id'))
                
                end_station = event_data.get('end_station_id')
                if end_station:
                    try:
                        if 7 <= int(end_station) <= 9:
                            target_endings += 1
                    except (ValueError, TypeError):
                        pass
        
        print(f"   Unique bikes: {len(bike_ids)}")
        print(f"   Unique stations: {len(station_ids)}")
        print(f"   Trips ending in 7-9: {target_endings}")
        print(f"   Sample bike IDs: {list(bike_ids)[:10]}")
        
        # Show station range
        numeric_stations = []
        for s in station_ids:
            if s and str(s).isdigit():
                numeric_stations.append(int(s))
        if numeric_stations:
            numeric_stations.sort()
            print(f"   Station range: {numeric_stations[0]} to {numeric_stations[-1]}")
    else:
        print(f"\n✅ SUCCESS! Found {len(output_stream.matches)} pattern matches")
        for i, match in enumerate(output_stream.matches[:3]):
            print(f"   Match {i+1}: {str(match)[:100]}...")
    
    # Check initial memory
    initial_memory = psutil.virtual_memory().percent
    initial_process = psutil.Process().memory_info().rss / 1024 / 1024
    
    print(f"Initial system memory: {initial_memory:.1f}%")
    print(f"Initial process memory: {initial_process:.1f} MB")
    
    # Import the memory-optimized module
    try:
        from memory_optimized_hotpath_demo import run_memory_optimized_hotpath_demo
        print("✓ Successfully imported memory-optimized module")
    except ImportError as e:
        print(f"✗ Failed to import module: {e}")
        return False
    
    # Test with a small dataset
    csv_pattern = "citybike_dataset/201306*.csv"  # Use 2013 data for reliability
    
    print(f"\nTesting with pattern: {csv_pattern}")
    print("Using conservative settings: 1000 events, 1 thread")
    
    try:
        results = run_memory_optimized_hotpath_demo(
            csv_files=csv_pattern,
            max_events=1000,  # Very small test
            num_threads=1     # Single thread for safety
        )
        
        # Check final memory
        final_memory = psutil.virtual_memory().percent
        final_process = psutil.Process().memory_info().rss / 1024 / 1024
        memory_increase = final_process - initial_process
        
        print(f"\n{'=' * 60}")
        print("MEMORY TEST RESULTS")
        print(f"{'=' * 60}")
        print(f"Final system memory: {final_memory:.1f}%")
        print(f"Final process memory: {final_process:.1f} MB")
        print(f"Memory increase: {memory_increase:.1f} MB")
        print(f"Memory increase rate: {(memory_increase / initial_process) * 100:.1f}%")
        
        # Validate results
        success = True
        
        if memory_increase > 500:  # More than 500MB increase is concerning
            print(f"⚠️  WARNING: Large memory increase ({memory_increase:.1f} MB)")
            success = False
        else:
            print(f"✓ Memory increase within acceptable range ({memory_increase:.1f} MB)")
            
        if final_memory > 90:
            print(f"⚠️  WARNING: High system memory usage ({final_memory:.1f}%)")
            success = False
        else:
            print(f"✓ System memory usage acceptable ({final_memory:.1f}%)")
            
        if results:
            total_matches = sum(r['analysis']['total_matches'] for r in results.values())
            print(f"✓ Processing completed with {total_matches} matches found")
        else:
            print("⚠️  No results returned")
            success = False
            
        return success
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the memory optimization test."""
    print("Memory-Optimized Hot Path Detection Test")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    
    # Check if data files exist
    import glob
    csv_files = glob.glob("citybike_dataset/201306*.csv")
    if not csv_files:
        print("✗ No CitiBike data files found. Please ensure citybike_dataset/ contains CSV files.")
        return False
        
    print(f"✓ Found {len(csv_files)} data files")
    
    # Run the test
    success = test_memory_optimized_hotpath()
    
    if success:
        print(f"\n🎉 MEMORY OPTIMIZATION TEST PASSED!")
        print("The memory-optimized version is working correctly.")
        print("\nRecommended usage:")
        print("  python memory_optimized_hotpath_demo.py --events 3000 --threads 2")
    else:
        print(f"\n❌ MEMORY OPTIMIZATION TEST FAILED!")
        print("Please review the memory optimization implementation.")
        
    return success

if __name__ == "__main__":
    main()
