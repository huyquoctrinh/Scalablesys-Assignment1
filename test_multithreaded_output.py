#!/usr/bin/env python3
"""
Test script for multi-threaded SimpleOutputStream functionality
"""

import sys
import os
import time
import threading
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the enhanced output stream
from enhanced_citybike_demo_hot_path import SimpleOutputStream

def test_multithreaded_output():
    """Test multi-threaded output stream functionality."""
    print("Testing Multi-threaded SimpleOutputStream")
    print("=" * 50)
    
    # Test configuration
    test_file = "test_mt_output.txt"
    num_items = 1000
    
    # Clean up any existing test file
    if os.path.exists(test_file):
        os.remove(test_file)
    
    # Create multi-threaded output stream
    print(f"Creating multi-threaded output stream for {num_items} items...")
    output_stream = SimpleOutputStream(
        file_path=test_file,
        is_async=False,
        enable_multithreading=True,
        writer_threads=2,
        batch_size=50,
        flush_interval=0.5
    )
    
    # Generate test items
    print("Adding test items...")
    start_time = time.time()
    
    for i in range(num_items):
        test_item = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'data': f'Test item {i}',
            'pattern_name': 'TestPattern' if i % 10 == 0 else None
        }
        output_stream.add_item(test_item)
        
        # Small delay to simulate realistic processing
        if i % 100 == 0:
            time.sleep(0.01)
    
    add_time = time.time() - start_time
    print(f"Items added in {add_time:.2f} seconds")
    
    # Get statistics before closing
    write_stats = output_stream.get_write_statistics()
    analysis = output_stream.get_analysis()
    
    print("\nWrite Statistics:")
    print(f"  Mode: {write_stats['mode']}")
    if write_stats['mode'] == 'multi_threaded':
        print(f"  Writer threads: {write_stats['writer_threads']}")
        print(f"  Batch size: {write_stats['batch_size']}")
        print(f"  Items queued: {write_stats['items_queued']}")
        print(f"  Items written: {write_stats['items_written']}")
        print(f"  Batches written: {write_stats['batches_written']}")
        print(f"  Write errors: {write_stats['write_errors']}")
        print(f"  Queue full events: {write_stats['queue_full_events']}")
        print(f"  Current batch size: {write_stats['current_batch_size']}")
    
    print("\nAnalysis:")
    print(f"  Total matches: {analysis['total_matches']}")
    print(f"  Pattern stats: {analysis['patterns']}")
    
    # Close and get final stats
    print("\nClosing output stream...")
    close_start = time.time()
    output_stream.close()
    close_time = time.time() - close_start
    print(f"Closed in {close_time:.2f} seconds")
    
    # Verify file contents
    if os.path.exists(test_file):
        with open(test_file, 'r') as f:
            lines = f.readlines()
        print(f"\nFile verification:")
        print(f"  Lines written: {len(lines)}")
        print(f"  Expected items: {num_items}")
        print(f"  Success: {len(lines) == num_items}")
        
        # Show first few lines
        print(f"  First 3 lines:")
        for i, line in enumerate(lines[:3]):
            print(f"    {i+1}: {line.strip()[:80]}...")
        
        # Clean up
        os.remove(test_file)
        print(f"  Test file cleaned up")
    else:
        print("ERROR: Output file was not created!")
    
    print("\nTest completed!")

def test_comparison():
    """Compare single-threaded vs multi-threaded performance."""
    print("\n" + "=" * 50)
    print("Performance Comparison Test")
    print("=" * 50)
    
    num_items = 2000
    test_modes = [
        ("Single-threaded", False, 1),
        ("Multi-threaded", True, 2)
    ]
    
    results = {}
    
    for mode_name, enable_mt, threads in test_modes:
        test_file = f"test_perf_{mode_name.lower().replace('-', '_')}.txt"
        
        print(f"\nTesting {mode_name} mode...")
        
        # Clean up
        if os.path.exists(test_file):
            os.remove(test_file)
        
        # Create output stream
        output_stream = SimpleOutputStream(
            file_path=test_file,
            is_async=False,
            enable_multithreading=enable_mt,
            writer_threads=threads,
            batch_size=100,
            flush_interval=0.5
        )
        
        # Time the operation
        start_time = time.time()
        
        for i in range(num_items):
            test_item = {
                'id': i,
                'data': f'Performance test item {i}',
                'pattern_name': 'PerfPattern' if i % 20 == 0 else None
            }
            output_stream.add_item(test_item)
        
        add_time = time.time() - start_time
        
        # Close and measure
        close_start = time.time()
        output_stream.close()
        close_time = time.time() - close_start
        
        total_time = add_time + close_time
        throughput = num_items / total_time
        
        results[mode_name] = {
            'add_time': add_time,
            'close_time': close_time,
            'total_time': total_time,
            'throughput': throughput
        }
        
        print(f"  Add time: {add_time:.3f}s")
        print(f"  Close time: {close_time:.3f}s")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Throughput: {throughput:.1f} items/sec")
        
        # Verify file
        if os.path.exists(test_file):
            with open(test_file, 'r') as f:
                lines = len(f.readlines())
            print(f"  File lines: {lines}")
            os.remove(test_file)
    
    # Compare results
    print(f"\nPerformance Summary:")
    st_result = results["Single-threaded"]
    mt_result = results["Multi-threaded"]
    
    speedup = mt_result['throughput'] / st_result['throughput']
    print(f"  Single-threaded: {st_result['throughput']:.1f} items/sec")
    print(f"  Multi-threaded:  {mt_result['throughput']:.1f} items/sec")
    print(f"  Speedup: {speedup:.2f}x")
    
    if speedup > 1.1:
        print("  ✓ Multi-threading provides performance benefit")
    elif speedup > 0.9:
        print("  ~ Multi-threading performance is comparable")
    else:
        print("  ⚠ Multi-threading may have overhead issues")

if __name__ == "__main__":
    try:
        test_multithreaded_output()
        test_comparison()
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()