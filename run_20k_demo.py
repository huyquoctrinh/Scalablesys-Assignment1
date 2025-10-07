#!/usr/bin/env python3
"""
Run CitiBike demo with 20,000 events using memory optimizations
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    print("CitiBike Hot Path Detection - 20K Events Demo")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Import after path setup
    from enhanced_citybike_demo_hot_path import run_enhanced_demo
    
    # Find data files
    data_patterns = [
        'test_data/*.csv',
        'test_data/201306-citibike-tripdata.csv',
        'test_data_split/*.csv'
    ]
    
    csv_files = None
    for pattern in data_patterns:
        if '*' in pattern:
            import glob
            matches = glob.glob(pattern)
            if matches:
                csv_files = pattern
                break
        elif os.path.exists(pattern):
            csv_files = pattern
            break
    
    if not csv_files:
        print("ERROR: No data files found!")
        print("Please ensure CitiBike CSV files are available in:")
        for pattern in data_patterns:
            print(f"  - {pattern}")
        return 1
    
    print(f"Using data: {csv_files}")
    
    # Configuration for 20K events
    config = {
        'csv_files': csv_files,
        'max_events': 20000,
        'force_async': False,
        'num_threads': 3  # Reduced for memory efficiency
    }
    
    print(f"Configuration:")
    print(f"  Max events: {config['max_events']:,}")
    print(f"  Threads: {config['num_threads']}")
    print(f"  Memory optimization: Enabled (automatic for >10K events)")
    print()
    
    try:
        # Run the demo
        print("Starting processing...")
        results = run_enhanced_demo(**config)
        
        if results:
            print("\n" + "=" * 60)
            print("20K EVENTS PROCESSING SUCCESSFUL!")
            print("=" * 60)
            
            for config_name, result in results.items():
                print(f"\nConfiguration: {config_name}")
                print(f"  Duration: {result['duration']:.2f} seconds")
                print(f"  Events processed: {result['events_processed']:,}")
                print(f"  Total matches: {result['analysis']['total_matches']:,}")
                
                if 'write_stats' in result and result['write_stats']['mode'] == 'multi_threaded':
                    ws = result['write_stats']
                    print(f"  Items written: {ws['items_written']:,}")
                    print(f"  Write errors: {ws['write_errors']}")
                
                if result.get('load_shedding'):
                    ls = result['load_shedding']
                    print(f"  Events dropped: {ls['events_dropped']:,}")
                    print(f"  Drop rate: {ls['drop_rate']:.1%}")
        else:
            print("No results returned - check for errors above")
            return 1
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)