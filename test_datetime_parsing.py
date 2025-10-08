#!/usr/bin/env python3
"""
Test datetime parsing for the CitiBike CSV files to identify the exact issue.
"""

import csv
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from memory_optimized_hotpath_demo import MemoryOptimizedCitiBikeCSVFormatter

def test_datetime_parsing():
    """Test datetime parsing on the problematic file."""
    csv_file = "201810-citibike-tripdata.csv"
    
    if not os.path.exists(csv_file):
        print(f"File {csv_file} not found. Looking for alternative...")
        # Try to find the file in citybike_dataset
        alt_path = os.path.join("citybike_dataset", csv_file)
        if os.path.exists(alt_path):
            csv_file = alt_path
        else:
            print("No CitiBike CSV files found to test.")
            return
    
    print(f"Testing datetime parsing on: {csv_file}")
    
    # Read first few lines manually to see raw datetime format
    print("\n--- Raw CSV sample ---")
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"Headers: {headers}")
        
        for i, row in enumerate(reader):
            if i >= 3:  # Only show first 3 rows
                break
            print(f"Row {i+1}:")
            # Show datetime-related fields
            for key in ['starttime', 'start_time', 'Start Time', 'stoptime', 'stop_time', 'Stop Time']:
                if key in row:
                    print(f"  {key}: '{row[key]}'")
    
    # Test with the formatter
    print("\n--- Testing with formatter ---")
    try:
        formatter = MemoryOptimizedCitiBikeCSVFormatter(csv_file, batch_size=10)
        count = 0
        for data in formatter:
            count += 1
            print(f"Processed event {count}: starttime='{data.get('starttime')}' -> ts='{data.get('ts')}'")
            if count >= 5:  # Only process first 5 events
                break
        print(f"Successfully processed {count} events")
    except Exception as e:
        print(f"Error with formatter: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_datetime_parsing()
