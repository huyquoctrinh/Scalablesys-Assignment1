#!/usr/bin/env python3
"""
Quick test for the specific datetime format: 10/1/2018  12:00:01 AM
"""

from datetime import datetime
import re

def test_specific_format():
    """Test the specific datetime format causing issues."""
    
    test_datetime = "10/1/2018  12:00:01 AM"
    print(f"Testing datetime: '{test_datetime}'")
    
    # Normalize spaces
    normalized = re.sub(r'\s+', ' ', test_datetime.strip())
    print(f"Normalized: '{normalized}'")
    
    # Try parsing with correct format
    formats_to_try = [
        '%m/%d/%Y %I:%M:%S %p',  # US format with AM/PM
        '%m/%d/%Y  %I:%M:%S %p', # US format with AM/PM (double space)
        '%m/%d/%Y %I:%M %p',     # US format AM/PM without seconds
    ]
    
    for fmt in formats_to_try:
        try:
            parsed = datetime.strptime(normalized, fmt)
            print(f"✅ SUCCESS with format '{fmt}': {parsed}")
            return parsed
        except ValueError as e:
            print(f"❌ Failed with format '{fmt}': {e}")
    
    print("❌ All formats failed!")
    return None

if __name__ == "__main__":
    test_specific_format()
