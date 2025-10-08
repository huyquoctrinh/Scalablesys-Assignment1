#!/usr/bin/env python3
"""
Test datetime parsing for CitiBike CSV files
"""

import csv
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_datetime_parsing():
    """Test datetime parsing on the problematic CSV file."""
    
    csv_file = "d:/scalable_courses/Scalablesys-Assignment1/201810-citibike-tripdata.csv"
    
    # Multiple datetime formats to try
    datetime_formats = [
        '%Y-%m-%d %H:%M:%S.%f',  # 2018+ format with microseconds
        '%Y-%m-%d %H:%M:%S',     # 2013-2017 format
        '%m/%d/%Y %H:%M:%S',     # Alternative format
        '%m/%d/%Y %H:%M',        # Without seconds
        '%m/%d/%Y  %I:%M:%S %p', # US format with AM/PM (double space)
        '%m/%d/%Y %I:%M:%S %p',  # US format with AM/PM (single space)
        '%m/%d/%Y  %I:%M %p',    # US format AM/PM without seconds (double space)
        '%m/%d/%Y %I:%M %p',     # US format AM/PM without seconds (single space)
    ]
    
    def parse_datetime_flexible(datetime_str: str) -> datetime:
        """Parse datetime string with multiple format fallbacks."""
        if not datetime_str or datetime_str.strip() == '':
            return datetime.now()
        
        datetime_str = datetime_str.strip().strip('"')
        original_str = datetime_str
        
        # Normalize spaces in US format datetime strings
        # Handle "10/1/2018  12:00:01 AM" -> "10/1/2018 12:00:01 AM"
        import re
        datetime_str = re.sub(r'\s+', ' ', datetime_str)  # Replace multiple spaces with single space
        
        # Try each format until one works
        for fmt in datetime_formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError as e:
                logger.debug(f"Format {fmt} failed for '{datetime_str}': {e}")
                continue
        
        # If all formats fail, try to handle edge cases
        try:
            # Handle microseconds with proper padding/truncation
            if '.' in datetime_str:
                main_part, micro_part = datetime_str.rsplit('.', 1)
                
                # Extract only numeric part from microseconds (remove any trailing non-numeric chars)
                micro_numeric = ''
                for char in micro_part:
                    if char.isdigit():
                        micro_numeric += char
                    else:
                        break  # Stop at first non-numeric character
                
                logger.info(f"Original micro part: '{micro_part}', numeric part: '{micro_numeric}'")
                
                # Pad or truncate to 6 digits
                if len(micro_numeric) < 6:
                    micro_numeric = micro_numeric.ljust(6, '0')  # Pad with zeros
                elif len(micro_numeric) > 6:
                    micro_numeric = micro_numeric[:6]  # Truncate to 6 digits
                
                datetime_str = f"{main_part}.{micro_numeric}"
                logger.info(f"Reconstructed datetime: '{datetime_str}'")
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError as e:
            logger.error(f"Edge case parsing failed for '{original_str}': {e}")
        
        # Try without microseconds as fallback
        try:
            if '.' in original_str:
                main_part = original_str.split('.')[0]
                logger.info(f"Trying without microseconds: '{main_part}'")
                return datetime.strptime(main_part, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.error(f"Fallback parsing failed for '{original_str}': {e}")
        
        # Last resort: return a default datetime
        logger.error(f"Could not parse datetime '{original_str}', using current time")
        return datetime.now()
    
    # Test with the actual CSV file
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            
            for row_num, row in enumerate(csv_reader, 1):
                if row_num > 10:  # Test first 10 rows
                    break
                    
                try:
                    # Try parsing start time
                    starttime = row.get('starttime', '')
                    if starttime:
                        logger.info(f"Row {row_num}: Parsing starttime '{starttime}'")
                        parsed_time = parse_datetime_flexible(starttime)
                        logger.info(f"Row {row_num}: Successfully parsed to {parsed_time}")
                    
                except Exception as e:
                    logger.error(f"Row {row_num}: Error parsing starttime '{starttime}': {e}")
                    
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")

if __name__ == "__main__":
    test_datetime_parsing()
