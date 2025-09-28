#!/usr/bin/env python3
"""
Simple test to debug CEP issues
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter
from CEP import CEP
from stream.Stream import InputStream, OutputStream
from base.DataFormatter import DataFormatter, EventTypeClassifier
from base.Event import Event
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import EqCondition
from condition.CompositeCondition import AndCondition
from condition.Condition import Variable


class TestEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return "BikeTrip"


class TestDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(TestEventTypeClassifier())
    
    def parse_event(self, raw_data: str):
        logger.info(f"parse_event called with: {type(raw_data)}")
        return {"test": "data", "ts": datetime.now()}
    
    def get_event_timestamp(self, event_payload: dict):
        logger.info(f"get_event_timestamp called")
        return datetime.now().timestamp()


class TestInputStream(InputStream):
    def __init__(self, num_events=3):
        super().__init__()
        self.num_events = num_events
        self.count = 0
    
    def get_item(self):
        logger.info(f"get_item called, count={self.count}")
        if self.count >= self.num_events:
            logger.info("Raising StopIteration")
            raise StopIteration
        
        self.count += 1
        formatter = TestDataFormatter()
        event = Event("test_data", formatter)
        logger.info(f"Created event: {event}")
        return event
    
    def has_next(self):
        has_more = self.count < self.num_events
        logger.info(f"has_next: {has_more}")
        return has_more


class TestOutputStream(OutputStream):
    def __init__(self):
        self.matches = []
    
    def add_item(self, item):
        logger.info(f"Match found: {item}")
        self.matches.append(item)


def test_basic_cep():
    logger.info("=== Starting basic CEP test ===")
    
    # Create a simple pattern
    logger.info("Creating pattern...")
    pattern = Pattern(
        PrimitiveEventStructure("BikeTrip", "a"),
        None,  # No condition for simplicity
        timedelta(minutes=5)
    )
    pattern.name = "SimpleTest"
    
    logger.info("Creating CEP...")
    cep = CEP([pattern])
    
    logger.info("Creating streams...")
    input_stream = TestInputStream(3)
    output_stream = TestOutputStream()
    data_formatter = TestDataFormatter()
    
    logger.info("Starting CEP run...")
    try:
        duration = cep.run(input_stream, output_stream, data_formatter)
        logger.info(f"CEP completed in {duration} seconds")
        logger.info(f"Matches found: {len(output_stream.matches)}")
        return True
    except Exception as e:
        logger.error(f"CEP failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_basic_cep()
    if success:
        print("✓ Test passed!")
    else:
        print("✗ Test failed!")