"""
Comprehensive Demo: Persistent Stateful Load Shedding for CitiBike CEP.

This demo showcases the complete integration of:
1. Multi-threaded CitiBike stream processing
2. Stateful load shedding with pattern completion tracking
3. State persistence across system restarts
4. Performance monitoring and adaptive thresholds
5. Comprehensive reporting and visualization
"""

import asyncio
import logging
import time
import signal
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List
import threading

# Import our CEP components
from CEP import CEP
from enhanced_citibike_demo import EnhancedCitiBikeStream, create_enhanced_patterns
from loadshedding.PersistentStatefulLoadShedding import (
    create_persistent_stateful_strategy,
    PersistentStatefulLoadSheddingStrategy
)
from loadshedding.StateAwareTreeBasedEvaluationMechanism import (
    create_state_aware_evaluation_mechanism
)
from loadshedding.LoadSheddingReporter import LoadSheddingReporter
from loadshedding.LoadSheddingDashboard import LoadSheddingDashboard

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('persistent_demo.log')
    ]
)
logger = logging.getLogger(__name__)


class PersistentCitiBikeDemo:
    """
    Comprehensive demo showing persistent stateful load shedding
    integrated with multi-threaded CitiBike stream processing.
    """
    
    def __init__(self, 
                 csv_pattern: str = "test_data/*.csv",
                 storage_type: str = "sqlite",
                 storage_path: str = "citibike_load_shedding.db",
                 strategy_name: str = "citibike_persistent",
                 max_workers: int = 3,
                 events_per_second: int = 100,
                 simulation_duration: int = 60):
        
        self.csv_pattern = csv_pattern
        self.storage_type = storage_type
        self.storage_path = storage_path
        self.strategy_name = strategy_name
        self.max_workers = max_workers
        self.events_per_second = events_per_second
        self.simulation_duration = simulation_duration
        
        # Components
        self.cep_engine = None
        self.stream = None
        self.load_shedding_strategy = None
        self.evaluation_mechanism = None
        self.reporter = None
        self.dashboard = None
        
        # Runtime state
        self.is_running = False
        self.start_time = None
        self.shutdown_event = threading.Event()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
    
    def initialize_components(self):
        """Initialize all CEP and load shedding components."""
        logger.info("Initializing components...")
        
        # 1. Create persistent stateful load shedding strategy
        self.load_shedding_strategy = create_persistent_stateful_strategy(
            strategy_name=self.strategy_name,
            storage_type=self.storage_type,
            storage_path=self.storage_path,
            drop_threshold=0.4,  # Start conservative
            confidence_threshold=0.6,
            save_interval=10  # Save every 10 seconds
        )
        
        # 2. Prepare patterns first so evaluation mechanism constructor gets them
        patterns = create_enhanced_patterns()
        if not patterns:
            logger.warning("No patterns created; proceeding with delayed registration mode")
        
        # 3. Create state-aware evaluation mechanism with initial patterns (if any)
        self.evaluation_mechanism = create_state_aware_evaluation_mechanism(
            patterns=patterns if patterns else None,
            load_shedding_strategy=self.load_shedding_strategy,
            optimization_interval=30  # Optimize every 30 seconds
        )
        
        # 4. Create CEP engine with state-aware evaluation
        self.cep_engine = CEP(evaluation_mechanism=self.evaluation_mechanism)
        
        # 5. (Patterns already in mechanism) Register them with CEP engine
        for pattern in patterns:
            # CEP.register_pattern may build internal structures; make sure name attr compatibility
            self.cep_engine.register_pattern(pattern)
            logger.info(f"Registered pattern: {getattr(pattern, 'pattern_name', getattr(pattern, 'name', 'unknown'))}")
        
    # 6. Create enhanced CitiBike stream with multi-threading
        self.stream = EnhancedCitiBikeStream(
            csv_pattern=self.csv_pattern,
            max_workers=self.max_workers,
            events_per_second=self.events_per_second
        )
        
        # 6. Create reporter and dashboard
        self.reporter = LoadSheddingReporter()
        self.dashboard = LoadSheddingDashboard(
            strategy=self.load_shedding_strategy,
            update_interval=5  # Update every 5 seconds
        )
        
        logger.info("All components initialized successfully")
    
    def check_for_existing_state(self):
        """Check if there's existing state from previous runs."""
        logger.info("Checking for existing persistent state...")
        
        # Get persistence statistics
        storage_stats = self.load_shedding_strategy.persistence.get_storage_stats()
        logger.info(f"Storage statistics: {storage_stats}")
        
        # Get comprehensive strategy stats
        strategy_stats = self.load_shedding_strategy.get_comprehensive_stats()
        
        if strategy_stats.get('persistence', {}).get('saved_states', 0) > 0:
            logger.info("Found existing persistent state from previous runs:")
            logger.info(f"  - Saved states: {strategy_stats['persistence']['saved_states']}")
            logger.info(f"  - Total events processed: {strategy_stats['total_events_processed']}")
            logger.info(f"  - Current drop rate: {strategy_stats['drop_rate']:.2%}")
        else:
            logger.info("No existing persistent state found - starting fresh")
    
    def run_load_simulation(self):
        """Run the main load simulation with different load scenarios."""
        logger.info("Starting load simulation scenarios...")
        
        scenarios = [
            {"name": "Low Load", "duration": 20, "load_multiplier": 0.5},
            {"name": "Normal Load", "duration": 20, "load_multiplier": 1.0},
            {"name": "High Load", "duration": 20, "load_multiplier": 2.0},
            {"name": "Extreme Load", "duration": 15, "load_multiplier": 4.0},
            {"name": "Recovery", "duration": 10, "load_multiplier": 0.3}
        ]
        
        total_events_sent = 0
        
        for i, scenario in enumerate(scenarios):
            if self.shutdown_event.is_set():
                break
                
            logger.info(f"\n{'='*50}")
            logger.info(f"SCENARIO {i+1}: {scenario['name']}")
            logger.info(f"Duration: {scenario['duration']}s, Load: {scenario['load_multiplier']}x")
            logger.info(f"{'='*50}")
            
            # Adjust stream rate for scenario
            original_eps = self.stream.events_per_second
            self.stream.events_per_second = int(original_eps * scenario['load_multiplier'])
            
            scenario_start = time.time()
            scenario_events = 0
            
            # Run scenario
            while (time.time() - scenario_start) < scenario['duration']:
                if self.shutdown_event.is_set():
                    break
                
                # Get next batch of events
                events = self.stream.get_next_events(batch_size=50)
                if not events:
                    logger.warning("No more events available from stream")
                    break
                
                # Process events through CEP engine
                for event in events:
                    try:
                        self.cep_engine.submit_event(event)
                        scenario_events += 1
                        total_events_sent += 1
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                
                # Short pause to control rate
                time.sleep(0.1)
            
            # Restore original rate
            self.stream.events_per_second = original_eps
            
            # Report scenario results
            current_stats = self.load_shedding_strategy.get_comprehensive_stats()
            logger.info(f"Scenario completed - Events sent: {scenario_events}")
            logger.info(f"Current drop rate: {current_stats['drop_rate']:.2%}")
            logger.info(f"Pattern matches found: {len(self.cep_engine.get_partial_matches())}")
            
            # Brief pause between scenarios
            if not self.shutdown_event.is_set():
                time.sleep(2)
        
        logger.info(f"\nLoad simulation completed! Total events sent: {total_events_sent}")
        return total_events_sent
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate comprehensive report of the demo run."""
        logger.info("Generating comprehensive report...")
        
        # Get final statistics
        strategy_stats = self.load_shedding_strategy.get_comprehensive_stats()
        cep_stats = self.cep_engine.get_evaluation_statistics()
        
        # Get pattern matches
        pattern_matches = self.cep_engine.get_partial_matches()
        
        report = {
            'demo_info': {
                'strategy_name': self.strategy_name,
                'storage_type': self.storage_type,
                'storage_path': self.storage_path,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': datetime.now().isoformat(),
                'duration_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            },
            'load_shedding_stats': strategy_stats,
            'cep_stats': cep_stats,
            'pattern_matches': {
                'total_matches': len(pattern_matches),
                'matches_by_pattern': {}
            },
            'persistence_info': {
                'storage_stats': self.load_shedding_strategy.persistence.get_storage_stats(),
                'pattern_names': self.load_shedding_strategy.persistence.get_pattern_names(),
                'config_names': self.load_shedding_strategy.persistence.get_config_names()
            }
        }
        
        # Group matches by pattern
        for match in pattern_matches:
            pattern_name = match.pattern.pattern_name
            if pattern_name not in report['pattern_matches']['matches_by_pattern']:
                report['pattern_matches']['matches_by_pattern'][pattern_name] = 0
            report['pattern_matches']['matches_by_pattern'][pattern_name] += 1
        
        return report
    
    def save_report(self, report: Dict):
        """Save comprehensive report to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"persistent_demo_report_{timestamp}.json"
        
        try:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"Report saved to: {report_file}")
            
            # Also create a summary file
            summary_file = f"persistent_demo_summary_{timestamp}.txt"
            with open(summary_file, 'w') as f:
                f.write("PERSISTENT STATEFUL LOAD SHEDDING DEMO SUMMARY\n")
                f.write("=" * 50 + "\n\n")
                
                f.write(f"Demo Configuration:\n")
                f.write(f"  Strategy: {report['demo_info']['strategy_name']}\n")
                f.write(f"  Storage: {report['demo_info']['storage_type']}\n")
                f.write(f"  Duration: {report['demo_info']['duration_seconds']:.1f}s\n\n")
                
                f.write(f"Load Shedding Performance:\n")
                f.write(f"  Events Processed: {report['load_shedding_stats']['total_events_processed']}\n")
                f.write(f"  Events Dropped: {report['load_shedding_stats']['total_events_dropped']}\n")
                f.write(f"  Drop Rate: {report['load_shedding_stats']['drop_rate']:.2%}\n")
                f.write(f"  Current Thresholds: drop={report['load_shedding_stats']['current_thresholds']['drop_threshold']:.2f}, "
                       f"confidence={report['load_shedding_stats']['current_thresholds']['confidence_threshold']:.2f}\n\n")
                
                f.write(f"Pattern Matching Results:\n")
                f.write(f"  Total Matches: {report['pattern_matches']['total_matches']}\n")
                for pattern_name, count in report['pattern_matches']['matches_by_pattern'].items():
                    f.write(f"  {pattern_name}: {count} matches\n")
                f.write("\n")
                
                f.write(f"Persistence Information:\n")
                f.write(f"  Stored Patterns: {len(report['persistence_info']['pattern_names'])}\n")
                f.write(f"  Stored Configs: {len(report['persistence_info']['config_names'])}\n")
                f.write(f"  Storage Stats: {report['persistence_info']['storage_stats']}\n")
            
            logger.info(f"Summary saved to: {summary_file}")
            
        except Exception as e:
            logger.error(f"Error saving report: {e}")
    
    def cleanup(self):
        """Cleanup resources and shutdown gracefully."""
        logger.info("Cleaning up resources...")
        
        try:
            # Stop dashboard
            if self.dashboard:
                self.dashboard.stop()
            
            # Shutdown load shedding strategy (saves final state)
            if self.load_shedding_strategy:
                self.load_shedding_strategy.shutdown()
            
            # Close stream
            if self.stream:
                self.stream.close()
            
            logger.info("Cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def run(self):
        """Run the complete persistent stateful load shedding demo."""
        try:
            logger.info("=" * 60)
            logger.info("PERSISTENT STATEFUL LOAD SHEDDING DEMO STARTING")
            logger.info("=" * 60)
            
            self.start_time = datetime.now()
            self.is_running = True
            
            # Initialize all components
            self.initialize_components()
            
            # Check for existing state
            self.check_for_existing_state()
            
            # Start dashboard in background
            self.dashboard.start_background_updates()
            
            # Run load simulation
            total_events = self.run_load_simulation()
            
            # Allow some time for final processing
            logger.info("Allowing time for final event processing...")
            time.sleep(10)
            
            # Generate and save comprehensive report
            report = self.generate_comprehensive_report()
            self.save_report(report)
            
            # Display summary
            logger.info("\n" + "=" * 60)
            logger.info("DEMO COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            logger.info(f"Total runtime: {(datetime.now() - self.start_time).total_seconds():.1f}s")
            logger.info(f"Events processed: {report['load_shedding_stats']['total_events_processed']}")
            logger.info(f"Events dropped: {report['load_shedding_stats']['total_events_dropped']}")
            logger.info(f"Final drop rate: {report['load_shedding_stats']['drop_rate']:.2%}")
            logger.info(f"Pattern matches: {report['pattern_matches']['total_matches']}")
            logger.info(f"Persistent states saved: {report['load_shedding_stats'].get('persistence', {}).get('saved_states', 0)}")
            
        except KeyboardInterrupt:
            logger.info("Demo interrupted by user")
        except Exception as e:
            logger.error(f"Demo failed with error: {e}")
            raise
        finally:
            self.is_running = False
            self.cleanup()


def main():
    """Main entry point for the persistent stateful load shedding demo."""
    
    # Check if CSV files exist
    csv_files = list(Path("test_data").glob("*.csv"))
    if not csv_files:
        logger.error("No CSV files found in test_data directory!")
        logger.info("Please ensure CitiBike CSV files are available in test_data/")
        return 1
    
    logger.info(f"Found {len(csv_files)} CSV files for processing")
    
    # Create demo configuration
    demo_config = {
        'csv_pattern': "test_data/*.csv",
        'storage_type': "sqlite",  # Use SQLite for better concurrency
        'storage_path': "citibike_persistent_demo.db",
        'strategy_name': "citibike_enhanced_persistent",
        'max_workers': 3,
        'events_per_second': 150,  # Higher rate to test load shedding
        'simulation_duration': 90  # 90 seconds total
    }
    
    # Create and run demo
    demo = PersistentCitiBikeDemo(**demo_config)
    
    try:
        demo.run()
        return 0
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)