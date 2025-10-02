"""
Enhanced load shedding integration demo showcasing all advanced improvements.
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add the project root to path to import OpenCEP modules
sys.path.append('/Users/anhdao/Aalto/Fall 2025/cs-e4780-scalable-systems/Scalablesys-Assignment1')

from loadshedding.PredictiveLoadMonitor import PredictiveLoadMonitor
from loadshedding.IntelligentEventBuffer import IntelligentEventBuffer
from loadshedding.MultiLevelLoadShedding import MultiLevelLoadShedding
from loadshedding.AdaptiveConfigurationManager import AdaptiveConfigurationManager
from loadshedding.EnhancedCitiBikeBenchmark import EnhancedCitiBikeBenchmark
from loadshedding.LoadSheddingDashboard import LoadSheddingDashboard, DashboardIntegratedLoadMonitor
from loadshedding.LoadMonitor import LoadMonitor
from loadshedding.LoadSheddingStrategy import ProbabilisticLoadShedding, SemanticLoadShedding, AdaptiveLoadShedding
from loadshedding.LoadAwareInputStream import LoadAwareInputStream
from city_bike_formatter import CitiBikeCSVFormatter


class EnhancedCEPSystem:
    """
    Demonstration of the enhanced CEP system with all advanced load shedding improvements.
    """
    
    def __init__(self):
        # Initialize all enhanced components
        self.predictive_monitor = PredictiveLoadMonitor()
        self.intelligent_buffer = IntelligentEventBuffer(max_size=10000)
        self.multi_level_shedding = MultiLevelLoadShedding()
        self.adaptive_config = AdaptiveConfigurationManager()
        
        # Initialize dashboard and monitoring
        self.dashboard = LoadSheddingDashboard(update_interval_ms=1000)
        self.dashboard_monitor = DashboardIntegratedLoadMonitor(self.dashboard)
        
        # Initialize benchmarking
        self.citibike_benchmark = EnhancedCitiBikeBenchmark()
        
        # System state
        self.is_running = False
        self.current_strategy = None
        
        print("🚀 Enhanced CEP System initialized with all improvements!")
    
    def demonstrate_predictive_monitoring(self):
        """Demonstrate predictive load monitoring capabilities."""
        print("\n" + "="*60)
        print("🔮 PREDICTIVE MONITORING DEMONSTRATION")
        print("="*60)
        
        # Simulate historical load data
        historical_data = []
        base_time = datetime.now() - timedelta(hours=2)
        
        for i in range(120):  # 2 hours of data, every minute
            timestamp = base_time + timedelta(minutes=i)
            # Simulate rush hour pattern
            hour = timestamp.hour
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                load = 800 + (i % 10) * 50  # Rush hour
            else:
                load = 200 + (i % 5) * 20   # Normal hours
            
            historical_data.append({
                'timestamp': timestamp,
                'cpu_usage': load / 1000,
                'memory_usage': (load + 100) / 1000,
                'event_rate': load
            })
        
        # Train predictive model
        print("📊 Training predictive model on historical data...")
        self.predictive_monitor.update_historical_data(historical_data)
        
        # Make predictions
        future_time = datetime.now() + timedelta(minutes=30)
        predictions = self.predictive_monitor.predict_future_load(future_time, horizon_minutes=60)
        
        print(f"🎯 Predictions for next 60 minutes starting {future_time.strftime('%H:%M')}:")
        for i, pred in enumerate(predictions[:6]):  # Show first 6 predictions
            pred_time = future_time + timedelta(minutes=i*10)
            print(f"   {pred_time.strftime('%H:%M')}: CPU {pred['cpu_usage']:.1%}, "
                  f"Memory {pred['memory_usage']:.1%}, Events {pred['event_rate']:.0f}/min")
        
        # Demonstrate proactive recommendations
        recommendations = self.predictive_monitor.get_proactive_recommendations()
        if recommendations:
            print("\n💡 Proactive Recommendations:")
            for rec in recommendations:
                print(f"   • {rec}")
    
    def demonstrate_intelligent_buffering(self):
        """Demonstrate intelligent event buffering with priority management."""
        print("\n" + "="*60)
        print("🧠 INTELLIGENT BUFFERING DEMONSTRATION")
        print("="*60)
        
        # Create sample events with different priorities
        events = [
            {'id': 1, 'type': 'trip_start', 'usertype': 'Subscriber', 'priority': 'high'},
            {'id': 2, 'type': 'trip_end', 'usertype': 'Customer', 'priority': 'medium'},
            {'id': 3, 'type': 'maintenance', 'station_id': 'critical_station', 'priority': 'critical'},
            {'id': 4, 'type': 'trip_start', 'usertype': 'Customer', 'priority': 'low'},
            {'id': 5, 'type': 'rebalancing', 'station_id': 'busy_station', 'priority': 'high'},
        ]
        
        print("📥 Adding events to intelligent buffer:")
        for event in events:
            self.intelligent_buffer.add_event(event)
            print(f"   Added {event['type']} event (Priority: {event['priority']})")
        
        print(f"\n📊 Buffer status: {len(self.intelligent_buffer.priority_queues)} priority levels")
        
        # Demonstrate priority-based retrieval
        print("\n🎯 Retrieving events by priority:")
        retrieved_count = 0
        while not self.intelligent_buffer.is_empty() and retrieved_count < 3:
            event = self.intelligent_buffer.get_next_event()
            if event:
                print(f"   Retrieved: {event['type']} (Priority: {event['priority']})")
                retrieved_count += 1
        
        # Demonstrate aging mechanism
        print("\n⏰ Demonstrating aging mechanism...")
        time.sleep(1)  # Wait for aging
        self.intelligent_buffer._age_events()
        
        # Check buffer optimization
        old_size = self.intelligent_buffer.max_size
        self.intelligent_buffer.optimize_performance()
        optimization_info = self.intelligent_buffer.get_optimization_info()
        print(f"🔧 Buffer optimization: {optimization_info}")
    
    def demonstrate_multilevel_shedding(self):
        """Demonstrate multi-level load shedding with circuit breaker."""
        print("\n" + "="*60)
        print("🛡️ MULTI-LEVEL LOAD SHEDDING DEMONSTRATION")
        print("="*60)
        
        # Simulate different load scenarios
        scenarios = [
            {'name': 'Normal Load', 'cpu': 0.3, 'memory': 0.4, 'queue_size': 100},
            {'name': 'Medium Load', 'cpu': 0.6, 'memory': 0.7, 'queue_size': 1000},
            {'name': 'High Load', 'cpu': 0.8, 'memory': 0.85, 'queue_size': 5000},
            {'name': 'Critical Load', 'cpu': 0.95, 'memory': 0.95, 'queue_size': 15000},
        ]
        
        for scenario in scenarios:
            print(f"\n📋 Scenario: {scenario['name']}")
            print(f"   CPU: {scenario['cpu']:.0%}, Memory: {scenario['memory']:.0%}, Queue: {scenario['queue_size']}")
            
            # Update system metrics
            metrics = {
                'cpu_usage': scenario['cpu'],
                'memory_usage': scenario['memory'],
                'queue_size': scenario['queue_size'],
                'timestamp': datetime.now()
            }
            
            # Determine shedding level
            level = self.multi_level_shedding.determine_shedding_level(metrics)
            print(f"   🎯 Shedding Level: {level}")
            
            # Check circuit breaker status
            circuit_status = self.multi_level_shedding.circuit_breaker.get_status()
            print(f"   🔌 Circuit Breaker: {circuit_status}")
            
            # Apply shedding strategy
            strategy = self.multi_level_shedding.get_shedding_strategy(level)
            print(f"   🛠️ Active Strategy: {type(strategy).__name__}")
            
            # Simulate event processing
            sample_event = {'id': 1, 'type': 'trip_start', 'usertype': 'Customer'}
            should_process = strategy.should_process_event(sample_event, metrics)
            print(f"   ✅ Process Sample Event: {should_process}")
            
            time.sleep(0.5)  # Brief pause between scenarios
    
    def demonstrate_adaptive_configuration(self):
        """Demonstrate adaptive configuration management."""
        print("\n" + "="*60)
        print("⚙️ ADAPTIVE CONFIGURATION DEMONSTRATION")
        print("="*60)
        
        # Initialize with base configuration
        base_config = {
            'cpu_threshold': 0.8,
            'memory_threshold': 0.85,
            'drop_rate_target': 0.05,
            'latency_target_ms': 200
        }
        
        self.adaptive_config.update_configuration(base_config)
        print("📝 Base configuration loaded:")
        for key, value in base_config.items():
            print(f"   {key}: {value}")
        
        # Simulate performance feedback over time
        performance_scenarios = [
            {'throughput': 450, 'drop_rate': 0.08, 'latency_ms': 250, 'description': 'Underperforming'},
            {'throughput': 600, 'drop_rate': 0.03, 'latency_ms': 180, 'description': 'Good performance'},
            {'throughput': 800, 'drop_rate': 0.12, 'latency_ms': 300, 'description': 'Overloaded'},
            {'throughput': 550, 'drop_rate': 0.05, 'latency_ms': 190, 'description': 'Optimal'},
        ]
        
        print("\n🔄 Adapting configuration based on performance feedback:")
        for i, scenario in enumerate(performance_scenarios):
            print(f"\n   Round {i+1}: {scenario['description']}")
            print(f"   Metrics - Throughput: {scenario['throughput']}, "
                  f"Drop Rate: {scenario['drop_rate']:.1%}, Latency: {scenario['latency_ms']}ms")
            
            # Update performance metrics
            self.adaptive_config.update_performance_metrics(scenario)
            
            # Get adapted configuration
            adapted_config = self.adaptive_config.get_adapted_configuration()
            changes = []
            for key in base_config:
                if abs(adapted_config[key] - base_config[key]) > 0.01:
                    change_direction = "↑" if adapted_config[key] > base_config[key] else "↓"
                    changes.append(f"{key} {change_direction} {adapted_config[key]:.3f}")
            
            if changes:
                print(f"   🎯 Configuration changes: {', '.join(changes)}")
            else:
                print("   ✅ No configuration changes needed")
            
            base_config = adapted_config.copy()
    
    def run_comprehensive_demo(self):
        """Run comprehensive demonstration of all enhancements."""
        print("🌟 ENHANCED CEP LOAD SHEDDING SYSTEM")
        print("🌟 Comprehensive Feature Demonstration")
        print("🌟 " + "="*60)
        
        try:
            # 1. Predictive Monitoring
            self.demonstrate_predictive_monitoring()
            
            # 2. Intelligent Buffering
            self.demonstrate_intelligent_buffering()
            
            # 3. Multi-level Shedding
            self.demonstrate_multilevel_shedding()
            
            # 4. Adaptive Configuration
            self.demonstrate_adaptive_configuration()
            
            print("\n" + "="*60)
            print("✅ ALL ENHANCEMENTS DEMONSTRATED SUCCESSFULLY!")
            print("="*60)
            
        except Exception as e:
            print(f"❌ Error during demonstration: {e}")
            import traceback
            traceback.print_exc()
    
    def run_citibike_benchmark(self):
        """Run CitiBike-specific benchmarking."""
        print("\n" + "="*60)
        print("🚴 CITIBIKE-SPECIFIC BENCHMARKING")
        print("="*60)
        
        # Load CitiBike data (using a smaller sample for demo)
        print("📊 Loading CitiBike sample data...")
        
        # Create sample CitiBike events
        sample_events = []
        base_time = datetime.now() - timedelta(hours=1)
        
        for i in range(100):  # Sample 100 events
            event_time = base_time + timedelta(minutes=i * 0.6)  # One event per 36 seconds
            event = {
                'ts': event_time,
                'start_station_id': f"station_{(i % 20) + 1}",  # 20 different stations
                'end_station_id': f"station_{((i + 5) % 20) + 1}",
                'usertype': 'Subscriber' if i % 3 == 0 else 'Customer',  # 1/3 subscribers
                'tripduration_s': 300 + (i % 10) * 60,  # 5-15 minute trips
                'event_type': 'trip_start' if i % 2 == 0 else 'trip_end'
            }
            sample_events.append(event)
        
        print(f"📈 Generated {len(sample_events)} sample CitiBike events")
        
        # Initialize strategies for testing
        strategies = [
            ProbabilisticLoadShedding(drop_probability=0.1),
            SemanticLoadShedding(),
            AdaptiveLoadShedding(),
            self.multi_level_shedding  # Our enhanced strategy
        ]
        
        print("🧪 Running enhanced CitiBike benchmark...")
        
        # Run the benchmark
        results = self.citibike_benchmark.run_realistic_citibike_benchmark(
            events=sample_events,
            strategies=strategies,
            scenarios=['rush_hour_simulation', 'weekend_leisure_pattern']
        )
        
        # Display results summary
        print("\n📊 Benchmark Results Summary:")
        print("-" * 40)
        
        data_analysis = results.get('data_analysis', {})
        if 'hourly_distribution' in data_analysis:
            print(f"⏰ Peak hours identified: {list(data_analysis['hourly_distribution'].keys())[:3]}")
        
        best_strategy = results.get('strategy_comparison', {}).get('best_strategy', 'Unknown')
        print(f"🏆 Best overall strategy: {best_strategy}")
        
        insights = results.get('domain_specific_insights', {}).get('load_shedding_recommendations', [])
        if insights:
            print(f"💡 Key recommendations:")
            for insight in insights[:3]:  # Show top 3
                print(f"   • {insight}")
        
        return results
    
    def start_dashboard_demo(self, duration_seconds: int = 30):
        """Start dashboard demonstration."""
        print("\n" + "="*60)
        print("📊 REAL-TIME DASHBOARD DEMONSTRATION")
        print("="*60)
        
        try:
            print("🚀 Starting real-time dashboard...")
            print("   (Close the plot window to continue)")
            
            # Start dashboard in background
            import threading
            dashboard_thread = threading.Thread(
                target=self.dashboard.start_dashboard,
                args=("Enhanced CEP Load Shedding Dashboard",)
            )
            dashboard_thread.daemon = True
            dashboard_thread.start()
            
            # Give dashboard time to initialize
            time.sleep(2)
            
            # Simulate realistic metrics
            print(f"📈 Simulating {duration_seconds} seconds of realistic metrics...")
            self.dashboard_monitor.simulate_metrics(duration_seconds=duration_seconds)
            
            # Generate performance report
            print("\n📋 Generating performance report...")
            report = self.dashboard.generate_performance_report()
            
            if 'error' not in report:
                summary = report.get('summary', {})
                print(f"   Data points collected: {summary.get('data_points', 0)}")
                print(f"   Average throughput: {summary.get('average_throughput', 0):.0f} events/sec")
                print(f"   Average drop rate: {summary.get('average_drop_rate', 0):.1f}%")
                print(f"   Average latency: {summary.get('average_latency', 0):.0f}ms")
                
                recommendations = report.get('recommendations', [])
                if recommendations:
                    print("💡 Performance recommendations:")
                    for rec in recommendations[:3]:
                        print(f"   • {rec}")
            
        except Exception as e:
            print(f"❌ Dashboard error: {e}")
        finally:
            try:
                self.dashboard.stop_dashboard()
            except:
                pass


def main():
    """Main function to run the enhanced system demonstration."""
    print("🚀 ENHANCED CEP LOAD SHEDDING SYSTEM")
    print("🚀 Starting comprehensive demonstration...")
    print("🚀 " + "="*70)
    
    # Create and run enhanced system
    enhanced_system = EnhancedCEPSystem()
    
    try:
        # Run comprehensive demo
        enhanced_system.run_comprehensive_demo()
        
        # Run CitiBike benchmark
        enhanced_system.run_citibike_benchmark()
        
        # Ask user if they want to see dashboard demo
        print("\n" + "="*60)
        response = input("Would you like to see the real-time dashboard demo? (y/N): ").lower().strip()
        
        if response == 'y' or response == 'yes':
            enhanced_system.start_dashboard_demo(duration_seconds=20)
        else:
            print("⏭️ Skipping dashboard demo")
        
        print("\n" + "="*60)
        print("🎉 ENHANCED SYSTEM DEMONSTRATION COMPLETE!")
        print("🎉 All advanced load shedding features showcased successfully!")
        print("🎉 " + "="*60)
        
        print("\n📚 Summary of Enhanced Features:")
        print("   🔮 Predictive Load Monitoring - Proactive load prediction")
        print("   🧠 Intelligent Event Buffering - Priority-based event management")
        print("   🛡️ Multi-Level Load Shedding - Graduated response with circuit breaker")
        print("   ⚙️ Adaptive Configuration Management - Real-time parameter tuning")
        print("   🚴 Enhanced CitiBike Benchmarking - Domain-specific testing")
        print("   📊 Real-Time Dashboard - Live performance monitoring")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Error in demonstration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n👋 Thank you for exploring the Enhanced CEP System!")


if __name__ == "__main__":
    main()
