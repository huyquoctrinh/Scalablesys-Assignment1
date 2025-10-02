#!/usr/bin/env python3
"""
CitiBike Data Analysis for Load Shedding Parameter Tuning
This script analyzes CitiBike data characteristics to help optimize load shedding configurations.
"""

import os
import sys
from collections import defaultdict, Counter
import statistics
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from city_bike_formatter import CitiBikeCSVFormatter


class CitiBikeDataAnalyzer:
    """Analyze CitiBike data to inform load shedding strategy."""
    
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        self.formatter = CitiBikeCSVFormatter(csv_file)
        
    def analyze_temporal_patterns(self, max_events: int = 50000):
        """Analyze temporal patterns in CitiBike data."""
        print("Analyzing temporal patterns...")
        
        hourly_counts = defaultdict(int)
        daily_counts = defaultdict(int)
        duration_by_hour = defaultdict(list)
        events_processed = 0
        
        for event in self.formatter:
            if events_processed >= max_events:
                break
                
            timestamp = event["ts"]
            hour = timestamp.hour
            day = timestamp.strftime("%Y-%m-%d")
            duration = event["tripduration_s"]
            
            hourly_counts[hour] += 1
            daily_counts[day] += 1
            
            if duration:
                duration_by_hour[hour].append(duration)
            
            events_processed += 1
            
            if events_processed % 10000 == 0:
                print(f"  Processed {events_processed} events...")
        
        # Calculate statistics
        print(f"\nTemporal Analysis Results ({events_processed} events):")
        print("-" * 50)
        
        # Peak hours
        sorted_hours = sorted(hourly_counts.items(), key=lambda x: x[1], reverse=True)
        print("Peak hours (events per hour):")
        for hour, count in sorted_hours[:5]:
            percentage = (count / events_processed) * 100
            print(f"  {hour:2d}:00 - {count:,} events ({percentage:.1f}%)")
        
        # Daily variation
        daily_values = list(daily_counts.values())
        if daily_values:
            print(f"\nDaily variation:")
            print(f"  Average events per day: {statistics.mean(daily_values):,.0f}")
            print(f"  Min events per day: {min(daily_values):,}")
            print(f"  Max events per day: {max(daily_values):,}")
            print(f"  Std deviation: {statistics.stdev(daily_values) if len(daily_values) > 1 else 0:,.0f}")
        
        # Duration analysis by hour
        print(f"\nTrip duration by hour (minutes):")
        for hour in [7, 8, 9, 12, 17, 18, 19]:  # Key hours
            if hour in duration_by_hour and duration_by_hour[hour]:
                durations_min = [d/60 for d in duration_by_hour[hour] if d]
                avg_duration = statistics.mean(durations_min)
                print(f"  {hour:2d}:00 - {avg_duration:.1f} min average")
        
        return {
            'hourly_counts': dict(hourly_counts),
            'daily_counts': dict(daily_counts),
            'duration_by_hour': {h: list(durations) for h, durations in duration_by_hour.items()},
            'total_events': events_processed
        }
    
    def analyze_station_patterns(self, max_events: int = 50000):
        """Analyze station usage patterns."""
        print("\nAnalyzing station patterns...")
        
        start_station_counts = Counter()
        end_station_counts = Counter()
        station_names = {}
        events_processed = 0
        
        for event in self.formatter:
            if events_processed >= max_events:
                break
            
            start_id = event["start_station_id"]
            end_id = event["end_station_id"]
            start_name = event["start_station_name"]
            end_name = event["end_station_name"]
            
            if start_id:
                start_station_counts[start_id] += 1
                station_names[start_id] = start_name
            
            if end_id:
                end_station_counts[end_id] += 1
                station_names[end_id] = end_name
            
            events_processed += 1
        
        print(f"\nStation Analysis Results ({events_processed} events):")
        print("-" * 50)
        
        # Most popular start stations
        print("Most popular start stations:")
        for station_id, count in start_station_counts.most_common(10):
            name = station_names.get(station_id, "Unknown")[:30]
            percentage = (count / events_processed) * 100
            print(f"  {station_id}: {count:,} trips ({percentage:.1f}%) - {name}")
        
        # Station imbalance (more starts than ends or vice versa)
        print(f"\nStation imbalance (top 10):")
        imbalances = []
        for station_id in set(list(start_station_counts.keys()) + list(end_station_counts.keys())):
            starts = start_station_counts.get(station_id, 0)
            ends = end_station_counts.get(station_id, 0)
            if starts + ends > 50:  # Only consider stations with significant traffic
                imbalance = abs(starts - ends) / max(starts + ends, 1)
                imbalances.append((station_id, imbalance, starts, ends))
        
        imbalances.sort(key=lambda x: x[1], reverse=True)
        for station_id, imbalance, starts, ends in imbalances[:10]:
            name = station_names.get(station_id, "Unknown")[:25]
            print(f"  {station_id}: {imbalance:.1%} imbalance ({starts} starts, {ends} ends) - {name}")
        
        return {
            'start_station_counts': dict(start_station_counts),
            'end_station_counts': dict(end_station_counts),
            'station_names': station_names,
            'total_events': events_processed
        }
    
    def analyze_user_patterns(self, max_events: int = 50000):
        """Analyze user type patterns."""
        print("\nAnalyzing user patterns...")
        
        user_type_counts = Counter()
        duration_by_usertype = defaultdict(list)
        hour_by_usertype = defaultdict(list)
        events_processed = 0
        
        for event in self.formatter:
            if events_processed >= max_events:
                break
            
            usertype = event["usertype"]
            duration = event["tripduration_s"]
            hour = event["ts"].hour
            
            user_type_counts[usertype] += 1
            
            if duration:
                duration_by_usertype[usertype].append(duration)
            
            hour_by_usertype[usertype].append(hour)
            events_processed += 1
        
        print(f"\nUser Pattern Analysis ({events_processed} events):")
        print("-" * 50)
        
        # User type distribution
        print("User type distribution:")
        for usertype, count in user_type_counts.most_common():
            percentage = (count / events_processed) * 100
            print(f"  {usertype}: {count:,} trips ({percentage:.1f}%)")
        
        # Duration patterns by user type
        print(f"\nTrip duration by user type (minutes):")
        for usertype, durations in duration_by_usertype.items():
            if durations:
                avg_duration = statistics.mean(durations) / 60
                print(f"  {usertype}: {avg_duration:.1f} min average")
        
        # Peak hours by user type
        print(f"\nPeak hours by user type:")
        for usertype, hours in hour_by_usertype.items():
            if hours:
                hour_counts = Counter(hours)
                top_hour = hour_counts.most_common(1)[0]
                print(f"  {usertype}: {top_hour[0]}:00 ({top_hour[1]} trips)")
        
        return {
            'user_type_counts': dict(user_type_counts),
            'duration_by_usertype': {ut: list(durations) for ut, durations in duration_by_usertype.items()},
            'hour_by_usertype': {ut: list(hours) for ut, hours in hour_by_usertype.items()},
            'total_events': events_processed
        }
    
    def generate_load_shedding_recommendations(self, temporal_data: dict, station_data: dict, user_data: dict):
        """Generate load shedding configuration recommendations based on analysis."""
        print("\n" + "=" * 60)
        print("LOAD SHEDDING CONFIGURATION RECOMMENDATIONS")
        print("=" * 60)
        
        recommendations = {}
        
        # Temporal-based recommendations
        hourly_counts = temporal_data['hourly_counts']
        peak_hours = sorted(hourly_counts.items(), key=lambda x: x[1], reverse=True)[:3]
        peak_hour_list = [hour for hour, _ in peak_hours]
        
        print(f"1. TEMPORAL PATTERNS:")
        print(f"   Peak hours: {', '.join([f'{h}:00' for h, _ in peak_hours])}")
        print(f"   Recommendation: Use higher thresholds during peak hours")
        
        recommendations['peak_hours'] = peak_hour_list
        recommendations['temporal_config'] = {
            'memory_threshold_peak': 0.7,  # Lower threshold during peak
            'memory_threshold_normal': 0.8,
            'cpu_threshold_peak': 0.8,
            'cpu_threshold_normal': 0.9
        }
        
        # Station-based recommendations
        start_counts = station_data['start_station_counts']
        total_stations = len(start_counts)
        high_traffic_stations = [sid for sid, count in start_counts.items() 
                               if count > statistics.mean(start_counts.values()) * 2]
        
        print(f"\n2. STATION PATTERNS:")
        print(f"   Total stations: {total_stations}")
        print(f"   High-traffic stations: {len(high_traffic_stations)} ({len(high_traffic_stations)/total_stations:.1%})")
        print(f"   Recommendation: Higher priority for high-traffic station events")
        
        recommendations['high_traffic_stations'] = high_traffic_stations[:20]  # Top 20
        
        # User type recommendations
        user_counts = user_data['user_type_counts']
        subscriber_percentage = user_counts.get('Subscriber', 0) / sum(user_counts.values()) * 100
        
        print(f"\n3. USER TYPE PATTERNS:")
        print(f"   Subscriber percentage: {subscriber_percentage:.1f}%")
        print(f"   Recommendation: Higher priority for subscriber trips")
        
        recommendations['user_priorities'] = {
            'Subscriber': 8.0,
            'Customer': 5.0
        }
        
        # Overall strategy recommendation
        print(f"\n4. RECOMMENDED LOAD SHEDDING STRATEGIES:")
        
        if subscriber_percentage > 70:
            strategy = "semantic"
            print(f"   Primary strategy: SEMANTIC (high subscriber ratio)")
            print(f"   - Use user type and station priorities")
            print(f"   - Preserve subscriber trips during peak hours")
        else:
            strategy = "adaptive"
            print(f"   Primary strategy: ADAPTIVE (mixed user base)")
            print(f"   - Learn from usage patterns")
            print(f"   - Adjust based on performance feedback")
        
        recommendations['primary_strategy'] = strategy
        
        # Sample configuration
        print(f"\n5. SAMPLE CONFIGURATION:")
        print(f"   LoadSheddingConfig(")
        print(f"       strategy_name='{strategy}',")
        print(f"       memory_threshold=0.75,")
        print(f"       cpu_threshold=0.85,")
        
        if strategy == 'semantic':
            print(f"       pattern_priorities={{")
            print(f"           'RushHourPattern': 9.0,")
            print(f"           'SubscriberPattern': 8.0,")
            print(f"           'HighTrafficStationPattern': 7.0,")
            print(f"           'RegularPattern': 5.0")
            print(f"       }},")
            print(f"       importance_attributes=['usertype', 'station_priority', 'time_priority']")
        else:
            print(f"       adaptive_learning_rate=0.05")
        
        print(f"   )")
        
        return recommendations
    
    def create_visualizations(self, temporal_data: dict, station_data: dict, user_data: dict, output_dir: str = "analysis_plots"):
        """Create visualization plots for the analysis."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Hourly distribution plot
        plt.figure(figsize=(12, 6))
        hours = list(range(24))
        counts = [temporal_data['hourly_counts'].get(h, 0) for h in hours]
        
        plt.bar(hours, counts, alpha=0.7, color='skyblue', edgecolor='navy')
        plt.title('CitiBike Trip Distribution by Hour of Day')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Trips')
        plt.xticks(hours)
        plt.grid(True, alpha=0.3)
        
        # Highlight peak hours
        peak_hours = sorted(temporal_data['hourly_counts'].items(), key=lambda x: x[1], reverse=True)[:3]
        for hour, count in peak_hours:
            plt.bar(hour, count, alpha=0.9, color='orange', edgecolor='red')
        
        plt.tight_layout()
        plot_path = os.path.join(output_dir, 'hourly_distribution.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  Saved: {plot_path}")
        
        # User type distribution
        if user_data['user_type_counts']:
            plt.figure(figsize=(8, 8))
            user_types = list(user_data['user_type_counts'].keys())
            counts = list(user_data['user_type_counts'].values())
            
            plt.pie(counts, labels=user_types, autopct='%1.1f%%', startangle=90)
            plt.title('CitiBike Trip Distribution by User Type')
            plt.axis('equal')
            
            plot_path = os.path.join(output_dir, 'user_type_distribution.png')
            plt.savefig(plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            print(f"  Saved: {plot_path}")


def main():
    """Main analysis function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CitiBike Data Analysis for Load Shedding')
    parser.add_argument('--csv', default='201309-citibike-tripdata.csv',
                       help='Path to CitiBike CSV file')
    parser.add_argument('--events', type=int, default=50000,
                       help='Maximum number of events to analyze')
    parser.add_argument('--output-dir', default='citibike_analysis',
                       help='Output directory for results')
    parser.add_argument('--plots', action='store_true',
                       help='Generate visualization plots')
    
    args = parser.parse_args()
    
    # Check if file exists
    if not os.path.exists(args.csv):
        print(f"Error: CitiBike CSV file not found: {args.csv}")
        print("Please ensure the file exists and the path is correct.")
        return
    
    print("CitiBike Data Analysis for Load Shedding Configuration")
    print("=" * 60)
    print(f"Data file: {args.csv}")
    print(f"Max events to analyze: {args.events:,}")
    print(f"Output directory: {args.output_dir}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        # Initialize analyzer
        analyzer = CitiBikeDataAnalyzer(args.csv)
        
        # Run analyses
        temporal_data = analyzer.analyze_temporal_patterns(args.events)
        station_data = analyzer.analyze_station_patterns(args.events)
        user_data = analyzer.analyze_user_patterns(args.events)
        
        # Generate recommendations
        recommendations = analyzer.generate_load_shedding_recommendations(
            temporal_data, station_data, user_data
        )
        
        # Create visualizations if requested
        if args.plots:
            print(f"\nGenerating visualization plots...")
            analyzer.create_visualizations(
                temporal_data, station_data, user_data, 
                os.path.join(args.output_dir, 'plots')
            )
        
        # Save results
        results_file = os.path.join(args.output_dir, 'analysis_results.txt')
        with open(results_file, 'w') as f:
            f.write("CitiBike Data Analysis Results\n")
            f.write("=" * 40 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Data source: {args.csv}\n")
            f.write(f"Events analyzed: {temporal_data['total_events']:,}\n\n")
            
            # Write key findings
            f.write("KEY FINDINGS:\n")
            f.write("-" * 15 + "\n")
            peak_hours = sorted(temporal_data['hourly_counts'].items(), key=lambda x: x[1], reverse=True)[:3]
            f.write(f"Peak hours: {', '.join([f'{h}:00' for h, _ in peak_hours])}\n")
            
            user_counts = user_data['user_type_counts']
            subscriber_pct = user_counts.get('Subscriber', 0) / sum(user_counts.values()) * 100
            f.write(f"Subscriber percentage: {subscriber_pct:.1f}%\n")
            
            f.write(f"Total unique stations: {len(station_data['start_station_counts'])}\n")
            f.write(f"Recommended strategy: {recommendations['primary_strategy']}\n")
        
        print(f"\nAnalysis completed successfully!")
        print(f"Results saved to: {results_file}")
        
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()