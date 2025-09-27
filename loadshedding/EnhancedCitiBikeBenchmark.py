"""
Enhanced CitiBike-specific benchmarking with realistic temporal patterns and domain knowledge.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import json
from .LoadSheddingBenchmark import LoadSheddingBenchmark
from .LoadSheddingStrategy import LoadSheddingStrategy
from .PerformanceEvaluator import PerformanceEvaluator


class CitiBikePatternAnalyzer:
    """Analyzes CitiBike data to understand real-world patterns for better benchmarking."""
    
    def __init__(self):
        self.temporal_patterns = {}
        self.station_patterns = {}
        self.user_behavior_patterns = {}
        
    def analyze_temporal_patterns(self, events: List[Dict]) -> Dict[str, Any]:
        """Analyze temporal patterns in CitiBike data."""
        if not events:
            return {}
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(events)
        df['hour'] = df['ts'].dt.hour
        df['day_of_week'] = df['ts'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        patterns = {
            'hourly_distribution': df['hour'].value_counts().sort_index().to_dict(),
            'daily_distribution': df['day_of_week'].value_counts().sort_index().to_dict(),
            'rush_hours': self._identify_rush_hours(df),
            'weekend_patterns': self._analyze_weekend_patterns(df),
            'seasonal_variations': self._detect_seasonal_patterns(df)
        }
        
        self.temporal_patterns = patterns
        return patterns
    
    def _identify_rush_hours(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        """Identify rush hour patterns."""
        weekday_hourly = df[~df['is_weekend']].groupby('hour').size()
        weekend_hourly = df[df['is_weekend']].groupby('hour').size()
        
        # Find peaks (rush hours are typically 7-9 AM and 5-7 PM on weekdays)
        weekday_mean = weekday_hourly.mean()
        weekend_mean = weekend_hourly.mean()
        
        weekday_rush_hours = weekday_hourly[weekday_hourly > weekday_mean * 1.5].index.tolist()
        weekend_busy_hours = weekend_hourly[weekend_hourly > weekend_mean * 1.3].index.tolist()
        
        return {
            'weekday_rush_hours': weekday_rush_hours,
            'weekend_busy_hours': weekend_busy_hours,
            'expected_weekday_rush': [7, 8, 9, 17, 18, 19],  # Expected patterns
            'actual_vs_expected_match': len(set(weekday_rush_hours) & set([7, 8, 9, 17, 18, 19])) > 3
        }
    
    def _analyze_weekend_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze weekend vs weekday patterns."""
        weekday_df = df[~df['is_weekend']]
        weekend_df = df[df['is_weekend']]
        
        return {
            'weekend_vs_weekday_ratio': len(weekend_df) / max(len(weekday_df), 1),
            'weekend_peak_hours': weekend_df.groupby('hour').size().idxmax() if len(weekend_df) > 0 else None,
            'weekday_peak_hours': weekday_df.groupby('hour').size().idxmax() if len(weekday_df) > 0 else None,
            'weekend_trip_duration_avg': weekend_df['tripduration_s'].mean() if 'tripduration_s' in weekend_df.columns else None,
            'weekday_trip_duration_avg': weekday_df['tripduration_s'].mean() if 'tripduration_s' in weekday_df.columns else None
        }
    
    def _detect_seasonal_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect seasonal patterns if data spans multiple days/months."""
        if 'ts' not in df.columns:
            return {}
        
        df['date'] = df['ts'].dt.date
        daily_counts = df.groupby('date').size()
        
        if len(daily_counts) < 7:
            return {'insufficient_data': True}
        
        return {
            'daily_variation_coefficient': daily_counts.std() / daily_counts.mean() if daily_counts.mean() > 0 else 0,
            'peak_days': daily_counts.nlargest(3).index.tolist(),
            'low_usage_days': daily_counts.nsmallest(3).index.tolist(),
            'trend': 'increasing' if daily_counts.iloc[-3:].mean() > daily_counts.iloc[:3].mean() else 'stable'
        }


class EnhancedCitiBikeBenchmark(LoadSheddingBenchmark):
    """
    Enhanced benchmarking framework specifically designed for CitiBike data patterns.
    """
    
    def __init__(self):
        super().__init__()
        self.pattern_analyzer = CitiBikePatternAnalyzer()
        self.domain_specific_metrics = {}
        
    def run_realistic_citibike_benchmark(self, 
                                       events: List[Dict],
                                       strategies: List[LoadSheddingStrategy],
                                       scenarios: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run comprehensive CitiBike-specific benchmarking.
        
        Args:
            events: CitiBike events from the formatter
            strategies: Load shedding strategies to test
            scenarios: Specific scenarios to test (optional)
        
        Returns:
            Comprehensive benchmark results
        """
        if scenarios is None:
            scenarios = [
                'rush_hour_simulation',
                'weekend_leisure_pattern',
                'weather_impact_simulation', 
                'station_rebalancing_event',
                'special_event_surge'
            ]
        
        # Analyze input data patterns
        print("Analyzing CitiBike data patterns...")
        temporal_patterns = self.pattern_analyzer.analyze_temporal_patterns(events)
        
        results = {
            'data_analysis': temporal_patterns,
            'scenario_results': {},
            'strategy_comparison': {},
            'domain_specific_insights': {}
        }
        
        # Run each scenario
        for scenario in scenarios:
            print(f"Running scenario: {scenario}")
            scenario_results = self._run_scenario(scenario, events, strategies, temporal_patterns)
            results['scenario_results'][scenario] = scenario_results
        
        # Generate strategy comparison
        results['strategy_comparison'] = self._generate_strategy_comparison(results['scenario_results'])
        
        # Generate domain-specific insights
        results['domain_specific_insights'] = self._generate_domain_insights(events, results)
        
        return results
    
    def _run_scenario(self, 
                     scenario: str, 
                     events: List[Dict], 
                     strategies: List[LoadSheddingStrategy],
                     temporal_patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Run a specific scenario simulation."""
        
        scenario_events = self._prepare_scenario_events(scenario, events, temporal_patterns)
        scenario_results = {}
        
        for strategy in strategies:
            print(f"  Testing strategy: {strategy.name}")
            
            # Simulate the scenario
            result = self._simulate_scenario_with_strategy(scenario_events, strategy, scenario)
            
            # Calculate domain-specific metrics
            domain_metrics = self._calculate_domain_specific_metrics(scenario_events, result, scenario)
            
            scenario_results[strategy.name] = {
                'basic_metrics': result,
                'domain_metrics': domain_metrics,
                'scenario_specific_insights': self._get_scenario_insights(scenario, result, domain_metrics)
            }
        
        return scenario_results
    
    def _prepare_scenario_events(self, 
                                scenario: str, 
                                events: List[Dict], 
                                temporal_patterns: Dict[str, Any]) -> List[Dict]:
        """Prepare events for specific scenario simulation."""
        
        if scenario == 'rush_hour_simulation':
            # Filter for rush hour events and amplify volume
            rush_hours = temporal_patterns.get('rush_hours', {}).get('weekday_rush_hours', [7, 8, 17, 18])
            rush_events = [e for e in events if e['ts'].hour in rush_hours]
            return self._amplify_event_volume(rush_events, factor=3.0)
        
        elif scenario == 'weekend_leisure_pattern':
            # Weekend events with different temporal distribution
            weekend_events = [e for e in events if e['ts'].weekday() in [5, 6]]
            return self._modify_temporal_distribution(weekend_events, peak_hours=[10, 11, 12, 13, 14, 15])
        
        elif scenario == 'weather_impact_simulation':
            # Simulate weather impact (e.g., rain reduces usage)
            return self._simulate_weather_impact(events, reduction_factor=0.3)
        
        elif scenario == 'station_rebalancing_event':
            # Simulate popular stations running out of bikes
            return self._simulate_station_capacity_issues(events)
        
        elif scenario == 'special_event_surge':
            # Simulate special event causing surge in specific area
            return self._simulate_event_surge(events, surge_factor=5.0, duration_hours=2)
        
        return events
    
    def _amplify_event_volume(self, events: List[Dict], factor: float) -> List[Dict]:
        """Amplify event volume by duplicating events with slight time variations."""
        amplified_events = []
        
        for event in events:
            amplified_events.append(event)  # Original event
            
            # Add duplicates with slight time offsets
            for i in range(int(factor) - 1):
                duplicate = event.copy()
                time_offset = timedelta(seconds=np.random.randint(1, 300))  # 1-5 minutes
                duplicate['ts'] = duplicate['ts'] + time_offset
                amplified_events.append(duplicate)
        
        return sorted(amplified_events, key=lambda x: x['ts'])
    
    def _modify_temporal_distribution(self, events: List[Dict], peak_hours: List[int]) -> List[Dict]:
        """Modify temporal distribution to emphasize certain hours."""
        modified_events = []
        
        for event in events:
            if event['ts'].hour in peak_hours:
                # Keep peak hour events
                modified_events.append(event)
                # Add additional events during peak hours
                if np.random.random() < 0.5:  # 50% chance to duplicate
                    duplicate = event.copy()
                    duplicate['ts'] = duplicate['ts'] + timedelta(minutes=np.random.randint(1, 30))
                    modified_events.append(duplicate)
            else:
                # Reduce non-peak hour events
                if np.random.random() < 0.7:  # Keep 70% of non-peak events
                    modified_events.append(event)
        
        return sorted(modified_events, key=lambda x: x['ts'])
    
    def _simulate_weather_impact(self, events: List[Dict], reduction_factor: float) -> List[Dict]:
        """Simulate weather impact on bike usage."""
        # Randomly remove events to simulate weather impact
        affected_events = []
        
        for event in events:
            if np.random.random() > reduction_factor:
                affected_events.append(event)
            # Weather might also affect trip duration
            elif np.random.random() < 0.3:  # Some people still ride but for shorter trips
                modified_event = event.copy()
                if 'tripduration_s' in modified_event:
                    modified_event['tripduration_s'] = int(modified_event['tripduration_s'] * 0.7)
                affected_events.append(modified_event)
        
        return affected_events
    
    def _simulate_station_capacity_issues(self, events: List[Dict]) -> List[Dict]:
        """Simulate station capacity issues affecting trip patterns."""
        # Identify popular stations
        station_counts = {}
        for event in events:
            start_station = event.get('start_station_id', 'unknown')
            station_counts[start_station] = station_counts.get(start_station, 0) + 1
        
        # Get top 20% most popular stations
        popular_stations = sorted(station_counts.keys(), 
                                key=lambda x: station_counts[x], 
                                reverse=True)[:max(1, len(station_counts) // 5)]
        
        # Reduce availability at popular stations during peak times
        modified_events = []
        for event in events:
            if (event.get('start_station_id') in popular_stations and 
                event['ts'].hour in [8, 9, 17, 18, 19]):
                # Simulate unavailable bikes - some trips don't happen
                if np.random.random() < 0.3:  # 30% of trips affected
                    continue
            modified_events.append(event)
        
        return modified_events
    
    def _simulate_event_surge(self, events: List[Dict], surge_factor: float, duration_hours: int) -> List[Dict]:
        """Simulate special event causing surge in demand."""
        # Pick a random time period for the event
        if not events:
            return events
        
        start_time = min(e['ts'] for e in events)
        end_time = max(e['ts'] for e in events)
        
        # Random event start time
        event_duration = timedelta(hours=duration_hours)
        max_event_start = end_time - event_duration
        
        if max_event_start <= start_time:
            return events
        
        event_start = start_time + timedelta(
            seconds=np.random.randint(0, int((max_event_start - start_time).total_seconds()))
        )
        event_end = event_start + event_duration
        
        # Amplify events during this period
        modified_events = []
        for event in events:
            modified_events.append(event)
            
            if event_start <= event['ts'] <= event_end:
                # Add surge events
                for _ in range(int(surge_factor)):
                    surge_event = event.copy()
                    surge_event['ts'] = surge_event['ts'] + timedelta(
                        seconds=np.random.randint(1, 3600)
                    )
                    # Mark as surge event
                    surge_event['event_type'] = 'special_event_surge'
                    modified_events.append(surge_event)
        
        return sorted(modified_events, key=lambda x: x['ts'])
    
    def _calculate_domain_specific_metrics(self, 
                                         events: List[Dict], 
                                         basic_result: Dict[str, Any],
                                         scenario: str) -> Dict[str, Any]:
        """Calculate CitiBike-specific metrics."""
        
        domain_metrics = {
            'trip_completion_rate': self._calculate_trip_completion_rate(events, basic_result),
            'station_availability_impact': self._calculate_station_impact(events, basic_result),
            'user_experience_score': self._calculate_user_experience_score(events, basic_result),
            'revenue_impact': self._calculate_revenue_impact(events, basic_result),
            'system_balance_score': self._calculate_system_balance_score(events, basic_result)
        }
        
        # Scenario-specific metrics
        if scenario == 'rush_hour_simulation':
            domain_metrics['commuter_satisfaction'] = self._calculate_commuter_satisfaction(events, basic_result)
        elif scenario == 'weekend_leisure_pattern':
            domain_metrics['leisure_user_impact'] = self._calculate_leisure_impact(events, basic_result)
        elif scenario == 'station_rebalancing_event':
            domain_metrics['rebalancing_effectiveness'] = self._calculate_rebalancing_effectiveness(events, basic_result)
        
        return domain_metrics
    
    def _calculate_trip_completion_rate(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate the rate of successful trip completions."""
        if not events:
            return 1.0
        
        total_events = len(events)
        dropped_events = result.get('events_dropped', 0)
        
        # Assume each dropped event represents a failed trip start
        completion_rate = (total_events - dropped_events) / total_events
        return completion_rate
    
    def _calculate_station_impact(self, events: List[Dict], result: Dict[str, Any]) -> Dict[str, float]:
        """Calculate impact on station availability."""
        station_usage = {}
        
        for event in events:
            start_station = event.get('start_station_id', 'unknown')
            end_station = event.get('end_station_id', 'unknown')
            
            station_usage[start_station] = station_usage.get(start_station, 0) + 1
            station_usage[end_station] = station_usage.get(end_station, 0) + 1
        
        # Calculate impact scores
        popular_stations = sorted(station_usage.keys(), 
                                key=lambda x: station_usage[x], 
                                reverse=True)[:10]
        
        impact_score = 0.0
        if popular_stations:
            # Assume higher drop rates affect popular stations more
            drop_rate = result.get('drop_rate', 0.0)
            impact_score = drop_rate * 0.8  # 80% of drop rate impacts station availability
        
        return {
            'overall_impact_score': impact_score,
            'popular_stations_affected': len(popular_stations),
            'estimated_availability_reduction': impact_score * 0.3  # Rough estimate
        }
    
    def _calculate_user_experience_score(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate overall user experience score."""
        base_score = 1.0
        
        # Reduce score based on drop rate
        drop_rate = result.get('drop_rate', 0.0)
        base_score -= drop_rate * 0.6  # Drop rate has major impact
        
        # Reduce score based on latency
        avg_latency = result.get('average_latency_ms', 0.0)
        if avg_latency > 100:  # More than 100ms is noticeable
            base_score -= min(0.3, (avg_latency - 100) / 1000)  # Latency penalty
        
        # Factor in throughput
        throughput = result.get('events_per_second', 0.0)
        expected_throughput = len(events) / 3600 if events else 0  # Events per second
        if throughput < expected_throughput * 0.8:
            base_score -= 0.2  # Throughput penalty
        
        return max(0.0, base_score)
    
    def _calculate_revenue_impact(self, events: List[Dict], result: Dict[str, Any]) -> Dict[str, float]:
        """Calculate estimated revenue impact."""
        # Assume average revenue per trip
        avg_revenue_per_trip = 3.50  # USD, typical CitiBike fare
        
        total_potential_revenue = len(events) * avg_revenue_per_trip
        dropped_events = result.get('events_dropped', 0)
        lost_revenue = dropped_events * avg_revenue_per_trip
        
        # Calculate subscriber vs casual impact (subscribers more valuable)
        subscriber_events = sum(1 for e in events if e.get('usertype', '').lower() == 'subscriber')
        casual_events = len(events) - subscriber_events
        
        # Estimate what percentage of drops were subscribers vs casual
        subscriber_drop_ratio = subscriber_events / max(len(events), 1)
        estimated_subscriber_drops = dropped_events * subscriber_drop_ratio
        estimated_casual_drops = dropped_events - estimated_subscriber_drops
        
        # Subscribers generate more lifetime value
        subscriber_ltv_multiplier = 5.0
        lost_subscriber_value = estimated_subscriber_drops * avg_revenue_per_trip * subscriber_ltv_multiplier
        lost_casual_value = estimated_casual_drops * avg_revenue_per_trip
        
        return {
            'immediate_revenue_loss': lost_revenue,
            'subscriber_impact': lost_subscriber_value,
            'casual_impact': lost_casual_value,
            'total_estimated_impact': lost_subscriber_value + lost_casual_value,
            'revenue_retention_rate': (total_potential_revenue - lost_revenue) / max(total_potential_revenue, 1)
        }
    
    def _calculate_system_balance_score(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate how well the system maintains station balance."""
        if not events:
            return 1.0
        
        # Calculate station imbalance
        station_net_change = {}
        
        for event in events:
            start_station = event.get('start_station_id', 'unknown')
            end_station = event.get('end_station_id', 'unknown')
            
            # Bike leaves start station (-1), arrives at end station (+1)
            station_net_change[start_station] = station_net_change.get(start_station, 0) - 1
            station_net_change[end_station] = station_net_change.get(end_station, 0) + 1
        
        # Calculate imbalance score (lower is better)
        imbalances = [abs(change) for change in station_net_change.values()]
        avg_imbalance = sum(imbalances) / max(len(imbalances), 1)
        
        # Normalize to 0-1 score (1 is perfect balance)
        # Assume perfect balance would be avg_imbalance near 0
        balance_score = max(0.0, 1.0 - (avg_imbalance / 10))  # Scale factor
        
        # Adjust based on drop rate (dropped events might help balance)
        drop_rate = result.get('drop_rate', 0.0)
        if drop_rate > 0.1:  # If dropping more than 10%
            # Might actually help with balance, but user experience suffers
            balance_score += drop_rate * 0.1
            balance_score = min(1.0, balance_score)
        
        return balance_score
    
    def _calculate_commuter_satisfaction(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate satisfaction score for commuter users."""
        # Focus on subscriber trips during rush hours
        commuter_events = [
            e for e in events 
            if (e.get('usertype', '').lower() == 'subscriber' and 
                e['ts'].hour in [7, 8, 9, 17, 18, 19])
        ]
        
        if not commuter_events:
            return 1.0
        
        # Commuters are less tolerant of delays and drops
        base_satisfaction = 1.0
        drop_rate = result.get('drop_rate', 0.0)
        
        # Heavy penalty for dropping commuter trips
        commuter_penalty = drop_rate * 1.2  # 20% more penalty than general users
        base_satisfaction -= commuter_penalty
        
        # Latency penalty (commuters are time-sensitive)
        avg_latency = result.get('average_latency_ms', 0.0)
        if avg_latency > 50:  # Commuters sensitive to any delay
            base_satisfaction -= min(0.4, (avg_latency - 50) / 500)
        
        return max(0.0, base_satisfaction)
    
    def _calculate_leisure_impact(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate impact on leisure users (weekend/casual riders)."""
        leisure_events = [
            e for e in events 
            if (e.get('usertype', '').lower() == 'customer' or 
                e['ts'].weekday() in [5, 6])  # Weekend or casual users
        ]
        
        if not leisure_events:
            return 1.0
        
        # Leisure users might be more tolerant of minor delays but sensitive to drops
        base_score = 1.0
        drop_rate = result.get('drop_rate', 0.0)
        
        # Moderate penalty for drops (leisure users have alternatives)
        leisure_penalty = drop_rate * 0.8
        base_score -= leisure_penalty
        
        return max(0.0, base_score)
    
    def _calculate_rebalancing_effectiveness(self, events: List[Dict], result: Dict[str, Any]) -> float:
        """Calculate how effective load shedding is for station rebalancing."""
        # This is a complex metric that would need real rebalancing data
        # For now, estimate based on how well load shedding preserves system balance
        
        balance_score = self._calculate_system_balance_score(events, result)
        drop_rate = result.get('drop_rate', 0.0)
        
        # Effective rebalancing through load shedding should maintain balance
        # while dropping less critical trips
        if drop_rate > 0 and balance_score > 0.7:
            # Good balance maintained with some drops = effective
            effectiveness = balance_score * (1 + drop_rate * 0.5)
        else:
            # Poor balance or no drops = less effective
            effectiveness = balance_score
        
        return min(1.0, effectiveness)
    
    def _get_scenario_insights(self, scenario: str, basic_result: Dict, domain_metrics: Dict) -> List[str]:
        """Generate human-readable insights for each scenario."""
        insights = []
        
        drop_rate = basic_result.get('drop_rate', 0.0)
        user_experience = domain_metrics.get('user_experience_score', 1.0)
        
        if scenario == 'rush_hour_simulation':
            if drop_rate > 0.2:
                insights.append("High drop rate during rush hour may significantly impact commuter satisfaction")
            if domain_metrics.get('commuter_satisfaction', 1.0) < 0.7:
                insights.append("Commuter satisfaction critically low - consider more aggressive load balancing")
            
        elif scenario == 'weekend_leisure_pattern':
            if user_experience < 0.8:
                insights.append("Weekend leisure users experiencing degraded service quality")
            
        elif scenario == 'station_rebalancing_event':
            effectiveness = domain_metrics.get('rebalancing_effectiveness', 0.0)
            if effectiveness > 0.8:
                insights.append("Load shedding effectively maintaining system balance during capacity issues")
            else:
                insights.append("Load shedding not effectively addressing station capacity problems")
        
        # Revenue insights
        revenue_impact = domain_metrics.get('revenue_impact', {})
        if revenue_impact.get('revenue_retention_rate', 1.0) < 0.9:
            insights.append(f"Estimated revenue impact: ${revenue_impact.get('immediate_revenue_loss', 0):.2f}")
        
        return insights
    
    def _generate_strategy_comparison(self, scenario_results: Dict) -> Dict[str, Any]:
        """Generate comparison across strategies and scenarios."""
        strategy_scores = {}
        
        for scenario, results in scenario_results.items():
            for strategy_name, strategy_result in results.items():
                if strategy_name not in strategy_scores:
                    strategy_scores[strategy_name] = {
                        'total_score': 0.0,
                        'scenario_scores': {},
                        'strengths': [],
                        'weaknesses': []
                    }
                
                # Calculate composite score for this scenario
                basic_metrics = strategy_result.get('basic_metrics', {})
                domain_metrics = strategy_result.get('domain_metrics', {})
                
                # Weighted composite score
                throughput_score = min(1.0, basic_metrics.get('events_per_second', 0) / 1000)  # Normalize
                quality_score = 1.0 - basic_metrics.get('drop_rate', 0.0)
                latency_score = max(0.0, 1.0 - basic_metrics.get('average_latency_ms', 0) / 1000)
                user_experience_score = domain_metrics.get('user_experience_score', 1.0)
                
                composite_score = (
                    throughput_score * 0.25 +
                    quality_score * 0.35 +
                    latency_score * 0.15 +
                    user_experience_score * 0.25
                )
                
                strategy_scores[strategy_name]['scenario_scores'][scenario] = composite_score
                strategy_scores[strategy_name]['total_score'] += composite_score
        
        # Normalize total scores
        for strategy_name in strategy_scores:
            strategy_scores[strategy_name]['total_score'] /= len(scenario_results)
        
        # Rank strategies
        ranked_strategies = sorted(strategy_scores.items(), 
                                 key=lambda x: x[1]['total_score'], 
                                 reverse=True)
        
        return {
            'strategy_scores': strategy_scores,
            'ranking': [name for name, _ in ranked_strategies],
            'best_strategy': ranked_strategies[0][0] if ranked_strategies else None,
            'performance_matrix': self._create_performance_matrix(scenario_results)
        }
    
    def _create_performance_matrix(self, scenario_results: Dict) -> Dict[str, Dict[str, float]]:
        """Create performance matrix for easy comparison."""
        matrix = {}
        
        for scenario, results in scenario_results.items():
            matrix[scenario] = {}
            for strategy_name, strategy_result in results.items():
                basic_metrics = strategy_result.get('basic_metrics', {})
                matrix[scenario][strategy_name] = {
                    'throughput': basic_metrics.get('events_per_second', 0),
                    'drop_rate': basic_metrics.get('drop_rate', 0),
                    'latency_ms': basic_metrics.get('average_latency_ms', 0),
                    'user_experience': strategy_result.get('domain_metrics', {}).get('user_experience_score', 1.0)
                }
        
        return matrix
    
    def _generate_domain_insights(self, events: List[Dict], results: Dict) -> Dict[str, Any]:
        """Generate overall insights specific to CitiBike domain."""
        insights = {
            'data_characteristics': self._analyze_data_characteristics(events),
            'load_shedding_recommendations': self._generate_recommendations(results),
            'operational_insights': self._generate_operational_insights(events, results),
            'business_impact_summary': self._summarize_business_impact(results)
        }
        
        return insights
    
    def _analyze_data_characteristics(self, events: List[Dict]) -> Dict[str, Any]:
        """Analyze characteristics of the CitiBike data."""
        if not events:
            return {}
        
        characteristics = {
            'total_events': len(events),
            'time_span': {
                'start': min(e['ts'] for e in events),
                'end': max(e['ts'] for e in events),
                'duration_hours': (max(e['ts'] for e in events) - min(e['ts'] for e in events)).total_seconds() / 3600
            },
            'user_types': {},
            'temporal_distribution': {},
            'geographic_spread': {}
        }
        
        # User type distribution
        for event in events:
            user_type = event.get('usertype', 'unknown')
            characteristics['user_types'][user_type] = characteristics['user_types'].get(user_type, 0) + 1
        
        return characteristics
    
    def _generate_recommendations(self, results: Dict) -> List[str]:
        """Generate load shedding strategy recommendations."""
        recommendations = []
        
        strategy_comparison = results.get('strategy_comparison', {})
        best_strategy = strategy_comparison.get('best_strategy')
        
        if best_strategy:
            recommendations.append(f"Recommended strategy: {best_strategy} based on overall performance")
        
        # Analyze scenario-specific recommendations
        scenario_results = results.get('scenario_results', {})
        
        for scenario, scenario_data in scenario_results.items():
            best_for_scenario = max(scenario_data.keys(), 
                                  key=lambda k: scenario_data[k].get('basic_metrics', {}).get('events_per_second', 0))
            recommendations.append(f"For {scenario}: {best_for_scenario} performs best")
        
        return recommendations
    
    def _generate_operational_insights(self, events: List[Dict], results: Dict) -> List[str]:
        """Generate operational insights for CitiBike system."""
        insights = []
        
        # Rush hour insights
        rush_hour_events = [e for e in events if e['ts'].hour in [7, 8, 9, 17, 18, 19]]
        rush_hour_ratio = len(rush_hour_events) / max(len(events), 1)
        
        if rush_hour_ratio > 0.4:
            insights.append("High rush hour concentration - consider rush hour specific load shedding")
        
        # Station balance insights
        if 'station_rebalancing_event' in results.get('scenario_results', {}):
            rebalancing_results = results['scenario_results']['station_rebalancing_event']
            best_rebalancing_strategy = max(rebalancing_results.keys(),
                                          key=lambda k: rebalancing_results[k].get('domain_metrics', {}).get('rebalancing_effectiveness', 0))
            insights.append(f"For station rebalancing: {best_rebalancing_strategy} most effective")
        
        return insights
    
    def _summarize_business_impact(self, results: Dict) -> Dict[str, Any]:
        """Summarize business impact across all scenarios."""
        total_revenue_impact = 0.0
        user_satisfaction_impact = 0.0
        operational_efficiency_impact = 0.0
        
        scenario_results = results.get('scenario_results', {})
        scenario_count = len(scenario_results)
        
        if scenario_count == 0:
            return {}
        
        for scenario_data in scenario_results.values():
            for strategy_result in scenario_data.values():
                domain_metrics = strategy_result.get('domain_metrics', {})
                revenue_impact = domain_metrics.get('revenue_impact', {})
                
                total_revenue_impact += revenue_impact.get('revenue_retention_rate', 1.0)
                user_satisfaction_impact += domain_metrics.get('user_experience_score', 1.0)
                operational_efficiency_impact += domain_metrics.get('system_balance_score', 1.0)
        
        # Average across all strategy-scenario combinations
        num_combinations = sum(len(scenario_data) for scenario_data in scenario_results.values())
        
        return {
            'average_revenue_retention': total_revenue_impact / max(num_combinations, 1),
            'average_user_satisfaction': user_satisfaction_impact / max(num_combinations, 1),
            'average_operational_efficiency': operational_efficiency_impact / max(num_combinations, 1),
            'overall_business_health_score': (
                (total_revenue_impact + user_satisfaction_impact + operational_efficiency_impact) / 
                (3 * max(num_combinations, 1))
            )
        }
