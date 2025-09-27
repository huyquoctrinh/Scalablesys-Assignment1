# CitiBike Load Shedding Integration

This document describes how to test and benchmark the load shedding implementation using real CitiBike dataset, as required for the CS-E4780 course project.

## Overview

The load shedding implementation has been specifically adapted to work with the CitiBike trip data, providing realistic testing scenarios for Complex Event Processing under varying load conditions. The integration includes:

1. **Data-specific adaptations** for CitiBike trip characteristics
2. **Semantic load shedding** tuned for bike-sharing patterns
3. **Comprehensive benchmarking** with real-world temporal patterns
4. **Performance analysis** tailored to transportation data

## Quick Start

### 1. Prerequisites

Ensure you have the CitiBike dataset file:
```bash
# Example filename: 201309-citibike-tripdata.csv
# The file should be in the project root directory
```

Install additional dependencies:
```bash
pip install -r requirements_load_shedding.txt
```

### 2. Basic Testing

Run a simple load shedding demonstration:

```bash
# Basic demo with 10,000 events
python simple_citibike_demo.py --csv 201309-citibike-tripdata.csv --events 10000

# Specify custom CSV path
python simple_citibike_demo.py --csv /path/to/your/citibike-data.csv --events 5000
```

### 3. Data Analysis

Analyze your CitiBike data to optimize load shedding parameters:

```bash
# Analyze data characteristics
python citibike_analysis.py --csv 201309-citibike-tripdata.csv --events 50000

# Generate visualization plots
python citibike_analysis.py --csv 201309-citibike-tripdata.csv --plots --output-dir analysis_results
```

### 4. Comprehensive Benchmarking

Run full benchmarking suite:

```bash
# Full benchmark (may take several minutes)
python citibike_load_shedding_benchmark.py

# Quick benchmark with fewer events
python citibike_load_shedding_benchmark.py --quick
```

## CitiBike-Specific Features

### Semantic Load Shedding for Bike Sharing

The implementation includes CitiBike-specific semantic attributes:

- **Trip Importance**: Based on duration, time of day, and user type
- **Station Priority**: Popular stations get higher priority
- **User Type Priority**: Subscribers vs. customers
- **Temporal Priority**: Rush hours vs. off-peak times

### Example Configuration

```python
from loadshedding import LoadSheddingConfig

# CitiBike-optimized configuration
citibike_config = LoadSheddingConfig(
    strategy_name='semantic',
    pattern_priorities={
        'RushHourCommute': 9.0,      # High priority for commuter analysis
        'SubscriberTrips': 8.0,      # Subscribers are regular users
        'WeekendLeisure': 5.0,       # Lower priority for leisure trips
        'LongDistanceTrip': 7.0      # Interesting for mobility patterns
    },
    importance_attributes=['importance', 'priority', 'usertype'],
    memory_threshold=0.75,           # Tuned for CitiBike event rate
    cpu_threshold=0.85
)

# Use with CEP
from CEP import CEP
cep = CEP(patterns, load_shedding_config=citibike_config)
```

## Testing Scenarios

### 1. Temporal Pattern Analysis

Tests load shedding across different time periods:

- **Rush Hours**: 7-9 AM, 5-7 PM (high load)
- **Business Hours**: 9 AM - 5 PM (medium load)
- **Off-Peak**: Evening, night, early morning (low load)
- **Weekend Patterns**: Different usage characteristics

### 2. User Type Analysis

Compares performance across user segments:

- **Subscribers**: Regular commuters, predictable patterns
- **Customers**: Tourists/casual users, variable patterns
- **Mixed Scenarios**: Combined user types

### 3. Station Popularity Analysis

Tests with varying station characteristics:

- **High-Traffic Stations**: Central locations, transit hubs
- **Low-Traffic Stations**: Residential areas, periphery
- **Imbalanced Stations**: More starts than ends (or vice versa)

### 4. Scalability Testing

Evaluates performance with different data volumes:

- **Small**: 1,000 - 5,000 events
- **Medium**: 10,000 - 25,000 events  
- **Large**: 50,000+ events

## Expected Results

### Performance Improvements

With properly tuned load shedding, you should see:

1. **Reduced Latency**: 20-40% improvement during high load
2. **Maintained Throughput**: Stable processing rate under stress
3. **Quality Preservation**: 85-95% of important patterns detected
4. **Resource Efficiency**: Lower CPU and memory usage

### Sample Benchmark Output

```
CITIBIKE LOAD SHEDDING BENCHMARK
=====================================

Testing: No Load Shedding
  ✓ Completed in 45.23 seconds
  ✓ Events processed: 10,000
  ✓ Matches found: 847
  ✓ Load shedding: Disabled

Testing: Semantic (CitiBike-tuned)  
  ✓ Completed in 32.18 seconds
  ✓ Events processed: 8,342
  ✓ Matches found: 798
  ✓ Load shedding strategy: SemanticLoadShedding
  ✓ Events dropped: 1,658 (16.6%)
  ✓ Drop rate: 16.6%
  ✓ Average throughput: 259.2 EPS

PERFORMANCE COMPARISON:
  Semantic vs baseline:
    Time improvement: 1.41x faster
    Events dropped: 16.6%
    Matches preserved: 798/847 (94.2%)
```

## Data Characteristics

### Temporal Patterns

Based on analysis of CitiBike data:

- **Peak Hours**: 8 AM, 6 PM (highest trip volume)
- **Rush Periods**: 7-10 AM, 5-8 PM (sustained high load)
- **Weekend Pattern**: Later start, more leisure trips
- **Seasonal Variation**: Weather-dependent usage

### Trip Characteristics

- **Average Duration**: 15-20 minutes
- **User Split**: ~75% Subscribers, 25% Customers
- **Popular Stations**: Central Manhattan, transit hubs
- **Trip Distance**: Mostly short-distance urban mobility

### Load Shedding Implications

- **High Priority**: Subscriber trips during rush hours
- **Medium Priority**: Regular business-hour trips
- **Low Priority**: Very short trips, off-peak leisure trips
- **Critical Events**: Long trips, popular station activity

## Benchmarking Commands

### Basic Performance Test
```bash
# Test different strategies with 5,000 events
python simple_citibike_demo.py --csv 201309-citibike-tripdata.csv --events 5000
```

### Data Analysis
```bash
# Analyze temporal and user patterns
python citibike_analysis.py --csv 201309-citibike-tripdata.csv --events 20000 --plots
```

### Comprehensive Benchmark
```bash
# Full benchmark suite (takes 10-15 minutes)
python citibike_load_shedding_benchmark.py
```

### Custom Configuration Test
```python
# In Python script or notebook
from simple_citibike_demo import run_basic_citibike_test
results = run_basic_citibike_test('201309-citibike-tripdata.csv', max_events=10000)
```

## Output Files

### Analysis Results
- `citibike_analysis/analysis_results.txt`: Data characteristics summary
- `citibike_analysis/plots/`: Visualization plots
  - `hourly_distribution.png`: Trip distribution by hour
  - `user_type_distribution.png`: User type breakdown

### Benchmark Results
- `citibike_benchmark_results/`: Benchmark output directory
- `citibike_analysis_YYYYMMDD_HHMMSS.txt`: Comprehensive report
- Performance charts and CSV exports

### Log Files
- Console output with real-time progress
- Error logs for troubleshooting
- Performance metrics and statistics

## Troubleshooting

### Common Issues

1. **CSV File Not Found**
   ```
   Error: CitiBike CSV file not found: 201309-citibike-tripdata.csv
   ```
   **Solution**: Ensure the CSV file is in the correct location or specify full path

2. **Memory Issues with Large Files**
   ```
   MemoryError during processing
   ```
   **Solution**: Reduce `--events` parameter or use data sampling

3. **Import Errors**
   ```
   ImportError: No module named 'loadshedding'
   ```
   **Solution**: Ensure load shedding module is properly installed

4. **Performance Issues**
   ```
   Processing very slow
   ```
   **Solution**: Increase `time_acceleration` parameter or reduce event count

### Performance Tuning

For optimal results with your specific dataset:

1. **Run data analysis first**: `python citibike_analysis.py`
2. **Use recommended configuration** from analysis output
3. **Start with smaller event counts** for testing
4. **Monitor system resources** during benchmarking
5. **Adjust thresholds** based on your hardware

## Integration with Course Project

### For Assignment Submission

1. **Required Files**:
   - `simple_citibike_demo.py`: Basic demonstration
   - `citibike_analysis.py`: Data analysis results
   - `CITIBIKE_RESULTS.md`: Your analysis report

2. **Recommended Tests**:
   ```bash
   # Generate results for submission
   python citibike_analysis.py --csv your-data.csv --events 50000 --plots
   python simple_citibike_demo.py --csv your-data.csv --events 10000 > results.txt
   ```

3. **Report Contents**:
   - Data characteristics analysis
   - Load shedding strategy comparison
   - Performance improvements achieved
   - Scalability analysis
   - Recommendations for real deployment

### Evaluation Metrics

Your implementation will be evaluated on:

- **Correctness**: Proper integration with CitiBike data
- **Performance**: Measurable improvements under load
- **Scalability**: Handling of different data volumes  
- **Quality**: Preservation of important patterns
- **Analysis**: Insights from real-world data

## Advanced Usage

### Custom Pattern Development

```python
# Create CitiBike-specific patterns
def create_rush_hour_pattern():
    pattern = Pattern()
    pattern.name = "RushHourCommute"
    # Define pattern logic for rush hour detection
    return pattern

def create_rebalancing_pattern():  
    pattern = Pattern()
    pattern.name = "StationImbalance"
    # Define pattern for station rebalancing needs
    return pattern
```

### Real-Time Simulation

```python
# Simulate real-time processing
input_stream = CitiBikeInputStream(
    'data.csv',
    time_acceleration=1.0,  # Real-time speed
    start_date='2013-09-01',
    end_date='2013-09-02'
)
```

### Custom Metrics

```python
# Add CitiBike-specific metrics
class CitiBikeMetricsCollector(LoadSheddingMetrics):
    def __init__(self):
        super().__init__()
        self.station_metrics = defaultdict(int)
        self.user_type_metrics = defaultdict(int)
    
    def record_citibike_event(self, event):
        self.station_metrics[event.start_station] += 1
        self.user_type_metrics[event.usertype] += 1
```

This CitiBike integration provides a comprehensive testing framework for the load shedding implementation, allowing you to demonstrate its effectiveness with real-world transportation data as required for the course project.