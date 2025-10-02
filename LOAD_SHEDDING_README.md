# Load Shedding for OpenCEP

This document describes the comprehensive load shedding implementation for the OpenCEP (Open Complex Event Processing) system, developed as part of the CS-E4780 Scalable Systems course project.

## Overview

Load shedding is a critical technique for maintaining system performance during high-load conditions by selectively dropping events when system resources are strained. This implementation provides multiple load shedding strategies with comprehensive benchmarking and evaluation capabilities.

## Features

### 🎯 Core Load Shedding Capabilities
- **Multiple Strategies**: Probabilistic, semantic, adaptive, and baseline strategies
- **Dynamic Configuration**: Runtime configuration updates without system restart
- **Comprehensive Monitoring**: System resource and performance metrics tracking
- **Adaptive Pattern Management**: Dynamic pattern activation/deactivation based on load

### 📊 Benchmarking and Evaluation
- **Synthetic Workload Generation**: Various load patterns (bursty, gradual increase, spikes, etc.)
- **Performance Metrics**: Throughput, latency, quality preservation, efficiency
- **Strategy Comparison**: Head-to-head comparison of different approaches
- **Stress Testing**: System behavior under increasing load conditions
- **Visualization**: Performance charts and reports

### 🔧 Integration
- **Seamless Integration**: Works with existing OpenCEP architecture
- **Backward Compatibility**: Non-breaking changes to existing code
- **Optional Activation**: Can be enabled/disabled per CEP instance

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CEP Engine    │────│ Load Shedding    │────│  Performance    │
│                 │    │    Module        │    │   Evaluation    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌──────────┐      ┌─────────────┐      ┌─────────────────┐
   │  Load    │      │  Shedding   │      │   Adaptive      │
   │ Monitor  │      │ Strategies  │      │   Pattern       │
   │          │      │             │      │   Manager       │
   └──────────┘      └─────────────┘      └─────────────────┘
```

## Quick Start

### 1. Basic Usage

```python
from CEP import CEP
from loadshedding import LoadSheddingConfig, PresetConfigs

# Create patterns
patterns = [pattern1, pattern2, pattern3]

# Configure load shedding
config = PresetConfigs.conservative()  # or create custom config

# Create CEP instance with load shedding
cep = CEP(patterns, load_shedding_config=config)

# Run processing
duration = cep.run(input_stream, output_stream, data_formatter)

# Get statistics
stats = cep.get_load_shedding_statistics()
print(f"Drop rate: {stats['drop_rate']:.1%}")
```

### 2. Custom Configuration

```python
from loadshedding import LoadSheddingConfig, LoadLevel

config = LoadSheddingConfig(
    enabled=True,
    strategy_name='probabilistic',
    memory_threshold=0.8,
    cpu_threshold=0.9,
    drop_probabilities={
        LoadLevel.NORMAL: 0.0,
        LoadLevel.MEDIUM: 0.1,
        LoadLevel.HIGH: 0.3,
        LoadLevel.CRITICAL: 0.6
    }
)
```

### 3. Benchmarking

```python
from loadshedding import LoadSheddingBenchmark, PerformanceEvaluator

benchmark = LoadSheddingBenchmark()
evaluator = PerformanceEvaluator()

# Run comprehensive benchmark
results = benchmark.run_synthetic_workload_test(strategies, patterns)
evaluation = evaluator.evaluate_single_test(metrics)
```

## Load Shedding Strategies

### 1. Probabilistic Load Shedding
Drops events based on predefined probabilities for each load level.

**Use Cases:**
- Predictable load patterns
- Simple implementation requirements
- When uniform dropping is acceptable

**Configuration:**
```python
config = LoadSheddingConfig(
    strategy_name='probabilistic',
    drop_probabilities={
        LoadLevel.NORMAL: 0.0,
        LoadLevel.MEDIUM: 0.1,
        LoadLevel.HIGH: 0.3,
        LoadLevel.CRITICAL: 0.6
    }
)
```

### 2. Semantic Load Shedding
Makes dropping decisions based on event importance and pattern priorities.

**Use Cases:**
- Quality-critical applications
- Events with varying importance levels
- Business-value-aware processing

**Configuration:**
```python
config = LoadSheddingConfig(
    strategy_name='semantic',
    pattern_priorities={
        'CriticalPattern': 10.0,
        'ImportantPattern': 7.0,
        'RegularPattern': 4.0
    },
    importance_attributes=['priority', 'severity', 'critical']
)
```

### 3. Adaptive Load Shedding
Learns from system behavior and adjusts dropping strategy dynamically.

**Use Cases:**
- Varying workload patterns
- Long-running systems
- When manual tuning is difficult

**Configuration:**
```python
config = LoadSheddingConfig(
    strategy_name='adaptive',
    adaptive_learning_rate=0.1
)
```

## Performance Metrics

### Throughput Metrics
- **Events per second**: Raw event processing rate
- **Patterns per second**: Pattern matching rate
- **Drop rate**: Percentage of events dropped
- **Processing efficiency**: Ratio of processed to total events

### Latency Metrics
- **Average latency**: Mean processing time per event
- **Percentile latencies**: P95, P99 processing times
- **Latency stability**: Coefficient of variation
- **End-to-end latency**: Complete processing time

### Quality Metrics
- **Precision/Recall**: Pattern matching accuracy
- **F1 Score**: Harmonic mean of precision and recall
- **Quality preservation**: Ratio of maintained vs. expected matches
- **False negative rate**: Missed matches due to dropping

### Efficiency Metrics
- **Resource utilization**: CPU and memory usage
- **Cost-benefit ratio**: Matches found per event processed
- **Load shedding effectiveness**: Recovery time and success rate

## Benchmarking Framework

### Synthetic Workload Patterns

1. **Constant Load**: Steady event rate
2. **Bursty**: Alternating high and low activity periods
3. **Gradual Increase**: Linearly increasing load
4. **Spike Pattern**: Sudden load spikes
5. **Periodic**: Sinusoidal load variation

### Test Types

1. **Strategy Comparison**: Head-to-head performance comparison
2. **Stress Testing**: Behavior under increasing load
3. **Long-term Stability**: Extended operation testing
4. **Recovery Testing**: System recovery after load spikes

### Example Benchmark Run

```bash
# Run comprehensive benchmark
python examples/load_shedding_demo.py --comparison --duration 120 --output-dir results

# Run stress test
python examples/load_shedding_demo.py --stress-test --output-dir stress_results

# Run specific strategy test
python examples/load_shedding_demo.py --strategy semantic --workload bursty --duration 60
```

## Configuration Reference

### LoadSheddingConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | True | Enable/disable load shedding |
| `strategy_name` | str | 'probabilistic' | Strategy to use |
| `memory_threshold` | float | 0.8 | Memory usage threshold (0.0-1.0) |
| `cpu_threshold` | float | 0.9 | CPU usage threshold (0.0-1.0) |
| `queue_threshold` | int | 10000 | Queue size threshold |
| `latency_threshold_ms` | float | 100.0 | Latency threshold in milliseconds |
| `monitoring_interval_sec` | float | 1.0 | Monitoring check interval |

### Preset Configurations

```python
# Conservative - drops only under severe load
config = PresetConfigs.conservative()

# Aggressive - starts dropping earlier
config = PresetConfigs.aggressive()

# Quality preserving - uses semantic strategy
config = PresetConfigs.quality_preserving()

# Adaptive learning - uses adaptive strategy
config = PresetConfigs.adaptive_learning()

# Disabled - no load shedding
config = PresetConfigs.disabled()
```

## Monitoring and Alerting

### Real-time Metrics

The system provides real-time access to load shedding metrics:

```python
# Get current statistics
stats = cep.get_load_shedding_statistics()

# Check if load shedding is active
if cep.is_load_shedding_enabled():
    metrics = cep.get_load_shedding_metrics()
    
    # Access detailed metrics
    throughput = metrics.get_average_throughput()
    latency = metrics.get_average_latency()
    drop_rate = metrics.get_current_drop_rate()
```

### Dashboard Integration

The system generates dashboard-ready data:

```python
from loadshedding import LoadSheddingReporter

reporter = LoadSheddingReporter()
dashboard_data = reporter.generate_summary_dashboard_data(metrics, evaluation)

# Dashboard data includes:
# - Key performance indicators
# - Alert conditions
# - Time series data
# - Resource utilization
```

## Visualization and Reporting

### Generated Reports

1. **Comprehensive Analysis Report**: Detailed text-based analysis
2. **Performance Visualization Charts**: Throughput, latency, resource usage
3. **Strategy Comparison Charts**: Head-to-head performance comparison
4. **CSV Data Exports**: Raw metrics for external analysis

### Example Visualizations

The system generates various charts:

- **Throughput over time**: Event processing rate trends
- **Latency distribution**: Processing time histograms
- **Resource utilization**: CPU, memory, and queue size over time
- **Load shedding effectiveness**: Load level vs. drop rate correlation

## Best Practices

### 1. Configuration Tuning
- Start with conservative settings and adjust based on observations
- Monitor quality metrics alongside performance metrics
- Use semantic strategy for business-critical applications

### 2. Pattern Priority Management
- Assign priorities based on business value
- Regularly review and update pattern priorities
- Use adaptive pattern management for dynamic environments

### 3. Monitoring Setup
- Set up alerts for high drop rates (>30%)
- Monitor latency trends and spikes
- Track quality preservation ratios

### 4. Testing Strategy
- Benchmark with realistic workload patterns
- Test recovery scenarios and failure modes
- Validate quality preservation under different loads

## Performance Considerations

### Memory Usage
- Load shedding adds ~5-10% memory overhead
- Metrics collection requires additional storage
- Pattern management scales with number of patterns

### CPU Overhead
- Monitoring adds ~2-5% CPU overhead
- Strategy complexity affects processing cost
- Adaptive strategies have higher computational cost

### Latency Impact
- Load shedding decisions add ~0.1-1ms per event
- Semantic evaluation can add 1-5ms for complex rules
- Buffered streams reduce latency variance

## Troubleshooting

### Common Issues

1. **High Drop Rates**
   - Check resource thresholds
   - Review pattern priorities
   - Consider system scaling

2. **Poor Quality Preservation**
   - Use semantic strategy
   - Adjust importance attributes
   - Review pattern priorities

3. **System Instability**
   - Reduce monitoring frequency
   - Increase recovery thresholds
   - Check for memory leaks

### Debug Mode

Enable debug logging for detailed information:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Load shedding components will provide detailed logs
```

## Integration with Existing Systems

### CEP Integration
The load shedding system integrates seamlessly with existing OpenCEP components:

- **Evaluation Mechanisms**: Works with all existing evaluation engines
- **Pattern Preprocessing**: Compatible with pattern transformation
- **Parallel Execution**: Supports multi-threaded processing
- **Stream Processing**: Compatible with all stream types

### External Monitoring
Integration points for external monitoring systems:

```python
# Export metrics to external systems
metrics_data = cep.get_load_shedding_metrics().get_summary_report()

# Custom metric exporters can be implemented
class PrometheusExporter:
    def export_metrics(self, metrics):
        # Export to Prometheus/Grafana
        pass
```

## Future Enhancements

### Planned Features
1. **Machine Learning Integration**: ML-based load prediction and strategy optimization
2. **Distributed Load Shedding**: Coordination across multiple CEP instances
3. **Advanced Visualization**: Real-time dashboard with interactive charts
4. **Custom Strategy Framework**: Easy development of custom strategies

### Research Directions
1. **Predictive Load Shedding**: Anticipate load spikes before they occur
2. **Quality-Aware Optimization**: Multi-objective optimization for quality vs. performance
3. **Federated Learning**: Shared learning across distributed deployments

## Contributing

### Development Setup
```bash
# Clone the repository
git clone <repository-url>

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Run load shedding demo
python examples/load_shedding_demo.py --comparison
```

### Adding New Strategies
To implement a custom load shedding strategy:

```python
from loadshedding import LoadSheddingStrategy

class CustomLoadShedding(LoadSheddingStrategy):
    def __init__(self, custom_params):
        super().__init__("CustomStrategy")
        self.custom_params = custom_params
    
    def should_drop_event(self, event, load_level):
        # Implement custom logic
        return decision
```

### Testing Guidelines
- Add unit tests for new strategies
- Include integration tests with CEP engine
- Benchmark performance impact
- Document configuration parameters

## References

1. **Load Shedding Techniques**: [Academic Paper References]
2. **Complex Event Processing**: [CEP Literature]
3. **Performance Optimization**: [System Performance Papers]
4. **Quality Preservation**: [QoS Research]

## License

This load shedding implementation is part of the OpenCEP project and follows the same licensing terms.

---

For questions, issues, or contributions, please contact the development team or create an issue in the project repository.