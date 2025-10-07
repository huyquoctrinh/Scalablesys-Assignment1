# Memory Optimization for 20K+ Events Processing

This document describes the memory optimizations implemented to handle large CitiBike datasets (20,000+ events) without overwhelming server resources.

## Problem

The original implementation could not handle 20,000 events due to:
- **Explosive memory growth** in Kleene closure patterns (max_len=21)
- **Unbounded state accumulation** in pattern matching
- **Memory-intensive attribute calculations**
- **Queue overflow** in multi-threaded processing

## Memory Optimization Solutions

### 1. Pattern Memory Limits
```python
# BEFORE: max_len=21 (explosive growth)
# AFTER: max_len=5-8 (controlled growth)
chain_inside_a = AdjacentChainingKC(max_len=8)  # Reduced from 21
```

### 2. Memory-Optimized Stream Processing
```python
# Automatic memory optimization for large datasets
memory_optimized = max_events > 10000

# Benefits:
- Smaller queue sizes (1000 vs 5000)
- Reduced station analysis samples (500 vs 1000)
- Limited thread count (3 vs 4+)
- Simplified importance calculations
```

### 3. Simplified Attribute Calculation
For datasets >10K events, use lightweight calculations:
```python
def _calculate_simple_importance(data):
    # Basic time + user type only (vs. 7+ factors)
    importance = 0.5
    if rush_hour: importance += 0.3
    if subscriber: importance += 0.1
    return importance
```

### 4. Aggressive Load Shedding
```python
# Memory-optimized configuration
LoadSheddingConfig(
    memory_threshold=0.6,  # vs 0.75 (more aggressive)
    cpu_threshold=0.7,     # vs 0.85 (more aggressive)
    importance_attributes=['importance', 'priority']  # vs 4+ attributes
)
```

### 5. Output Stream Optimization
```python
if memory_optimized:
    writer_threads = 1      # vs 3 (reduce thread overhead)
    batch_size = max(100, max_events // 100)  # larger batches
    flush_interval = 2.0    # vs 1.0 (less frequent I/O)
```

### 6. Memory Monitoring
```python
# Real-time memory tracking
initial_memory = get_memory_usage()
# ... processing ...
final_memory = get_memory_usage()
memory_delta = final_memory['rss_mb'] - initial_memory['rss_mb']
```

## Usage

### Automatic Optimization
```bash
# Memory optimization enables automatically for >10K events
python enhanced_citybike_demo_hot_path.py --events 20000
```

### Manual Testing
```bash
# Test memory scaling across different sizes
python test_memory_scaling.py

# Run optimized 20K demo
python run_20k_demo.py
```

## Performance Characteristics

| Events | Memory Mode | Max Length | Threads | Expected Memory | Throughput |
|--------|-------------|------------|---------|-----------------|------------|
| 1K-5K  | Standard    | 21         | 4       | 100-500 MB      | High       |
| 5K-10K | Standard    | 8          | 3       | 500-1000 MB     | Medium     |
| 10K+   | Optimized   | 5          | 3       | 1000-2000 MB    | Medium     |
| 20K+   | Optimized   | 5          | 3       | 2000-4000 MB    | Lower      |

## Memory-Safe Configuration Examples

### Small Datasets (< 5K events)
```python
max_len = 21        # Full pattern complexity
threads = 4         # Full parallelism
load_shedding = Optional
```

### Medium Datasets (5K-10K events)
```python
max_len = 8         # Reduced complexity
threads = 3         # Moderate parallelism
load_shedding = Recommended
```

### Large Datasets (10K+ events)
```python
max_len = 5         # Minimal complexity
threads = 3         # Limited parallelism
load_shedding = Required
memory_optimized = True
```

## Key Improvements

1. **Linear Memory Scaling**: Changed from exponential to linear memory growth
2. **Controlled State**: Limited pattern state accumulation
3. **Predictable Performance**: Memory usage is now predictable
4. **Graceful Degradation**: System remains stable under memory pressure
5. **Real-time Monitoring**: Track memory usage during processing

## Monitoring Commands

```bash
# Check system memory before running
free -h

# Monitor memory during processing (Linux/Mac)
watch -n 1 'ps aux | grep python | head -5'

# Windows memory monitoring
Get-Process python | Select-Object Name,WorkingSet,PagedMemorySize
```

## Troubleshooting

### Out of Memory
- Reduce `max_events` to 15K or 10K
- Enable load shedding with lower thresholds
- Reduce `max_len` to 3-4
- Use single thread (`num_threads=1`)

### Slow Performance
- Balance memory vs speed by adjusting `max_len`
- Use moderate load shedding (memory_threshold=0.7)
- Increase batch sizes for I/O efficiency

### Pattern Quality Issues
- Increase `max_len` slightly (6-7) if memory allows
- Use time-aware importance weighting
- Monitor pattern completion rates

This optimization approach allows processing 20,000+ events while maintaining system stability and reasonable performance.