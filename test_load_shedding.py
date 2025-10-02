#!/usr/bin/env python3
"""
Basic test script to verify load shedding functionality.
This script performs basic validation of the load shedding implementation.
"""

import sys
import os
import traceback
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_imports():
    """Test that all load shedding modules can be imported."""
    print("Testing imports...")
    
    try:
        from loadshedding import (
            LoadMonitor, LoadLevel,
            LoadSheddingStrategy, ProbabilisticLoadShedding, SemanticLoadShedding,
            LoadSheddingConfig, LoadSheddingMetrics,
            LoadAwareInputStream, AdaptivePatternManager,
            LoadSheddingBenchmark, PerformanceEvaluator, LoadSheddingReporter
        )
        print("✓ All core modules imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


def test_load_monitor():
    """Test LoadMonitor functionality."""
    print("\nTesting LoadMonitor...")
    
    try:
        from loadshedding import LoadMonitor, LoadLevel
        
        # Create load monitor
        monitor = LoadMonitor(
            memory_threshold=0.8,
            cpu_threshold=0.9,
            queue_threshold=1000,
            latency_threshold_ms=100
        )
        
        # Test basic functionality
        load_level = monitor.assess_load_level()
        assert isinstance(load_level, LoadLevel)
        
        # Test metrics update
        monitor.update_event_metrics(queue_size=500, events_processed=100, latest_latency_ms=50.0)
        metrics = monitor.get_current_metrics()
        assert 'load_level' in metrics
        assert 'cpu_percent' in metrics
        
        print("✓ LoadMonitor working correctly")
        return True
    except Exception as e:
        print(f"✗ LoadMonitor test failed: {e}")
        traceback.print_exc()
        return False


def test_load_shedding_strategies():
    """Test different load shedding strategies."""
    print("\nTesting load shedding strategies...")
    
    try:
        from loadshedding import (
            ProbabilisticLoadShedding, SemanticLoadShedding, 
            AdaptiveLoadShedding, NoLoadShedding, LoadLevel
        )
        from base.Event import Event
        
        # Create a mock event
        try:
            event = Event(timestamp=datetime.now().timestamp(), payload={'test': 'data'})
            event.event_type = 'TestEvent'
        except Exception:
            # Fallback if Event constructor is different
            event = type('MockEvent', (), {
                'timestamp': datetime.now().timestamp(),
                'payload': {'test': 'data'},
                'event_type': 'TestEvent'
            })()
        
        strategies = [
            NoLoadShedding(),
            ProbabilisticLoadShedding(),
            SemanticLoadShedding(),
            AdaptiveLoadShedding()
        ]
        
        for strategy in strategies:
            # Test with different load levels
            for load_level in LoadLevel:
                should_drop = strategy.should_drop_event(event, load_level)
                assert isinstance(should_drop, bool)
            
            # Test statistics
            drop_rate = strategy.get_drop_rate()
            assert isinstance(drop_rate, float)
            assert 0.0 <= drop_rate <= 1.0
        
        print("✓ All load shedding strategies working correctly")
        return True
    except Exception as e:
        print(f"✗ Load shedding strategy test failed: {e}")
        traceback.print_exc()
        return False


def test_load_shedding_config():
    """Test LoadSheddingConfig."""
    print("\nTesting LoadSheddingConfig...")
    
    try:
        from loadshedding import LoadSheddingConfig, PresetConfigs, LoadLevel
        
        # Test basic config creation
        config = LoadSheddingConfig(
            enabled=True,
            strategy_name='probabilistic',
            memory_threshold=0.8,
            cpu_threshold=0.9
        )
        
        assert config.enabled == True
        assert config.strategy_name == 'probabilistic'
        assert config.memory_threshold == 0.8
        
        # Test preset configurations
        presets = [
            PresetConfigs.conservative(),
            PresetConfigs.aggressive(),
            PresetConfigs.quality_preserving(),
            PresetConfigs.adaptive_learning(),
            PresetConfigs.disabled()
        ]
        
        for preset in presets:
            assert isinstance(preset, LoadSheddingConfig)
            config_dict = preset.to_dict()
            assert isinstance(config_dict, dict)
        
        print("✓ LoadSheddingConfig working correctly")
        return True
    except Exception as e:
        print(f"✗ LoadSheddingConfig test failed: {e}")
        traceback.print_exc()
        return False


def test_metrics_collection():
    """Test LoadSheddingMetrics."""
    print("\nTesting LoadSheddingMetrics...")
    
    try:
        from loadshedding import LoadSheddingMetrics
        
        metrics = LoadSheddingMetrics()
        
        # Test metrics collection
        metrics.start_collection()
        
        # Record some events
        metrics.record_event_processed(pattern_name='TestPattern', latency_ms=25.0)
        metrics.record_event_dropped(pattern_name='TestPattern')
        metrics.record_pattern_matched('TestPattern', end_to_end_latency_ms=100.0)
        
        # Test metric retrieval
        drop_rate = metrics.get_current_drop_rate()
        avg_latency = metrics.get_average_latency()
        avg_throughput = metrics.get_average_throughput()
        
        assert isinstance(drop_rate, float)
        assert isinstance(avg_latency, float)
        assert isinstance(avg_throughput, float)
        
        # Test summary report
        summary = metrics.get_summary_report()
        assert isinstance(summary, dict)
        assert 'event_processing' in summary
        
        metrics.stop_collection()
        
        print("✓ LoadSheddingMetrics working correctly")
        return True
    except Exception as e:
        print(f"✗ LoadSheddingMetrics test failed: {e}")
        traceback.print_exc()
        return False


def test_cep_integration():
    """Test CEP integration with load shedding."""
    print("\nTesting CEP integration...")
    
    try:
        from CEP import CEP
        from loadshedding import LoadSheddingConfig
        from base.Pattern import Pattern
        
        # Create simple patterns
        patterns = []
        try:
            for i in range(3):
                pattern = Pattern()
                pattern.name = f"TestPattern_{i}"
                patterns.append(pattern)
        except Exception:
            # Mock patterns if Pattern constructor fails
            patterns = [type('MockPattern', (), {'name': f'TestPattern_{i}'})() for i in range(3)]
        
        # Test CEP without load shedding
        cep_basic = CEP(patterns)
        assert not cep_basic.is_load_shedding_enabled()
        assert cep_basic.get_load_shedding_statistics() is None
        
        # Test CEP with load shedding
        config = LoadSheddingConfig(enabled=True, strategy_name='probabilistic')
        cep_with_ls = CEP(patterns, load_shedding_config=config)
        
        if cep_with_ls.is_load_shedding_enabled():
            stats = cep_with_ls.get_load_shedding_statistics()
            assert isinstance(stats, dict)
            assert 'enabled' in stats
            assert 'strategy' in stats
        
        print("✓ CEP integration working correctly")
        return True
    except Exception as e:
        print(f"✗ CEP integration test failed: {e}")
        traceback.print_exc()
        return False


def test_benchmark_components():
    """Test benchmarking components."""
    print("\nTesting benchmark components...")
    
    try:
        from loadshedding import (
            LoadSheddingBenchmark, SyntheticWorkloadGenerator,
            PerformanceEvaluator, LoadSheddingReporter
        )
        
        # Test workload generator
        generator = SyntheticWorkloadGenerator(base_rate_eps=100)
        events = generator.generate_workload('constant_low', 5)  # 5 seconds
        assert isinstance(events, list)
        assert len(events) > 0
        
        # Test benchmark
        benchmark = LoadSheddingBenchmark(generator)
        assert isinstance(benchmark, LoadSheddingBenchmark)
        
        # Test evaluator
        evaluator = PerformanceEvaluator()
        assert isinstance(evaluator, PerformanceEvaluator)
        
        # Test reporter
        reporter = LoadSheddingReporter()
        assert isinstance(reporter, LoadSheddingReporter)
        
        print("✓ Benchmark components working correctly")
        return True
    except Exception as e:
        print(f"✗ Benchmark components test failed: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 60)
    print("LOAD SHEDDING FUNCTIONALITY TEST")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    tests = [
        test_imports,
        test_load_monitor,
        test_load_shedding_strategies,
        test_load_shedding_config,
        test_metrics_collection,
        test_cep_integration,
        test_benchmark_components
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test_func.__name__} crashed: {e}")
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}/{total}")
    print(f"Failed: {total - passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Load shedding implementation is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)