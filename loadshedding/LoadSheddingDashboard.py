"""
Real-time performance dashboard for load shedding monitoring and visualization.
"""

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import threading
import queue
import json
import time
from collections import deque, defaultdict
import seaborn as sns


class LoadSheddingDashboard:
    """
    Real-time dashboard for monitoring load shedding performance with live visualizations.
    """
    
    def __init__(self, max_history: int = 1000, update_interval_ms: int = 1000):
        """
        Initialize the dashboard.
        
        Args:
            max_history: Maximum number of data points to keep in history
            update_interval_ms: Dashboard update interval in milliseconds
        """
        self.max_history = max_history
        self.update_interval_ms = update_interval_ms
        
        # Data storage
        self.metrics_history = {
            'timestamps': deque(maxlen=max_history),
            'throughput': deque(maxlen=max_history),
            'drop_rate': deque(maxlen=max_history),
            'latency': deque(maxlen=max_history),
            'cpu_usage': deque(maxlen=max_history),
            'memory_usage': deque(maxlen=max_history),
            'queue_size': deque(maxlen=max_history),
            'strategy_changes': deque(maxlen=max_history)
        }
        
        self.current_metrics = {}
        self.alerts = deque(maxlen=100)  # Store recent alerts
        
        # Visualization setup
        self.fig = None
        self.axes = {}
        self.lines = {}
        self.animation = None
        
        # Threading
        self.data_queue = queue.Queue()
        self.is_running = False
        self.update_thread = None
        
        # Configuration
        self.thresholds = {
            'high_drop_rate': 0.1,      # 10% drop rate alert
            'high_latency': 500,        # 500ms latency alert  
            'low_throughput': 100,      # 100 events/sec minimum
            'high_cpu': 0.8,            # 80% CPU usage alert
            'high_memory': 0.85,        # 85% memory usage alert
            'large_queue': 10000        # 10k events in queue alert
        }
        
        # Colors and styling
        plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
        self.colors = {
            'throughput': '#2E86C1',
            'drop_rate': '#E74C3C', 
            'latency': '#F39C12',
            'cpu': '#8E44AD',
            'memory': '#D35400',
            'queue': '#17A2B8'
        }
    
    def start_dashboard(self, title: str = "Load Shedding Performance Dashboard"):
        """Start the real-time dashboard."""
        if self.is_running:
            print("Dashboard is already running")
            return
        
        print("Starting load shedding dashboard...")
        self.is_running = True
        
        # Setup the figure and subplots
        self.fig, self.axes = plt.subplots(2, 3, figsize=(15, 10))
        self.fig.suptitle(title, fontsize=16, fontweight='bold')
        
        # Initialize all subplots
        self._setup_subplots()
        
        # Start the animation
        self.animation = animation.FuncAnimation(
            self.fig, 
            self._update_plots,
            interval=self.update_interval_ms,
            blit=False,
            cache_frame_data=False
        )
        
        # Start data update thread
        self.update_thread = threading.Thread(target=self._process_data_queue)
        self.update_thread.daemon = True
        self.update_thread.start()
        
        # Show the dashboard
        plt.tight_layout()
        plt.show()
    
    def _setup_subplots(self):
        """Setup all subplots for the dashboard."""
        
        # Throughput and Drop Rate (top left)
        ax1 = self.axes[0, 0]
        ax1.set_title('Throughput & Drop Rate', fontweight='bold')
        ax1.set_ylabel('Events/sec', color=self.colors['throughput'])
        ax1.tick_params(axis='y', labelcolor=self.colors['throughput'])
        
        # Create secondary axis for drop rate
        ax1_twin = ax1.twinx()
        ax1_twin.set_ylabel('Drop Rate (%)', color=self.colors['drop_rate'])
        ax1_twin.tick_params(axis='y', labelcolor=self.colors['drop_rate'])
        
        self.lines['throughput'] = ax1.plot([], [], color=self.colors['throughput'], 
                                          label='Throughput', linewidth=2)[0]
        self.lines['drop_rate'] = ax1_twin.plot([], [], color=self.colors['drop_rate'], 
                                              label='Drop Rate', linewidth=2)[0]
        
        # Latency (top middle)
        ax2 = self.axes[0, 1]
        ax2.set_title('Response Latency', fontweight='bold')
        ax2.set_ylabel('Latency (ms)', color=self.colors['latency'])
        ax2.tick_params(axis='y', labelcolor=self.colors['latency'])
        
        self.lines['latency'] = ax2.plot([], [], color=self.colors['latency'], 
                                       label='Latency', linewidth=2)[0]
        
        # System Resources (top right)
        ax3 = self.axes[0, 2]
        ax3.set_title('System Resources', fontweight='bold')
        ax3.set_ylabel('Usage (%)')
        ax3.set_ylim(0, 100)
        
        self.lines['cpu'] = ax3.plot([], [], color=self.colors['cpu'], 
                                   label='CPU', linewidth=2)[0]
        self.lines['memory'] = ax3.plot([], [], color=self.colors['memory'], 
                                      label='Memory', linewidth=2)[0]
        ax3.legend(loc='upper left')
        
        # Queue Size (bottom left)
        ax4 = self.axes[1, 0]
        ax4.set_title('Event Queue Size', fontweight='bold')
        ax4.set_ylabel('Queue Size', color=self.colors['queue'])
        ax4.tick_params(axis='y', labelcolor=self.colors['queue'])
        
        self.lines['queue'] = ax4.plot([], [], color=self.colors['queue'], 
                                     label='Queue Size', linewidth=2)[0]
        
        # Strategy Performance Matrix (bottom middle)
        ax5 = self.axes[1, 1]
        ax5.set_title('Strategy Performance', fontweight='bold')
        
        # This will be updated with heatmap data
        self.strategy_heatmap_ax = ax5
        
        # Alerts and Status (bottom right)
        ax6 = self.axes[1, 2]
        ax6.set_title('System Status & Alerts', fontweight='bold')
        ax6.axis('off')  # Turn off axis for text display
        
        self.status_ax = ax6
        
        # Store axes references
        self.axes_dict = {
            'throughput_drop': (ax1, ax1_twin),
            'latency': ax2,
            'resources': ax3,
            'queue': ax4,
            'strategy': ax5,
            'status': ax6
        }
    
    def update_metrics(self, metrics: Dict[str, Any]):
        """
        Update dashboard with new metrics.
        
        Args:
            metrics: Dictionary containing performance metrics
        """
        timestamp = datetime.now()
        
        # Add to queue for thread-safe processing
        self.data_queue.put({
            'timestamp': timestamp,
            'metrics': metrics.copy()
        })
    
    def _process_data_queue(self):
        """Process incoming metrics data in a separate thread."""
        while self.is_running:
            try:
                data = self.data_queue.get(timeout=1.0)
                self._add_data_point(data['timestamp'], data['metrics'])
                self._check_alerts(data['metrics'])
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error processing dashboard data: {e}")
    
    def _add_data_point(self, timestamp: datetime, metrics: Dict[str, Any]):
        """Add a new data point to the history."""
        self.metrics_history['timestamps'].append(timestamp)
        self.metrics_history['throughput'].append(metrics.get('throughput', 0))
        self.metrics_history['drop_rate'].append(metrics.get('drop_rate', 0) * 100)  # Convert to percentage
        self.metrics_history['latency'].append(metrics.get('latency_ms', 0))
        self.metrics_history['cpu_usage'].append(metrics.get('cpu_usage', 0) * 100)  # Convert to percentage
        self.metrics_history['memory_usage'].append(metrics.get('memory_usage', 0) * 100)
        self.metrics_history['queue_size'].append(metrics.get('queue_size', 0))
        self.metrics_history['strategy_changes'].append(metrics.get('current_strategy', 'Unknown'))
        
        # Update current metrics
        self.current_metrics = metrics.copy()
    
    def _check_alerts(self, metrics: Dict[str, Any]):
        """Check for alert conditions and add to alerts queue."""
        alerts = []
        
        drop_rate = metrics.get('drop_rate', 0)
        if drop_rate > self.thresholds['high_drop_rate']:
            alerts.append(f"HIGH DROP RATE: {drop_rate*100:.1f}%")
        
        latency = metrics.get('latency_ms', 0)
        if latency > self.thresholds['high_latency']:
            alerts.append(f"HIGH LATENCY: {latency:.0f}ms")
        
        throughput = metrics.get('throughput', 0)
        if throughput < self.thresholds['low_throughput']:
            alerts.append(f"LOW THROUGHPUT: {throughput:.0f} events/sec")
        
        cpu_usage = metrics.get('cpu_usage', 0)
        if cpu_usage > self.thresholds['high_cpu']:
            alerts.append(f"HIGH CPU: {cpu_usage*100:.1f}%")
        
        memory_usage = metrics.get('memory_usage', 0)
        if memory_usage > self.thresholds['high_memory']:
            alerts.append(f"HIGH MEMORY: {memory_usage*100:.1f}%")
        
        queue_size = metrics.get('queue_size', 0)
        if queue_size > self.thresholds['large_queue']:
            alerts.append(f"LARGE QUEUE: {queue_size:,} events")
        
        # Add alerts with timestamps
        for alert in alerts:
            self.alerts.append({
                'timestamp': datetime.now(),
                'message': alert,
                'severity': 'HIGH' if 'HIGH' in alert else 'MEDIUM'
            })
    
    def _update_plots(self, frame):
        """Update all plots with current data."""
        if not self.metrics_history['timestamps']:
            return []
        
        # Convert timestamps to matplotlib format
        timestamps = list(self.metrics_history['timestamps'])
        time_nums = [t.timestamp() for t in timestamps]
        
        updated_artists = []
        
        # Update throughput and drop rate
        if 'throughput' in self.lines:
            throughput_data = list(self.metrics_history['throughput'])
            self.lines['throughput'].set_data(time_nums, throughput_data)
            updated_artists.append(self.lines['throughput'])
            
            # Update axis limits
            ax1, ax1_twin = self.axes_dict['throughput_drop']
            if throughput_data:
                ax1.set_ylim(0, max(throughput_data) * 1.1 + 1)
        
        if 'drop_rate' in self.lines:
            drop_rate_data = list(self.metrics_history['drop_rate'])
            self.lines['drop_rate'].set_data(time_nums, drop_rate_data)
            updated_artists.append(self.lines['drop_rate'])
            
            # Update axis limits
            _, ax1_twin = self.axes_dict['throughput_drop']
            if drop_rate_data:
                ax1_twin.set_ylim(0, max(max(drop_rate_data) * 1.1, 10))
        
        # Update latency
        if 'latency' in self.lines:
            latency_data = list(self.metrics_history['latency'])
            self.lines['latency'].set_data(time_nums, latency_data)
            updated_artists.append(self.lines['latency'])
            
            ax2 = self.axes_dict['latency']
            if latency_data:
                ax2.set_ylim(0, max(latency_data) * 1.1 + 1)
        
        # Update system resources
        if 'cpu' in self.lines:
            cpu_data = list(self.metrics_history['cpu_usage'])
            self.lines['cpu'].set_data(time_nums, cpu_data)
            updated_artists.append(self.lines['cpu'])
        
        if 'memory' in self.lines:
            memory_data = list(self.metrics_history['memory_usage'])
            self.lines['memory'].set_data(time_nums, memory_data)
            updated_artists.append(self.lines['memory'])
        
        # Update queue size
        if 'queue' in self.lines:
            queue_data = list(self.metrics_history['queue_size'])
            self.lines['queue'].set_data(time_nums, queue_data)
            updated_artists.append(self.lines['queue'])
            
            ax4 = self.axes_dict['queue']
            if queue_data:
                ax4.set_ylim(0, max(queue_data) * 1.1 + 1)
        
        # Update time axes for all plots
        if time_nums:
            time_range = max(time_nums) - min(time_nums) 
            time_margin = time_range * 0.05 if time_range > 0 else 1
            
            for ax_key in ['throughput_drop', 'latency', 'resources', 'queue']:
                if ax_key == 'throughput_drop':
                    ax, ax_twin = self.axes_dict[ax_key]
                    ax.set_xlim(min(time_nums) - time_margin, max(time_nums) + time_margin)
                    ax_twin.set_xlim(min(time_nums) - time_margin, max(time_nums) + time_margin)
                else:
                    ax = self.axes_dict[ax_key]
                    ax.set_xlim(min(time_nums) - time_margin, max(time_nums) + time_margin)
        
        # Update strategy performance heatmap
        self._update_strategy_heatmap()
        
        # Update status and alerts
        self._update_status_display()
        
        return updated_artists
    
    def _update_strategy_heatmap(self):
        """Update the strategy performance heatmap."""
        ax = self.strategy_heatmap_ax
        ax.clear()
        ax.set_title('Strategy Performance', fontweight='bold')
        
        # Create sample strategy performance data
        strategies = ['Probabilistic', 'Semantic', 'Adaptive', 'Predictive']
        metrics = ['Throughput', 'Quality', 'Latency', 'Efficiency']
        
        # Generate sample performance matrix (in real implementation, this would come from actual data)
        if self.current_metrics:
            performance_matrix = np.random.rand(len(strategies), len(metrics))  # Placeholder
            
            # Create heatmap
            im = ax.imshow(performance_matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
            
            # Set labels
            ax.set_xticks(range(len(metrics)))
            ax.set_xticklabels(metrics)
            ax.set_yticks(range(len(strategies)))
            ax.set_yticklabels(strategies)
            
            # Add text annotations
            for i in range(len(strategies)):
                for j in range(len(metrics)):
                    text = ax.text(j, i, f'{performance_matrix[i, j]:.2f}',
                                 ha="center", va="center", color="black", fontweight='bold')
        else:
            ax.text(0.5, 0.5, 'No Data Available', transform=ax.transAxes,
                   ha='center', va='center', fontsize=12)
    
    def _update_status_display(self):
        """Update the status and alerts display."""
        ax = self.status_ax
        ax.clear()
        ax.axis('off')
        
        # Current status
        status_text = "SYSTEM STATUS\n" + "="*20 + "\n"
        
        if self.current_metrics:
            current_strategy = self.current_metrics.get('current_strategy', 'Unknown')
            throughput = self.current_metrics.get('throughput', 0)
            drop_rate = self.current_metrics.get('drop_rate', 0) * 100
            
            status_text += f"Strategy: {current_strategy}\n"
            status_text += f"Throughput: {throughput:.0f} events/sec\n"
            status_text += f"Drop Rate: {drop_rate:.1f}%\n"
            
            # System health indicator
            if drop_rate < 5 and throughput > 100:
                health_status = "🟢 HEALTHY"
            elif drop_rate < 15:
                health_status = "🟡 CAUTION"
            else:
                health_status = "🔴 CRITICAL"
            
            status_text += f"Health: {health_status}\n\n"
        
        # Recent alerts
        status_text += "RECENT ALERTS\n" + "="*15 + "\n"
        
        recent_alerts = list(self.alerts)[-5:]  # Last 5 alerts
        if recent_alerts:
            for alert in recent_alerts[-5:]:  # Show last 5 alerts
                time_str = alert['timestamp'].strftime('%H:%M:%S')
                severity_symbol = "🔴" if alert['severity'] == 'HIGH' else "🟡"
                status_text += f"{severity_symbol} {time_str}: {alert['message']}\n"
        else:
            status_text += "No recent alerts\n"
        
        # Display the text
        ax.text(0.05, 0.95, status_text, transform=ax.transAxes, 
               fontsize=9, verticalalignment='top', fontfamily='monospace')
    
    def stop_dashboard(self):
        """Stop the dashboard and cleanup."""
        print("Stopping dashboard...")
        self.is_running = False
        
        if self.animation:
            self.animation.event_source.stop()
        
        if self.update_thread and self.update_thread.is_alive():
            self.update_thread.join(timeout=2.0)
        
        if self.fig:
            plt.close(self.fig)
    
    def export_metrics(self, filename: str):
        """Export current metrics history to file."""
        if not self.metrics_history['timestamps']:
            print("No data to export")
            return
        
        # Convert to DataFrame
        data = {
            'timestamp': list(self.metrics_history['timestamps']),
            'throughput': list(self.metrics_history['throughput']),
            'drop_rate': list(self.metrics_history['drop_rate']),
            'latency': list(self.metrics_history['latency']),
            'cpu_usage': list(self.metrics_history['cpu_usage']),
            'memory_usage': list(self.metrics_history['memory_usage']),
            'queue_size': list(self.metrics_history['queue_size']),
            'strategy': list(self.metrics_history['strategy_changes'])
        }
        
        df = pd.DataFrame(data)
        
        if filename.endswith('.csv'):
            df.to_csv(filename, index=False)
        elif filename.endswith('.json'):
            # Convert timestamps to strings for JSON serialization
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df.to_json(filename, indent=2, orient='records')
        else:
            print("Unsupported file format. Use .csv or .json")
            return
        
        print(f"Metrics exported to {filename}")
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report."""
        if not self.metrics_history['timestamps']:
            return {"error": "No data available for report generation"}
        
        # Calculate statistics
        throughput_data = list(self.metrics_history['throughput'])
        drop_rate_data = list(self.metrics_history['drop_rate'])
        latency_data = list(self.metrics_history['latency'])
        
        report = {
            'summary': {
                'data_points': len(throughput_data),
                'time_span_minutes': (max(self.metrics_history['timestamps']) - 
                                    min(self.metrics_history['timestamps'])).total_seconds() / 60,
                'average_throughput': np.mean(throughput_data) if throughput_data else 0,
                'average_drop_rate': np.mean(drop_rate_data) if drop_rate_data else 0,
                'average_latency': np.mean(latency_data) if latency_data else 0
            },
            'performance_metrics': {
                'throughput': {
                    'mean': np.mean(throughput_data) if throughput_data else 0,
                    'std': np.std(throughput_data) if throughput_data else 0,
                    'min': np.min(throughput_data) if throughput_data else 0,
                    'max': np.max(throughput_data) if throughput_data else 0,
                    'percentiles': {
                        '50th': np.percentile(throughput_data, 50) if throughput_data else 0,
                        '95th': np.percentile(throughput_data, 95) if throughput_data else 0,
                        '99th': np.percentile(throughput_data, 99) if throughput_data else 0
                    }
                },
                'drop_rate': {
                    'mean': np.mean(drop_rate_data) if drop_rate_data else 0,
                    'std': np.std(drop_rate_data) if drop_rate_data else 0,
                    'min': np.min(drop_rate_data) if drop_rate_data else 0,
                    'max': np.max(drop_rate_data) if drop_rate_data else 0
                },
                'latency': {
                    'mean': np.mean(latency_data) if latency_data else 0,
                    'std': np.std(latency_data) if latency_data else 0,
                    'min': np.min(latency_data) if latency_data else 0,
                    'max': np.max(latency_data) if latency_data else 0,
                    'percentiles': {
                        '50th': np.percentile(latency_data, 50) if latency_data else 0,
                        '95th': np.percentile(latency_data, 95) if latency_data else 0,
                        '99th': np.percentile(latency_data, 99) if latency_data else 0
                    }
                }
            },
            'alerts_summary': {
                'total_alerts': len(self.alerts),
                'high_severity_alerts': sum(1 for alert in self.alerts if alert['severity'] == 'HIGH'),
                'recent_alerts': list(self.alerts)[-10:] if self.alerts else []
            },
            'recommendations': self._generate_performance_recommendations(throughput_data, drop_rate_data, latency_data)
        }
        
        return report
    
    def _generate_performance_recommendations(self, throughput_data: List[float], 
                                           drop_rate_data: List[float], 
                                           latency_data: List[float]) -> List[str]:
        """Generate performance improvement recommendations."""
        recommendations = []
        
        if throughput_data:
            avg_throughput = np.mean(throughput_data)
            if avg_throughput < 100:
                recommendations.append("Low average throughput detected - consider optimizing event processing")
        
        if drop_rate_data:
            avg_drop_rate = np.mean(drop_rate_data)
            if avg_drop_rate > 10:
                recommendations.append("High drop rate detected - consider more aggressive load balancing")
            elif avg_drop_rate < 1:
                recommendations.append("Very low drop rate - system may be under-utilized")
        
        if latency_data:
            avg_latency = np.mean(latency_data)
            p95_latency = np.percentile(latency_data, 95)
            
            if avg_latency > 200:
                recommendations.append("High average latency - investigate processing bottlenecks")
            if p95_latency > 500:
                recommendations.append("High 95th percentile latency - optimize worst-case scenarios")
        
        # Strategy-specific recommendations
        strategy_changes = list(self.metrics_history['strategy_changes'])
        if strategy_changes:
            unique_strategies = set(strategy_changes)
            if len(unique_strategies) > 3:
                recommendations.append("Frequent strategy changes detected - consider tuning adaptation thresholds")
        
        return recommendations


# Example usage and integration
class DashboardIntegratedLoadMonitor:
    """
    Load monitor that integrates with the dashboard for real-time visualization.
    """
    
    def __init__(self, dashboard: LoadSheddingDashboard):
        self.dashboard = dashboard
        self.monitor_data = {
            'throughput': 0,
            'drop_rate': 0,
            'latency_ms': 0,
            'cpu_usage': 0,
            'memory_usage': 0,
            'queue_size': 0,
            'current_strategy': 'None'
        }
    
    def update_metrics(self, 
                      throughput: float = None,
                      drop_rate: float = None, 
                      latency_ms: float = None,
                      cpu_usage: float = None,
                      memory_usage: float = None,
                      queue_size: int = None,
                      current_strategy: str = None):
        """Update metrics and push to dashboard."""
        
        if throughput is not None:
            self.monitor_data['throughput'] = throughput
        if drop_rate is not None:
            self.monitor_data['drop_rate'] = drop_rate
        if latency_ms is not None:
            self.monitor_data['latency_ms'] = latency_ms
        if cpu_usage is not None:
            self.monitor_data['cpu_usage'] = cpu_usage
        if memory_usage is not None:
            self.monitor_data['memory_usage'] = memory_usage
        if queue_size is not None:
            self.monitor_data['queue_size'] = queue_size
        if current_strategy is not None:
            self.monitor_data['current_strategy'] = current_strategy
        
        # Push to dashboard
        self.dashboard.update_metrics(self.monitor_data)
    
    def simulate_metrics(self, duration_seconds: int = 60):
        """Simulate metrics for testing the dashboard."""
        import time
        import random
        
        print(f"Simulating metrics for {duration_seconds} seconds...")
        
        strategies = ['Probabilistic', 'Semantic', 'Adaptive', 'Predictive']
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            # Simulate realistic metrics with some randomness
            base_throughput = 500 + random.uniform(-100, 100)
            base_drop_rate = 0.05 + random.uniform(-0.02, 0.08)  # 3-13% drop rate
            base_latency = 150 + random.uniform(-50, 200)  # 100-350ms
            
            # Add some correlation (higher throughput might mean higher latency)
            if base_throughput > 550:
                base_latency += 50
                base_drop_rate += 0.02
            
            self.update_metrics(
                throughput=max(0, base_throughput),
                drop_rate=max(0, min(1, base_drop_rate)),
                latency_ms=max(10, base_latency),
                cpu_usage=random.uniform(0.3, 0.9),
                memory_usage=random.uniform(0.4, 0.8),
                queue_size=random.randint(100, 5000),
                current_strategy=random.choice(strategies)
            )
            
            time.sleep(1)  # Update every second


if __name__ == "__main__":
    # Example usage
    dashboard = LoadSheddingDashboard(update_interval_ms=1000)
    monitor = DashboardIntegratedLoadMonitor(dashboard)
    
    try:
        # Start dashboard in a separate thread
        import threading
        dashboard_thread = threading.Thread(target=dashboard.start_dashboard)
        dashboard_thread.daemon = True
        dashboard_thread.start()
        
        # Give dashboard time to initialize
        time.sleep(2)
        
        # Simulate metrics
        monitor.simulate_metrics(duration_seconds=30)
        
        # Keep dashboard running
        input("Press Enter to stop dashboard...")
        
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        dashboard.stop_dashboard()
