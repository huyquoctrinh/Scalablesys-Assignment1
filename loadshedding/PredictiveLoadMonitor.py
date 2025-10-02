"""
Predictive load monitoring with time series analysis for proactive load shedding.
"""

import numpy as np
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from collections import deque
from .LoadMonitor import LoadMonitor, LoadLevel


class PredictiveLoadMonitor(LoadMonitor):
    """
    Enhanced load monitor that predicts future load levels using time series analysis.
    This allows for proactive load shedding before the system becomes overloaded.
    """
    
    def __init__(self, *args, prediction_window_minutes=5, history_size=100, **kwargs):
        """
        Initialize predictive load monitor.
        
        Args:
            prediction_window_minutes: How far ahead to predict (minutes)
            history_size: Number of historical data points to keep for prediction
        """
        super().__init__(*args, **kwargs)
        
        self.prediction_window = timedelta(minutes=prediction_window_minutes)
        self.history_size = history_size
        
        # Historical data for prediction
        self.load_history = deque(maxlen=history_size)
        self.timestamp_history = deque(maxlen=history_size)
        self.event_rate_history = deque(maxlen=history_size)
        
        # Prediction parameters
        self.trend_weight = 0.3
        self.seasonal_weight = 0.4
        self.recent_weight = 0.3
        
    def assess_load_level(self) -> LoadLevel:
        """Enhanced load assessment with prediction."""
        current_load = super().assess_load_level()
        
        # Record current state for prediction
        now = datetime.now()
        self.load_history.append(current_load)
        self.timestamp_history.append(now)
        
        # Get predicted load for early warning
        predicted_load = self._predict_future_load()
        
        # Use more aggressive load level if prediction shows incoming spike
        if predicted_load and self._load_level_to_numeric(predicted_load) > self._load_level_to_numeric(current_load):
            # Gradually increase current load level based on prediction confidence
            return self._interpolate_load_levels(current_load, predicted_load, 0.7)
        
        return current_load
    
    def _predict_future_load(self) -> Optional[LoadLevel]:
        """Predict future load level using time series analysis."""
        if len(self.load_history) < 10:  # Need minimum history
            return None
            
        try:
            # Convert load levels to numeric values
            numeric_loads = [self._load_level_to_numeric(level) for level in self.load_history]
            
            # Simple trend analysis
            trend = self._calculate_trend(numeric_loads)
            
            # Seasonal pattern detection (look for recurring patterns)
            seasonal_component = self._detect_seasonal_pattern(numeric_loads)
            
            # Recent average
            recent_avg = np.mean(numeric_loads[-5:])
            
            # Weighted prediction
            predicted_numeric = (
                self.trend_weight * trend +
                self.seasonal_weight * seasonal_component +
                self.recent_weight * recent_avg
            )
            
            return self._numeric_to_load_level(predicted_numeric)
            
        except Exception as e:
            print(f"Prediction error: {e}")
            return None
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend using linear regression."""
        if len(values) < 2:
            return values[-1] if values else 0
            
        x = np.arange(len(values))
        y = np.array(values)
        
        # Simple linear regression
        slope, _ = np.polyfit(x, y, 1)
        
        # Project trend forward
        future_x = len(values) + (self.prediction_window.total_seconds() / 60)  # Convert to minutes
        predicted_value = values[-1] + slope * (future_x - len(values))
        
        return predicted_value
    
    def _detect_seasonal_pattern(self, values: List[float]) -> float:
        """Detect recurring patterns in the load data."""
        if len(values) < 20:
            return values[-1] if values else 0
        
        # Look for patterns at different intervals (5min, 15min, 60min cycles)
        intervals = [5, 15, 30]  # Check these intervals
        best_pattern_value = values[-1]
        
        for interval in intervals:
            if len(values) >= interval * 2:
                # Compare current position with same position in previous cycles
                pattern_values = []
                for i in range(len(values) - interval, 0, -interval):
                    if i - interval >= 0:
                        pattern_values.append(values[i - interval])
                
                if pattern_values:
                    # Use median of historical values at this position
                    pattern_value = np.median(pattern_values)
                    if abs(pattern_value - values[-1]) < 1.0:  # If pattern is consistent
                        best_pattern_value = pattern_value
                        break
        
        return best_pattern_value
    
    def _load_level_to_numeric(self, level: LoadLevel) -> float:
        """Convert load level to numeric value for calculations."""
        mapping = {
            LoadLevel.NORMAL: 0.0,
            LoadLevel.MEDIUM: 1.0,
            LoadLevel.HIGH: 2.0,
            LoadLevel.CRITICAL: 3.0
        }
        return mapping.get(level, 0.0)
    
    def _numeric_to_load_level(self, value: float) -> LoadLevel:
        """Convert numeric value back to load level."""
        if value <= 0.5:
            return LoadLevel.NORMAL
        elif value <= 1.5:
            return LoadLevel.MEDIUM
        elif value <= 2.5:
            return LoadLevel.HIGH
        else:
            return LoadLevel.CRITICAL
    
    def _interpolate_load_levels(self, current: LoadLevel, predicted: LoadLevel, confidence: float) -> LoadLevel:
        """Interpolate between current and predicted load levels based on confidence."""
        current_numeric = self._load_level_to_numeric(current)
        predicted_numeric = self._load_level_to_numeric(predicted)
        
        interpolated = current_numeric + confidence * (predicted_numeric - current_numeric)
        return self._numeric_to_load_level(interpolated)
