"""
Load-aware input stream for the OpenCEP system.
This module provides an enhanced input stream that incorporates load shedding capabilities.
"""

from typing import Optional
from stream.Stream import InputStream
from base.Event import Event
from .LoadMonitor import LoadMonitor, LoadLevel
from .LoadSheddingStrategy import LoadSheddingStrategy
from .LoadSheddingMetrics import LoadSheddingMetrics
import time
from datetime import datetime


class LoadAwareInputStream(InputStream):
    def __init__(self,
                 original_stream: InputStream,
                 load_monitor: LoadMonitor,
                 shedding_strategy: LoadSheddingStrategy,
                 metrics_collector: Optional[LoadSheddingMetrics] = None):
        # Satisfy parent class init (if any) and PyCharm analyzer
        try:
            super().__init__()
        except Exception:
            # If InputStream has no __init__ or requires args, ignore safely
            pass

        self.original_stream = original_stream
        self.load_monitor = load_monitor
        self.shedding_strategy = shedding_strategy
        self.metrics_collector = metrics_collector

        # Internal tracking
        self._queue_size_estimate = 0
        self._events_processed_count = 0
        self._events_seen_count = 0
        self._events_dropped_count = 0
        self._last_latency_update = datetime.now()
        self._recent_processing_times = []  # rolling list of processing times (sec)

        # Load shedding state
        self._current_load_level = LoadLevel.NORMAL
        self._last_load_check = datetime.now()
        self._consecutive_drops = 0
        self._max_consecutive_drops = 1000  # starvation guard

    # ---------- PUBLIC STREAM API ----------

    def get_item(self):
        """
        Non-recursive: loop until we either return a kept event or hit StopIteration.
        """
        while True:
            start_time = time.time()
            try:
                event = self.original_stream.get_item()
                if event is None:
                    raise StopIteration
            except StopIteration:
                raise
            except Exception as e:
                # transient source error; try next iteration without recursing
                print(f"Error in LoadAwareInputStream (source): {e}")
                continue

            # Seen an event
            self._events_seen_count += 1
            self._queue_size_estimate = max(0, self._queue_size_estimate - 1)

            # Periodic load assessment
            current_time = datetime.now()
            if (current_time - self._last_load_check).total_seconds() >= 0.1:
                self._update_load_assessment()
                self._last_load_check = current_time

            # Shedding decision
            try:
                should_drop = self.shedding_strategy.should_drop_event(event, self._current_load_level)
            except Exception as e:
                # If strategy misbehaves, fail open to avoid stalls
                print(f"Error in shedding_strategy.should_drop_event: {e}")
                should_drop = False

            if should_drop:
                self._handle_dropped_event(event)
                self._events_dropped_count += 1
                self._consecutive_drops += 1

                # Break starvation occasionally
                if self._consecutive_drops > self._max_consecutive_drops:
                    self._consecutive_drops = 0
                    self._handle_processed_event(event, start_time)
                    return event
                # else: fetch next event (loop continues)
                continue

            # Keep event
            self._consecutive_drops = 0
            self._handle_processed_event(event, start_time)
            return event

    def has_next(self) -> bool:
        # Delegate if underlying stream exposes it
        has_next_fn = getattr(self.original_stream, "has_next", None)
        if callable(has_next_fn):
            return has_next_fn()
        # Fallback: unknown; return True so consumer attempts to read
        return True

    def peek_next(self):
        # Not supported safely with shedding decisions ahead of time
        return None

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get_item()
        except StopIteration:
            raise

    def close(self):
        """Close the stream and emit final stats."""
        if hasattr(self.original_stream, 'close'):
            try:
                self.original_stream.close()
            except Exception:
                pass
        if self.metrics_collector:
            final_stats = self.get_drop_statistics()
            print(f"LoadAwareInputStream closing - Final stats: {final_stats}")

    # ---------- METRICS / CONTROL ----------

    def get_drop_statistics(self) -> dict:
        seen = self._events_seen_count
        dropped = self._events_dropped_count
        observed_drop_rate = (dropped / seen) if seen else 0.0
        return {
            'strategy_name': self.shedding_strategy.name,
            'strategy_drop_rate': self.shedding_strategy.get_drop_rate(),  # strategy’s own view
            'observed_drop_rate': observed_drop_rate,                      # wrapper-observed
            'current_load_level': self._current_load_level.value,
            'events_seen': seen,
            'events_processed': self._events_processed_count,
            'events_dropped': dropped,
            'queue_size_estimate': self._queue_size_estimate,
            'consecutive_drops': self._consecutive_drops
        }

    def reset_statistics(self):
        self._events_processed_count = 0
        self._events_seen_count = 0
        self._events_dropped_count = 0
        self._queue_size_estimate = 0
        self._consecutive_drops = 0
        self._recent_processing_times.clear()
        try:
            self.shedding_strategy.reset_statistics()
        except Exception:
            pass

    def update_queue_size(self, new_size: int):
        self._queue_size_estimate = max(0, int(new_size))

    def force_load_assessment(self) -> LoadLevel:
        self._update_load_assessment()
        return self._current_load_level

    # ---------- INTERNAL HELPERS (were missing / unresolved) ----------

    def _update_load_assessment(self):
        """Update the current load level assessment based on recent metrics."""
        avg_latency_ms = 0.0
        if self._recent_processing_times:
            avg_latency_ms = (sum(self._recent_processing_times) / len(self._recent_processing_times)) * 1000.0

        # Notify monitor
        try:
            self.load_monitor.update_event_metrics(
                queue_size=self._queue_size_estimate,
                events_processed=self._events_processed_count,
                latest_latency_ms=avg_latency_ms
            )
            self._current_load_level = self.load_monitor.assess_load_level()
        except Exception:
            # If monitor fails, keep last known level
            pass

        # Record snapshot
        if self.metrics_collector:
            try:
                monitor_metrics = self.load_monitor.get_current_metrics()
            except Exception:
                monitor_metrics = {}
            try:
                self.metrics_collector.record_system_snapshot(
                    load_level=self._current_load_level.value,
                    cpu_percent=monitor_metrics.get('cpu_percent', 0.0),
                    memory_percent=monitor_metrics.get('memory_percent', 0.0),
                    queue_size=self._queue_size_estimate
                )
            except Exception:
                pass

    def _handle_dropped_event(self, event: Event):
        """Record a dropped event in metrics."""
        if self.metrics_collector:
            try:
                pattern_name = getattr(event, 'pattern_name', None) or getattr(event, 'event_type', None)
                self.metrics_collector.record_event_dropped(pattern_name)
            except Exception:
                pass

    def _handle_processed_event(self, event: Event, start_time: float):
        """Record a processed event + latency in metrics."""
        processing_time = time.time() - start_time
        self._recent_processing_times.append(processing_time)
        if len(self._recent_processing_times) > 20:
            self._recent_processing_times = self._recent_processing_times[-20:]

        self._events_processed_count += 1

        if self.metrics_collector:
            try:
                pattern_name = getattr(event, 'pattern_name', None) or getattr(event, 'event_type', None)
                self.metrics_collector.record_event_processed(
                    pattern_name=pattern_name,
                    latency_ms=processing_time * 1000.0
                )
            except Exception:
                pass


class BufferedLoadAwareInputStream(LoadAwareInputStream):
    """
    Enhanced version with internal buffering for better load assessment.
    """

    def __init__(self,
                 original_stream: InputStream,
                 load_monitor: LoadMonitor,
                 shedding_strategy: LoadSheddingStrategy,
                 metrics_collector: Optional[LoadSheddingMetrics] = None,
                 buffer_size: int = 100):
        super().__init__(original_stream, load_monitor, shedding_strategy, metrics_collector)
        self.buffer_size: int = buffer_size
        self._internal_buffer: list = []     # used later in get_item()
        self._buffer_filled: bool = False    # checked in get_item()

    def _fill_buffer(self):
        """Fill the internal buffer from the original stream."""
        try:
            while len(self._internal_buffer) < self.buffer_size:
                ev = self.original_stream.get_item()
                if ev is None:
                    break
                self._internal_buffer.append(ev)
        except StopIteration:
            pass
        except Exception as e:
            print(f"BufferedLoadAwareInputStream _fill_buffer error: {e}")

        self._buffer_filled = True
        self._queue_size_estimate = len(self._internal_buffer)

    def get_item(self):
        """
        Non-recursive, buffer-aware fetch with shedding.
        """
        # Ensure buffer filled at least once
        if not self._buffer_filled:
            self._fill_buffer()

        while True:
            # Exhausted?
            if not self._internal_buffer:
                raise StopIteration

            start_time = time.time()

            # Pop one event from buffer
            event = self._internal_buffer.pop(0)
            self._queue_size_estimate = len(self._internal_buffer)

            # Try to refill (best-effort)
            try:
                next_event = self.original_stream.get_item()
                if next_event is not None:
                    self._internal_buffer.append(next_event)
                    self._queue_size_estimate = len(self._internal_buffer)
            except StopIteration:
                pass
            except Exception as e:
                print(f"BufferedLoadAwareInputStream refill error: {e}")

            # Count seen
            self._events_seen_count += 1

            # Periodic load assessment
            current_time = datetime.now()
            if (current_time - self._last_load_check).total_seconds() >= 0.1:
                self._update_load_assessment()
                self._last_load_check = current_time

            # Shedding decision
            try:
                should_drop = self.shedding_strategy.should_drop_event(event, self._current_load_level)
            except Exception as e:
                print(f"Error in shedding_strategy.should_drop_event (buffered): {e}")
                should_drop = False

            if should_drop:
                self._handle_dropped_event(event)
                self._events_dropped_count += 1
                self._consecutive_drops += 1

                if self._consecutive_drops > self._max_consecutive_drops:
                    self._consecutive_drops = 0
                    self._handle_processed_event(event, start_time)
                    return event
                # else: continue loop to try the next buffered event
                continue

            # Keep event
            self._consecutive_drops = 0
            self._handle_processed_event(event, start_time)
            return event
