import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


with patch('cache.cache_manager.CacheClient') as MockCacheClient:
    from metrics.collector import MetricsCollector

def test_metrics_collector_events():
    mock_bus = MagicMock()
    
    with patch('cache.cache_manager.CacheClient') as MockCacheClient:
        collector = MetricsCollector(mock_bus)
        
        assert collector.telemetry["workers_alive"] == 0
        assert collector.telemetry["queue_depth"] == 0
        
        collector.on_register({"worker_id": "worker-1"})
        assert collector.telemetry["workers_alive"] == 1
        
        collector.on_job_submitted({"id": "task-1"})
        assert collector.telemetry["queue_depth"] == 1
        
        collector.on_task_assigned({"job": {"id": "task-1"}})
        assert collector.telemetry["queue_depth"] == 0
        
        collector.on_task_failed({"worker_id": "worker-1", "job": {"id": "task-1"}})
        assert collector.telemetry["failures"] == 1
