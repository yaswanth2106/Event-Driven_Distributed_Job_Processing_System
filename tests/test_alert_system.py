import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import json
import pytest
from event_bus.event_bus import InternalEventBus
from metrics.collector import MetricsCollector
from event_bus.event_types import REGISTER, WORKER_FAILED, TASK_FAILED, ALERT_EVENT
from cache.lru_cache import LRUCacheServer
from event_bus.broker_service import PubSubBroker
import threading

def test_alert_generation_and_storage():
    server = LRUCacheServer(host="127.0.0.1", port=16381, max_capacity=100, aof_file="tests/test_alert_cache.aof")
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    broker = PubSubBroker(port=19500)
    broker_thread = threading.Thread(target=broker.start, daemon=True)
    broker_thread.start()
    time.sleep(0.5)

    try:
        bus = InternalEventBus(port=19500)
        collector = MetricsCollector(bus)
        collector.metrics_cache.port = 16381
        
        bus.start()
        time.sleep(0.5)

        bus.publish({"event": REGISTER, "worker_id": "test-worker-01"})
        bus.publish({"event": WORKER_FAILED, "worker_id": "test-worker-01"})
        bus.publish({"event": TASK_FAILED, "worker_id": "test-worker-01", "job": {"id": "job-abc"}})
        bus.publish({"event": ALERT_EVENT, "level": "CRITICAL", "message": "Circuit breaker OPEN", "timestamp": time.time()})

        time.sleep(1.0)

        alerts_raw = collector.metrics_cache.get("alerts")
        assert alerts_raw is not None, "Alerts list should be in the cache"
        
        alerts = json.loads(alerts_raw)
        assert len(alerts) >= 4, f"Expected at least 4 alerts, got {len(alerts)}"
        messages = [a["message"] for a in alerts]
        assert any("registered" in msg for msg in messages)
        assert any("heartbeat timeout" in msg for msg in messages)
        assert any("failed on worker" in msg for msg in messages)
        assert any("Circuit breaker OPEN" in msg for msg in messages)

        for i in range(20):
            bus.publish({"event": ALERT_EVENT, "level": "INFO", "message": f"Flood alert {i}", "timestamp": time.time()})

        time.sleep(1.0)
        
        alerts_final = json.loads(collector.metrics_cache.get("alerts"))
        assert len(alerts_final) == 15, f"Alerts should be capped at 15, but got {len(alerts_final)}"

        assert alerts_final[0]["message"] == "Flood alert 19"

        bus.stop()

    finally:
         server.stop()
         try:
             broker.stop()
         except:
             pass
         if os.path.exists("tests/test_alert_cache.aof"):
            try:
                os.remove("tests/test_alert_cache.aof")
            except:
                pass

if __name__ == "__main__":
    print("Running alert system tests...")
    test_alert_generation_and_storage()
    print("success")
