import sys
import os
import time
import json
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from event_bus.event_types import JOB_SUBMITTED, JOB_COMPLETED, TASK_ASSIGNED, TASK_FAILED, REGISTER, WORKER_FAILED, JOB_STARTED, ALERT_EVENT
from cache.cache_manager import CacheClient

class MetricsCollector:
    def __init__(self, event_bus):
        self.bus = event_bus
        self.metrics_cache = CacheClient(port=6381)
        
        self.lock = threading.Lock()
        self.window_lock = threading.Lock()
        
        self.telemetry = {
            "workers_alive": 0,
            "queue_depth": 0,
            "latency": 0.0,
            "rps": 0.0,
            "failures": 0
        }
        
        self.start_times = {} 
        self.completed_count = 0
        self.window_start = time.time()
        
        self.bus.subscribe(REGISTER, self.on_register)
        self.bus.subscribe(WORKER_FAILED, self.on_worker_failed)
        self.bus.subscribe(JOB_SUBMITTED, self.on_job_submitted)
        self.bus.subscribe(TASK_ASSIGNED, self.on_task_assigned)
        self.bus.subscribe(JOB_STARTED, self.on_job_started)
        self.bus.subscribe(JOB_COMPLETED, self.on_job_completed)
        self.bus.subscribe(TASK_FAILED, self.on_task_failed)
        self.bus.subscribe(ALERT_EVENT, self.on_alert_received)

    def flush_to_cache(self):
        now = time.time()
        with self.window_lock:
            elapsed = now - self.window_start
            if elapsed > 0:
                self.telemetry["rps"] = round(self.completed_count / elapsed, 2)
            
        payload = json.dumps(self.telemetry)
        self.metrics_cache.set("telemetry", payload)

    def on_register(self, event):
        with self.lock:
            self.telemetry["workers_alive"] += 1
            self.flush_to_cache()
            worker_id = event.get("worker_id", "unknown")
            self.trigger_alert("INFO", f"Worker '{worker_id}' registered ")

    def on_worker_failed(self, event):
        with self.lock:
            self.telemetry["workers_alive"] = max(0, self.telemetry["workers_alive"] - 1)
            self.flush_to_cache()
            worker_id = event.get("worker_id", "unknown")
            self.trigger_alert("CRITICAL", f"Worker '{worker_id}' heartbeat timeout")

    def on_job_submitted(self, event):
        with self.lock:
            self.telemetry["queue_depth"] += 1
            self.flush_to_cache()

    def on_task_assigned(self, event):
        with self.lock:
            self.telemetry["queue_depth"] = max(0, self.telemetry["queue_depth"] - 1)
            self.flush_to_cache()

    def on_job_started(self, event):
        with self.lock:
            job = event.get("job", {})
            task_id = job.get("id")
            if task_id:
                self.start_times[task_id] = time.time()

    def on_job_completed(self, event):
        with self.lock:
            self.completed_count += 1
            job = event.get("job", {})
            task_id = job.get("id")
            
            if task_id in self.start_times:
                latency = time.time() - self.start_times[task_id]
                old_latency = self.telemetry["latency"]
                self.telemetry["latency"] = round((old_latency * 0.9) + (latency * 0.1), 3)
                del self.start_times[task_id]
                
            self.flush_to_cache()

    def on_task_failed(self, event):
        with self.lock:
            self.telemetry["failures"] += 1
            self.flush_to_cache()
            worker_id = event.get("worker_id", "unknown")
            job = event.get("job", {})
            job_id = job.get("id", "unknown")
            self.trigger_alert("WARNING", f"Task '{job_id}' failed on worker '{worker_id}'.")

    def trigger_alert(self, level, message):
        event = {
            "event": ALERT_EVENT,
            "level": level,
            "message": message,
            "timestamp": time.time()
        }
        self.bus.publish(event)

    def on_alert_received(self, event):
        with self.lock:
            level = event.get("level", "INFO")
            message = event.get("message", "")
            timestamp = event.get("timestamp", time.time())
            
            alerts_raw = self.metrics_cache.get("alerts")
            alerts = []
            if alerts_raw:
                try:
                    alerts = json.loads(alerts_raw)
                except Exception:
                    pass
                    
            new_alert = {
                "timestamp": timestamp,
                "level": level,
                "message": message
            }
            alerts = [new_alert] + alerts[:14]
            self.metrics_cache.set("alerts", json.dumps(alerts))
