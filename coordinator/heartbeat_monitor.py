import time
import threading

class HeartbeatMonitor:
    def __init__(self, registry, event_bus, timeout=10.0):
        self.registry = registry
        self.event_bus = event_bus
        self.timeout = timeout
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    def _monitor_loop(self):
        from event_bus.event_types import WORKER_FAILED
        
        while self.running:
            time.sleep(2.0)
            now = time.time()
            workers = self.registry.get_all_workers()
            
            for wid, data in workers.items():
                if now - data["last_heartbeat"] > self.timeout:
                    print(f"[HEARTBEAT] Worker '{wid}' timed out")
                    self.event_bus.publish({"event": WORKER_FAILED, "worker_id": wid})

    def stop(self):
        self.running = False
