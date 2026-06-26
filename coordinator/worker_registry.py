import threading

class WorkerRegistry:
    def __init__(self):
        self.workers = {}  
        self.lock = threading.Lock()

    def register_worker(self, worker_id, capacity, timestamp):
        with self.lock:
            self.workers[worker_id] = {
                "last_heartbeat": timestamp,
                "capacity": capacity,
                "current_load": 0
            }
            print(f"[REGISTRY] New Worker Registered: '{worker_id}' | Capacity: {capacity}")

    def update_heartbeat(self, worker_id, timestamp):
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]["last_heartbeat"] = timestamp

    def increment_load(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]["current_load"] += 1

    def decrement_load(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id]["current_load"] = max(0, self.workers[worker_id]["current_load"] - 1)

    def get_available_workers(self):
        with self.lock:
            return {
                wid: data for wid, data in self.workers.items() 
                if data["capacity"] > data["current_load"]
            }

    def remove_worker(self, worker_id):
        with self.lock:
            if worker_id in self.workers:
                del self.workers[worker_id]
                print(f"[REGISTRY] Worker '{worker_id}' evicted from registry.")

    def get_all_workers(self):
        with self.lock:
            return dict(self.workers)
