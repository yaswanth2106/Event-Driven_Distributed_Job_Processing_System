class FailureDetector:
    def __init__(self, registry):
        self.registry = registry

    def handle_worker_failure(self, event):
        worker_id = event.get("worker_id")
        if worker_id:
            print(f"[FAILURE DETECTOR] Investigating worker '{worker_id}'")
            self.registry.remove_worker(worker_id)
            print(f"[FAILURE DETECTOR] Worker '{worker_id}' purged")
