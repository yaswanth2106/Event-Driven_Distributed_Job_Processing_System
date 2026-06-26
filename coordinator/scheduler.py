from queue_engine.priority_queue import JobPriorityQueue

class Scheduler:
    def __init__(self, registry, circuit_breaker_manager=None):
        self.registry = registry
        self.circuit_breaker_manager = circuit_breaker_manager
        self.job_queue = JobPriorityQueue()

    def queue_job(self, job_event):
        self.job_queue.add_job(job_event)
        print(f"[SCHEDULER] Job appended; Total backlog: {self.job_queue.qsize()}")

    def get_next_job_and_worker(self):
        if self.job_queue.is_empty():
            return None, None
        
        available_workers = self.registry.get_available_workers()
        if not available_workers:
            return None, None

        if self.circuit_breaker_manager:
            filtered = {}
            for wid, data in available_workers.items():
                if self.circuit_breaker_manager.can_assign_task(wid):
                    filtered[wid] = data
            available_workers = filtered

        if not available_workers:
            return None, None

        chosen_worker = None
        max_free_capacity = -1
        
        for wid, data in available_workers.items():
            free_capacity = data["capacity"] - data["current_load"]
            if free_capacity > max_free_capacity:
                max_free_capacity = free_capacity
                chosen_worker = wid

        if chosen_worker:
            job = self.job_queue.get_next_job()
            return job, chosen_worker
            
        return None, None
