import threading
import time

class TaskAssigner:
    def __init__(self, scheduler, registry, event_bus):
        self.scheduler = scheduler
        self.registry = registry
        self.event_bus = event_bus
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self._assign_loop, daemon=True).start()

    def _assign_loop(self):
        from event_bus.event_types import TASK_ASSIGNED
        
        while self.running:
            job, worker_id = self.scheduler.get_next_job_and_worker()
            
            if job and worker_id:
                self.registry.increment_load(worker_id)
                assignment_event = {
                    "event": TASK_ASSIGNED,
                    "worker_id": worker_id,
                    "job": job
                }
                
                self.event_bus.publish(assignment_event)
                print(f"[TASK ASSIGNER] Assigned '{job.get('job_type', 'unknown task')}' to worker '{worker_id}'")
            else:
                time.sleep(0.5)

    def stop(self):
        self.running = False
