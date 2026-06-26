import time
from coordinator.scheduler import Scheduler
from coordinator.worker_registry import WorkerRegistry

def test_scheduler_assigns_job():
    registry = WorkerRegistry()
    scheduler = Scheduler(registry)
    
    job, worker = scheduler.get_next_job_and_worker()
    assert job is None
    assert worker is None
    
    job_event = {"id": "job-1", "job_type": "password-leak-audit", "priority": "HIGH"}
    scheduler.queue_job(job_event)
    
    registry.register_worker("worker-1", capacity=4, timestamp=time.time())
    
    picked_job, picked_worker = scheduler.get_next_job_and_worker()
    
    assert picked_worker == "worker-1"
    assert picked_job["id"] == "job-1"
