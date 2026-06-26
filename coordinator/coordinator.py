import os
import sys
import time
import uuid
from types import ModuleType
from typing import Optional

from event_bus.event_types import JOB_SUBMITTED, HEARTBEAT, WORKER_FAILED, JOB_COMPLETED, REGISTER, TASK_FAILED, TASK_ASSIGNED
from event_bus.event_bus import InternalEventBus, EventLoop
from worker_registry import WorkerRegistry
from scheduler import Scheduler
from circuit_breaker import CircuitBreakerManager
from heartbeat_monitor import HeartbeatMonitor
from failure_detector import FailureDetector
from task_assigner import TaskAssigner
from storage.postgres_store import PostgresStore
from storage.task_repository import TaskRepository
from storage.worker_repository import WorkerRepository
from queue_engine.retry_queue import RetryQueue
from queue_engine.dead_letter_queue import DeadLetterQueue
from metrics.collector import MetricsCollector
from logging_service.structured_logger import StructuredLogger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

try:
    from .autoscaler import Autoscaler
except ImportError:
    from autoscaler import Autoscaler

class CoordinatorService:
    def __init__(self):
        print("[COORDINATOR] Booting")
        
        bus_host = config.EVENT_BROKER_HOST if config else "127.0.0.1"
        bus_port = config.EVENT_BROKER_PORT if config else 9500
        self.bus = InternalEventBus(host=bus_host, port=bus_port)
        
        coord_host = config.COORDINATOR_HOST if config else "127.0.0.1"
        coord_port = config.COORDINATOR_PORT if config else 9000
        self.network = EventLoop(host=coord_host, port=coord_port, event_bus=self.bus)

        self.store = PostgresStore()
        self.task_repo = TaskRepository(self.store)
        self.worker_repo = WorkerRepository(self.store)

        failure_threshold = config.CIRCUIT_FAILURE_THRESHOLD if config else 3
        recovery_timeout = config.CIRCUIT_RECOVERY_TIMEOUT if config else 5.0
        self.circuit_breaker_manager = CircuitBreakerManager(
            self.bus, 
            failure_threshold=failure_threshold, 
            recovery_timeout=recovery_timeout
        )
        self.registry = WorkerRegistry()
        self.scheduler = Scheduler(self.registry, self.circuit_breaker_manager)
        self.heartbeat_monitor = HeartbeatMonitor(self.registry, self.bus)
        self.failure_detector = FailureDetector(self.registry)
        self.task_assigner = TaskAssigner(self.scheduler, self.registry, self.bus)

        self.dlq = DeadLetterQueue(self.task_repo, self.bus)
        self.retry_engine = RetryQueue(self.scheduler, self.task_repo)

        self.metrics_collector = MetricsCollector(self.bus)
        self.structured_logger = StructuredLogger(self.bus)
        self.autoscaler = Autoscaler(min_workers=0, max_workers=5)

        self.bus.subscribe(REGISTER, self.handle_register)
        self.bus.subscribe(JOB_SUBMITTED, self.handle_job_submitted)
        self.bus.subscribe(HEARTBEAT, self.handle_heartbeat)
        self.bus.subscribe(WORKER_FAILED, self.failure_detector.handle_worker_failure)
        self.bus.subscribe(WORKER_FAILED, self.handle_worker_failed)
        self.bus.subscribe(JOB_COMPLETED, self.handle_job_completed)
        self.bus.subscribe(TASK_FAILED, self.handle_task_failed)

        self.bus.subscribe(REGISTER, self.persist_worker)
        self.bus.subscribe(JOB_SUBMITTED, self.persist_task)
        self.bus.subscribe(TASK_ASSIGNED, self.persist_task_assigned)
        self.bus.subscribe(JOB_COMPLETED, self.persist_task_completed)

    def handle_register(self, event):
        worker_id = event.get("worker_id")
        capacity = event.get("capacity", 8)
        if worker_id:
            self.registry.register_worker(worker_id, capacity, time.time())

    def handle_job_submitted(self, event):
        if "id" not in event:
            event["id"] = str(uuid.uuid4())
        print(f"[COORDINATOR] Consumed JOB_SUBMITTED event ({event['id']})")
        self.scheduler.queue_job(event)

    def handle_heartbeat(self, event):
        worker_id = event.get("worker_id")
        if worker_id:
            self.registry.update_heartbeat(worker_id, time.time())

    def handle_worker_failed(self, event):
        worker_id = event.get("worker_id")
        if worker_id:
            self.circuit_breaker_manager.remove_worker(worker_id)

    def handle_job_completed(self, event):
        worker_id = event.get("worker_id")
        if worker_id:
            print(f"[COORDINATOR] Worker '{worker_id}' completed a job; Freeing capacity;")
            self.registry.decrement_load(worker_id)
            self.circuit_breaker_manager.record_success(worker_id)

    def handle_task_failed(self, event):
        worker_id = event.get("worker_id")
        if worker_id:
            self.registry.decrement_load(worker_id)
            self.circuit_breaker_manager.record_failure(worker_id)
        self.retry_engine.handle_task_failed(event, self.dlq)

    def persist_worker(self, event):
        self.worker_repo.save_worker(event.get("worker_id"), event.get("capacity"), "ACTIVE", time.time())

    def persist_task(self, event):
        self.task_repo.save_task(event["id"], event.get("job_type", "unknown"), event.get("priority", "MEDIUM"), "PENDING")

    def persist_task_assigned(self, event):
        self.task_repo.update_status(event["job"]["id"], "ASSIGNED", worker_id=event["worker_id"])

    def persist_task_completed(self, event):
        self.task_repo.update_status(event["job"]["id"], "COMPLETED", result=event.get("result"))

    def start_autoscaler_loop(self):
        import threading
        
        def loop():
            time.sleep(5.0)
            
            while hasattr(self, 'network') and self.network.running:
                try:
                    qsize = self.scheduler.job_queue.qsize()
                    available_workers = self.registry.get_available_workers()
                    active_dynamic = len(self.autoscaler.active_subprocesses)
                    
                    if qsize > 0:
                        print(f"[AUTOSCALER LOOP] Backlog Queue: {qsize} | Available Capacity: {len(available_workers)} | Active Dynamic Workers: {active_dynamic}")
                    
                    self.autoscaler.clean_dead_processes()
                    
                    if qsize > 0 and len(available_workers) == 0:
                        self.autoscaler.scale_up()
                        
                    elif qsize == 0 and active_dynamic > 0:
                        all_workers = self.registry.get_all_workers()
                        dynamic_idle = True
                        for wid, data in all_workers.items():
                            if wid.startswith("worker-dynamic-") and data["current_load"] > 0:
                                dynamic_idle = False
                                break
                        
                        if dynamic_idle:
                            self.autoscaler.scale_down()
                except Exception as e:
                    print(f"[AUTOSCALER LOOP ERROR] {e}")
                
                time.sleep(2.0)
                
        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def start(self):
        self.bus.start()
        self.network.start()
        self.heartbeat_monitor.start()
        self.task_assigner.start()
        
        self.start_autoscaler_loop()
        
        print("[COORDINATOR] started")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down coordinator")
            print("[COORDINATOR] Stopping dynamic autoscaled subprocesses...")
            for wid, proc in list(self.autoscaler.active_subprocesses.items()):
                try:
                    proc.terminate()
                except Exception:
                    pass
            self.task_assigner.stop()
            self.heartbeat_monitor.stop()
            self.network.stop()
            self.bus.stop()

if __name__ == "__main__":
    service = CoordinatorService()
    service.start()
