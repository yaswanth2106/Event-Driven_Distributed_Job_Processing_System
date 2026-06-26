from datetime import datetime
from storage.postgres_store import WorkerDB

class WorkerRepository:
    def __init__(self, store):
        self.store = store 

    def save_worker(self, worker_id, capacity, status, last_heartbeat):
        if not self.store.online:
            return
        
        if isinstance(last_heartbeat, (int, float)):
            last_heartbeat_dt = datetime.fromtimestamp(last_heartbeat)
        else:
            last_heartbeat_dt = last_heartbeat

        try:
            with self.store.session_scope() as session:
                worker = session.query(WorkerDB).filter_by(worker_id=worker_id).first()
                if worker:
                    worker.capacity = capacity
                    worker.status = status
                    worker.last_heartbeat = last_heartbeat_dt
                else:
                    worker = WorkerDB(
                        worker_id=worker_id,
                        capacity=capacity,
                        status=status,
                        last_heartbeat=last_heartbeat_dt
                    )
                    session.add(worker)
        except Exception as e:
            print(f"[WORKER REPO ERROR] Failed to save worker {worker_id}: {e}")

    def update_status(self, worker_id, status):
        if not self.store.online:
            return
        try:
            with self.store.session_scope() as session:
                worker = session.query(WorkerDB).filter_by(worker_id=worker_id).first()
                if worker:
                    worker.status = status
        except Exception as e:
            print(f"[WORKER REPO ERROR] Failed to update status of worker {worker_id}: {e}")
