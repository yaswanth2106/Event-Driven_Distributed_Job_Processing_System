import json
from cache.cache_manager import task_cache
from storage.postgres_store import TaskDB

class TaskRepository:
    def __init__(self, store):
        self.store = store 

    def save_task(self, task_id, job_type, priority, status="PENDING", retries=0, worker_id=None):
        task_data = {
            "task_id": task_id,
            "job_type": str(job_type),
            "priority": str(priority),
            "status": status,
            "retries": retries,
            "worker_id": worker_id
        }
        try:
            task_cache.set(f"task:{task_id}", json.dumps(task_data))
        except Exception as e:
            print(f"[CACHE ERROR] Failed to cache task {task_id} during save: {e}")
            
        if not self.store.online:
            return
        try:
            with self.store.session_scope() as session:
                task = session.query(TaskDB).filter_by(task_id=task_id).first()
                if task:
                    task.job_type = str(job_type)
                    task.priority = str(priority)
                    task.status = status
                    task.retries = retries
                    task.worker_id = worker_id
                else:
                    task = TaskDB(
                        task_id=task_id,
                        job_type=str(job_type),
                        priority=str(priority),
                        status=status,
                        retries=retries,
                        worker_id=worker_id
                    )
                    session.add(task)
        except Exception as e:
            print(f"[TASK REPO ERROR] Failed to save task {task_id}: {e}")

    def update_status(self, task_id, status, worker_id=None, result=None, error=None):
        cached_data = task_cache.get(f"task:{task_id}")
        if cached_data:
            try:
                data = json.loads(cached_data)
                data["status"] = status
                if worker_id:
                    data["worker_id"] = worker_id
                if result is not None:
                    data["result"] = result
                if error is not None:
                    data["error"] = error
                task_cache.set(f"task:{task_id}", json.dumps(data))
            except Exception as e:
                print(f"[CACHE ERROR] Failed to update status in cache for task {task_id}: {e}")
                
        if not self.store.online:
            return
        try:
            with self.store.session_scope() as session:
                task = session.query(TaskDB).filter_by(task_id=task_id).first()
                if task:
                    task.status = status
                    if worker_id:
                        task.worker_id = worker_id
        except Exception as e:
            print(f"[TASK REPO ERROR] Failed to update status of task {task_id}: {e}")

    def increment_retry(self, task_id):
        cached_data = task_cache.get(f"task:{task_id}")
        if cached_data:
            try:
                data = json.loads(cached_data)
                data["retries"] = data.get("retries", 0) + 1
                task_cache.set(f"task:{task_id}", json.dumps(data))
            except Exception as e:
                print(f"[CACHE ERROR] Failed to increment retry in cache for task {task_id}: {e}")
                
        if not self.store.online:
            return
        try:
            with self.store.session_scope() as session:
                task = session.query(TaskDB).filter_by(task_id=task_id).first()
                if task:
                    task.retries += 1
        except Exception as e:
            print(f"[TASK REPO ERROR] Failed to increment retry for task {task_id}: {e}")
