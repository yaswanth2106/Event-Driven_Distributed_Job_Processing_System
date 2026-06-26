class RetryQueue:
    def __init__(self, scheduler, task_repo, max_retries=3):
        self.scheduler = scheduler
        self.task_repo = task_repo
        self.max_retries = max_retries
        self.retries_tracker = {}

    def handle_task_failed(self, event, dlq):
        task = event.get("job", {})
        task_id = task.get("id", "unknown")
        error_msg = event.get("error", "Unknown error")
        
        self.retries_tracker[task_id] = self.retries_tracker.get(task_id, 0) + 1
        self.task_repo.increment_retry(task_id)

        print(f"[RETRY ENGINE] Task '{task_id}' failed; Retry {self.retries_tracker[task_id]}/{self.max_retries}")
        
        if self.retries_tracker[task_id] <= self.max_retries:
            print(f"[RETRY ENGINE] Re-queueing task '{task_id}' for retry")
            self.task_repo.update_status(task_id, "RETRYING", error=error_msg)
            self.scheduler.queue_job(task)
        else:
            print(f"[RETRY ENGINE] Max retries reached for '{task_id}'; to DLQ transfer")
            dlq.add_to_dlq(task, error=error_msg)
