class DeadLetterQueue:
    def __init__(self, task_repo, event_bus=None):
        self.dlq = []
        self.task_repo = task_repo
        self.event_bus = event_bus

    def add_to_dlq(self, task, error=None):
        task_id = task.get("id", "unknown")
        print(f"[DEAD LETTER QUEUE] [DLQ] Task '{task_id}' permanently failed ; moved to DLQ")
        self.dlq.append(task)
        self.task_repo.update_status(task_id, "DLQ", error=error)
        
        if self.event_bus:
            from event_bus.event_types import MOVE_TO_DLQ
            self.event_bus.publish({
                "event": MOVE_TO_DLQ,
                "task_id": task_id,
                "job": task,
                "error": error
            })
