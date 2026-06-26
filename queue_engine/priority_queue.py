import queue

class JobPriorityQueue:
    def __init__(self):
        self.pq = queue.PriorityQueue()
        self.counter = 0

    def add_job(self, job):
        raw_priority = job.get("priority", "MEDIUM")
        
        if isinstance(raw_priority, str):
            priority_str = raw_priority.upper()
            priority_map = {"HIGH": 1, "MEDIUM": 5, "LOW": 10}
            priority_num = priority_map.get(priority_str, 5)
        elif isinstance(raw_priority, int):
            priority_num = 11 - raw_priority
        else:
            priority_num = 5
            
        self.counter += 1
        self.pq.put((priority_num, self.counter, job))

    def get_next_job(self):
        try:
            priority, count, job = self.pq.get_nowait()
            return job
        except queue.Empty:
            return None

    def is_empty(self):
        return self.pq.empty()

    def qsize(self):
        return self.pq.qsize()
