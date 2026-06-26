import time
import threading

class CircuitBreakerState:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class WorkerCircuitBreaker:
    def __init__(self, failure_threshold=9, recovery_timeout=5.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.consecutive_failures = 0
        self.last_failure_time = 0.0
        self.trial_task_assigned = False
        self.lock = threading.Lock()

    def record_success(self):
        with self.lock:
            old_state = self.state
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                self.consecutive_failures = 0
                self.trial_task_assigned = False
                print(f"[CIRCUIT BREAKER] Worker trial done, State: {old_state} -> {self.state}")
            elif self.state == CircuitBreakerState.CLOSED:
                self.consecutive_failures = 0

    def record_failure(self):
        with self.lock:
            self.consecutive_failures += 1
            self.last_failure_time = time.time()
            old_state = self.state
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.trial_task_assigned = False
                print(f"[CIRCUIT BREAKER] Worker trial failed, State: {old_state} -> {self.state} (cooldown reset)")
            elif self.state == CircuitBreakerState.CLOSED and self.consecutive_failures >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                print(f"[CIRCUIT BREAKER] Worker hit failure threshold ({self.consecutive_failures}), State: {old_state} -> {self.state}")

    def can_assign_task(self):
        with self.lock:
            now = time.time()
            if self.state == CircuitBreakerState.OPEN:
                if now - self.last_failure_time >= self.recovery_timeout:
                    old_state = self.state
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.trial_task_assigned = True
                    print(f"[CIRCUIT BREAKER] Recovery timeout elapsed, State: {old_state} -> {self.state} (assigned trial task)")
                    return True
                return False
            elif self.state == CircuitBreakerState.HALF_OPEN:
                if self.trial_task_assigned:
                    return False
                self.trial_task_assigned = True
                return True
            return True

class CircuitBreakerManager:
    def __init__(self, event_bus=None, failure_threshold=3, recovery_timeout=5.0):
        self.event_bus = event_bus
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.breakers = {}
        self.lock = threading.Lock()

    def get_breaker(self, worker_id):
        with self.lock:
            if worker_id not in self.breakers:
                self.breakers[worker_id] = WorkerCircuitBreaker(self.failure_threshold, self.recovery_timeout)
            return self.breakers[worker_id]

    def record_success(self, worker_id):
        breaker = self.get_breaker(worker_id)
        old_state = breaker.state
        breaker.record_success()
        new_state = breaker.state
        if old_state != new_state:
            self.publish_alert(worker_id, old_state, new_state)

    def record_failure(self, worker_id):
        breaker = self.get_breaker(worker_id)
        old_state = breaker.state
        breaker.record_failure()
        new_state = breaker.state
        if old_state != new_state:
            self.publish_alert(worker_id, old_state, new_state)

    def can_assign_task(self, worker_id):
        breaker = self.get_breaker(worker_id)
        old_state = breaker.state
        allowed = breaker.can_assign_task()
        new_state = breaker.state
        if old_state != new_state:
            self.publish_alert(worker_id, old_state, new_state)
        return allowed

    def get_state(self, worker_id):
        breaker = self.get_breaker(worker_id)
        return breaker.state

    def remove_worker(self, worker_id):
        with self.lock:
            if worker_id in self.breakers:
                del self.breakers[worker_id]
                print(f"[CIRCUIT BREAKER] Purged tracker for worker '{worker_id}'")

    def publish_alert(self, worker_id, old_state, new_state):
        if self.event_bus:
            from event_bus.event_types import ALERT_EVENT
            level = "CRITICAL" if new_state == "OPEN" else "INFO"
            msg = f"Circuit breaker for worker '{worker_id}' transitioned from {old_state} to {new_state}"
            self.event_bus.publish({
                "event": ALERT_EVENT,
                "level": level,
                "message": msg,
                "timestamp": time.time()
            })
