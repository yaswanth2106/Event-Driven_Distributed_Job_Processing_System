import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time

from coordinator.circuit_breaker import WorkerCircuitBreaker, CircuitBreakerState

def test_circuit_breaker_transitions():
    cb = WorkerCircuitBreaker(failure_threshold=3, recovery_timeout=0.2)
    
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.can_assign_task() is True

    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.can_assign_task() is True
    cb.record_failure()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.can_assign_task() is True

    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.can_assign_task() is False

    time.sleep(0.25)
    
    assert cb.can_assign_task() is True
    assert cb.state == CircuitBreakerState.HALF_OPEN
    assert cb.can_assign_task() is False

    cb.record_success()
    assert cb.state == CircuitBreakerState.CLOSED
    assert cb.consecutive_failures == 0
    assert cb.can_assign_task() is True

def test_circuit_breaker_trial_failure():
    cb = WorkerCircuitBreaker(failure_threshold=3, recovery_timeout=0.1)
    
    cb.record_failure()
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    
    time.sleep(0.15)
    
    assert cb.can_assign_task() is True
    assert cb.state == CircuitBreakerState.HALF_OPEN
    
    cb.record_failure()
    assert cb.state == CircuitBreakerState.OPEN
    assert cb.can_assign_task() is False

if __name__ == "__main__":
    print("Running circuit breaker tests")
    test_circuit_breaker_transitions()
    test_circuit_breaker_trial_failure()
    print("success")
