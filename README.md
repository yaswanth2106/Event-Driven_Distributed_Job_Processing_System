# Event-Driven Distributed Job Processing System (Zero Application Framework)

Event-driven distributed job processing system implemented in Python without external application frameworks. The architecture coordinates tasks, manages dynamic worker autoscaling, and executes cryptographic security workloads reactively.

---

## Architectural Flow

```mermaid
graph TD
    Client[("Frontend Web Client\n(Port 8081)")]
    Proxy[("app.py Proxy")]
    Gateway[("API Gateway\n(Port 8000)")]
    Bus[("Internal Event Bus Broker\n(Port 9500)")]
    Coord[("Coordinator Service\n(Port 9000)")]
    WorkerAlpha[("worker-alpha\n(Static Node)")]
    WorkerDynamic[("worker-dynamic-N\n(Autoscaled Subprocesses)")]
    Autoscaler[("Autoscaler Module")]
    Cache[("LRU Task Cache\n(Port 6381)")]
    DB[("PostgreSQL Database")]
    Logger[("Structured Logger")]
    Metrics[("Metrics Collector")]

    Client -->|1. Submit Job| Proxy
    Proxy -->|2. Proxy Request| Gateway
    Gateway -->|3. Publish JOB_SUBMITTED| Bus
    Bus -->|4. Broadcast event| Coord
    Coord -->|5. Queue Job & Persist PENDING| Cache
    Coord -.->|6. Persist| DB
    
    Coord -->|7. Check Queue Size| Autoscaler
    Autoscaler -->|8. Scale Up / Spawn| WorkerDynamic
    
    Coord -->|9. Publish TASK_ASSIGNED| Bus
    Bus -->|10. Dispatch Task| WorkerAlpha
    Bus -->|10. Dispatch Task| WorkerDynamic
    
    WorkerAlpha -->|11. Execute handler & Publish event| Bus
    WorkerDynamic -->|11. Execute handler & Publish event| Bus
    
    Bus -->|12. Broadcast JOB_COMPLETED/TASK_FAILED| Coord
    Bus -->|12. Broadcast to reactive listener| Gateway
    
    Coord -->|13. Persist Output| Cache
    Gateway -->|14. Return synchronous result| Proxy
    Proxy -->|15. Display formatted card| Client
    
    Bus -->|Log Event| Logger
    Bus -->|Metric Event| Metrics
```

---

## Core Components

1.  **Frontend Workload Dispatcher (Port 8081)**:
    A threaded proxy client that presents an interactive web interface to choose and submit jobs (Password Leak Audit, MFA TOTP Verifier, Signature Verifier) along with their priorities and custom parameters.
2.  **API Gateway (Port 8000)**:
    Accepts HTTP connections, authenticates requests, publishes job submissions to the internal event bus, and uses a reactive socket listener thread to block and return completion results synchronously.
3.  **Internal Event Bus (Port 9500)**:
    A lightweight, standalone pub-sub broker that manages subscription state and distributes framed, line-delimited TCP packets across system modules.
4.  **Coordinator Service (Port 9000)**:
    Maintains scheduling priorities, registers active workers, heartbeat states, and runs the background **Autoscaler** thread loop.
5.  **Autoscaling Daemon**:
    Spins up dynamic worker subprocesses (`worker-dynamic-N`) during high backlog conditions, and gently terminates them once tasks are drained and idle.
6.  **Event-Driven Workers**:
    Thread-pool executors that connect to the coordinator, accept task assignments, run cryptographic operations (e.g. SHA-1 hashes, RFC 6238 TOTP validation, or Ed25519 asymmetric signature checks), and report results.
7.  **Telemetry & Logging**:
    *   **Metrics Collector**: Tracks alive workers, queue depth, latencies, and failures in a thread-safe telemetry cache.
    *   **Structured Logger**: Outputs JSON records with ISO 8601 formatting, correlation/trace IDs, log level filters, and Rotating File handlers.

---

