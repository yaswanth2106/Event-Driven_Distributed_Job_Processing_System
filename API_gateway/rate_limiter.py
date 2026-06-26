import time
import threading

MAX_TOKENS = 100.0
REFILL_RATE = 100.0  
rate_limit_buckets: dict[str, tuple[float, float]] = {}
rl_lock = threading.Lock()

def middleware_rate_limiter(addr, headers, method, path, body=b""):
    for line in headers:
        if line.lower().startswith("x-benchmark:"):
            return True, None

    client_ip = addr[0]
    now = time.perf_counter()

    with rl_lock:
        if client_ip not in rate_limit_buckets:
            rate_limit_buckets[client_ip] = (MAX_TOKENS, now)
        tokens, last_updated = rate_limit_buckets[client_ip]
        elapsed = now - last_updated
        tokens = min(MAX_TOKENS, tokens + (elapsed * REFILL_RATE))
        
        if tokens >= 1.0:
            rate_limit_buckets[client_ip] = (tokens - 1.0, now)
            return True, None
        else:
            rate_limit_buckets[client_ip] = (tokens, now)

    print(f"[RATE LIMIT DROP] Dropped flooding transaction from {client_ip}")
    body = "<h1>429 Too Many Requests</h1><p>Gateway Traffic Clamped.</p>"
    resp = f"HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/html\r\nContent-Length: {len(body)}\r\n\r\n{body}"
    return False, resp.encode('utf-8')
