import time
import json
from rate_limiter import middleware_rate_limiter
from auth import middleware_authenticator

def middleware_logger(addr, headers, method, path, body=b""):
    print(f"[LOG] [{time.strftime('%H:%M:%S')}] {addr[0]}:{addr[1]} --> {method} '{path}'")
    return True, None

def middleware_request_validator(addr, headers, method, path, body=b""):
    if path.startswith("/submit") and method == "POST":
        try:
            body_str = body.decode('utf-8')
            data = json.loads(body_str)
            if "job_type" not in data:
                raise ValueError("Missing job_type")
        except Exception as e:
            print(f"[VALIDATION FAILED] Invalid job payload from {addr[0]}")
            err_body = '{"error": "Bad Request: Invalid Job Payload"}'
            resp = f"HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {len(err_body)}\r\n\r\n{err_body}"
            return False, resp.encode('utf-8')
    return True, None

MIDDLEWARE_PIPELINE = [
    middleware_logger, 
    middleware_authenticator, 
    middleware_rate_limiter, 
    middleware_request_validator
]
