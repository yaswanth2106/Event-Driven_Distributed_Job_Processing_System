import json
import os
import socket
import sys
import threading
import time
import uuid
from types import ModuleType
from typing import Any, Optional

from .connection_pool import bus_pool
from .middleware import MIDDLEWARE_PIPELINE
from cache.cache_manager import task_cache

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

waiting_requests: dict[str, tuple[threading.Event, dict[str, Any]]] = {}
waiting_requests_lock = threading.Lock()

def start_event_broker_listener():
    def listen_loop():
        import selectors
        sel = selectors.DefaultSelector()
        host = config.EVENT_BROKER_HOST if config else "127.0.0.1"
        port = config.EVENT_BROKER_PORT if config else 9500
        
        from event_bus.event_types import JOB_COMPLETED, MOVE_TO_DLQ
        
        while True:
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                
                sock.sendall(f"SUB {JOB_COMPLETED}\n".encode('utf-8'))
                sock.recv(1024)
                
                sock.sendall(f"SUB {MOVE_TO_DLQ}\n".encode('utf-8'))
                sock.recv(1024)
                
                sock.setblocking(False)
                sel.register(sock, selectors.EVENT_READ)
                
                buffer = ""
                running = True
                while running:
                    events = sel.select(timeout=1.0)
                    if not events:
                        continue
                    
                    for key, mask in events:
                        chunk = sock.recv(8192)
                        if not chunk:
                            running = False
                            break
                        buffer += chunk.decode('utf-8', errors='ignore')
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                event = json.loads(line)
                                event_type = event.get("event")
                                task_id = None
                                
                                if event_type == JOB_COMPLETED:
                                    task_id = event.get("job", {}).get("id")
                                    status = "COMPLETED"
                                    result_val = event.get("result")
                                    worker_id = event.get("worker_id")
                                elif event_type == MOVE_TO_DLQ:
                                    task_id = event.get("task_id") or event.get("job", {}).get("id")
                                    status = "DLQ"
                                    result_val = None
                                    worker_id = None
                                    
                                if task_id:
                                    with waiting_requests_lock:
                                        if task_id in waiting_requests:
                                            ev, res = waiting_requests[task_id]
                                            res["status"] = status
                                            if event_type == JOB_COMPLETED:
                                                res["result"] = result_val
                                                res["worker_id"] = worker_id
                                            elif event_type == MOVE_TO_DLQ:
                                                res["error"] = event.get("error")
                                            ev.set()
                            except Exception:
                                pass
            except Exception:
                pass
            finally:
                if sock:
                    try:
                        sel.unregister(sock)
                    except Exception:
                        pass
                    try:
                        sock.close()
                    except Exception:
                        pass
            time.sleep(1.0) 

    t = threading.Thread(target=listen_loop, daemon=True)
    t.start()

start_event_broker_listener()

MAX_HEADER_SIZE = config.MAX_HEADER_SIZE if config else 8192
MAX_CONTENT_LENGTH = config.MAX_CONTENT_LENGTH if config else 1024 * 1024
REQUEST_TIMEOUT = config.REQUEST_TIMEOUT if config else 5.0

def parse_http_request(client_conn):

    client_conn.settimeout(REQUEST_TIMEOUT)
    buffer = b""
    header_end = -1
    
    while len(buffer) < MAX_HEADER_SIZE:
        try:
            chunk = client_conn.recv(1024)
            if not chunk:
                break
            buffer += chunk
            header_end = buffer.find(b"\r\n\r\n")
            if header_end != -1:
                break
        except socket.timeout:
            raise TimeoutError("Header read timeout exceeded.")
            
    if header_end == -1:
        if len(buffer) >= MAX_HEADER_SIZE:
            raise ValueError("431 Request Header Fields Too Large")
        raise ConnectionError("Connection closed before headers completed.")

    header_part = buffer[:header_end]
    body_part = buffer[header_end + 4:]
    
    lines = header_part.decode('utf-8', errors='ignore').split("\r\n")
    if not lines or not lines[0]:
        raise ValueError("400 Bad Request: Empty Status Line")
        
    parts = lines[0].split(" ")
    if len(parts) < 3:
        raise ValueError("400 Bad Request: Invalid Request Line")
    method, path, _ = parts[0], parts[1], parts[2]
    
    headers = lines[1:]
    content_length = 0
    for line in headers:
        if line.lower().startswith("content-length:"):
            try:
                content_length = int(line.split(":", 1)[1].strip())
            except Exception:
                content_length = 0
            break
            
    if content_length > MAX_CONTENT_LENGTH:
        raise ValueError("413 Content Too Large")
        
    body = body_part
    while len(body) < content_length:
        try:
            remaining = content_length - len(body)
            chunk = client_conn.recv(min(4096, remaining))
            if not chunk:
                break
            body += chunk
        except socket.timeout:
            raise TimeoutError("Body read timeout exceeded.")
            
    if len(body) < content_length:
        raise ConnectionError("Connection closed before body reading was finalized.")
        
    return method, path, headers, body[:content_length]

def process_client_socket(client_conn, client_addr):
    with client_conn:
        try:
            try:
                method, original_path, lines, body_part = parse_http_request(client_conn)
            except Exception as e:
                err_msg = str(e)
                if "431" in err_msg:
                    status = "431 Request Header Fields Too Large"
                    body = '{"error": "Request Header Fields Too Large"}'
                elif "413" in err_msg:
                    status = "413 Content Too Large"
                    body = '{"error": "Payload Content Too Large (Limit 1MB)"}'
                elif "400" in err_msg:
                    status = "400 Bad Request"
                    body = '{"error": "Bad Request: Invalid format"}'
                else:
                    status = "400 Bad Request"
                    body = json.dumps({"error": f"Bad Request: {err_msg}"})
                
                resp = f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
                try:
                    client_conn.sendall(resp.encode('utf-8'))
                except Exception:
                    pass
                return

            for middleware in MIDDLEWARE_PIPELINE:
                passed, error_payload = middleware(client_addr, lines, method, original_path, body_part)
                if not passed:
                    client_conn.sendall(error_payload)
                    return

            if original_path == "/submit":
                task_id = None
                try:
                    body_data = json.loads(body_part.decode('utf-8'))
                    if "id" not in body_data:
                        body_data["id"] = str(uuid.uuid4())
                        body_part = json.dumps(body_data).encode('utf-8')
                    task_id = body_data["id"]
                except Exception as e:
                    print(f"[GATEWAY] Payload JSON parsing error: {e}")

                bus_sock = bus_pool.get_connection()
                if not bus_sock:
                    body = '{"error": "Event Bus Unavailable"}'
                    client_conn.sendall(f"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n{body}".encode('utf-8'))
                    return
                
                try:
                    bus_sock.sendall(body_part if body_part else b'{"event": "ping"}')
                    bus_sock.recv(1024)
                    bus_pool.release_connection(bus_sock, status_ok=True)
                    
                    if task_id:
                        status = "PENDING"
                        completed = False
                        worker_id = None
                        task_result = None
                        task_error = None
                        
                        try:
                            task_raw = task_cache.get(f"task:{task_id}")
                            if task_raw:
                                task_data = json.loads(task_raw)
                                status = task_data.get("status")
                                worker_id = task_data.get("worker_id")
                                task_result = task_data.get("result")
                                task_error = task_data.get("error")
                                if status in ["COMPLETED", "DLQ"]:
                                    completed = True
                        except Exception:
                            pass
                            
                        if not completed:
                            event = threading.Event()
                            result = {"status": "PENDING"}
                            with waiting_requests_lock:
                                waiting_requests[task_id] = (event, result)
                                
                            completed_in_time = event.wait(timeout=30.0)
                            
                            with waiting_requests_lock:
                                waiting_requests.pop(task_id, None)
                                
                            if completed_in_time:
                                status = result.get("status", "PENDING")
                                worker_id = result.get("worker_id")
                                task_result = result.get("result")
                                task_error = result.get("error")
                            else:

                                try:
                                    task_raw = task_cache.get(f"task:{task_id}")
                                    if task_raw:
                                        task_data = json.loads(task_raw)
                                        status = task_data.get("status", "PENDING")
                                        worker_id = task_data.get("worker_id")
                                        task_result = task_data.get("result")
                                        task_error = task_data.get("error")
                                except Exception:
                                    pass

                        if status == "COMPLETED":
                            resp_body = json.dumps({
                                "status": "COMPLETED",
                                "task_id": task_id,
                                "message": "Workload processed and completed successfully.",
                                "worker_id": worker_id,
                                "result": task_result
                            })
                            client_conn.sendall(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(resp_body)}\r\n\r\n{resp_body}".encode('utf-8'))
                        elif status == "DLQ":
                            resp_body = json.dumps({
                                "status": "DLQ",
                                "task_id": task_id,
                                "worker_id": worker_id,
                                "error": task_error or "Workload execution permanently failed and moved to Dead Letter Queue (DLQ)."
                            })
                            client_conn.sendall(f"HTTP/1.1 422 Unprocessable Entity\r\nContent-Type: application/json\r\nContent-Length: {len(resp_body)}\r\n\r\n{resp_body}".encode('utf-8'))
                        else:
                            resp_body = json.dumps({
                                "status": status,
                                "task_id": task_id,
                                "error": task_error or "Gateway Timeout",
                                "message": "Task was dispatched but failed to complete within the timeout boundary."
                            })
                            client_conn.sendall(f"HTTP/1.1 504 Gateway Timeout\r\nContent-Type: application/json\r\nContent-Length: {len(resp_body)}\r\n\r\n{resp_body}".encode('utf-8'))
                    else:
                        resp_body = '{"status": "Event Dispatched", "message": "Successfully forwarded to Internal Event Bus (no ID tracking)"}'
                        client_conn.sendall(f"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {len(resp_body)}\r\n\r\n{resp_body}".encode('utf-8'))
                
                except Exception as e:
                    bus_pool.release_connection(bus_sock, status_ok=False)
                    body = json.dumps({"error": "Event Bus Forwarding Error", "detail": str(e)})
                    client_conn.sendall(f"HTTP/1.1 500 Internal Server Error\r\nContent-Length: {len(body)}\r\n\r\n{body}".encode('utf-8'))
            else:
                body = '{"status": "Gateway OK"}'
                client_conn.sendall(f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n{body}".encode('utf-8'))

        except Exception as e:
            print(f"[FATAL GATEWAY EXCEPTION] System Fault: {e}")
