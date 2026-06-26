import selectors
import socket
import json
import threading
import queue
import time
import sys
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor
from types import ModuleType
from typing import Optional

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
        
    def log_message(self, format, *args):
        pass

def start_health_server(port):
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        server.serve_forever()
    except Exception as e:
        print(f"[HEALTH SERVER WARNING] Failed to start health server: {e}")

class EventDrivenWorker:
    def __init__(self, worker_id, gateway_host, gateway_port, capacity=8):
        self.worker_id = worker_id
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.capacity = capacity
        
        self.sel = selectors.DefaultSelector()
        self.task_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=capacity)
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.running = False

    def connect(self):
        connected = False
        retry_delay = 1.0
        max_retries = 15
        retries = 0
        while not connected and retries < max_retries:
            try:
                self.sock.connect((self.gateway_host, self.gateway_port))
                connected = True
            except (ConnectionRefusedError, OSError) as e:
                retries += 1
                print(f"[{self.worker_id}] Connection to Coordinator refused ({self.gateway_host}:{self.gateway_port}). Retrying {retries}/{max_retries} in {retry_delay}s... ({e})", flush=True)
                time.sleep(retry_delay)
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                
        if not connected:
            raise ConnectionError(f"[{self.worker_id}] Failed to connect to Coordinator {max_retries}")

        self.sock.setblocking(False)
        self.sel.register(self.sock, selectors.EVENT_READ, self.read_from_gateway)
        print(f"[{self.worker_id}] Socket connected to Coordinator", flush=True)

        reg_event = {"event": "REGISTER", "worker_id": self.worker_id, "capacity": self.capacity}
        self.send_event(reg_event)

    def send_event(self, event):
        try:
            payload = json.dumps(event) + "\n"
            self.sock.sendall(payload.encode('utf-8'))
        except BlockingIOError:
            pass

    def read_from_gateway(self, conn):
        try:
            data = conn.recv(4096)
            if data:
                payloads = data.decode('utf-8').strip().split('\n')
                for payload in payloads:
                    if payload == "ACK" or not payload:
                        continue
                    try:
                        event = json.loads(payload)
                        if event.get("event") == "TASK_ASSIGNED":
                            job = event.get("job")
                            print(f"[{self.worker_id}] Received Task Assignment: {job.get('id')}")
                            self.task_queue.put(job)
                    except json.JSONDecodeError:
                        pass
            else:
                self.running = False
        except BlockingIOError:
            pass
        except Exception:
            self.running = False

    def _event_loop(self):
        print(f"[{self.worker_id}] Non-blocking Event Loop")
        while self.running:
            events = self.sel.select(timeout=1.0)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)

    def _task_consumer(self):
        while self.running:
            try:
                job = self.task_queue.get(timeout=1.0)
                self.executor.submit(self._execute_task, job)
            except queue.Empty:
                pass

    def _execute_task(self, job):
        job_id = job.get("id")
        job_type = job.get("job_type")
        payload = job.get("payload", {})
        print(f"[{self.worker_id}] Executing Thread Task {job_type} ({job_id})")
        self.send_event({"event": "JOB_STARTED", "worker_id": self.worker_id, "job": job})
        self.send_event({"event": "LOG_EVENT", "level": "INFO", "component": f"worker-{self.worker_id}", "message": f"Started execution {job_id}"})
        
        time.sleep(1.0)
        
        if job_type == "password-leak-audit":
            try:
                from workers.job_handlers import audit_password
                result = audit_password(payload.get("password"))
                print(f"[{self.worker_id}] Task {job_id} processed: {result}")
                self.send_event({"event": "JOB_COMPLETED", "worker_id": self.worker_id, "job": job, "result": result})
            except Exception as e:
                print(f"[{self.worker_id}] Task {job_id} failed: {e}")
                self.send_event({"event": "TASK_FAILED", "worker_id": self.worker_id, "job": job, "error": str(e)})
        elif job_type == "mfa-totp-verifier":
            try:
                from workers.job_handlers import verify_totp
                result = verify_totp(payload.get("secret"), payload.get("code"))
                print(f"[{self.worker_id}] Task {job_id} processed: {result}")
                self.send_event({"event": "JOB_COMPLETED", "worker_id": self.worker_id, "job": job, "result": result})
            except Exception as e:
                print(f"[{self.worker_id}] Task {job_id} failed: {e}")
                self.send_event({"event": "TASK_FAILED", "worker_id": self.worker_id, "job": job, "error": str(e)})
        elif job_type == "signature-verifier":
            try:
                from workers.job_handlers import verify_signature
                result = verify_signature(payload.get("public_key"), payload.get("message"), payload.get("signature"))
                print(f"[{self.worker_id}] Task {job_id} processed: {result}")
                self.send_event({"event": "JOB_COMPLETED", "worker_id": self.worker_id, "job": job, "result": result})
            except Exception as e:
                print(f"[{self.worker_id}] Task {job_id} failed: {e}")
                self.send_event({"event": "TASK_FAILED", "worker_id": self.worker_id, "job": job, "error": str(e)})
        else:
            if "fail" in str(job_type).lower() or "error" in str(job_type).lower():
                print(f"[{self.worker_id}] Task {job_id} failed ")
                self.send_event({"event": "TASK_FAILED", "worker_id": self.worker_id, "job": job, "error": "Simulated failure"})
            else:
                print(f"[{self.worker_id}] Task {job_id}  processed")
                self.send_event({"event": "JOB_COMPLETED", "worker_id": self.worker_id, "job": job, "result": {"message": "Success"}})

    def _heartbeat_loop(self):
        while self.running:
            self.send_event({"event": "HEARTBEAT", "worker_id": self.worker_id})
            time.sleep(5.0)

    def start(self):
        self.running = True
        
        health_port_str = os.getenv("PORT")
        if health_port_str:
            try:
                health_port = int(health_port_str)
                threading.Thread(target=start_health_server, args=(health_port,), daemon=True).start()
            except ValueError:
                pass

        self.connect()

        threading.Thread(target=self._event_loop, daemon=True).start()
        threading.Thread(target=self._task_consumer, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            self.sock.close()
            print(f"[{self.worker_id}] Shutting down worker")

if __name__ == "__main__":
    coord_host = config.COORDINATOR_HOST if config else "127.0.0.1"
    coord_port = config.COORDINATOR_PORT if config else 9000
    
    worker_id = "worker-alpha"
    if len(sys.argv) > 1:
        worker_id = sys.argv[1]
        
    w = EventDrivenWorker(worker_id, coord_host, coord_port, capacity=4)
    w.start()
