import subprocess
import time
import sys
import os
import config


os.environ["PYTHONUNBUFFERED"] = "1"

print("DISTRIBUTED JOB PROCESSING PLATFORM ")

root_dir = os.path.abspath(os.path.dirname(__file__))
log_dir = os.path.join(root_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
services = [
    (f"Event Broker Service (Port {config.EVENT_BROKER_PORT})", [sys.executable, "event_bus/broker_service.py", str(config.EVENT_BROKER_PORT)], root_dir, 1.0),
    (f"LRU Cache (Port {config.TASK_CACHE_PORT})", [sys.executable, "cache/lru_cache.py", str(config.TASK_CACHE_PORT)], root_dir, 1.0),
    ("Coordinator Service", [sys.executable, "coordinator/coordinator.py"], root_dir, 1.0),
    ("API Gateway Service", [sys.executable, "gateway.py"], os.path.join(root_dir, "API_gateway"), 1.0),
    ("WebSocket Dashboard Server", [sys.executable, "dashboard/websocket_server.py"], root_dir, 0.5),
    ("Event-Driven Worker", [sys.executable, "workers/worker.py"], root_dir, 0.5),
    ("Frontend Workload Dispatcher", [sys.executable, "frontend/app.py"], root_dir, 0.5)
]

processes = []
log_files = []

try:
    for name, cmd, cwd, delay in services:
        print(f"[BOOT] Starting {name}")
        safe_name = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(":", "")
        log_path = os.path.join(log_dir, f"{safe_name}.log")
        
        log_file = open(log_path, "w", encoding="utf-8")
        log_files.append(log_file)
        
        p = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=log_file,
            stderr=log_file
        )
        processes.append((name, p))
        time.sleep(delay)
    print(f"- Log Directory:  {log_dir}")
    print(f"- API Gateway:    http://{config.GATEWAY_HOST}:{config.GATEWAY_PORT}")
    print(f"- Web Dashboard:  http://localhost:{config.WEBSOCKET_DASHBOARD_PORT} ")
    print(f"- Dispatcher UI:  http://localhost:{config.FRONTEND_PORT}")

    
    while True:
        for name, p in processes:
            poll = p.poll()
            if poll is not None:
                print(f"\n[ALERT] {name} exited  with {poll}")
                raise SystemExit
        time.sleep(1.0)
        
except KeyboardInterrupt:
    print("\n\n[SHUTDOWN]")
finally:
    for name, p in processes:
        print(f"[SHUTDOWN] Stopping {name}")
        try:
            p.terminate()
            p.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            print(f"[SHUTDOWN] killing {name}")
            p.kill()
        except Exception:
            pass
            
    for f in log_files:
        try:
            f.close()
        except:
            pass
    print("[SHUTDOWN] ")

