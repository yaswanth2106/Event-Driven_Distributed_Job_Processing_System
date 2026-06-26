import queue
import threading
import selectors
import socket
import json
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None

class InternalEventBus:
    def __init__(self, host=None, port=None):
        self.host = host if host is not None else (config.EVENT_BROKER_HOST if config else "127.0.0.1")
        self.port = port if port is not None else (config.EVENT_BROKER_PORT if config else 9500)
        self.running = True
        self.threads = []
        self.sockets = []
        self.lock = threading.Lock()

    def start(self):
        self.running = True
        print("[EVENT BUS CLIENT] Event bus started")

    def subscribe(self, event_type, handler_func):
        t = threading.Thread(
            target=self._subscriber_loop, 
            args=(event_type, handler_func), 
            daemon=True
        )
        self.threads.append(t)
        t.start()

    def _subscriber_loop(self, event_type, handler_func):
        while self.running:
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.host, self.port))
                with self.lock:
                    self.sockets.append(sock)
                
                sock.sendall(f"SUB {event_type}\n".encode('utf-8'))
                
                resp = sock.recv(1024)
                if not resp.startswith(b"+OK"):
                    raise ConnectionError(f"Subscription failed: {resp.decode()}")

                buffer = ""
                while self.running:
                    chunk = sock.recv(8192)
                    if not chunk:
                        break
                    buffer += chunk.decode('utf-8', errors='ignore')
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            event = json.loads(line)
                            handler_func(event)
                        except Exception as e:
                            print(f"[EVENT BUS CLIENT] Error for event {event_type}: {e}")
            except Exception:
                time.sleep(1.0)
            finally:
                if sock:
                    try:
                        sock.close()
                    except:
                        pass
                    with self.lock:
                        if sock in self.sockets:
                            self.sockets.remove(sock)

    def publish(self, event):
        try:
            topic = None
            if isinstance(event, dict):
                if "event" in event:
                    topic = event["event"]
                elif "job_type" in event:
                    from event_bus.event_types import JOB_SUBMITTED
                    topic = JOB_SUBMITTED
            
            if not topic:
                raise ValueError("No topic/event field in event dict")

            payload = json.dumps(event)
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                s.sendall(f"PUB {topic} {payload}\n".encode('utf-8'))
                s.recv(1024) 
        except Exception as e:
            print(f"[EVENT BUS CLIENT] Publish failed for topic {topic if 'topic' in locals() else 'unknown'}: {e}")

    def stop(self):
        self.running = False
        with self.lock:
            for sock in list(self.sockets):
                try:
                    sock.close()
                except:
                    pass


class EventLoop:
    def __init__(self, host, port, event_bus):
        self.host = host
        self.port = port
        self.event_bus = event_bus
        self.sel = selectors.DefaultSelector()
        self.running = False
        self.worker_sockets = {} 
        self.buffers = {} 

        from event_bus.event_types import TASK_ASSIGNED
        self.event_bus.subscribe(TASK_ASSIGNED, self.send_to_worker)

    def accept_wrapper(self, sock):
        try:
            conn, addr = sock.accept()
            conn.setblocking(False)
            self.sel.register(conn, selectors.EVENT_READ, self.read_wrapper)
            self.buffers[conn] = b""
        except Exception:
            pass

    def read_wrapper(self, conn):
        try:
            data = conn.recv(4096)
            if data:
                if data.startswith(b"GET ") or data.startswith(b"HEAD "):
                    try:
                        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nOK")
                    except:
                        pass
                    try:
                        self.sel.unregister(conn)
                    except:
                        pass
                    try:
                        conn.close()
                    except:
                        pass
                    self.buffers.pop(conn, None)
                    return
                
 
                current_buffer = self.buffers.get(conn, b"") + data
                
         
                while b"\n" in current_buffer:
                    line_bytes, current_buffer = current_buffer.split(b"\n", 1)
                    line = line_bytes.decode('utf-8', errors='ignore').strip()
                    if not line:
                        continue
                    
                    try:
                        event = json.loads(line)
                        if event.get("event") in ["REGISTER", "HEARTBEAT"]:
                            wid = event.get("worker_id")
                            if wid:
                                self.worker_sockets[wid] = conn
                                
                        self.event_bus.publish(event)
                    except json.JSONDecodeError:
                        pass
                
           
                self.buffers[conn] = current_buffer
                
                try:
                    conn.sendall(b"ACK\n")
                except Exception:
                    pass
            else:
                try:
                    self.sel.unregister(conn)
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass
                self.buffers.pop(conn, None)
        except BlockingIOError:
            pass
        except Exception:
            try:
                self.sel.unregister(conn)
            except:
                pass
            try:
                conn.close()
            except:
                pass
            self.buffers.pop(conn, None)

    def send_to_worker(self, event):
        worker_id = event.get("worker_id")
        if worker_id and worker_id in self.worker_sockets:
            sock = self.worker_sockets[worker_id]
            try:
                payload = json.dumps(event) + "\n"
                sock.sendall(payload.encode('utf-8'))
            except Exception as e:
                print(f"[NETWORK] Failed to send to {worker_id}: {e}")

    def start(self):
        self.running = True
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, self.port))
        lsock.listen(512)
        lsock.setblocking(False)
        self.sel.register(lsock, selectors.EVENT_READ, self.accept_wrapper)
        print(f"[NETWORK] Event Loop listening on {self.host}:{self.port}")
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while self.running:
            events = self.sel.select(timeout=1.0)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj)

    def stop(self):
        self.running = False
        try:
            self.sel.close()
        except:
            pass
