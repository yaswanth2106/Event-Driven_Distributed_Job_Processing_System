import socket
import queue
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None

class EventBusConnectionPool:
    def __init__(self, host, port, max_size=64):
        self.host = host
        self.port = port
        self.pool = queue.Queue(maxsize=max_size)

    def init_pool(self, initial_size=5):
        for _ in range(initial_size):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.host, self.port))
                self.pool.put(s)
            except Exception as e:
                print(f"[POOL WARNING] Failed to init connection: {e}")

    def get_connection(self):
        try:
            return self.pool.get(timeout=2.0)
        except queue.Empty:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.host, self.port))
                return s
            except:
                return None

    def release_connection(self, sock, status_ok=True):
        if status_ok:
            try:
                self.pool.put(sock, timeout=0.5)
            except queue.Full:
                sock.close()
        else:
            try:
                sock.close()
            except:
                pass


coordinator_host = config.COORDINATOR_HOST if config else "127.0.0.1"
coordinator_port = config.COORDINATOR_PORT if config else 9000
bus_pool = EventBusConnectionPool(coordinator_host, coordinator_port, max_size=64)
