import socket
import threading
import os
from collections import OrderedDict
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None

from cache.cache_manager import parse_resp

class LRUCacheServer:
    def __init__(self, host="127.0.0.1", port=6379, max_capacity=1000, aof_file="cache.aof"):
        self.host = host
        self.port = port
        self.max_capacity = max_capacity
        self.aof_file = aof_file
        
        self.data_store = OrderedDict()
        self.store_lock = threading.Lock()
        self.aof_lock = threading.Lock()
        self.running = False
        self.server_socket = None

    def append_to_log(self, cmd_array):
        req = f"*{len(cmd_array)}\r\n"
        for arg in cmd_array:
            arg_str = str(arg)
            req += f"${len(arg_str.encode('utf-8'))}\r\n{arg_str}\r\n"
            
        with self.aof_lock:
            with open(self.aof_file, "a", encoding="utf-8") as f:
                f.write(req)

    def replay_aof_recovery(self):
        if not os.path.exists(self.aof_file):
            return

        print(f"[LRU CACHE {self.port}] Replaying logs")
        try:
            with open(self.aof_file, "r", encoding="utf-8") as f:
                buffer = f.read()
        except Exception as e:
            print(f"[LRU CACHE] Failed to read AOF log: {e}")
            return

        while buffer:
            parts, rest = parse_resp(buffer)
            if parts is None or (parts is None and rest == buffer):
                break
            buffer = rest
            
            if isinstance(parts, list) and len(parts) >= 2:
                action = parts[0].upper()
                if action == "SET" and len(parts) >= 3:
                    self.execute_lru_set(parts[1], parts[2], log_write=False)
                elif action in ["DELETE", "DEL"] and len(parts) >= 2:
                    key = parts[1]
                    with self.store_lock:
                        if key in self.data_store:
                            del self.data_store[key]
                            
        print(f"[LRU CACHE {self.port}] Crash recovery done, Active RAM pool holds {len(self.data_store)} items")

    def execute_lru_set(self, key, value, log_write=True):
        evicted_key = None

        with self.store_lock:
            if key in self.data_store:
                self.data_store[key] = value
                self.data_store.move_to_end(key)
            else:
                if len(self.data_store) >= self.max_capacity:
                    evicted_key, _ = self.data_store.popitem(last=False)
                self.data_store[key] = value

        if log_write:
            if evicted_key:
                print(f"[LRU CACHE] Evicting oldest key: {evicted_key}")
                self.append_to_log(["DEL", evicted_key])
            self.append_to_log(["SET", key, value])

    def execute_lru_get(self, key):
        with self.store_lock:
            if key in self.data_store:
                value = self.data_store[key]
                self.data_store.move_to_end(key)
                return value
            return None

    def handle_client(self, conn, addr):
        with conn:
            buffer = ""
            while self.running:
                try:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    if chunk.startswith(b"GET ") or chunk.startswith(b"HEAD "):
                        try:
                            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nOK")
                        except Exception:
                            pass
                        return
                    buffer += chunk.decode('utf-8', errors='ignore')
                    
                    while True:
                        parts, rest = parse_resp(buffer)
                        if parts is None or (parts is None and rest == buffer):
                            break
                        buffer = rest
                        
                        if isinstance(parts, list) and len(parts) > 0:
                            action = parts[0].upper()

                            if action == "SET":
                                if len(parts) < 3:
                                    conn.sendall(b"-ERR missing arguments\r\n")
                                    continue
                                self.execute_lru_set(parts[1], parts[2])
                                conn.sendall(b"+OK\r\n")
                                
                            elif action == "GET":
                                if len(parts) < 2:
                                    conn.sendall(b"-ERR missing key parameter\r\n")
                                    continue
                                val = self.execute_lru_get(parts[1])
                                if val is not None:
                                    conn.sendall(f"${len(val)}\r\n{val}\r\n".encode('utf-8'))
                                else:
                                    conn.sendall(b"$-1\r\n")
                                    
                            elif action in ["DELETE", "DEL"]:
                                if len(parts) < 2:
                                    conn.sendall(b"-ERR missing key parameter\r\n")
                                    continue
                                target_key = parts[1]
                                with self.store_lock:
                                    if target_key in self.data_store:
                                        del self.data_store[target_key]
                                        status = b":1\r\n"
                                        self.append_to_log(["DEL", target_key])
                                    else:
                                        status = b":0\r\n"
                                conn.sendall(status)
                            elif action == "QUIT":
                                conn.sendall(b"+OK\r\n")
                                return
                            else:
                                conn.sendall(b"-ERR unknown command syntax\r\n")
                except Exception:
                    break

    def start(self):
        self.replay_aof_recovery()
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(512)
        print(f"[LRU MEMORY CACHE] RESP states at TCP://{self.host}:{self.port}...")
        
        try:
            while self.running:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()
        except KeyboardInterrupt:
            print("\n[LRU CACHE] Shutting down")
            self.stop()
        except Exception:
            if self.running:
                raise

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

if __name__ == "__main__":
    default_port = config.TASK_CACHE_PORT if config else 6379
    port = int(sys.argv[1]) if len(sys.argv) > 1 else default_port
    host = config.CACHE_HOST if config else "127.0.0.1"
    aof_path = os.path.join(os.path.dirname(__file__), f"cache_{port}.aof")
    
    server = LRUCacheServer(host=host, port=port, max_capacity=5000, aof_file=aof_path)
    server.start()
