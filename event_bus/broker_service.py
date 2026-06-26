import socket
import selectors
import sys
from types import ModuleType
from typing import Optional

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

class PubSubBroker:
    def __init__(self, host="127.0.0.1", port=9500):
        self.host = host
        self.port = port
        self.sel = selectors.DefaultSelector()
        self.subscribers = {} 
        self.buffers = {}
        self.running = False

    def start(self):
        self.running = True
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind((self.host, self.port))
        lsock.listen(512)
        lsock.setblocking(False)
        self.sel.register(lsock, selectors.EVENT_READ, self.accept_connection)
        print(f"[EVENT BROKER] Standalone Message Broker on TCP://{self.host}:{self.port}...")

        try:
            while self.running:
                events = self.sel.select(timeout=1.0)
                for key, mask in events:
                    callback = key.data
                    callback(key.fileobj)
        except KeyboardInterrupt:
            print("\n[EVENT BROKER] Shutdown")
        finally:
            try:
                self.sel.close()
            except Exception:
                pass
            try:
                lsock.close()
            except Exception:
                pass

    def stop(self):
        self.running = False

    def accept_connection(self, sock):
        try:
            conn, addr = sock.accept()
            conn.setblocking(False)
            self.sel.register(conn, selectors.EVENT_READ, self.handle_client)
            self.buffers[conn] = b""
        except Exception:
            pass

    def handle_client(self, conn):
        try:
            data = conn.recv(8192)
            if not data:
                self.cleanup_client(conn)
                return
            
            if data.startswith(b"GET ") or data.startswith(b"HEAD "):
                try:
                    conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nOK")
                except Exception:
                    pass
                try:
                    self.sel.unregister(conn)
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                self.buffers.pop(conn, None)
                return
            
            
            current_buffer = self.buffers.get(conn, b"") + data
            
            while b"\n" in current_buffer:
                line_bytes, current_buffer = current_buffer.split(b"\n", 1)
                line = line_bytes.decode('utf-8', errors='ignore').strip()
                if not line:
                    continue
                
                parts = line.split(" ", 2)
                cmd = parts[0].upper()
                if cmd == "SUB" and len(parts) >= 2:
                    topic = parts[1]
                    if topic not in self.subscribers:
                        self.subscribers[topic] = set()
                    self.subscribers[topic].add(conn)
                    conn.sendall(b"+OK Subscribed\n")
                    
                elif cmd == "PUB" and len(parts) >= 3:
                    topic = parts[1]
                    msg = parts[2]
                    
                    if topic in self.subscribers:
                        dead_links = []
                        for sub_conn in self.subscribers[topic]:
                            try:
                                sub_conn.sendall((msg + "\n").encode('utf-8'))
                            except Exception:
                                dead_links.append(sub_conn)
                        
                        for dead_conn in dead_links:
                            self.cleanup_client(dead_conn)
                            
                    conn.sendall(b"+OK Published\n")
                else:
                    conn.sendall(b"-ERR Unknown Command\n")
            

            self.buffers[conn] = current_buffer
        except Exception:
            self.cleanup_client(conn)

    def cleanup_client(self, conn):
        try:
            self.sel.unregister(conn)
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        self.buffers.pop(conn, None)
        
        for topic, conns in list(self.subscribers.items()):
            if conn in conns:
                conns.remove(conn)
                if not conns:
                    del self.subscribers[topic]

if __name__ == "__main__":
    default_port = config.EVENT_BROKER_PORT if config else 9500
    port = int(sys.argv[1]) if len(sys.argv) > 1 else default_port
    host = config.EVENT_BROKER_HOST if config else "127.0.0.1"
    broker = PubSubBroker(host=host, port=port)
    broker.start()
