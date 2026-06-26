import socket
import queue
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None

def parse_resp(buffer):
    if not buffer:
        return None, buffer
    
    first_char = buffer[0]
    if first_char == '+':
        if '\r\n' not in buffer:
            return None, buffer
        line, rest = buffer.split('\r\n', 1)
        return line[1:], rest
        
    elif first_char == '-':
        if '\r\n' not in buffer:
            return None, buffer
        line, rest = buffer.split('\r\n', 1)
        return Exception(line[1:]), rest
        
    elif first_char == ':':
        if '\r\n' not in buffer:
            return None, buffer
        line, rest = buffer.split('\r\n', 1)
        return int(line[1:]), rest
        
    elif first_char == '$':
        if '\r\n' not in buffer:
            return None, buffer
        len_line, rest = buffer.split('\r\n', 1)
        try:
            str_len = int(len_line[1:])
        except ValueError:
            return None, buffer
            
        if str_len == -1:
            return None, rest
        
        if len(rest) < str_len + 2:
            return None, buffer
            
        value = rest[:str_len]
        rest = rest[str_len + 2:]
        return value, rest
        
    elif first_char == '*':
        if '\r\n' not in buffer:
            return None, buffer
        len_line, rest = buffer.split('\r\n', 1)
        try:
            num_elements = int(len_line[1:])
        except ValueError:
            return None, buffer
            
        if num_elements == -1:
            return None, rest
            
        elements = []
        temp_buffer = rest
        for _ in range(num_elements):
            val, temp_buffer = parse_resp(temp_buffer)
            if val is None and temp_buffer == rest:
                return None, buffer
            elements.append(val)
            
        return elements, temp_buffer
        
    else:

        if '\r\n' in buffer:
            line, rest = buffer.split('\r\n', 1)
            tokens = [t.strip('"') for t in line.split()]
            return tokens, rest
        elif '\n' in buffer:
            line, rest = buffer.split('\n', 1)
            tokens = [t.strip('"') for t in line.split()]
            return tokens, rest
        return None, buffer

class CacheClient:
    def __init__(self, host=None, port=6379, pool_size=10):
        self.host = host if host is not None else (config.CACHE_HOST if config else "127.0.0.1")
        self.port = port
        self.pool_size = pool_size
        self.pool = queue.Queue(maxsize=pool_size)

    def _get_connection(self):
        try:
            return self.pool.get_nowait()
        except queue.Empty:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.host, self.port))
                return sock
            except Exception as e:
                print(f"[CACHE CLIENT] Connection failed to {self.host}:{self.port}: {e}")
                return None

    def _release_connection(self, sock, status_ok=True):
        if sock is None:
            return
        if status_ok:
            try:
                self.pool.put_nowait(sock)
            except queue.Full:
                try:
                    sock.close()
                except Exception:
                    pass
        else:
            try:
                sock.close()
            except Exception:
                pass

    def _send_command(self, cmd_array):
        sock = self._get_connection()
        if sock is None:
            raise ConnectionError("No cache server connection")

        status_ok = True
        try:
            req = f"*{len(cmd_array)}\r\n"
            for arg in cmd_array:
                arg_str = str(arg)
                req += f"${len(arg_str.encode('utf-8'))}\r\n{arg_str}\r\n"
                
            sock.sendall(req.encode('utf-8'))
            
            buffer = ""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    status_ok = False
                    raise ConnectionError("Connection closed by server")
                buffer += chunk.decode('utf-8', errors='ignore')
                val, rest = parse_resp(buffer)
                if val is not None or (val is None and rest != buffer):
                    if isinstance(val, Exception):
                        raise val
                    return val
        except Exception as e:
            status_ok = False
            raise e
        finally:
            self._release_connection(sock, status_ok)

    def set(self, key, value):
        try:
            resp = self._send_command(["SET", key, value])
            return resp == "OK"
        except Exception as e:
            print(f"[CACHE CLIENT] Failed to SET key {key}: {e}")
            return False

    def get(self, key):
        try:
            return self._send_command(["GET", key])
        except Exception as e:
            print(f"[CACHE CLIENT] Failed to GET key {key}: {e}")
            return None

    def delete(self, key):
        try:
            resp = self._send_command(["DEL", key])
            return resp == 1 or resp == "OK"
        except Exception as e:
            print(f"[CACHE CLIENT] Failed to DELETE key {key}: {e}")
            return False

    def close(self):
        while not self.pool.empty():
            try:
                sock = self.pool.get_nowait()
                try:
                    sock.sendall(b"*1\r\n$4\r\nQUIT\r\n")
                except Exception:
                    pass
                sock.close()
            except queue.Empty:
                break

cache_host = config.CACHE_HOST if config else "127.0.0.1"
auth_port = config.AUTH_CACHE_PORT if config else 6379
worker_port = config.WORKER_CACHE_PORT if config else 6380
task_port = config.TASK_CACHE_PORT if config else 6381

auth_cache = CacheClient(host=cache_host, port=auth_port)
worker_cache = CacheClient(host=cache_host, port=worker_port)
task_cache = CacheClient(host=cache_host, port=task_port)
