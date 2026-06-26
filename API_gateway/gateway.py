import os
import sys
import socket
import threading
from types import ModuleType
from typing import Optional

from .router import process_client_socket
from .connection_pool import bus_pool

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

GATEWAY_HOST = config.GATEWAY_HOST if config else "127.0.0.1"
GATEWAY_PORT = config.GATEWAY_PORT if config else 8000
EVENT_BUS_PORT = config.COORDINATOR_PORT if config else 9000

bus_pool.init_pool(initial_size=32)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as gateway:
    gateway.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    gateway.bind((GATEWAY_HOST, GATEWAY_PORT))
    gateway.listen(512)

    print(f"\n[API GATEWAY] Gateway at http://{GATEWAY_HOST}:{GATEWAY_PORT}")
    while True:
        try:
            conn, addr = gateway.accept()
            threading.Thread(
                target=process_client_socket,
                args=(conn, addr),
                daemon=True
            ).start()
        except KeyboardInterrupt:
            print("\nShutting down")
            break


