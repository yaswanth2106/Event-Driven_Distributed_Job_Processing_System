import asyncio
import websockets
from websockets.asyncio.server import ServerConnection, serve
import json
import sys
import os
import logging

logging.getLogger("websockets.server").setLevel(logging.ERROR)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from types import ModuleType
from typing import Optional

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

from cache.cache_manager import CacheClient  # noqa: E402

cache_host = config.CACHE_HOST if config else "127.0.0.1"
cache_port = config.TASK_CACHE_PORT if config else 6381
metrics_cache = CacheClient(host=cache_host, port=cache_port) 

class RenderWSConnection(ServerConnection):
    def data_received(self, data):
        if b"Upgrade: websocket" not in data and (data.startswith(b"GET ") or data.startswith(b"HEAD ") or data.startswith(b"OPTIONS ")):
            try:
                self.transport.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            except Exception:
                pass
            try:
                self.transport.close()
            except Exception:
                pass
            return
        super().data_received(data)

async def broadcast_telemetry(websocket):
    print("[DASHBOARD] Client connected")
    try:
        while True:
            data_str = metrics_cache.get("telemetry")
            alerts_str = metrics_cache.get("alerts")
            
            telemetry = {
                "workers_alive": 0,
                "queue_depth": 0,
                "latency": 0.0,
                "rps": 0.0,
                "failures": 0,
                "alerts": []
            }
            
            if data_str:
                try:
                    telemetry.update(json.loads(data_str))
                except Exception:
                    pass
            
            if alerts_str:
                try:
                    telemetry["alerts"] = json.loads(alerts_str)
                except Exception:
                    pass
                    
            await websocket.send(json.dumps(telemetry))
            await asyncio.sleep(1)
    except websockets.exceptions.ConnectionClosed:
        print("[DASHBOARD] Client disconnected")

async def main():
    ws_port = config.WEBSOCKET_DASHBOARD_PORT if config else 8080
    print(f"[DASHBOARD] WbS server started on ws://0.0.0.0:{ws_port}")
    async with serve(broadcast_telemetry, "0.0.0.0", ws_port, create_connection=RenderWSConnection):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
