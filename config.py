import os

def load_env(env_path=None):
    if env_path is None:
        env_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), ".env")
        
    if not os.path.exists(env_path):
        return
        
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if " #" in line:
                line = line.split(" #", 1)[0].strip()
            elif line.startswith(" #"):
                continue
                
            if "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                
                if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                    val = val[1:-1]
                    
                if key not in os.environ:
                    os.environ[key] = val

load_env()

def is_production():
    return os.getenv("RENDER") is not None or os.getenv("PRODUCTION") == "true"

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/platform_db")

CACHE_HOST = os.getenv("CACHE_HOST", "127.0.0.1")
AUTH_CACHE_PORT = int(os.getenv("AUTH_CACHE_PORT", "6379"))
WORKER_CACHE_PORT = int(os.getenv("WORKER_CACHE_PORT", "6380"))
TASK_CACHE_PORT = int(os.getenv("TASK_CACHE_PORT", "6381"))

EVENT_BROKER_HOST = os.getenv("EVENT_BROKER_HOST", "127.0.0.1")
EVENT_BROKER_PORT = int(os.getenv("EVENT_BROKER_PORT", "9500"))

COORDINATOR_HOST = os.getenv("COORDINATOR_HOST", "127.0.0.1")
COORDINATOR_PORT = int(os.getenv("COORDINATOR_PORT", "9000"))

GATEWAY_HOST = os.getenv("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8000"))

GATEWAY_AUTH_TOKEN = os.getenv("GATEWAY_AUTH_TOKEN")
if not GATEWAY_AUTH_TOKEN:
    if is_production():
        raise ValueError("GATEWAY_AUTH_TOKEN environment variable must be set in production!")
    GATEWAY_AUTH_TOKEN = "secret-token-key"
elif GATEWAY_AUTH_TOKEN == "secret-token-key" and is_production():
    raise ValueError("Default secret-token-key is not allowed in production environments!")

MAX_HEADER_SIZE = int(os.getenv("MAX_HEADER_SIZE", "8192"))
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", "1048576"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "5.0"))

CIRCUIT_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "3"))
CIRCUIT_RECOVERY_TIMEOUT = float(os.getenv("CIRCUIT_RECOVERY_TIMEOUT", "5.0"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "10.0"))
WEBSOCKET_DASHBOARD_PORT = int(os.getenv("WEBSOCKET_DASHBOARD_PORT", "8080"))
FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "8081"))
