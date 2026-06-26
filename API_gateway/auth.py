import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from types import ModuleType
from typing import Optional

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

def middleware_authenticator(addr, headers, method, path, body=b""):
    if path.startswith("/status") or path.startswith("/health"):
        return True, None

    token = config.GATEWAY_AUTH_TOKEN if config else "secret-token-key"
    token_header = f"authorization: bearer {token}".lower()

    is_authenticated = False
    for line in headers:
        lowered = line.lower()
        if lowered.startswith(token_header):
            is_authenticated = True
            break
        if lowered.startswith("cookie:") and "session_id=valid-session-id" in lowered:
            is_authenticated = True
            break

    if not is_authenticated:
        print(f"[AUTH BLOCKED] Access denied for {addr[0]} requesting route '{path}'")
        body = "<h1>401 Unauthorized</h1><p>Gateway Shield Exception: Invalid Bearer Token.</p>"
        resp = f"HTTP/1.1 401 Unauthorized\r\nContent-Type: text/html\r\nContent-Length: {len(body)}\r\n\r\n{body}"
        return False, resp.encode('utf-8')
    return True, None
