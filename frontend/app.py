import http.server
import socketserver
import json
import socket
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None

PORT = config.FRONTEND_PORT if config else 8081
GATEWAY_HOST = config.GATEWAY_HOST if config else "127.0.0.1"
GATEWAY_PORT = config.GATEWAY_PORT if config else 8000

def submit_task(job_type, priority, payload=None):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((GATEWAY_HOST, GATEWAY_PORT))
        
        event = {
            "event": "JOB_SUBMITTED",
            "job_type": job_type,
            "priority": priority
        }
        if payload is not None:
            event["payload"] = payload
        
        auth_token = config.GATEWAY_AUTH_TOKEN if config else "secret-token-key"
        body = json.dumps(event)
        http_request = (
            f"POST /submit HTTP/1.1\r\n"
            f"Host: {GATEWAY_HOST}:{GATEWAY_PORT}\r\n"
            f"Authorization: Bearer {auth_token}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body.encode('utf-8'))}\r\n"
            f"Connection: close\r\n\r\n"
            f"{body}"
        )
        
        s.sendall(http_request.encode('utf-8'))
        
        response = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            response += chunk
            
        parts = response.split(b"\r\n\r\n", 1)
        resp_body = parts[1].decode('utf-8', errors='ignore') if len(parts) > 1 else "{}"
        print(f"[GATEWAY RESPONSE] {resp_body.strip()}")
        return response

class FrontendProxyHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            
            html_path = os.path.join(os.path.dirname(__file__), "index.html")
            with open(html_path, "r", encoding="utf-8") as f:
                self.wfile.write(f.read().encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/submit":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')
            
            try:
                data = json.loads(post_data)
                job_type = data.get("job_type")
                priority = data.get("priority", "MEDIUM")
                payload = data.get("payload")
                
                if not job_type:
                    raise ValueError("job_type missing")
                
                print(f"[FRONTEND APP] Proxying '{job_type}' ({priority}) to Gateway")
                raw_gateway_resp = submit_task(job_type, priority, payload)
                
                parts = raw_gateway_resp.split(b"\r\n\r\n", 1)
                body = parts[1] if len(parts) > 1 else b"{}"
                
                header_lines = parts[0].decode('utf-8', errors='ignore').split("\r\n")
                status_line = header_lines[0]
                status_code = int(status_line.split(" ")[1])
                
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
                
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Failed to submit: {str(e)}"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

class ThreadingHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "cli":
        submit_task("urn", "LOW")
    else:
        ThreadingHTTPServer.allow_reuse_address = True
        with ThreadingHTTPServer(("", PORT), FrontendProxyHandler) as httpd:
            print(f"[FRONTEND APP] Server started at http://localhost:{PORT}")
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                print("\nShutting down frontend server")
