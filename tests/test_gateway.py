import sys
import os
from unittest.mock import MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from API_gateway.router import parse_http_request

def test_parse_http_request():
    mock_conn = MagicMock()

    mock_conn.recv.side_effect = [
        b"POST /submit HTTP/1.1\r\nContent-Length: 16\r\n\r\n",
        b'{"test": "data"}'
    ]
    
    method, path, headers, body = parse_http_request(mock_conn)
    
    assert method == "POST"
    assert path == "/submit"
    assert b'{"test": "data"}' in body
    assert any("Content-Length" in h for h in headers)
