import json
from unittest.mock import MagicMock, patch
from API_gateway.router import process_client_socket

def test_process_client_socket_submit():
    client_conn = MagicMock()
    request_payload = (
        b"POST /submit HTTP/1.1\r\n"
        b"Authorization: Bearer secret-token-key\r\n"
        b"Content-Length: 51\r\n\r\n"
        b'{"job_type": "audio-enhancement", "id": "task-123"}'
    )
    client_conn.recv.return_value = request_payload

    mock_bus_sock = MagicMock()
    mock_bus_sock.recv.return_value = b"ACK\n"

    with patch('API_gateway.router.bus_pool.get_connection', return_value=mock_bus_sock):
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"202 Accepted" in sent_data
    assert b"PENDING" in sent_data
    assert b"task-123" in sent_data

def test_process_client_socket_status_completed():
    client_conn = MagicMock()
    request_payload = (
        b"GET /status?id=task-123 HTTP/1.1\r\n\r\n"
    )
    client_conn.recv.return_value = request_payload

    mock_task_cache = MagicMock()
    mock_task_cache.get.return_value = json.dumps({"task_id": "task-123", "status": "COMPLETED", "result": "done"})

    with patch('API_gateway.router.task_cache', mock_task_cache):
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"200 OK" in sent_data
    assert b"COMPLETED" in sent_data

def test_process_client_socket_status_pending():
    client_conn = MagicMock()
    request_payload = (
        b"GET /status?id=task-123 HTTP/1.1\r\n\r\n"
    )
    client_conn.recv.return_value = request_payload

    mock_task_cache = MagicMock()
    mock_task_cache.get.return_value = None

    with patch('API_gateway.router.task_cache', mock_task_cache):
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"200 OK" in sent_data
    assert b"PENDING" in sent_data

if __name__ == "__main__":
    print("Running gateway response tests")
    test_process_client_socket_submit()
    test_process_client_socket_status_completed()
    test_process_client_socket_status_pending()
    print("success")
