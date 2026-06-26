import json
from unittest.mock import MagicMock, patch
from API_gateway.router import process_client_socket

def test_process_client_socket_completed():
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
    
    mock_task_cache = MagicMock()
    mock_task_cache.get.side_effect = [
        None,
        json.dumps({"task_id": "task-123", "status": "COMPLETED"})
    ]

    with patch('API_gateway.router.bus_pool.get_connection', return_value=mock_bus_sock), \
         patch('API_gateway.router.task_cache', mock_task_cache), \
         patch('API_gateway.router.time.sleep'):
         
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"200 OK" in sent_data
    assert b"COMPLETED" in sent_data

def test_process_client_socket_dlq():
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
    
    mock_task_cache = MagicMock()
    mock_task_cache.get.side_effect = [
        json.dumps({"task_id": "task-123", "status": "DLQ"})
    ]

    with patch('API_gateway.router.bus_pool.get_connection', return_value=mock_bus_sock), \
         patch('API_gateway.router.task_cache', mock_task_cache), \
         patch('API_gateway.router.time.sleep'):
         
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"422 Unprocessable Entity" in sent_data
    assert b"DLQ" in sent_data

def test_process_client_socket_timeout():
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
    
    mock_task_cache = MagicMock()
    mock_task_cache.get.return_value = json.dumps({"task_id": "task-123", "status": "PENDING"})

    with patch('API_gateway.router.bus_pool.get_connection', return_value=mock_bus_sock), \
         patch('API_gateway.router.task_cache', mock_task_cache), \
         patch('API_gateway.router.time.sleep') as mock_sleep:
         
        def side_effect(interval):
            pass
        mock_sleep.side_effect = side_effect
        
        process_client_socket(client_conn, ("127.0.0.1", 12345))

    sent_data = b"".join([call[0][0] for call in client_conn.sendall.call_args_list])
    assert b"504 Gateway Timeout" in sent_data

if __name__ == "__main__":
    print("Running gateway response tests")
    test_process_client_socket_completed()
    test_process_client_socket_dlq()
    test_process_client_socket_timeout()
    print("success")
