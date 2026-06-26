import sys
import os
import pytest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from event_bus.event_bus import InternalEventBus, EventLoop
from event_bus.event_types import JOB_SUBMITTED

def test_event_bus_initialization():
    bus = InternalEventBus(host="127.0.0.1", port=9999)
    assert bus.host == "127.0.0.1"
    assert bus.port == 9999
    assert bus.running is True

@patch('socket.socket')
def test_event_bus_publish(mock_socket):
    mock_conn = MagicMock()
    mock_socket.return_value.__enter__.return_value = mock_conn
    
    bus = InternalEventBus(host="127.0.0.1", port=9999)
    event = {"event": JOB_SUBMITTED, "job_type": "test"}
    bus.publish(event)
    
    mock_socket.return_value.__enter__.return_value.connect.assert_called_with(("127.0.0.1", 9999))
    send_data = mock_conn.sendall.call_args[0][0]
    assert b"PUB" in send_data
    assert b"JOB_SUBMITTED" in send_data
