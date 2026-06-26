import json
import os
import sys
import time
import datetime
import logging
import threading
import re
from logging.handlers import RotatingFileHandler

from types import ModuleType
from typing import Optional

config: Optional[ModuleType] = None
try:
    import config
except ImportError:
    config = None

DEFAULT_LOG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "platform.log"))
MIN_LOG_LEVEL = getattr(config, "MIN_LOG_LEVEL", "INFO") if config else "INFO"

LEVEL_ORDER = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50
}

class StructuredLogger:
    def __init__(self, event_bus, log_file=DEFAULT_LOG_FILE, min_level=MIN_LOG_LEVEL):
        self.bus = event_bus
        self.log_file = log_file
        self.min_level = min_level.upper()
        self.lock = threading.Lock()
        
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        

        self.logger = logging.getLogger("platform_structured_logger")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        

        if self.logger.handlers:
            self.logger.handlers.clear()
            
        handler = RotatingFileHandler(
            self.log_file,
            maxBytes=10 * 1024 * 1024, 
            backupCount=5,
            encoding="utf-8"
        )

        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        from event_bus.event_types import LOG_EVENT
        self.bus.subscribe(LOG_EVENT, self.handle_log)

    def handle_log(self, event):
        level = event.get("level", "INFO").upper()
        
    
        event_priority = LEVEL_ORDER.get(level, 20)
        min_priority = LEVEL_ORDER.get(self.min_level, 20)
        if event_priority < min_priority:
            return
        
        correlation_id = (
            event.get("correlation_id") or 
            event.get("trace_id") or 
            event.get("job_id") or 
            event.get("task_id")
        )
        if not correlation_id and "job" in event:
            correlation_id = event["job"].get("id")
            
        if not correlation_id:
            msg = event.get("message", "")
            uuid_match = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', msg, re.IGNORECASE)
            if uuid_match:
                correlation_id = uuid_match.group(0)
            else:
                correlation_id = "global"

        
        timestamp = event.get("timestamp") or time.time()
        iso_timestamp = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc).isoformat()
        
        log_entry = {
            "timestamp": iso_timestamp,
            "level": level,
            "component": event.get("component", "unknown"),
            "correlation_id": correlation_id,
            "message": event.get("message", "")
        }
        

        with self.lock:
            try:
                self.logger.info(json.dumps(log_entry))
            except Exception as e:
                print(f"[STRUCTURED LOGGER ERROR] Failed to write log: {e}", file=sys.stderr)
