import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import threading
import pytest
from cache.cache_manager import CacheClient
from cache.lru_cache import LRUCacheServer

def test_cache_client_pool_concurrency():
    server = LRUCacheServer(host="127.0.0.1", port=16379, max_capacity=100, aof_file="tests/test_cache.aof")
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    
    time.sleep(0.5)

    try:
        client = CacheClient(port=16379, pool_size=5)
        
        assert client.set("hello", "world") is True
        assert client.get("hello") == "world"

        num_threads = 10
        num_ops = 50
        errors = []

        def worker_task(thread_idx):
            for i in range(num_ops):
                key = f"key-{thread_idx}-{i}"
                val = f"val-{thread_idx}-{i}"
                try:
                    if not client.set(key, val):
                        errors.append(f"Failed to set {key}")
                    if client.get(key) != val:
                        errors.append(f"Failed to get correct val for {key}")
                except Exception as e:
                    errors.append(f"Thread {thread_idx} encountered exception: {e}")

        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker_task, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Concurrency errors: {errors}"
        
    finally:
        server.stop()
        if os.path.exists("tests/test_cache.aof"):
            try:
                os.remove("tests/test_cache.aof")
            except:
                pass

if __name__ == "__main__":
    print("Running cache pool tests")
    test_cache_client_pool_concurrency()
    print("success")
