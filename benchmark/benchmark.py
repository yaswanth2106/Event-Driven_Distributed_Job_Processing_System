import asyncio
import aiohttp
import time
import json
import argparse
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import config
except ImportError:
    config = None
from cache.cache_manager import CacheClient

class MockWorker:
    def __init__(self, worker_id, host="127.0.0.1", port=9000):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.task = None
        self.hb_task = None
        self.running = False

    async def start(self):
        self.running = True
        retries = 0
        max_retries = 15
        connected = False
        while not connected and retries < max_retries:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                connected = True
            except Exception as e:
                retries += 1
                await asyncio.sleep(1.0)
                
        if not connected:
            print(f"[MOCK WORKER ERROR] {self.worker_id} failed to connect to Coordinator")
            return

        try:
            reg_event = {"event": "REGISTER", "worker_id": self.worker_id, "capacity": 8}
            self.writer.write((json.dumps(reg_event) + "\n").encode())
            await self.writer.drain()
            
            self.task = asyncio.create_task(self._loop())
            self.hb_task = asyncio.create_task(self._heartbeat())
        except Exception as e:
            print(f"[MOCK WORKER ERROR] {self.worker_id} failed post-connection: {e}")

    async def _heartbeat(self):
        try:
            while self.running:
                await asyncio.sleep(4.0)
                if self.writer:
                    hb_event = {"event": "HEARTBEAT", "worker_id": self.worker_id}
                    self.writer.write((json.dumps(hb_event) + "\n").encode())
                    await self.writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _loop(self):
        buffer = b""
        try:
            while self.running:
                data = await self.reader.read(4096)
                if not data:
                    break
                buffer += data
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    line = line.strip()
                    if not line or line == b"ACK":
                        continue
                    try:
                        event = json.loads(line.decode())
                        if event.get("event") == "TASK_ASSIGNED":
                            job = event.get("job")
                            started_event = {
                                "event": "JOB_STARTED",
                                "worker_id": self.worker_id,
                                "job": job
                            }
                            completed_event = {
                                "event": "JOB_COMPLETED",
                                "worker_id": self.worker_id,
                                "job": job
                            }
                            self.writer.write((json.dumps(started_event) + "\n").encode())
                            self.writer.write((json.dumps(completed_event) + "\n").encode())
                            await self.writer.drain()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[MOCK WORKER ERROR] {self.worker_id} crashed: {e}")
        finally:
            await self.close_sockets()

    async def close_sockets(self):
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
            self.writer = None

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        if self.hb_task:
            self.hb_task.cancel()
        await self.close_sockets()

async def submit_job(session, job_id, semaphore, url, pbar_callback):
    token = config.GATEWAY_AUTH_TOKEN if config else "secret-token-key"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Benchmark": "true",
        "Content-Type": "application/json"
    }
    payload = {
        "job_type": "benchmark-task",
        "priority": "HIGH",
        "id": job_id
    }
    async with semaphore:
        start_time = time.perf_counter()
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                status = response.status
                latency = time.perf_counter() - start_time
                success = status in [200, 202]
                pbar_callback(success)
                return success, latency
        except Exception:
            pbar_callback(False)
            return False, 0.0

class ProgressBar:
    def __init__(self, total):
        self.total = total
        self.success = 0
        self.failed = 0
        self.last_printed = 0
        self.start_time = time.time()

    def update(self, success=True):
        if success:
            self.success += 1
        else:
            self.failed += 1
        
        current = self.success + self.failed
        if current - self.last_printed >= max(1, self.total // 50) or current == self.total:
            self.last_printed = current
            pct = (current / self.total) * 100
            elapsed = time.time() - self.start_time
            rps = current / elapsed if elapsed > 0 else 0.0
            print(f"\rProgress: {pct:5.1f}% | Submitted: {current}/{self.total} | Success: {self.success} | Failed: {self.failed} | Rate: {rps:.1f} req/s", end="", flush=True)

async def run_benchmark(num_jobs, num_workers, concurrency):
    print(f" BENCHMARK: {num_jobs} jobs | {num_workers} workers | {concurrency} clients")

    workers = []
    coord_host = config.COORDINATOR_HOST if config else "127.0.0.1"
    coord_port = config.COORDINATOR_PORT if config else 9000
    for i in range(num_workers):
        w = MockWorker(f"benchmark-worker-{i}", host=coord_host, port=coord_port)
        await w.start()
        workers.append(w)
    
    await asyncio.sleep(2.0)
    print(f"\n[BENCHMARK] Registered {num_workers}")

    semaphore = asyncio.Semaphore(concurrency)
    pbar = ProgressBar(num_jobs)
    gw_host = config.GATEWAY_HOST if config else "127.0.0.1"
    gw_port = config.GATEWAY_PORT if config else 8000
    url = f"http://{gw_host}:{gw_port}/submit"
    
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)
    
    start_time = time.perf_counter()
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i in range(num_jobs):
            job_id = f"job-bench-{i}-{int(time.time())}"
            tasks.append(submit_job(session, job_id, semaphore, url, pbar.update))
        
        results = await asyncio.gather(*tasks)
    
    total_duration = time.perf_counter() - start_time
    print("\n")
    print("[BENCHMARK] All requests dispatched. Waiting for queue to clear...")
    
    await asyncio.sleep(3.0)

    successful_requests = sum(1 for success, _ in results if success)
    latencies = [lat for _, lat in results if lat > 0.0]
    avg_gateway_latency = sum(latencies) / len(latencies) if latencies else 0.0
    throughput = successful_requests / total_duration if total_duration > 0 else 0.0

    cache_host = config.CACHE_HOST if config else "127.0.0.1"
    cache_port = config.TASK_CACHE_PORT if config else 6381
    metrics_cache = CacheClient(host=cache_host, port=cache_port)
    telemetry_raw = metrics_cache.get("telemetry")
    platform_avg_latency = 0.0
    platform_failures = 0
    
    if telemetry_raw:
        try:
            telemetry = json.loads(telemetry_raw)
            platform_avg_latency = telemetry.get("latency", 0.0)
            platform_failures = telemetry.get("failures", 0)
        except Exception:
            pass

    print("BENCHMARK SUMMARY")

    print(f"Total Jobs Submitted:         {num_jobs}")
    print(f"Successful HTTP Submissions:  {successful_requests}")
    print(f"Total Duration:               {total_duration:.2f} seconds")
    print(f"Submission Throughput (RPS):  {throughput:.2f} req/s")
    print(f"Avg Gateway Latency:          {avg_gateway_latency*1000:.2f} ms")
    print(f"Platform Average Latency:     {platform_avg_latency*1000:.2f} ms")
    print(f"Platform Failed Tasks:        {platform_failures}")



    for w in workers:
        await w.stop()
    print("[BENCHMARK] completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Platform Benchmarking Suite")
    parser.add_argument("--jobs", type=int, default=100000, help="Number of jobs to simulate")
    parser.add_argument("--workers", type=int, default=50, help="Number of simulated workers")
    parser.add_argument("--clients", type=int, default=1000, help="Number of concurrent async clients")
    args = parser.parse_args()
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(run_benchmark(args.jobs, args.workers, args.clients))
