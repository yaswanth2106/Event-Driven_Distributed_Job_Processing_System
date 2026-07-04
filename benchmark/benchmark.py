import argparse
import asyncio
import json
import math
import os
import sys
import time
import aiohttp
from types import ModuleType


from cache.cache_manager import CacheClient

psutil = None
try:
    import psutil as _psutil
    psutil = _psutil
except ImportError:
    psutil = None

config: ModuleType | None = None


def compute_percentile(values, percentile):
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, max(0, math.ceil((percentile / 100) * len(sorted_values)) - 1))
    return sorted_values[index]


def find_coordinator_process(port):
    if psutil is None:
        return None
    try:
        for conn in psutil.net_connections(kind='inet'):
            if conn.laddr and conn.laddr.port == port and conn.status == psutil.CONN_LISTEN:
                try:
                    return psutil.Process(conn.pid)
                except Exception:
                    pass
    except Exception:
        pass

    try:
        for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
            cmdline = proc.info.get("cmdline") or []
            if any("coordinator" in str(item).lower() for item in cmdline):
                return proc
    except Exception:
        pass

    return None


def measure_cpu_usage(proc):
    if proc is None:
        return None
    try:
        proc.cpu_percent(interval=None)
        time.sleep(0.1)
        return proc.cpu_percent(interval=None)
    except Exception:
        return None

try:
    import config
except ImportError:
    config = None

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
        self.assigned_tasks = 0
        self.busy_time = 0.0

    async def start(self):
        self.running = True
        retries = 0
        max_retries = 15
        connected = False
        while not connected and retries < max_retries:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                connected = True
            except Exception:
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
                            self.assigned_tasks += 1
                            task_start = time.perf_counter()
                            job = event.get("job")
                            started_event = {
                                "event": "JOB_STARTED",
                                "worker_id": self.worker_id,
                                "job": job
                            }
                            self.writer.write((json.dumps(started_event) + "\n").encode())
                            await self.writer.drain()

                            async def complete_job_after_delay(job_info, start_time):
                                await asyncio.sleep(0.2)
                                completed_event = {
                                    "event": "JOB_COMPLETED",
                                    "worker_id": self.worker_id,
                                    "job": job_info
                                }
                                if self.writer:
                                    try:
                                        self.writer.write((json.dumps(completed_event) + "\n").encode())
                                        await self.writer.drain()
                                    except Exception:
                                        pass
                                self.busy_time += time.perf_counter() - start_time

                            asyncio.create_task(complete_job_after_delay(job, task_start))
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
            except Exception:
                pass
            self.writer = None

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        if self.hb_task:
            self.hb_task.cancel()
        await self.close_sockets()

async def submit_job(session, job_id, semaphore, url, pbar_callback, submission_times):
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
        submission_times[job_id] = time.time()
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

    import threading
    metrics_lock = threading.Lock()
    
    submission_times = {}
    e2e_latencies = []
    
    failed_worker_id = None
    failed_worker_tasks = set()
    worker_fail_trigger_time = None
    worker_fail_detected_time = None
    reassigned_durations = []
    
    scale_up_trigger_times = {}
    autoscaling_response_times = []

    def event_handler(event):
        nonlocal worker_fail_detected_time, worker_fail_trigger_time
        ev_type = event.get("event")
        
        if ev_type == "JOB_COMPLETED":
            job = event.get("job", {})
            job_id = job.get("id")
            if job_id:
                with metrics_lock:
                    if job_id in submission_times:
                        e2e_latencies.append(time.time() - submission_times[job_id])
                        
        elif ev_type == "WORKER_FAILED":
            wid = event.get("worker_id")
            if wid == failed_worker_id:
                with metrics_lock:
                    worker_fail_detected_time = time.time()
                    print(f"\n[BENCHMARK EVENT] WORKER_FAILED detected for '{wid}' at {worker_fail_detected_time}")
                    
        elif ev_type == "TASK_ASSIGNED":
            job = event.get("job", {})
            job_id = job.get("id")
            wid = event.get("worker_id")
            if job_id:
                with metrics_lock:
                    if failed_worker_id and wid != failed_worker_id:
                        if job_id in failed_worker_tasks:
                            reassign_duration = time.time() - (worker_fail_detected_time or worker_fail_trigger_time)
                            reassigned_durations.append(reassign_duration)
                            failed_worker_tasks.discard(job_id)
                            print(f"\n[BENCHMARK EVENT] Job '{job_id}' reassigned to '{wid}' in {reassign_duration:.2f}s")
                    
                    if wid == failed_worker_id and not worker_fail_detected_time:
                        failed_worker_tasks.add(job_id)
                        
        elif ev_type == "AUTOSCALE_TRIGGERED":
            wid = event.get("worker_id")
            if wid:
                with metrics_lock:
                    scale_up_trigger_times[wid] = time.time()
                    print(f"\n[BENCHMARK EVENT] AUTOSCALE_TRIGGERED for '{wid}' at {scale_up_trigger_times[wid]}")
                    
        elif ev_type == "REGISTER":
            wid = event.get("worker_id")
            if wid and wid.startswith("worker-dynamic-"):
                with metrics_lock:
                    if wid in scale_up_trigger_times:
                        duration = time.time() - scale_up_trigger_times[wid]
                        autoscaling_response_times.append(duration)
                        print(f"\n[BENCHMARK EVENT] Worker '{wid}' registered (scaled up) in {duration:.2f}s")

    workers = []
    coord_host = config.COORDINATOR_HOST if config else "127.0.0.1"
    coord_port = config.COORDINATOR_PORT if config else 9000
    for i in range(num_workers):
        w = MockWorker(f"benchmark-worker-{i}", host=coord_host, port=coord_port)
        await w.start()
        workers.append(w)
    
    await asyncio.sleep(2.0)
    print(f"\n[BENCHMARK] Registered {num_workers}")

    # Start Event Bus client for live telemetry tracking
    from event_bus.event_bus import InternalEventBus
    from event_bus.event_types import REGISTER, WORKER_FAILED, TASK_ASSIGNED, JOB_COMPLETED
    event_bus = InternalEventBus(host=coord_host, port=9500)
    event_bus.start()
    event_bus.subscribe(JOB_COMPLETED, event_handler)
    event_bus.subscribe(WORKER_FAILED, event_handler)
    event_bus.subscribe(TASK_ASSIGNED, event_handler)
    event_bus.subscribe(REGISTER, event_handler)
    event_bus.subscribe("AUTOSCALE_TRIGGERED", event_handler)

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
            tasks.append(submit_job(session, job_id, semaphore, url, pbar.update, submission_times))
        
        # Simulate worker failure in the background
        async def simulate_worker_failure():
            nonlocal failed_worker_id, worker_fail_trigger_time
            await asyncio.sleep(1.0)
            if workers:
                target_worker = workers[0]
                failed_worker_id = target_worker.worker_id
                print(f"\n[BENCHMARK] Simulating worker failure for: {failed_worker_id}")
                worker_fail_trigger_time = time.time()
                await target_worker.stop()
                
        asyncio.create_task(simulate_worker_failure())
        results = await asyncio.gather(*tasks)
    
    total_duration = time.perf_counter() - start_time
    print("\n")
    print("[BENCHMARK] All requests dispatched. Waiting for queue to clear and jobs to complete...")
    
    successful_requests = sum(1 for success, _ in results if success)
    
    wait_start = time.time()
    while len(e2e_latencies) < successful_requests and time.time() - wait_start < 25.0:
        await asyncio.sleep(0.5)

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

    p50_gateway = compute_percentile(latencies, 50)
    p95_gateway = compute_percentile(latencies, 95)
    p99_gateway = compute_percentile(latencies, 99)

    p50_e2e = compute_percentile(e2e_latencies, 50)
    p95_e2e = compute_percentile(e2e_latencies, 95)
    p99_e2e = compute_percentile(e2e_latencies, 99)

    detection_time = None
    if worker_fail_detected_time and worker_fail_trigger_time:
        detection_time = worker_fail_detected_time - worker_fail_trigger_time
        
    avg_reassignment_time = None
    if reassigned_durations:
        avg_reassignment_time = sum(reassigned_durations) / len(reassigned_durations)
        
    avg_autoscaling_time = None
    if autoscaling_response_times:
        avg_autoscaling_time = sum(autoscaling_response_times) / len(autoscaling_response_times)

    total_assigned_tasks = sum(w.assigned_tasks for w in workers)
    worker_capacity = 8
    total_capacity = num_workers * worker_capacity
    worker_utilization = min(100.0, (total_assigned_tasks / total_capacity) * 100.0) if total_capacity else 0.0

    coordinator_proc = await asyncio.to_thread(find_coordinator_process, coord_port)
    coordinator_cpu = None
    if coordinator_proc is not None:
        coordinator_cpu = await asyncio.to_thread(measure_cpu_usage, coordinator_proc)

    print("BENCHMARK SUMMARY")

    print(f"Total Jobs Submitted:         {num_jobs}")
    print(f"Successful HTTP Submissions:  {successful_requests}")
    print(f"Total Duration:               {total_duration:.2f} seconds")
    print(f"Submission Throughput (RPS):  {throughput:.2f} req/s")
    
    print(f"Gateway Latency (p50):        {p50_gateway*1000:.2f} ms")
    print(f"Gateway Latency (p95):        {p95_gateway*1000:.2f} ms")
    print(f"Gateway Latency (p99):        {p99_gateway*1000:.2f} ms")
    
    print(f"End-to-End Latency (p50):     {p50_e2e*1000:.2f} ms")
    print(f"End-to-End Latency (p95):     {p95_e2e*1000:.2f} ms")
    print(f"End-to-End Latency (p99):     {p99_e2e*1000:.2f} ms")

    if detection_time is not None:
        print(f"Worker Failure Detection:     {detection_time:.2f} seconds")
    else:
        print(f"Worker Failure Detection:     N/A (No failure detected)")
        
    if avg_reassignment_time is not None:
        print(f"Task Reassignment Latency:   {avg_reassignment_time:.2f} seconds")
    else:
        print(f"Task Reassignment Latency:   N/A (No task reassigned)")
        
    if avg_autoscaling_time is not None:
        print(f"Autoscaling Response Time:    {avg_autoscaling_time:.2f} seconds")
    else:
        print(f"Autoscaling Response Time:    N/A (No scaling triggered)")

    print(f"Worker Utilization:           {worker_utilization:.2f}% of capacity")
    if coordinator_cpu is not None:
        print(f"Coordinator CPU Usage:        {coordinator_cpu:.2f}%")
    else:
        print(f"Coordinator CPU Usage:        N/A")
    print(f"Platform Average Latency:     {platform_avg_latency*1000:.2f} ms")
    print(f"Platform Failed Tasks:        {platform_failures}")

    for w in workers:
        await w.stop()
    event_bus.stop()
    print("[BENCHMARK] completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Platform Benchmarking Suite")
    parser.add_argument("--jobs", type=int, default=100000, help="Number of jobs to simulate")
    parser.add_argument("--workers", type=int, default=50, help="Number of simulated workers")
    parser.add_argument("--clients", type=int, default=1000, help="Number of concurrent async clients")
    args = parser.parse_args()
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(run_benchmark(args.jobs, args.workers, args.clients))
