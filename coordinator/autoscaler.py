import subprocess
import os
import sys

class Autoscaler:
    def __init__(self, min_workers=0, max_workers=5):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.active_subprocesses = {} 

    def clean_dead_processes(self):
        for worker_id, proc in list(self.active_subprocesses.items()):
            if proc.poll() is not None:
                print(f"[AUTOSCALER] Dynamic worker '{worker_id}' terminated. Removing from tracking.")
                self.active_subprocesses.pop(worker_id)

    def scale_up(self):
        self.clean_dead_processes()
        if len(self.active_subprocesses) >= self.max_workers:
            print(f"[AUTOSCALER] Max workers boundary reached ({self.max_workers}). Cannot scale up.")
            return
        
     
        worker_id = f"worker-dynamic-{len(self.active_subprocesses) + 1}"
        print(f"[AUTOSCALER] Scaling up: Starting dynamic subprocess '{worker_id}'...")
        
      
        cmd = [sys.executable, "-m", "workers.worker", worker_id]
        cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        proc = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.active_subprocesses[worker_id] = proc

    def scale_down(self):
        self.clean_dead_processes()
        if len(self.active_subprocesses) <= self.min_workers:
            return
            
   
        worker_id = list(self.active_subprocesses.keys())[-1]
        print(f"[AUTOSCALER] Scaling down: Stopping dynamic subprocess '{worker_id}'...")
        
     
        proc = self.active_subprocesses.pop(worker_id)
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
