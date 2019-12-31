import psutil
import subprocess
import time


class ProcessTimer:
    def __init__(self, command):
        self.command = command
        self.execution_state = False

    def execute(self):
        self.rss_memory_list = []
        self.cpu_percent_list = []
        self.time_list = []

        self.t1 = None
        self.t0 = time.time()
        self.p = subprocess.Popen(self.command, shell=False)
        self.execution_state = True
        self.rb1 = None
        self.rb0 = psutil.disk_io_counters().read_bytes

    def poll(self):
        if not self.check_execution_state():
            return False

        self.t1 = time.time()

        try:
            pp = psutil.Process(self.p.pid)

            self.rb1 = psutil.disk_io_counters().read_bytes
            cpu_percent_now = psutil.cpu_percent()
            time.sleep(0.1)
            cpu_percent_now = psutil.cpu_percent()

            # obtain a list of the subprocess and all its descendants
            descendants = list(pp.children(recursive=True))
            descendants = descendants + [pp]

            rss_memory = 0

            # calculate and sum up the memory of the subprocess and all its descendants
            for descendant in descendants:
                try:
                    mem_info = descendant.memory_info()
                    rss_memory += mem_info[0]
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    # sometimes a subprocess descendant will have terminated between the time
                    # we obtain a list of descendants, and the time we actually poll this
                    # descendant's memory usage.
                    pass
            self.cpu_percent_list.append(cpu_percent_now)
            self.rss_memory_list.append(rss_memory)
            self.time_list.append(int((int((self.t1-self.t0)*10)/10.0)*2)/2.0)

        except psutil.NoSuchProcess:
            return self.check_execution_state()

        return self.check_execution_state()

    def is_running(self):
        return psutil.pid_exists(self.p.pid) and self.p.poll() == None

    def check_execution_state(self):
        if not self.execution_state:
            return False
        if self.is_running():
            return True
        self.executation_state = False
        self.t1 = time.time()
        return False

    def close(self, kill=False):

        try:
            pp = psutil.Process(self.p.pid)
            if kill:
                pp.kill()
            else:
                pp.terminate()
        except psutil.NoSuchProcess:
            pass
