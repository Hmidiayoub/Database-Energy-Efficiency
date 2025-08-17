import psutil
import time

class Tracker:
    def __init__(self):
        self.cpu_start = None
        self.cpu_end = None
        self.ram_start = None
        self.ram_end = None
        self.disk_start = None
        self.disk_end = None
        self.start_time = None
        self.end_time = None

    def start(self):
        self.cpu_start = psutil.cpu_times()
        self.ram_start = psutil.virtual_memory().used
        self.disk_start = psutil.disk_io_counters()
        self.start_time = time.time()

    def stop(self):
        self.cpu_end = psutil.cpu_times()
        self.ram_end = psutil.virtual_memory().used
        self.disk_end = psutil.disk_io_counters()
        self.end_time = time.time()

    def cpu_consumption(self):
        # Example: user time delta
        return self.cpu_end.user - self.cpu_start.user

    def ram_consumption(self):
        # Example: RAM used difference in MB
        return (self.ram_end - self.ram_start) / (1024 * 1024)

    def disk_consumption(self):
        # Example: sum of read and write bytes delta in MB
        read_bytes = self.disk_end.read_bytes - self.disk_start.read_bytes
        write_bytes = self.disk_end.write_bytes - self.disk_start.write_bytes
        return (read_bytes + write_bytes) / (1024 * 1024)

    @property
    def duration(self):
        return self.end_time - self.start_time