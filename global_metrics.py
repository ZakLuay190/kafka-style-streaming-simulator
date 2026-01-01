import time

class GlobalMetrics:
    def __init__(self):
        self.total_messages = 0
        self.total_errors = 0
        self.start_time = time.time()

    def add_messages(self, count):
        self.total_messages += count

    def add_errors(self, count):
        self.total_errors += count

    def throughput(self):
        runtime = time.time() - self.start_time
        if runtime == 0:
            return 0
        return round(self.total_messages / runtime, 2)
