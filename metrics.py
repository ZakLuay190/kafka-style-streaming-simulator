import time

class Metrics:
    def __init__(self):
        self.total_messages = 0
        self.errors = 0
        self.start_time = time.time()

    def inc_messages(self):
        self.total_messages += 1

    def inc_errors(self):
        self.errors += 1

    def throughput(self):
        runtime = time.time() - self.start_time
        if runtime == 0:
            return 0
        return round(self.total_messages / runtime, 2)
