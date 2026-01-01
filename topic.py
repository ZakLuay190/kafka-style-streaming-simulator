from collections import deque

class Topic:
    def __init__(self):
        self.messages = deque()

    def publish(self, msg):
        self.messages.append(msg)

    def read_from(self, index):
        return list(self.messages)[index:]
