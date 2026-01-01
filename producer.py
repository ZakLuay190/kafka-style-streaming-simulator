import os, sys
sys.path.append(os.path.dirname(__file__))
import time
from logger import logger
def produce(topic):
    messages = ["Mark1", "Mark2", "Mark3", "Mark4", "Mark5"]
    for msg in messages:
        print(f"Producer sent: {msg}")
        topic.publish(msg)
        time.sleep(1)
