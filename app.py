from global_metrics import GlobalMetrics
from logger import logger
from topic import Topic
from producer import produce
from consumer import consume
import time

topic = Topic()
global_metrics = GlobalMetrics()

print("\n--- Producer sending messages ---")
produce(topic)

print("\n--- Two Consumers start reading ---")
last_c1, m1 = consume(topic, 0, "Consumer-1")
last_c2, m2 = consume(topic, 1, "Consumer-2")

print("\n--- Producer sends more messages ---")
topic.publish("Mark6")
topic.publish("Mark7")
                                                                      
print("\n--- Producer sending messages ---")
produce(topic)

print("\n--- Two Consumers start reading ---")
last_c1, m1 = consume(topic, 0, "Consumer-1")
last_c2, m2 = consume(topic, 1, "Consumer-2")

print("\n--- Producer sends more messages ---")
topic.publish("Mark6")
topic.publish("Mark7")

print("\n--- Consumers resume from last offsets ---")
last_c1, m1b = consume(topic, last_c1, "Consumer-1")
last_c2, m2b = consume(topic, last_c2, "Consumer-2")

# Aggregate metrics (NOW they all exist)
for m in [m1, m2, m1b, m2b]:
    global_metrics.add_messages(m.total_messages)
    global_metrics.add_errors(m.errors)


# System Summary
logger.info("\n========== SYSTEM STATUS ==========")
logger.info(f"Consumers active: 2")
logger.info(f"Total messages processed: {global_metrics.total_messages}")
logger.info(f"Total errors: {global_metrics.total_errors}")
logger.info(f"Avg throughput: {global_metrics.throughput()} msg/sec")

if global_metrics.total_errors == 0:
    logger.info("Health: OK")
else:
    logger.warning("Health: ISSUES DETECTED")

logger.info("===================================")

# logger.info("Starting Consumer-1...")
# last_c1, m1 = consume(topic, 0, "Consumer-1")
# logger.info("Restarting Consumer-1...")
# last_c1, m1 = consume(topic, last_c1, "Consumer-1")
