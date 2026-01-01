import time
import random
from logger import logger, dlq_logger
from metrics import Metrics

# Retry config
MAX_RETRIES = 3
RETRY_DELAY = 1   # seconds


def consume(topic, start_index=0, consumer_name="consumer"):
    index = start_index
    metrics = Metrics()

    logger.info(f"{consumer_name} starting from offset {index}")

    while True:
        msgs = topic.read_from(index)

        # No new messages
        if not msgs:
            logger.warning(f"{consumer_name}: idle — no new messages")
            break

        for msg in msgs:
            retries = 0

            while True:
                try:
                    # 💥 Random simulated crash (20% chance)
                    if random.random() < 0.2:
                        raise Exception("Simulated consumer crash!")

                    # 💥 FORCE DLQ TEST for Mark4
                    # (Only for learning. Remove/comment later.)
                    if msg == "Mark4":
                        raise ValueError("Permanent bad message - cannot process")

                    # 🚫 Handle empty / corrupted message
                    if msg is None:
                        raise ValueError("Empty message received")

                    # ✅ Process the message
                    metrics.inc_messages()
                    logger.info(f"{consumer_name} processed {msg}")

                    # Advance offset ONLY AFTER success
                    index += 1
                    time.sleep(1)

                    break  # success → leave retry loop

                except Exception as e:
                    retries += 1
                    metrics.inc_errors()

                    logger.error(
                        f"{consumer_name} failed on {msg} (attempt {retries}): {e}"
                    )

                    # ❌ If retries exceeded → send to DLQ
                    if retries > MAX_RETRIES:
                        dlq_logger.error(
                            f"{consumer_name} moved {msg} to DLQ — reason: {e}"
                        )

                        logger.error(
                            f"{consumer_name} skipping {msg} after retries — advancing offset"
                        )

                        # Skip this message — do NOT stop the consumer
                        index += 1
                        break

                    # ⏳ backoff delay
                    time.sleep(RETRY_DELAY)

    # 🧮 End-of-run stats
    logger.info(f"{consumer_name} total messages: {metrics.total_messages}")
    logger.info(f"{consumer_name} errors: {metrics.errors}")
    logger.info(f"{consumer_name} throughput msgs/sec: {metrics.throughput()}")

    return index, metrics
