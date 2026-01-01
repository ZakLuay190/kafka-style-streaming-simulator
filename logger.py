import logging

# ---------- MAIN SYSTEM LOGGER ----------
logger = logging.getLogger("streaming-system")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

file_handler = logging.FileHandler("system.log")
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ---------- DEAD LETTER QUEUE LOGGER ----------
dlq_logger = logging.getLogger("dead-letter-queue")
dlq_logger.setLevel(logging.ERROR)

dlq_handler = logging.FileHandler("dead_letter.log")
dlq_handler.setFormatter(formatter)

if not dlq_logger.handlers:
    dlq_logger.addHandler(dlq_handler)
