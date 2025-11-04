import logging
import os
from logging.handlers import RotatingFileHandler

def get_logger(name: str):
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    os.makedirs("logs", exist_ok=True)

    log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    file_handler = RotatingFileHandler(
        "logs/etl_cron.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )

    file_handler.setFormatter(logging.Formatter(log_format))
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger