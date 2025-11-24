import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str):
    level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Tentukan lokasi folder logs (relatif terhadap file ini)
    LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(LOG_DIR, exist_ok=True)

    log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

    # File log utama
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "etl_cron.log"),
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )

    # Handler ke file dan ke console
    file_handler.setFormatter(logging.Formatter(log_format))
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Hindari duplikasi handler
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    logger.propagate = False
    return logger
