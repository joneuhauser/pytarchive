from logging.config import fileConfig
import logging.handlers
from pathlib import Path


config = "/etc/pytarchive/logging.conf"
if Path(config).is_file():
    fileConfig(config)
    logger = logging.getLogger()
else:
    print("No log config file")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    smtp_handler = logging.handlers.RotatingFileHandler(
        filename="/var/log/pytarchive.log", backupCount=3, maxBytes=100000
    )
    logger.addHandler(smtp_handler)
    smtp_handler.setLevel(logging.INFO)
