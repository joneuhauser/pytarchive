from logging.config import fileConfig
import logging.handlers
from pyarchive.service.config import ConfigReader

conf = ConfigReader().get_logging_config()

if conf is not None:
    fileConfig(conf)
    logger = logging.getLogger()
else:
    print("No log config file")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    smtp_handler = logging.handlers.RotatingFileHandler(
        filename="/var/log/pyarchive.log", backupCount=3, maxBytes=100000
    )
    logger.addHandler(smtp_handler)
    smtp_handler.setLevel(logging.INFO)
