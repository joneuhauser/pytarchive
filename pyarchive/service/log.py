import logging
from systemd.journal import JournalHandler


logger = logging.getLogger(__name__)
journal_handler = JournalHandler(SYSLOG_IDENTIFIER="pyarchive")
journal_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(journal_handler)
logger.setLevel(logging.INFO)
