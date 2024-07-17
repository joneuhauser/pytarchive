from threading import Lock
from systemd.journal import JournalHandler
import logging

logger = logging.getLogger(__name__)
journal_handler = JournalHandler()
journal_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(journal_handler)
logger.setLevel(logging.INFO)

operations_lock = Lock()
running_operations = {}


async def main():
    pass


def run_operation(command, description, function):
    """Run the requested operation."""
    command_type = command

    with operations_lock:
        if command_type in running_operations:
            response = f"Operation '{command_type}' is already running"
            logger.info(response)
            return response

        running_operations[command_type] = description
        logger.info(f"Started operation: {command_type}")

    try:
        return function()
    except Exception as e:
        response = f"Error during operation '{command_type}': {e}"
        logger.error(response)
    finally:
        with operations_lock:
            del running_operations[command_type]
            logger.info(f"Finished operation: {command_type}")

    return response
