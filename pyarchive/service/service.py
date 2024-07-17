import os
import sys
import fcntl
import signal
import socket
import logging
from systemd.journal import JournalHandler

# Configuration
PID_FILE = "/tmp/pyarchive_service.pid"
SOCKET_FILE = "/tmp/pyarchive_service.sock"

# Logging setup
logger = logging.getLogger(__name__)
journal_handler = JournalHandler()
journal_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(journal_handler)
logger.setLevel(logging.INFO)


def handle_signal(sig, frame):
    """Handle termination signals to gracefully shutdown the service."""
    logger.info("Shutting down service")
    os.unlink(PID_FILE)
    os.unlink(SOCKET_FILE)
    sys.exit(0)


def recv_all(sock):
    """Helper function to receive all data from a socket."""
    data = b""
    while True:
        part = sock.recv(1024)
        data += part
        if len(part) < 1024:
            break
    return data


def main():
    # Check for an existing PID file to ensure the service is not already running
    if os.path.isfile(PID_FILE):
        logger.error("Service is already running.")
        sys.exit(1)

    # Create a PID file and lock it
    with open(PID_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(str(os.getpid()))

    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Setup a Unix domain socket for inter-process communication
    if os.path.exists(SOCKET_FILE):
        os.remove(SOCKET_FILE)

    server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_socket.bind(SOCKET_FILE)
    os.chmod(SOCKET_FILE, 0o600)
    server_socket.listen(1)

    logger.info("Service started, waiting for commands...")

    try:
        while True:
            client_socket, _ = server_socket.accept()
            with client_socket:
                command = recv_all(client_socket).decode()
                if command:
                    logger.info(f"Received command: {command}")
                    # Here we can handle various commands, for simplicity we'll just log them
                    client_socket.sendall(b"Command received\n")
    except Exception as e:
        logger.error(f"Service encountered an error: {e}")
    finally:
        os.remove(PID_FILE)
        os.remove(SOCKET_FILE)


if __name__ == "__main__":
    main()
