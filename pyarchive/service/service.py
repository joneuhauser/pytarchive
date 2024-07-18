from __future__ import annotations
import argparse
import asyncio
from contextlib import redirect_stderr
import io
import os
import sys
import fcntl
import signal
import logging
import traceback
from pyarchive.service.db import JsonDatabase
from pyarchive.service.library import Library
import pyarchive.service.tasks as tasks
from systemd.journal import JournalHandler


# Configuration
PID_FILE = "/tmp/pyarchive_service.pid"
SOCKET_FILE = "/tmp/pyarchive_service.sock"
DEFAULT_PRIORITY = 100
EXPLORE_PRIORITY = 20

# Logging setup
logger = logging.getLogger(__name__)
journal_handler = JournalHandler(SYSLOG_IDENTIFIER="pyarchive")
journal_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(journal_handler)
logger.setLevel(logging.INFO)


def recv_all(sock):
    """Helper function to receive all data from a socket."""
    data = b""
    while True:
        part = sock.recv(1024)
        data += part
        if len(part) < 1024:
            break
    return data


def handle_command(command, client_socket, queue: WorkList):
    f = io.StringIO()

    parser = argparse.ArgumentParser(prog="pyarchive")
    subparsers = parser.add_subparsers(dest="command")

    # Synchronous commands
    subparsers.add_parser("queue")
    subparsers.add_parser("summary")
    subparsers.add_parser("todo")

    # Asynchronous commands
    parser_prepare = subparsers.add_parser("prepare")
    parser_prepare.add_argument("folder")
    parser_prepare.add_argument("description")
    parser_prepare.add_argument("--priority", type=int, default=0)

    parser_archive = subparsers.add_parser("archive")
    parser_archive.add_argument("folder")
    parser_archive.add_argument("tapenumber")
    parser_archive.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_restore = subparsers.add_parser("restore")
    parser_restore.add_argument("folder")
    parser_restore.add_argument("restore_path")
    parser_restore.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_explore = subparsers.add_parser("explore")
    parser_explore.add_argument("tapenumber")
    parser_explore.add_argument("--priority", type=int, default=EXPLORE_PRIORITY)

    try:
        f = io.StringIO()
        with redirect_stderr(f):
            args = parser.parse_args(command.split())
    except SystemExit:
        s = f.getvalue()
        client_socket.write(s.encode())
        return

    if args.command == "queue":
        data = sorted(queue, key=lambda i: i[0])  # Sort by priority
        top = queue.current
        res = "\n".join(
            f"{i}: {item[2]} ({item[0]}) {'[running]' if item == top else ''}"
            for i, item in enumerate(data)
        )

        client_socket.write(f"Queue state: {len(queue)} tasks\n{res}".encode())
    elif args.command == "summary":
        res = f"{Library().get_all_tapes()}"
        res = res.encode()
        client_socket.write(
            f"{JsonDatabase().format(Library().get_all_tapes())}".encode()
        )
    elif args.command == "todo":
        client_socket.write("TODO list:\n".encode())
    elif args.command == "prepare":
        description = f"Preparing folder: {args.folder} - {args.description}"
        queue.append((args.priority, tasks.get_size(args.folder), description))
    elif args.command == "archive":
        description = f"Archiving folder: {args.folder} to tape {args.tapenumber}"
        queue.append(
            (args.priority, tasks.archive(args.folder, args.tapenumber), description)
        )
        client_socket.write(b"Archiving queued")
    elif args.command == "restore":
        description = f"Restoring folder: {args.folder} to {args.restore_path}"
        queue.append(
            (args.priority, tasks.restore(args.folder, args.restore_path), description)
        )
        client_socket.write(b"Restoring queued")
    elif args.command == "explore":
        description = f"Exploring tape: {args.tapenumber}"
        queue.append((args.priority, tasks.explore(args.tapenumber), description))
        client_socket.write(b"Exploring queued")
    else:
        client_socket.write(parser.format_help().encode())


class WorkList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current = None

    def get_top(self):
        return sorted(self, key=lambda i: i[0])[0]


def handle_client(client_socket, queue):
    with client_socket:
        command = recv_all(client_socket).decode()
        if command:
            logger.info(f"Received command: {command}")
            try:
                handle_command(command, client_socket, queue)
            except Exception as e:
                client_socket.sendall(f"Error: {e}\n{traceback.format_exc()}".encode())


def handle_signal(sig, frame):
    """Handle termination signals to gracefully shutdown the service."""
    logger.info("Shutting down service")
    os.unlink(PID_FILE)
    os.unlink(SOCKET_FILE)
    sys.exit(0)


class PyArchiveServer(asyncio.Protocol):
    def __init__(self, queue):
        self.queue = queue

    def connection_made(self, transport):
        self.transport = transport
        self.addr = transport.get_extra_info("peername")

    def data_received(self, data):
        command = data.decode().strip()
        logger.info(f"Received command: {command}")
        handle_command(command, self.transport, self.queue)


async def worker(queue):
    while True:
        if not len(queue):
            await asyncio.sleep(1)
            continue

        item = queue.get_top()
        queue.current = item
        _, coroutine, description = item
        logger.info(f"starting {description}")
        try:
            result = await coroutine
            logger.info(f"{description} - Result: {result}\n")

        except Exception as e:
            logger.info(f"{description} - Error: {e}\n")
        queue.remove(item)


async def main():
    if os.path.isfile(PID_FILE):
        logger.error("Service is already running.")
        sys.exit(1)

    with open(PID_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(str(os.getpid()))

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if os.path.exists(SOCKET_FILE):
        os.remove(SOCKET_FILE)
    queue = WorkList()
    asyncio.create_task(worker(queue))

    server = await asyncio.get_event_loop().create_unix_server(
        lambda: PyArchiveServer(queue), SOCKET_FILE
    )
    logger.info(f"Service started, waiting for commands on {SOCKET_FILE}")

    try:
        async with server:
            await server.serve_forever()
    except Exception as e:
        logger.error(f"Service encountered an error: {e}")
    finally:
        os.remove(PID_FILE)
        os.remove(SOCKET_FILE)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(main())
