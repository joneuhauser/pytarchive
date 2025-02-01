from __future__ import annotations
import argparse
import asyncio
from contextlib import redirect_stderr, redirect_stdout
import io
import os
import sys
import fcntl
import signal
from pytarchive.service.config import ConfigReader
from pytarchive.service.handlers import (
    handle_abort,
    handle_archive,
    handle_deletable,
    handle_explore,
    handle_prepare,
    handle_queue,
    handle_requeue,
    handle_restore,
    handle_summary,
    handle_inventory,
)
from pytarchive.service.log import logger
from pytarchive.service.work_queue import WorkList

# Configuration
PID_FILE = "/tmp/pytarchive_service.pid"
SOCKET_FILE = "/tmp/pytarchive_service.sock"
DEFAULT_PRIORITY = 100
EXPLORE_PRIORITY = 20
INVENTORY_PRIORITY = 200


def recv_all(sock):
    """Helper function to receive all data from a socket."""
    data = b""
    while True:
        part = sock.recv(1024)
        data += part
        if len(part) < 1024:
            break
    return data


def handle_command(command: bytes, client_socket, queue: WorkList):
    f = io.StringIO()

    parser = argparse.ArgumentParser(prog="pytarchive")
    subparsers = parser.add_subparsers(dest="command")

    # Synchronous commands
    subparsers.add_parser("queue", help="Lists all currently running and failed tasks")
    subparsers.add_parser(
        "summary",
        help="Shows a list of all known tapes with the directories archived on them",
    )

    parser_abort = subparsers.add_parser(
        "abort",
        help="Aborts a task using its task id, permanently removing it from the queue",
    )
    parser_abort.add_argument("task")

    parser_abort = subparsers.add_parser(
        "requeue", help="Restarts a failed task using its task id"
    )
    parser_abort.add_argument("failedtask")

    # Asynchronous commands
    parser_prepare = subparsers.add_parser(
        "prepare", help="Prepares a directory (computes the directories' size)"
    )
    parser_prepare.add_argument("folder")
    parser_prepare.add_argument("description")
    parser_prepare.add_argument("--priority", type=int, default=0)

    parser_archive = subparsers.add_parser(
        "archive", help="Archives a prepared directory."
    )
    parser_archive.add_argument("folder")
    parser_archive.add_argument("tapelabel")
    parser_archive.add_argument("-t", "--targetname", default=None)
    parser_archive.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_restore = subparsers.add_parser(
        "restore", help="Restores an archived directory to another location."
    )
    parser_restore.add_argument("folder")
    parser_restore.add_argument("restore_path")
    parser_restore.add_argument("-s", "--subfolder", default="")
    parser_restore.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_explore = subparsers.add_parser(
        "explore",
        help="Mounts a tape for a specified time so you can explore its contents.",
    )
    parser_explore.add_argument("tapelabel")
    parser_explore.add_argument("-t", "--time", type=int, default=600)
    parser_explore.add_argument("--priority", type=int, default=EXPLORE_PRIORITY)

    parser_inventory = subparsers.add_parser(
        "inventory",
        help="Reports a summary of the subfolders of a given directory. Without argument, the source_folders key in ine [General] section of the config file is used.",
    )
    parser_inventory.add_argument(
        "folders", nargs="*", default=ConfigReader().get_source_folders()
    )
    parser_inventory.add_argument("--priority", type=int, default=INVENTORY_PRIORITY)

    parser_deleteable = subparsers.add_parser(
        "deleteable",
        help="Report directories that are archived but still present in the original location",
    )
    parser_deleteable.add_argument("--ignore", nargs="*")
    try:
        f = io.StringIO()
        f2 = io.StringIO()
        with redirect_stderr(f):
            with redirect_stdout(f2):
                displ = [i.decode() for i in command.split(b"\00")]
                args = parser.parse_args(displ)
    except SystemExit:
        s = f.getvalue()
        s2 = f2.getvalue()

        print(f"Exited with {s} and {s2}")
        if len(s) > 0:
            client_socket.write(s.encode())
        else:
            client_socket.write(s2.encode())
        return

    logger.debug(args.command)

    if args.command == "queue":
        handle_queue(client_socket, queue)
    elif args.command == "summary":
        handle_summary(client_socket, queue)
    elif args.command == "abort":
        handle_abort(args, client_socket, queue)

    elif args.command == "requeue":
        handle_requeue(args, client_socket, queue)
    elif args.command == "prepare":
        handle_prepare(args, client_socket, queue)
    elif args.command == "archive":
        handle_archive(args, client_socket, queue)
    elif args.command == "restore":
        handle_restore(args, client_socket, queue)
    elif args.command == "explore":
        handle_explore(args, client_socket, queue)
    elif args.command == "inventory":
        handle_inventory(args, client_socket, queue)
    elif args.command == "deleteable":
        handle_deletable(args, client_socket)
    else:
        client_socket.write(parser.format_help().encode())


def handle_signal(sig, frame):
    """Handle termination signals to gracefully shutdown the service."""
    logger.info("Shutting down service")
    os.unlink(PID_FILE)
    os.unlink(SOCKET_FILE)
    sys.exit(0)


class pytarchiveServer(asyncio.Protocol):
    def __init__(self, queue):
        self.queue = queue

    def connection_made(self, transport):
        self.transport = transport
        self.addr = transport.get_extra_info("peername")

    def data_received(self, data):
        command = data.strip()
        displ = [i.decode() for i in command.split(b"\00")]
        logger.info(f"Received command: {displ}")
        try:
            handle_command(command, self.transport, self.queue)
        except Exception as e:
            logger.log("Error on data receive", e)


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
    asyncio.create_task(queue.worker())

    server = await asyncio.get_event_loop().create_unix_server(
        lambda: pytarchiveServer(queue), SOCKET_FILE
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
