from __future__ import annotations
import argparse
import asyncio
from contextlib import redirect_stderr
from dataclasses import dataclass, field
import datetime
import io
import os
import random
import sys
import fcntl
import humanize
import signal
from textwrap import dedent
import traceback
from typing import Coroutine, Optional
from pyarchive.service.config import ConfigReader
from pyarchive.service.db import JsonDatabase
from pyarchive.service.library import Library
import pyarchive.service.tasks as tasks
from pyarchive.service.log import logger

# Configuration
PID_FILE = "/tmp/pyarchive_service.pid"
SOCKET_FILE = "/tmp/pyarchive_service.sock"
DEFAULT_PRIORITY = 100
EXPLORE_PRIORITY = 20


def recv_all(sock):
    """Helper function to receive all data from a socket."""
    data = b""
    while True:
        part = sock.recv(1024)
        data += part
        if len(part) < 1024:
            break
    return data


@dataclass
class WorkItem:
    priority: int
    coroutine: Coroutine
    description: str
    _progress: Optional[str] = field(init=False)
    _abort_handle: asyncio.Event = field(init=False)
    _created: datetime.datetime = field(init=False)
    _hashseed: float = field(init=False)
    _running: bool = field(init=False)

    def __post_init__(self):
        self._abort_handle = asyncio.Event()
        self._created = datetime.datetime.now()
        self._hashseed = random.random()
        self._progress = None
        self._running = False

    def update_progress(self, data: str):
        self._progress = data

    def is_running(self) -> bool:
        return self._running

    def request_abort(self) -> bool:
        self._abort_handle.set()

    async def run(self) -> bool:
        self._running = True
        await self.coroutine(self.update_progress, self._abort_handle)

    def __hash__(self) -> int:
        return hash(self._hashseed)

    def format_hash(self) -> str:
        h = hash(self) + sys.maxsize + 1
        return f"{h:#0{10}x}"[2:10]

    def __str__(self) -> str:
        ret = f"[{self.format_hash()}] {self.priority} - {self.description}"
        if self.is_running():
            ret += f" [{self._progress}]"
        return ret


def handle_command(command: bytes, client_socket, queue: WorkList[WorkItem]):
    f = io.StringIO()

    parser = argparse.ArgumentParser(prog="pyarchive")
    subparsers = parser.add_subparsers(dest="command")

    # Synchronous commands
    subparsers.add_parser("queue")
    subparsers.add_parser("summary")
    subparsers.add_parser("todo")

    parser_abort = subparsers.add_parser("abort")
    parser_abort.add_argument("task")

    # Asynchronous commands
    parser_prepare = subparsers.add_parser("prepare")
    parser_prepare.add_argument("folder")
    parser_prepare.add_argument("description")
    parser_prepare.add_argument("--priority", type=int, default=0)

    parser_archive = subparsers.add_parser("archive")
    parser_archive.add_argument("folder")
    parser_archive.add_argument("tapelabel")
    parser_archive.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_restore = subparsers.add_parser("restore")
    parser_restore.add_argument("folder")
    parser_restore.add_argument("restore_path")
    parser_restore.add_argument("--priority", type=int, default=DEFAULT_PRIORITY)

    parser_explore = subparsers.add_parser("explore")
    parser_explore.add_argument("tapelabel")
    parser_explore.add_argument("-t", "--time", type=int, default=600)
    parser_explore.add_argument("--priority", type=int, default=EXPLORE_PRIORITY)

    try:
        f = io.StringIO()
        with redirect_stderr(f):
            print("COMMAND", command, type(command))
            print(command.split(b"\00"))

            displ = [i.decode() for i in command.split(b"\00")]
            args = parser.parse_args(displ)
    except SystemExit:
        s = f.getvalue()
        client_socket.write(s.encode())
        return

    if args.command == "queue":
        data = sorted(queue, key=lambda i: i.priority)  # Sort by priority
        res = "\n".join(str(item) for i, item in enumerate(data))

        client_socket.write(f"Queue state: {len(queue)} tasks\n{res}".encode())
    elif args.command == "summary":
        res = f"{Library().get_all_tapes()}"
        res = res.encode()
        client_socket.write(
            f"{JsonDatabase().format(Library().get_all_tapes())}".encode()
        )
    elif args.command == "todo":
        client_socket.write("TODO list:\n".encode())
    elif args.command == "abort":
        for task in queue:
            if task.format_hash() == args.task:
                if task.is_running():
                    task.request_abort()
                    client_socket.write(b"Task abort scheduled, cleaning up...")
                else:
                    queue.remove(task)
                    client_socket.write(b"Task removed from queue")
                return
        client_socket.write(b"Task ID not found")
    elif args.command == "prepare":
        try:
            os.stat(args.folder)
            entry = JsonDatabase().create_entry(args.folder, args.description)
        except (ValueError, FileNotFoundError, PermissionError) as e:
            client_socket.write(f"Error: {e}".encode())
            return

        description = f"Preparing folder: {args.folder} - {args.description}"

        queue.append(
            WorkItem(
                args.priority,
                lambda progress, abort: tasks.get_size(entry, progress, abort),
                description,
            )
        )
        client_socket.write(b"Preparation queued")
    elif args.command == "archive":
        description = f"Archiving folder: {args.folder} to tape {args.tapenumber}"
        # Check that the free size on this tape is enough
        entry = JsonDatabase()._get_folder(args.folder)
        size = entry["size"] * 1024
        on_tape = sum(
            e["size"] for e in JsonDatabase().get_directories_on_tape(args.tapenumber)
        )
        maxsize = ConfigReader().get_maxsize()
        if maxsize < on_tape + size:
            client_socket.write(
                dedent(f"""
                                        There is most likely not enough space on that tape. 
                                        Required: {humanize.naturalsize(size * 1024, binary=True)}
                                        Available: {humanize.naturalsize((maxsize- on_tape) * 1024, binary=True)}.
                                        Please select a different tape""").encode()
            )

        queue.append(
            WorkItem(
                args.priority,
                lambda progress, abort: tasks.archive(
                    args.folder, args.tapenumber, progress, abort
                ),
                description,
            )
        )
        client_socket.write(b"Archiving queued")
    elif args.command == "restore":
        description = f"Restoring folder: {args.folder} to {args.restore_path}"
        queue.append(
            [
                args.priority,
                lambda progress, abort: tasks.restore(
                    args.folder, args.restore_path, progress
                ),
                description,
            ]
        )
        client_socket.write(b"Restoring queued")
    elif args.command == "explore":
        description = f"Exploring tape: {args.tapelabel}"
        if Library().find_tape(args.tapelabel) is None:
            client_socket.write(
                f"Requested tape not found. Available tapes: {sorted(list(Library().get_available_tapes().values()))}".encode()
            )
            return

        queue.append(
            WorkItem(
                args.priority,
                lambda progress, abort: tasks.explore(
                    args.tapenumber, args.time, progress, abort
                ),
                description,
            )
        )
        client_socket.write(b"Exploring queued")
    else:
        client_socket.write(parser.format_help().encode())


class WorkList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_top(self) -> WorkItem:
        return sorted(self, key=lambda i: i.priority)[0]


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
        command = data.strip()
        displ = [i.decode() for i in command.split(b"\00")]
        logger.info(f"Received command: {displ}")
        handle_command(command, self.transport, self.queue)


async def worker(queue: WorkList[WorkItem]):
    while True:
        if not len(queue):
            await asyncio.sleep(1)
            continue

        item = queue.get_top()
        description = item.description

        logger.info(f"starting {description}")
        try:
            result = await item.run()
            logger.info(f"{description} - Result: {result}\n")

        except Exception as e:
            logger.info(f"{description} - Error: {e} {traceback.format_exc()}\n")
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
