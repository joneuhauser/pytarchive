from __future__ import annotations
import os
from pathlib import Path
import humanize
from textwrap import dedent
from pytarchive.service.config import ConfigReader
from pytarchive.service.db import JsonDatabase
from pytarchive.service.library import Library
from pytarchive.service.work_queue import WorkItem, WorkList
from pytarchive.service.is_dir import is_dir_with_timeout


def handle_queue(client_socket, queue: WorkList):
    res = ""
    data = [i for i in queue if i.is_error()]  # Sort by priority
    if len(data) != 0:
        res += "FAILED TASKS:\n"
        res += "\n".join(str(item) for i, item in enumerate(data))
        res += "\n\n"

    res += "RUNNING AND QUEUED TASKS:\n"
    data = sorted(
        [i for i in queue if not i.is_error()], key=lambda i: i.priority
    )  # Sort by priority
    res += "\n".join(str(item) for i, item in enumerate(data))

    client_socket.write(f"Queue state: {len(queue)} tasks\n\n{res}".encode())


def handle_summary(client_socket, queue: WorkList):
    client_socket.write(f"{JsonDatabase().format(Library().get_all_tapes())}".encode())


def handle_abort(args, client_socket, queue: WorkList):
    def process_task(id):
        if isinstance(id, list):
            id = id[0]
        for task in queue:
            if task.format_hash() == id:
                if task.is_error():
                    queue.remove(task)
                    return f"Failed task {id} removed from queue"
                if task.is_running():
                    task.request_abort()
                    return f"Task {id}: abort scheduled, cleaning up..."
                else:
                    queue.remove(task)
                    return f"Task {id} removed from queue"
        return f"Task {id} not found"

    client_socket.write("\n".join(process_task(id) for id in args.task).encode())


def handle_requeue(args, client_socket, queue: WorkList):
    def process_task(id):
        if isinstance(id, list):
            id = id[0]
        for task in queue:
            if task.format_hash() == id:
                if task.is_error():
                    task.error_msg = ""
                    return f"Task {id}: Error state reset. Task was added back into the queue"
                else:
                    return f"Task {id} was already queued"
        return f"Task {id} not found"

    client_socket.write("\n".join(process_task(id) for id in args.failedtask).encode())


def handle_prepare(args, client_socket, queue: WorkList):
    try:
        os.stat(args.folder)
        JsonDatabase().create_entry(args.folder, args.description)
    except (ValueError, FileNotFoundError, PermissionError) as e:
        client_socket.write(f"Error: {e}".encode())
        return

    description = f"Preparing folder: {args.folder} - {args.description}"

    queue.append(
        WorkItem(
            args.priority,
            "prepare",
            [args.folder, args.compress],
            description,
        )
    )
    client_socket.write(b"Preparation queued")


def handle_archive(args, client_socket, queue: WorkList):
    description = f"Archiving folder: {args.folder} to tape {args.tapelabel}"
    # Check that the free size on this tape is enough
    try:
        entry = JsonDatabase()._get_folder(args.folder)
    except ValueError:
        client_socket.write(
            f"Folder not prepared yet. Run pytarchive prepare {args.folder} first.".encode()
        )
        return

    if entry["state"] != "prepared":
        client_socket.write(
            "Folder is in the wrong state according to database. Maybe it's already archived?".encode()
        )
        return
    size = entry["size"]
    on_tape = sum(
        e["size"] for e in JsonDatabase().get_directories_on_tape(args.tapelabel)
    )
    maxsize = ConfigReader().get_maxsize()
    if maxsize < on_tape + size:
        client_socket.write(
            dedent(f"""
                    There is most likely not enough space on that tape. 
                        Required: {humanize.naturalsize(size * 1024, binary=True)}
                        Available: {humanize.naturalsize((maxsize- on_tape) * 1024, binary=True)}.
                    Please select a different tape.""").encode()
        )
        return
    if Library().find_tape(args.tapelabel) is None:
        client_socket.write(
            f"Requested tape not found. Available tapes: {sorted(list(Library().get_available_tapes().values()))}".encode()
        )
        return
    target_filename = args.targetname or JsonDatabase().suggest_ontape_name(entry)

    if target_filename == "" or target_filename in [
        e.get("path_on_tape")
        for e in JsonDatabase().get_directories_on_tape(args.tapelabel)
        if e["state"] == "archived"
    ]:
        client_socket.write(
            f"Directory {target_filename} already exists on tape {args.tapelabel}, choose a different name".encode()
        )
        return
    try:
        os.stat(args.folder)
    except (FileNotFoundError, PermissionError) as e:
        client_socket.write(f"Error: {e}".encode())
        return

    msg = b""
    existing = [
        i for i in queue if i.args[0] == args.folder and i.coroutine == "archive"
    ]
    if len(existing) > 0:
        if existing[0]._running and not existing[0].is_error():
            client_socket.write(b"Folder is already in the process of being archived")
            return
        else:
            queue.remove(existing[0])
            msg = b"Removed existing archiving task for folder\n"

    entry["path_on_tape"] = target_filename + (".tar.gz" if entry["compressed"] else "")
    entry["tape"] = args.tapelabel

    queue.append(
        WorkItem(
            args.priority,
            "archive",
            [args.folder, args.tapelabel, target_filename],
            description,
        )
    )
    client_socket.write(msg + b"Archiving queued")


def handle_restore(args, client_socket, queue: WorkList):
    try:
        entry = JsonDatabase()._get_folder(args.folder)
    except ValueError:
        client_socket.write(
            f"Folder not prepared yet. Run pytarchive prepare {args.folder} first.".encode()
        )
        return

    if entry["state"] != "archived":
        client_socket.write("Folder not archived yet.".encode())
        return

    description = f"Restoring folder: {args.folder} to {args.restore_path}"
    restore_path = Path(args.restore_path)
    # If the restore path exists, it needs to be empty.
    if restore_path.is_dir():
        if not len(list(restore_path.iterdir())) == 0:
            client_socket.write("Directory to restore to is not empty".encode())
            return
        restore_path.rmdir()

    subfolder: str = args.subfolder
    if subfolder != "":
        subfolder.removeprefix("/")
        if not subfolder.endswith("/"):
            subfolder = subfolder + "/"

    queue.append(
        WorkItem(
            args.priority,
            "restore",
            [args.folder, args.restore_path, subfolder],
            description,
        )
    )
    client_socket.write(b"Restoring queued")


def handle_explore(args, client_socket, queue: WorkList):
    description = f"Exploring tape: {args.tapelabel}"
    if Library().find_tape(args.tapelabel) is None:
        client_socket.write(
            f"Requested tape not found. Available tapes: {sorted(list(Library().get_available_tapes().values()))}".encode()
        )
        return
    queue.append(
        WorkItem(
            args.priority,
            "explore",
            [args.tapelabel, args.time, args.email],
            description,
        )
    )
    client_socket.write(b"Exploring queued")


def handle_inventory(args, client_socket, queue: WorkList):
    for folder in args.folders:
        description = f"Taking inventory of: {folder}"
        queue.append(
            WorkItem(
                args.priority,
                "inventory",
                [folder],
                description,
            )
        )
    client_socket.write(b"Inventory queued")


def handle_deletable(args, client_socket):
    result = []
    result_none = []
    for folder in JsonDatabase().get_entries_by_state("archived"):
        dir = folder["original_directory"]
        if isinstance(args.ignore, list) and any(
            dir.startswith(i) for i in args.ignore
        ):
            continue
        res = is_dir_with_timeout(Path(folder["original_directory"]), timeout=0.1)
        if res is None:
            result_none.append(f"{dir} ({folder['description']})")
        if res:
            result.append(
                f"{dir} ({folder['description']}, archived on {folder['tape']})"
            )
    if len(result) == 0 and len(result_none) == 0:
        client_socket.write(b"Nothing to delete")

    else:
        res = b""
        if len(result_none) > 0:
            res = (
                b"\033[33mThe following directories could not be queried:\n\t"
                + "\n\t".join(result_none).encode()
                + b"\033[0m"
            )
        res += (
            b"\n\nThe following directories can be deleted:\n\t"
            + "\n\t".join(result).encode()
        )
        client_socket.write(res)
