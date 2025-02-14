import asyncio
from dataclasses import dataclass
import datetime
from itertools import groupby
import os
from pathlib import Path
import socket
from typing import List, Optional

import humanize
from pytarchive.service.config import ConfigReader
from pytarchive.service.db import JsonDatabase
from pytarchive.service.library import Library
from pytarchive.service.command_runner import run_command
from pytarchive.service.log import logger
from pytarchive.service.utils import send_to_addr, send_to_logging_addr


async def _get_size(folder: str, abort: asyncio.Event, inodes=False):
    stdout, _ = await run_command(
        *(["du", "-s"] + (["--inodes"] if inodes else [])),
        folder,
        abort_event=abort,
        preserve_stderr=True,
        preserve_stdout=True,
    )
    if abort.is_set():
        return 0

    return int(stdout.split()[0])


async def prepare(folder: str, compress, progress, abort: asyncio.Event):
    entry = JsonDatabase()._get_folder(folder)
    progress(f"Querying size of folder {entry['original_directory']}")

    size = await _get_size(entry["original_directory"], abort)

    if abort.is_set():
        JsonDatabase().data.remove(entry)
        return

    # Also get the number of inodes
    inodes = await _get_size(entry["original_directory"], abort, True)
    if inodes > 5e5 or compress:
        # Tapes don't like a lot of small files, so we have to compress
        compress = True

    if abort.is_set():
        JsonDatabase().data.remove(entry)
        return

    if compress:
        base = Path(entry["original_directory"])
        compressed_path = str(base.parent / base.with_suffix(".tar.gz"))
        await run_command("tar", "czf", compressed_path, str(base), abort_event=abort)
        size = await _get_size(compressed_path, abort)

    if abort.is_set():
        JsonDatabase().data.remove(entry)
        return

    assert entry in JsonDatabase().data

    JsonDatabase().set_prepared(entry, size, compress)

    return size


async def check_folders_equal(
    path1: str, excludes1: List[str], path2: str, excludes2: List[str]
) -> bool:
    """Check whether two folders have the same contents by simply comparing the file sizes of each file."""

    excludes1 = sum((["-not", "-path", f"./{i}/*"] for i in excludes1), [])
    excludes2 = sum((["-not", "-path", f"./{i}/*"] for i in excludes2), [])
    # Now check that we copied everything
    filesizes, _ = await run_command(
        "find",
        *excludes1,
        "-type",
        "f",
        "-printf",
        "%p %s\n",
        preserve_stderr=True,
        preserve_stdout=True,
        log_stderr=logger.error,
        log_stdout=lambda _: None,
        cwd=path1,
    )

    filesizes_on_tape, _ = await run_command(
        "find",
        *excludes2,
        "-type",
        "f",
        "-printf",
        "%p %s\n",
        preserve_stderr=True,
        preserve_stdout=True,
        log_stderr=logger.error,
        log_stdout=lambda _: None,
        cwd=path2,
    )

    filesizes = sorted(filesizes.splitlines())
    filesizes_on_tape = sorted(filesizes_on_tape.splitlines())

    if filesizes == filesizes_on_tape:
        return True
    else:
        with open("/tmp/source.txt", "w") as f:
            f.write("\n".join(filesizes))
        with open("/tmp/target.txt", "w") as f:
            f.write("\n".join(filesizes_on_tape))
        return False


async def archive(
    folder: str,
    tape_label: str,
    target_filename: str,
    progress_callback,
    abort_event: asyncio.Event,
):
    entry = JsonDatabase()._get_folder(folder)
    await Library().ensure_tape_mounted(tape_label, progress_callback, abort_event)

    if abort_event.is_set():
        return

    # Now check that there is actually enough free space.

    progress_callback(f"Checking free disk space on {tape_label}")
    stdout, _ = await run_command(
        "df",
        preserve_stdout=True,
    )
    found = False
    for line in stdout.splitlines():
        comp = line.strip().split()
        if comp[-1] == "/ltfs":
            available = comp[3]

            if entry["size"] > int(available):
                raise ValueError(
                    f"Not enough space on tape {tape_label}. Available: {humanize.naturalsize(available * 1024, binary=True)}. Required: {humanize.naturalsize(entry['size'] * 1024, binary=True)}"
                )
            found = True
    assert found

    if entry["compressed"]:
        base = Path(entry["original_directory"])
        source = str(base.parent / base.with_suffix(".tar.gz"))
        pth = (Path("/ltfs/") / target_filename).with_suffix(".tar.gz")
        if pth.exists():
            raise Exception("Unable to create archive on tape, file already exists.")
        await run_command(
            "rsync",
            "-auvp",
            source,
            str(pth),
            "--info=progress2",
            log_stdout=lambda str: None,
            stdout_callback=lambda str: progress_callback(f"Copying: {str}"),
        )
        progress_callback("Deleting temp archive")
        await run_command("rm", source)

    else:
        # Find all files and pass that to ltfs_ordered_copy
        excludes = sum(
            (
                ["-not", "-path", f"./{i}/*"]
                for i in ConfigReader().get_exclude_folders()
            ),
            [],
        )
        path = "/ltfs/" + target_filename

        os.mkdir(path)
        progress_callback("Assembling a list of files...")
        files, _ = await run_command(
            "find",
            *excludes,
            "-type",
            "f",
            preserve_stderr=True,
            preserve_stdout=True,
            log_stderr=logger.error,
            log_stdout=lambda _: None,
            abort_event=abort_event,
            cwd=entry["original_directory"],
        )

        with open("/tmp/fileslist.txt", "w") as f:
            f.write(files)

        if abort_event.is_set():
            os.rmdir(path)
            return

        progress_callback("Ordering the files for writing to tape...")

        # Copy
        stdout, _ = await run_command(
            "python3",
            str(Path(__file__).parent / "ordered_copy.py"),
            "-t",
            path,
            "--keep-tree=.",
            stdin=files,
            log_stderr=logger.error,
            log_stdout=lambda _: None,
            stdout_callback=lambda str: progress_callback(f"Copying: {str}"),
            abort_event=abort_event,
            cwd=entry["original_directory"],
        )

        with open("/tmp/orderedcopy.txt", "w") as f:
            f.write(stdout)

        if abort_event.is_set():
            # Afterwards, we don't allow to abort, we're practically done anyway.
            os.rmdir(path)
            return

        progress_callback("Checking that the folders are equal...")

        equal = await check_folders_equal(
            entry["original_directory"], ConfigReader().get_exclude_folders(), path, []
        )
        if not equal:
            raise ValueError(
                "After-copy consistency check failed. Please check manually. File lists written to /tmp/source.txt and /tmp/target.txt"
            )

    progress_callback("Querying size of folder on tape...")

    stdout, _ = await run_command(
        "du",
        "-s",
        path,
        preserve_stderr=True,
        preserve_stdout=True,
    )

    size = int(stdout.split()[0])

    JsonDatabase().set_archived(entry, size)
    entry["tape"] = tape_label

    # Finally, delete the source folder
    # shutil.rmtree(entry["original_directory"])
    for i in range(10):
        try:
            progress_callback(f"Unmounting the tape (attempt {i} of 10)...")
            await Library().ensure_tape_unmounted(progress_callback)
            break
        except:  # noqa: E722
            await asyncio.sleep(30)

    await asyncio.sleep(200)
    return f"Archived {entry['original_directory']} to tape {tape_label}"


async def restore(
    folder: str,
    restore_path: Path,
    subfolder,
    progress_callback,
    abort_event: asyncio.Event,
):
    entry = JsonDatabase()._get_folder(folder)
    tape_label = entry["tape"]
    await Library().ensure_tape_mounted(tape_label, progress_callback, abort_event)

    if abort_event.is_set():
        return

    # Create the restore path
    Path(restore_path).mkdir(parents=True)

    ontape = f"/ltfs/{entry['path_on_tape']}/{subfolder}"

    # Copy
    await run_command(
        "python3",
        str(Path(__file__).parent / "ordered_copy.py"),
        ontape,
        str(restore_path),
        "-a",
        stdout_callback=lambda str: progress_callback(f"Restoring: {str}"),
        abort_event=abort_event,
    )

    equal = await check_folders_equal(ontape, [], str(restore_path), [])
    if not equal:
        raise ValueError(
            "After-copy consistency check failed. Please check manually. File lists written to /tmp/source.txt and /tmp/target.txt"
        )

    progress_callback("Unmounting the tape...")

    await Library().ensure_tape_unmounted(progress_callback)

    return f"Restored [{tape_label}] {ontape} to {restore_path}"


async def explore(
    tape_label,
    time: int,
    email: Optional[str],
    progress_callback,
    abort_event: asyncio.Event,
):
    await Library().ensure_tape_mounted(
        tape_label, progress_callback, abort_event, path="/ltfs"
    )

    # Now start the NFS mount
    await run_command(
        "exportfs",
        "-o",
        ConfigReader().get("Export", "settings"),
        ConfigReader().get("Export", "to") + ":/ltfs",
    )
    if email is not None:
        send_to_addr(
            f"You can now explore tape {tape_label}",
            f"""
                 You can access it read-only by accessing //{socket.gethostname()}/ltfs via nfs. 
                 The tape will be unmounted at {(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=time)).strftime("%Y-%m-%d %H:%M:%S %Z")} UTC""",
            [email],
        )

    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() < start_time + time:
        progress_callback(
            f"{int(asyncio.get_event_loop().time() - start_time)}s / {time}s"
        )
        await asyncio.sleep(1)
        if abort_event.is_set():
            break

    await run_command("exportfs", "-u", ConfigReader().get("Export", "to") + ":/ltfs")

    # Kill all processes that are still running on the tape
    try:
        await run_command("fuser", "-km", "/ltfs")
    except Exception as ex:
        # fuser returns 1 if no accessing process was found
        if "exited with code 1" not in ex.args[0]:
            raise ex

    await Library().ensure_tape_unmounted(progress_callback)

    return f"Explored tape {tape_label}"


age_groups = [
    ("2 years", datetime.timedelta(365 * 2)),
    ("1 years", datetime.timedelta(365 * 2)),
    ("6 months", datetime.timedelta(365 / 2)),
    ("Recent", datetime.timedelta(0)),
]


@dataclass
class InventoryItem:
    dir: str
    size: int
    last_modified: datetime.datetime

    @property
    def age_category(self):
        return next(
            (
                i
                for i, (_, delta) in enumerate(age_groups)
                if datetime.datetime.now() - self.last_modified >= delta
            ),
            3,
        )

    def __str__(self):
        return (
            f"    {self.dir.ljust(50)} "
            f"{humanize.naturalsize(self.size * 1024, binary=True).ljust(10)} "
            f"{(humanize.naturaldelta(datetime.datetime.now()-self.last_modified) + ' ago').ljust(30)}"
        )


async def inventory(folder, progress_callback, abort_event: asyncio.Event):
    result = []
    for subfolder in [f.path for f in os.scandir(folder) if f.is_dir()]:
        progress_callback(f"Getting size of {subfolder}..")
        size = await _get_size(subfolder, abort_event)

        if abort_event.is_set():
            return

        # This just reads the mtime, but usually this is set when a user logs in
        last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(subfolder))

        result.append(InventoryItem(subfolder, size, last_modified))

    grouped = {
        k: list(v)
        for k, v in groupby(
            sorted(result, key=lambda x: (x.age_category, -x.size)),
            key=lambda x: x.age_category,
        )
    }
    resulting_str = []
    for category, items in grouped.items():
        resulting_str.append(
            f"Older than {age_groups[category][0]}:"
            if category != 3
            else "Recently changed:"
        )
        resulting_str.append(
            f"    {'Directory'.ljust(50)} {'Size'.ljust(10)} {'Last changed'.ljust(30)}"
        )
        for item in items:
            resulting_str.append("    " + str(item))

        resulting_str.append("")

    logger.info("\n".join(resulting_str))

    send_to_logging_addr(f"Inventory report for {folder}", "\n".join(resulting_str))
