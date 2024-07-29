import asyncio
import os
from pathlib import Path
from typing import List, Optional

import humanize
from pytarchive.service.config import ConfigReader
from pytarchive.service.db import JsonDatabase
from pytarchive.service.library import Library
from pytarchive.service.command_runner import run_command


async def get_size(folder: str, progress, abort: asyncio.Event):
    entry = JsonDatabase()._get_folder(folder)
    progress(f"Querying size of folder {entry['original_directory']}")

    stdout, _ = await run_command(
        "du",
        "-s",
        entry["original_directory"],
        abort_event=abort,
        preserve_stderr=True,
        preserve_stdout=True,
    )

    if abort.is_set():
        JsonDatabase().data.remove(entry)
        return

    size = int(stdout.split()[0])

    assert entry in JsonDatabase().data

    JsonDatabase().set_prepared(entry, size)

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
        log_stderr=True,
        log_stdout=False,
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
        log_stderr=True,
        log_stdout=False,
        cwd=path2,
    )

    filesizes = sorted(filesizes.splitlines())
    filesizes_on_tape = sorted(filesizes_on_tape.splitlines())

    if filesizes == filesizes_on_tape:
        return
    else:
        with open("/tmp/source.txt", "w") as f:
            f.write("\n".join(filesizes))
        with open("/tmp/target.txt", "w") as f:
            f.write("\n".join(filesizes_on_tape))


async def archive(
    folder: str, progress_callback, abort_event: Optional[asyncio.Event] = None
):
    entry = JsonDatabase()._get_folder(folder)
    tape_label = entry["tape"]
    await Library().ensure_tape_mounted(tape_label, progress_callback, abort_event)

    if abort_event.is_set():
        return

    # Now check that there is actually enough free space.

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
                    f"Not enough space on tape {tape_label}. Available: {humanize.naturalsize(available, binary=True)}. Required: {humanize.naturalsize(entry['size'], binary=True)}"
                )
            found = True
    assert found

    path = "/ltfs/" + entry["path_on_tape"]

    # Great. Let's create a directory
    os.mkdir(path)

    # Find all files and pass that to ltfs_ordered_copy
    excludes = sum(
        (["-not", "-path", f"./{i}/*"] for i in ConfigReader().get_exclude_folders()),
        [],
    )

    progress_callback("Assembling a list of files...")
    files, _ = await run_command(
        "find",
        *excludes,
        "-type",
        "f",
        preserve_stderr=True,
        preserve_stdout=True,
        log_stderr=True,
        log_stdout=False,
        abort_event=abort_event,
        cwd=entry["original_directory"],
    )

    if abort_event.is_set():
        os.rmdir(path)
        return

    progress_callback("Ordering the files for writing to tape...")

    # Copy
    await run_command(
        "python3",
        str(Path(__file__).parent / "ordered_copy.py"),
        "-t",
        path,
        "--keep-tree=.",
        stdin=files,
        log_stderr=True,
        log_stdout=False,
        stdout_callback=lambda str: progress_callback(f"Copying: {str}"),
        abort_event=abort_event,
        cwd=entry["original_directory"],
    )

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

    JsonDatabase().set_archived(entry)

    # Finally, delete the source folder
    # shutil.rmtree(entry["original_directory"])

    return f"Archived {entry['original_directory']} to tape {tape_label}"


async def restore(
    folder: str,
    restore_path: Path,
    subfolder="",
    progress_callback=lambda _: None,
    abort_event: Optional[asyncio.Event] = None,
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

    return f"Restored [{tape_label}] {ontape} to {restore_path}"


async def explore(tape_label, time: int, progress_callback, abort_event: asyncio.Event):
    await Library().ensure_tape_mounted(tape_label, progress_callback, abort_event)

    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() < start_time + time:
        progress_callback(
            f"{int(asyncio.get_event_loop().time() - start_time)}s / {time}s"
        )
        await asyncio.sleep(1)
        if abort_event.is_set():
            break

    await Library().ensure_tape_unmounted(progress_callback)

    return f"Explored tape {tape_label}"
