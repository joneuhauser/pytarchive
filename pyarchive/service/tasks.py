import asyncio
from typing import Optional
from pyarchive.service.db import JsonDatabase
from pyarchive.service.library import Library
from pyarchive.service.utils import run_command


async def get_size(folder: str, description: str):
    entry = JsonDatabase().create_entry(folder, description)
    process = await asyncio.create_subprocess_exec(
        "du",
        "-s",
        entry["original_path"],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        raise ChildProcessError(stderr.decode())

    size = int(stdout.decode().split()[0])

    JsonDatabase().set_prepared(entry, size)


async def load_and_mount(tape_id):
    Library().load_tape(tape_id)


async def archive(
    folder, tape_label, progress_callback, abort_event: Optional[asyncio.Event] = None
):
    await run_command(
        "python3",
        "test_progress.py",
        stdout_callback=progress_callback,
        abort_event=abort_event,
    )
    return f"Archived {folder} to tape {tape_label}"


async def restore(folder, restore_path):
    # Simulate a slow operation
    await asyncio.sleep(10)
    return f"Restored {folder} to {restore_path}"


async def explore(tape_label, time: int, progress_callback, abort_event: asyncio.Event):
    await Library().ensure_tape_mounted(tape_label, progress_callback, abort_event)

    start_time = asyncio.get_event_loop().time()
    print(start_time, time)
    while asyncio.get_event_loop().time() < start_time + time:
        progress_callback(
            f"{int(asyncio.get_event_loop().time() - start_time)}s / {time}s"
        )
        await asyncio.sleep(1)
        if abort_event.is_set():
            break

    await Library().ensure_tape_unmounted(progress_callback)

    return f"Explored tape {tape_label}"
