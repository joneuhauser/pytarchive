import asyncio
from pyarchive.service.db import JsonDatabase


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


async def archive(folder, tapenumber=None):
    # Simulate a slow operation
    await asyncio.sleep(30)
    return f"Archived {folder} to tape {tapenumber}"


async def restore(folder, restore_path):
    # Simulate a slow operation
    await asyncio.sleep(10)
    return f"Restored {folder} to {restore_path}"


async def explore(tapenumber):
    # Simulate a slow operation
    await asyncio.sleep(50)
    return f"Explored tape {tapenumber}"
