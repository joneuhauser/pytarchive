import asyncio
from typing import Optional
from pyarchive.service.log import logger


def singleton(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]

    return getinstance


async def run_command(
    command,
    *args,
    stdout_callback=lambda _: None,
    abort_event: Optional[asyncio.Event] = None,
):
    print(command, *args)
    process = await asyncio.create_subprocess_exec(
        command, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    # Function to log output in real time
    async def log_output(stream, log_method):
        while True:
            line = await stream.readline()
            if line:
                dec = line.decode().strip()
                log_method(dec)
                stdout_callback(dec)
            else:
                break

    async def monitor_abort():
        while True:
            if abort_event is not None and abort_event.is_set():
                logger.info(f"Aborting process: {args}")
                process.terminate()
                return
            await asyncio.sleep(0.1)

    # Log both stdout and stderr
    await asyncio.gather(
        log_output(process.stdout, logger.info),
        log_output(process.stderr, logger.error),
        monitor_abort(),
    )

    await process.wait()
