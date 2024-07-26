import asyncio
import subprocess
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
    preserve_stdout=False,
    preserve_stderr=True,
):
    print(command, *args)
    process = await asyncio.create_subprocess_exec(
        command, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    # Function to log output in real time
    async def log_output(stream, log_method, preserve=False, result=None):
        while True:
            line = await stream.readline()
            if line:
                dec = line.decode().strip()
                stdout_callback(dec)
                # to avoid memory overruns, and to access the result even if canceled
                if preserve:
                    result.append(dec)
                    log_method(dec)
            else:
                break

    async def monitor_abort():
        while True:
            if abort_event is not None and abort_event.is_set():
                logger.info(f"Aborting process: {command} {args}")
                process.terminate()
                return
            await asyncio.sleep(0.1)

    stdout_res = []
    stderr_res = []

    # Log both stdout and stderr, and await cancellation. Wait until one of those tasks is finished.
    done, pending = await asyncio.wait(
        [
            log_output(process.stdout, logger.info, preserve_stdout, stdout_res),
            log_output(process.stderr, logger.error, preserve_stderr, stderr_res),
            monitor_abort(),
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for pend in list(pending):
        pend.cancel()

    stdout = "\n".join(stdout_res)
    stderr = "\n".join(stderr_res)

    exit_code = await process.wait()
    if abort_event is not None and abort_event.is_set():
        logger.info("Process aborted")
    elif exit_code != 0:
        raise subprocess.CalledProcessError(
            exit_code,
            command,
            stdout,
            stderr,
        )
    return stdout, stderr
