import asyncio
import subprocess
from typing import Optional
from pytarchive.service.log import logger


async def run_command(
    command,
    *args,
    stdout_callback=lambda _: None,
    abort_event: Optional[asyncio.Event] = None,
    preserve_stdout=False,
    preserve_stderr=True,
    log_stdout=logger.info,
    log_stderr=logger.error,
    stdin=None,
    **kwargs,
):
    logger.info(command, *args)
    process = await asyncio.create_subprocess_exec(
        command,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        **kwargs,
    )

    # Function to log output in real time
    async def log_output(stream, log_method, preserve=False, result=None):
        while True:
            await asyncio.sleep(0)
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

    async def write_stdin(pipe, stdin, log):
        pipe.write(stdin)
        log(f"Wrote {len(stdin)} bytes to stdin")
        await pipe.drain()
        log("Stdin drained")
        pipe.close()
        log("Stdin closed")

    stdout_res = []
    stderr_res = []

    stdout_task = asyncio.create_task(
        log_output(process.stdout, log_stdout, preserve_stdout, stdout_res)
    )
    stderr_task = asyncio.create_task(
        log_output(process.stderr, log_stderr, preserve_stderr, stderr_res)
    )
    if stdin is not None and len(stdin) != 0:
        write_task = asyncio.create_task(
            write_stdin(process.stdin, stdin.encode(), logger.info)
        )
    abort_task = asyncio.create_task(monitor_abort())

    exit_code = await process.wait()

    stdout = "\n".join(stdout_res)
    stderr = "\n".join(stderr_res)

    try:
        stdout_task.cancel()
        stderr_task.cancel()
        abort_task.cancel()
        write_task.cancel()
    except:  # noqa
        pass

    if abort_event is not None and abort_event.is_set():
        logger.info("Process aborted")
        raise subprocess.CalledProcessError(
            exit_code,
            command,
            stdout,
            stderr,
        )
    return stdout, stderr
