import asyncio
from typing import List, Optional
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
    logger.info([command, *args])
    process = await asyncio.create_subprocess_exec(
        command,
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        **kwargs,
    )

    if stdin is not None and process.stdin is not None:
        stdin = stdin.encode()
        process.stdin.write(stdin)
        await process.stdin.drain()
        process.stdin.close()

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

    stdout_res: List[str] = []
    stderr_res: List[str] = []

    # Log both stdout and stderr, and await cancellation. Wait until one of those tasks is finished.
    done, pending = await asyncio.wait(
        [
		asyncio.create_task(log_output(process.stdout, logger.info, preserve_stdout, stdout_res)),
        	asyncio.create_task(log_output(process.stderr, logger.error, preserve_stderr, stderr_res)),
        	asyncio.create_task(monitor_abort()),        
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
        raise Exception(f"""Command {command} {args} exited with code {exit_code}.
        Stdout: {stdout}. Stderr: {stderr}""")
    return stdout, stderr
