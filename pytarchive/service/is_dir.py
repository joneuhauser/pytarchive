import subprocess


def is_dir_with_timeout(path, timeout=1.0):
    """
    Check if the given path is a directory using a separate process.

    Parameters:
        path (str or pathlib.Path): The path to check.
        timeout (float): Maximum time in seconds to wait for the result.

    Returns:
        bool: True if the path is a directory, False if it is not.
        None: if the operation did not complete within the timeout.
    """
    try:
        subprocess.run(
            ["timeout", str(timeout) + "s", "test", "-d", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print("The command timed out or failed:", e)
        if e.returncode == 124:
            return None
        return False
