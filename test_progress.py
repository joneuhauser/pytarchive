import shutil
import sys
import time


def copy_file(src, dest):
    shutil.copy2(src, dest)


def copy_files_with_progress():
    for i in range(100):
        time.sleep(1)
        print(f"file {i} / {100} copied")
        sys.stdout.flush()


if __name__ == "__main__":
    copy_files_with_progress()
    print()  # Print a newline at the end
