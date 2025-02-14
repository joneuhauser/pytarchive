import asyncio
import os
import subprocess
import re
from pytarchive.service.config import ConfigReader
from pytarchive.service.db import JsonDatabase
from pytarchive.service.command_runner import run_command
from pytarchive.service.log import logger


class Library:
    def _get_status(self):
        try:
            result = subprocess.run(
                ["mtx", "-f", ConfigReader().get_library_path(), "status"],
                capture_output=True,
                text=True,
            )
            if result.stdout.strip() == "":
                raise RuntimeError(f"Failed to get status from mtx: {result.stderr}")
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get status from mtx: {e}")

    def get_status(self):
        status_output = self._get_status()
        slots = {}
        for line in status_output.splitlines():
            match = re.match(
                r"\s*Storage Element (\d+):(\w+)( .*:VolumeTag=(\w+))?", line
            )
            if match:
                slot = int(match.group(1))
                status = match.group(2)
                volume_tag = match.group(4)
                slots[slot] = {
                    "status": status,
                    "volume_tag": volume_tag,
                    "type": "storage_element",
                }
            elif "Data Transfer Element" in line:
                match = re.match(
                    r"\s*Data Transfer Element (\d)+:(\w+) ?\(.*?\):VolumeTag = (\w+)?",
                    line,
                )
                volume_tag = None
                if match:
                    status = match.group(2)
                    volume_tag = match.group(3)
                else:
                    match = re.match(r"\s*Data Transfer Element (\d)+:Empty", line)
                    if match:
                        status = "Empty"
                if not match:
                    raise ValueError("Unable to parse mtx status output")
                slots[int(match.group(1))] = {
                    "status": status,
                    "volume_tag": volume_tag,
                    "type": "data_transfer_element",
                }

        return slots

    def get_available_tapes(self):
        slots = self.get_status()
        return {
            slot: info["volume_tag"]
            for slot, info in slots.items()
            if info["status"] == "Full" and info["volume_tag"]
        }

    def get_all_tapes(self):
        db_tapes = set(
            entry["tape"]
            for entry in JsonDatabase().data
            if entry.get("tape") is not None
        )
        try:
            for tape in self.get_available_tapes().values():
                db_tapes.add(tape)
        except Exception as _:
            pass
        return db_tapes

    def get_empty_slots(self):
        slots = self.get_status()
        return [slot for slot, info in slots.items() if info["status"] == "Empty"]

    def find_tape(self, volume_tag):
        slots = self.get_status()
        for slot, info in slots.items():
            if info["volume_tag"] == (volume_tag):
                return slot
        return None

    def drive_empty(self) -> bool:
        slots = self.get_status()
        return slots[0]["status"] == "Empty"

    async def _load_tape(self, volume_tag, progress):
        if not self.drive_empty():
            raise ValueError("Drive is not empty")
        device = ConfigReader().get_library_path()

        slot_id = self.find_tape(volume_tag)
        progress(f"Loading tape from slot {slot_id}...")
        await run_command("mtx", "-f", device, "load", str(slot_id))
        progress(f"Tape loaded from slot {slot_id}")

    async def _mount_tape(self, progress, path="/ltfs"):
        device = ConfigReader().get_drive_serial()
        progress(f"Mounting tape on {path}...")
        if self.drive_empty():
            raise ValueError("No tape loaded")
        await run_command("ltfs", "-o", f"devname={device}", path)
        progress(f"Tape mounted on {path} with device {device}")

    async def _create_filesystem(self, progress):
        """Creates a file system on the currently mounted tape"""
        slots = self.get_status()
        device = ConfigReader().get_drive_serial()
        if self.drive_empty():
            raise ValueError("No tape loaded")
        volume_tag: str = slots[0]["volume_tag"]
        assert volume_tag.endswith("L9")
        progress(f"Creating filesystem on tape {volume_tag}...")
        await run_command("mkltfs", "-d", device, "-s", volume_tag[0:6], "-c")
        progress(f"Filesystem created on tape {volume_tag} with device {device}")

    async def _unmount_tape(self, progress, path="/ltfs"):
        progress("Unmounting tape...")
        await run_command("umount", path)
        progress("Tape unmounted")

    async def _unload(self, progress):
        target = self.get_empty_slots()[0]
        device = ConfigReader().get_library_path()
        progress("Unloading tape...")
        await run_command("mtx", "-f", device, "unload", str(target))
        progress(f"Tape unloaded into slot {target}")

    def is_mounted(self, path="/ltfs"):
        with open("/proc/mounts", "r") as f:
            for line in f.readlines():
                if line.split()[1] == path:
                    return True
            return False

    def check_tape_consistency(self):
        status = self.get_status()
        tape_barcode = status[0]["volume_tag"]
        on_tape = JsonDatabase().get_directories_on_tape(tape_barcode)
        should_be_on_tape = sorted(
            [i["path_on_tape"] for i in on_tape if i["state"] == "archived"]
        )
        if any("/" in i for i in should_be_on_tape):
            raise NotImplementedError("Subpath folders need extra work")
        assert self.is_mounted()

        dirs_on_tape = sorted([name for name in os.listdir("/ltfs")])

        if dirs_on_tape != should_be_on_tape:
            logger.error(
                """Tape consistency check on %s failed. 
                Expected contents:\n%s, 
                Actual contents:\n%s.""",
                tape_barcode,
                "\n".join(should_be_on_tape),
                "\n".join(dirs_on_tape),
            )
        else:
            logger.info("Tape consistency check on %s successful.", tape_barcode)

    async def ensure_tape_unmounted(self, progress):
        for path in ["/ltfs", "/ltfs"]:
            if self.is_mounted(path):
                await self._unmount_tape(progress, path)
                await asyncio.sleep(5)

    async def ensure_tape_unloaded(self, progress, cancel_event: asyncio.Event):
        if not self.drive_empty():
            await self.ensure_tape_unmounted(progress)
            if cancel_event.is_set():
                return
            await self._unload(progress)

    async def ensure_tape_loaded(
        self, tape_barcode, progress, cancel_event: asyncio.Event
    ):
        if tape_barcode.startswith("CLN"):
            raise ValueError("Can't load cleaning tape!")
        # Maybe we already have the correct tape loaded?
        status = self.get_status()
        if not self.drive_empty():
            if status[0]["volume_tag"] == tape_barcode:
                logger.info("Tape already loaded")
                return
            await self.ensure_tape_unloaded(progress, cancel_event)

        if cancel_event.is_set():
            return
        assert self.drive_empty()

        await self._load_tape(tape_barcode, progress)

    async def ensure_tape_mounted(
        self, tape_barcode, progress, cancel_event: asyncio.Event, path="/ltfs"
    ):
        status = self.get_status()
        if not self.drive_empty():
            if status[0]["volume_tag"] == tape_barcode and self.is_mounted(path):
                # The correct tape is mounted in the correct location
                logger.info("Tape already mounted")
                self.check_tape_consistency()
                return

        # Make sure the tape is loaded
        await self.ensure_tape_loaded(tape_barcode, progress, cancel_event)

        if cancel_event.is_set():
            return

        # Now we check if we expect a filesystem on this tape
        on_tape = JsonDatabase().get_directories_on_tape(tape_barcode)
        on_tape = [i for i in on_tape if i["state"] == "archived"]
        should_have_fs = len(on_tape) > 0
        if not should_have_fs:
            # We create a filesystem. If there is already one, the command will error
            try:
                await self._create_filesystem(progress)
            except subprocess.CalledProcessError as e:
                logger.error(f"mkltfs returned {e.stderr}")
                if "LTFS15047E Medium is already formatted" not in e.stderr:
                    raise e

        if cancel_event.is_set():
            return

        await self._mount_tape(progress, path)

        self.check_tape_consistency()
