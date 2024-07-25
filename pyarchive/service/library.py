import subprocess
import re
from pyarchive.service.config import ConfigReader
from pyarchive.service.db import JsonDatabase


class Library:
    def _get_status(self):
        try:
            result = subprocess.run(
                ["mtx", "-d", ConfigReader().get_library_path(), "status"],
                capture_output=True,
                text=True,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get status from mtx: {e}")

    def get_status(self):
        self.status_output = self._get_status()
        slots = {}
        for line in self.status_output.splitlines():
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
            if info["volume_tag"] == (
                volume_tag
                if (volume_tag.endswith("L9") or volume_tag.endswith("L1"))
                else (volume_tag + "L9")
            ):
                return slot
        return None

    def load_tape(self, volume_tag):
        slots = self.get_status()
        assert 0 in slots
        if slots[0]["status"] != "Empty":
            raise ValueError("Drive is not empty")
        device = ConfigReader().get_library_path()

        slot_id = self.find_tape(volume_tag)
        try:
            subprocess.run(["mtx", "-d", device, "load", str(slot_id)])
            print(f"Tape loaded from slot {slot_id}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to load tape from slot {slot_id}: {e}")

    def mount_tape(self):
        device = ConfigReader().get_drive_serial()
        slots = self.get_status()
        if 0 not in slots:
            raise ValueError("No tape loaded")
        try:
            subprocess.run(["ltfs", "-o", f"devname={device}", "/ltfs"])
            print(f"Tape mounted on /ltfs with device {device}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to mount tape with device {device}: {e}")

    def _create_filesystem(self):
        """Creates a file system on the currently mounted tape"""
        slots = self.get_status()
        device = ConfigReader().get_drive_serial()
        if 0 not in slots:
            raise ValueError("No tape loaded")
        volume_tag = slots[0]["volume_tag"]
        try:
            subprocess.run(["mkltfs", "-d", device, "-s", volume_tag])
            print(f"Filesystem created on tape {volume_tag} with device {device}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to create filesystem on tape {volume_tag} with device {device}: {e}"
            )

    async def unmount_and_unload_tape(self, progress_func):
        target = self.get_empty_slots()[0]
        device = ConfigReader().get_drive_serial()
        try:
            subprocess.run(["umount", "/ltfs"])
            print("Tape unmounted from /ltfs")
            subprocess.run(["mtx", "-d", device, "unload", str(target)])
            print(f"Tape unloaded into slot {target}")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to unmount and unload tape into slot {target}: {e}"
            )
