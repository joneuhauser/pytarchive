import itertools
import json
from datetime import datetime
from typing import List, Dict, Any

import humanize

from pyarchive.service.config import ConfigReader
from pyarchive.service.utils import singleton


@singleton
class JsonDatabase:
    def __init__(self):
        self.json_file = "/var/lib/pyarchive/database.json"
        self.data = self._read_json()

    def _read_json(self) -> List[Dict[str, Any]]:
        try:
            with open(self.json_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def _write_json(self) -> None:
        with open(self.json_file, "w") as f:
            json.dump(self.data, f, indent=4)

    def get_all_folders(self) -> List[Dict[str, Any]]:
        return self.data

    def _get_folder(self, original_directory: str) -> Dict[str, Any]:
        for folder in self.data:
            if folder["original_directory"] == original_directory:
                return folder
        raise ValueError(
            f"Folder with original_directory '{original_directory}' not found."
        )

    def create_entry(self, original_directory: str, description: str) -> Dict[str, Any]:
        if any(
            folder["original_directory"] == original_directory for folder in self.data
        ):
            raise ValueError(
                f"Folder with original_directory '{original_directory}' already exists in database."
            )

        new_entry = {
            "original_directory": original_directory,
            "state": "preparing",
            "description": description,
        }
        self.data.append(new_entry)
        self._write_json()
        return new_entry

    def set_prepared(self, entry: Dict[str, Any], size: int) -> Dict[str, Any]:
        if entry["state"] == "preparing":
            entry["state"] = "prepared"
            entry["size"] = size
            entry["size_queried"] = datetime.now().strftime("%b %d %Y %H:%M:%S")
            self._write_json()
            return entry
        else:
            raise ValueError(
                f"Invalid state transition from {entry['state']} to prepared."
            )

    def set_archiving_queued(self, entry: Dict[str, Any], tape: str) -> Dict[str, Any]:
        if entry["state"] == "prepared":
            entry["state"] = "archiving_queued"
            entry["tape"] = tape
            self._write_json()
            return entry
        else:
            raise ValueError(
                f"Invalid state transition from {entry['state']} to archiving_queued."
            )

    def set_archiving(self, entry: Dict[str, Any], path_on_tape: str) -> Dict[str, Any]:
        if entry["state"] == "archiving_queued":
            entry["state"] = "archiving"
            entry["path_on_tape"] = path_on_tape
            self._write_json()
            return entry
        else:
            raise ValueError(
                f"Invalid state transition from {entry['state']} to archiving."
            )

    def set_archived(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        if entry["state"] == "archiving":
            entry["state"] = "archived"
            entry["archived"] = datetime.now().strftime("%b %d %Y %H:%M:%S")
            self._write_json()
            return entry
        else:
            raise ValueError(
                f"Invalid state transition from {entry['state']} to archived."
            )

    def get_entries_by_state(self, state: str) -> List[Dict[str, Any]]:
        return [folder for folder in self.data if folder["state"] == state]

    def get_directories_on_tape(self, tape: str) -> List[Dict[str, Any]]:
        return [folder for folder in self.data if folder.get("tape") == tape]

    def place_directory(self, entry: Dict[str, Any], all_tapes: List[str]) -> str:
        """Finds a placement of a directory on tape. Places it on the fullest tape where the directory fits"""
        if entry["state"] != "prepared":
            raise ValueError(f"Directory must be prepared, is {entry['state']}")
        sums = {i: 0 for i in all_tapes}

        for folder in self.data:
            tape = folder.get("tape")
            if tape is not None:
                if tape not in sums:
                    sums[tape] = 0
                sums[tape] += folder.get("size")
        ordered = sorted(sums.items(), key=lambda e: e[1], reverse=True)
        ordered = [
            i for i in ordered if (i[1] + entry["size"]) < ConfigReader().get_maxsize()
        ]
        if len(ordered) == 0:
            return "doesn't fit"
        return ordered[0][0]

    @staticmethod
    def sizeof_fmt(num):
        return humanize.naturalsize(num, binary=True)

    def format(self, all_tapes: List[str]) -> str:
        res = []
        state_order = {
            "preparing": 1,
            "prepared": 2,
            "archiving_queued": 3,
            "archiving": 4,
            "archived": 5,
        }
        sorted_data = sorted(self.data, key=lambda x: state_order.get(x["state"], 99))

        for state, group in itertools.groupby(sorted_data, key=lambda x: x["state"]):
            if state == "preparing":
                res.append("[preparing]")
                for folder in group:
                    res.append(
                        f"{folder['original_directory']}: {folder['description']}"
                    )
            elif state == "prepared":
                res.append("[prepared]")
                prepared_folders = sorted(
                    group,
                    key=lambda x: datetime.strptime(
                        x.get("size_queried", "Jan 01 1970 00:00:00"),
                        "%b %d %Y %H:%M:%S",
                    ),
                    reverse=True,
                )
                for folder in prepared_folders:
                    size_str = self.sizeof_fmt(folder["size"] * 1024)
                    suggested = self.place_directory(folder, all_tapes)
                    res.append(
                        f"{folder['original_directory']} ({size_str} as of {folder.get('size_queried', 'Unknown date')}) -> (suggested: {suggested})"
                    )
                    res.append(f"    {folder['description']}")
            elif state == "archiving_queued":
                res.append("[archiving_queued]")
                for folder in group:
                    size_str = self.sizeof_fmt(folder["size"] * 1024)
                    res.append(
                        f"{folder['original_directory']} ({size_str} as of {folder.get('size_queried', 'Unknown date')}) -> {folder['tape']}"
                    )
            elif state == "archiving":
                res.append("[archiving]")
                for folder in group:
                    size_str = self.sizeof_fmt(folder["size"] * 1024)
                    res.append(
                        f"{folder['original_directory']} ({size_str}) -> {folder['tape']}"
                    )
            elif state == "archived":
                res.append("Tape overview:")
                for tape in sorted(all_tapes):
                    tape_entries = sorted(
                        [folder for folder in self.data if folder.get("tape") == tape],
                        key=lambda x: -float(x["size"]),
                    )
                    total_size = (
                        sum(folder.get("size") for folder in tape_entries) * 1024
                    )
                    maxsize = ConfigReader().get_maxsize() * 1024
                    res.append(
                        f"{tape} {humanize.naturalsize(total_size, binary=True)} / {humanize.naturalsize(maxsize, binary=True)} ({total_size / maxsize * 100:.2f}%)"
                    )
                    for entry in tape_entries:
                        size_str = self.sizeof_fmt(entry["size"] * 1024)
                        if entry["state"] == "archived":
                            res.append(
                                f"    {entry['original_directory']} ({size_str}) {entry['description']}"
                            )
                        else:
                            res.append(
                                f"\033[33m    {entry['original_directory']} ({size_str}) {entry['description']} [{entry['state']}]\033[0m"
                            )
                res.append("")  # empty line between tapes
            res.append("")  # empty line between sections

        return "\n".join(res[:-2])
