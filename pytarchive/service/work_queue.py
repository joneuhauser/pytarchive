import asyncio
from dataclasses import dataclass, field
import datetime
import json
import random
import sys
from textwrap import indent
import traceback
from typing import Coroutine, Dict, Iterable, List, Optional, Any

from pytarchive.service import tasks
from pytarchive.service.utils import singleton
from pytarchive.service.log import logger


@dataclass
class WorkItem:
    priority: int
    coroutine: Coroutine
    args: List[Any]
    description: str
    error_msg: str = field(init=False)
    _progress: Optional[str] = field(init=False)
    _abort_handle: asyncio.Event = field(init=False)
    _created: datetime.datetime = field(init=False)
    _hashseed: float = field(init=False)
    _running: bool = field(init=False)

    def __post_init__(self):
        self._abort_handle = asyncio.Event()
        self._created = datetime.datetime.now()
        self._hashseed = random.random()
        self._progress = None
        self._running = False
        self.error_msg = ""

    def update_progress(self, data: str):
        self._progress = data

    def is_running(self) -> bool:
        return self._running

    def is_error(self) -> bool:
        return self.error_msg != ""

    def request_abort(self) -> bool:
        self._abort_handle.set()

    async def run(self) -> bool:
        self._running = True
        await getattr(tasks, self.coroutine)(
            *self.args, self.update_progress, self._abort_handle
        )
        self._running = False

    def __hash__(self) -> int:
        return hash(self._hashseed)

    def format_hash(self) -> str:
        h = hash(self) + sys.maxsize + 1
        return f"{h:#0{10}x}"[2:10]

    def __str__(self) -> str:
        ret = f"[{self.format_hash()}] {self.priority} - {self.description}"
        if self.is_running():
            ret += f" [{self._progress}]"
        if self.is_error():
            ret += "\n" + indent(self.error_msg, "\t")
        return ret


@singleton
class WorkList(List[WorkItem]):
    def __init__(self):
        self.callback = lambda: None
        self.json_file = "/var/lib/pytarchive/queue.json"
        for entry in self._read_json():
            wi = WorkItem(
                entry["priority"],
                entry["coroutine"],
                entry["args"],
                entry["description"],
            )
            wi.error_msg = entry["error_msg"]
            wi._created = datetime.datetime.strptime(
                entry["created"], "%b %d %Y %H:%M:%S"
            )
            self.append(wi)

        self.callback = self._write_json

    def append(self, object: Any) -> None:
        res = super().append(object)
        self.callback()
        return res

    def remove(self, value: Any) -> None:
        res = super().remove(value)
        self.callback()
        return res

    def extend(self, iterable: Iterable) -> None:
        res = super().extend(iterable)
        self.callback()
        return res

    def pop(self, index: int) -> Any:
        res = super().pop(index)
        self.callback()
        return res

    def get_top(self) -> WorkItem:
        list = sorted([i for i in self if i.error_msg == ""], key=lambda i: i.priority)
        if len(list) == 0:
            return None
        else:
            return list[0]

    async def worker(self):
        while True:
            item = self.get_top()
            if item is None:
                await asyncio.sleep(1)
                continue

            description = item.description

            logger.info(f"starting {description}")
            try:
                result = await item.run()
                logger.info(f"{description} - Result: {result}\n")

            except Exception as e:
                logger.critical(
                    f"{description} - Error: {e} {traceback.format_exc()}\n"
                )
                item.error_msg = f"{e} {traceback.format_exc()}"
                self.callback()
                continue

            self.remove(item)

    def _read_json(self) -> List[Dict[str, Any]]:
        try:
            with open(self.json_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return []

    def _write_json(self) -> None:
        with open(self.json_file, "w") as f:
            data = [
                {
                    "priority": i.priority,
                    "coroutine": i.coroutine,
                    "args": i.args,
                    "description": i.description,
                    "created": i._created.strftime("%b %d %Y %H:%M:%S"),
                    "error_msg": i.error_msg,
                }
                for i in self
            ]
            json.dump(data, f, indent=4)
