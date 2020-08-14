import asyncio
from asyncio.tasks import Task
import time
from typing import Any, Dict, Tuple

from bluefly.utils import ReadableDevice, Status

from scanpointgenerator import CompoundGenerator


ConfigDict = Dict[str, Any]


class FlyScanLogic:
    async def go(generator: CompoundGenerator):
        raise NotImplementedError(self)


class FlyScanDevice(ReadableDevice):
    """Generic fly scan device that wraps some custom routines"""
    def __init__(self, logic: FlyScanLogic):
        self._logic = logic
        self._configuration : Dict[str, Dict[str, Any]] = {}
        self._update_configuration(dict(generator=CompoundGenerator()))
        self._task: Task = None

    def _update_configuration(self, d: ConfigDict):
        now = time.time()
        for k, v in d.items():
            self._configuration[k] = dict(
                value=v,
                timestamp=now,
            )

    def configure(self, d: ConfigDict) -> Tuple[ConfigDict, ConfigDict]:
        old_config = self.read_configuration()
        unknown = set(d) - set(self._configuration)
        assert not unknown, f"Configure passed unknown parameters {sorted(unknown)}"
        self._update_configuration(d)
        new_config = self.read_configuration()
        return old_config, new_config

    def read_configuration(self) -> ConfigDict:
        return self._configuration.copy()

    def describe_configuration(self) -> ConfigDict:
        return dict(
            generator=dict(
                source="user supplied parameter",
                dtype="object",
                shape=[]
            )
        )

    def trigger(self) -> Status:
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._logic.go(self._configuration["generator"]))
        return Status(self._task)


