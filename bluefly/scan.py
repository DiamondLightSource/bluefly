import asyncio
import json
import time
from asyncio.tasks import Task
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

from scanpointgenerator import CompoundGenerator

from bluefly.core import Device, Status


class FlyScanLogic:
    async def go(
        self, generator: CompoundGenerator, completed: int = 0
    ) -> AsyncGenerator[int, None]:
        raise NotImplementedError(self)
        yield 0

    async def stop(self):
        raise NotImplementedError(self)


ConfigDict = Dict[str, Dict[str, Any]]


class FlyScanDevice(Device):
    """Generic fly scan device that wraps some custom routines"""

    def __init__(self, logic: FlyScanLogic):
        self._logic = logic
        self._generator = CompoundGenerator(generators=[])
        self._when_configured = time.time()
        self._when_triggered = time.time()
        self._when_updated = time.time()
        self._completed_steps = 0
        self._total_steps = 0
        self._watchers: List[Callable] = []
        self._trigger_task: Optional[Task] = None
        self._pause_task: Optional[Task] = None
        self._resuming = False

    def configure(self, d: Dict[str, Any]) -> Tuple[ConfigDict, ConfigDict]:
        old_config = self.read_configuration()
        self._when_configured = time.time()
        self._generator = d["generator"]
        new_config = self.read_configuration()
        return old_config, new_config

    def read_configuration(self) -> ConfigDict:
        return dict(
            generator=dict(
                value=json.dumps(self._generator.to_dict()),
                timestamp=self._when_configured,
            )
        )

    def describe_configuration(self) -> ConfigDict:
        return dict(
            generator=dict(source="user supplied parameter", dtype="string", shape=[])
        )

    def read(self) -> ConfigDict:
        return dict(
            completed_steps=dict(
                value=self._completed_steps, timestamp=self._when_updated
            )
        )

    def describe(self) -> ConfigDict:
        return dict(completed_steps=dict(source="progress", dtype="number", shape=[]))

    def trigger(self) -> Status:
        self._trigger_task = asyncio.create_task(self._trigger())
        status = Status(self._trigger_task, self._watchers.append)
        return status

    def pause(self):
        # TODO: would be good to return a Status object here
        assert self._trigger_task, "Trigger not called"
        self._trigger_task.cancel()
        self._pause_task = asyncio.create_task(self._logic.stop())

    def resume(self):
        assert self._pause_task.done(), "You didn't wait for pause to finish"
        self._resuming = True

    async def _trigger(self):
        if self._resuming:
            # Pause already did configure for us
            self._resuming = False
        else:
            self._generator.prepare()
            self._completed_steps = 0
            self._total_steps = self._generator.size
            self._when_triggered = time.time()
        async for step in self._logic.go(self._generator, self._completed_steps):
            self._completed_steps = step
            self._when_updated = time.time()
            for watcher in self._watchers:
                watcher(
                    name=self.name,
                    current=step,
                    initial=0,
                    target=self._total_steps,
                    unit="",
                    precision=0,
                    time_elapsed=self._when_updated - self._when_triggered,
                    fraction=step / self._total_steps,
                )
