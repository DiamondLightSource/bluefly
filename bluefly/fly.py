import asyncio
import json
import time
from asyncio.tasks import Task
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

from scanpointgenerator import CompoundGenerator

from bluefly import motor, pmac
from bluefly.core import ConfigDict, Device, RemainingPoints, Status

# Interface
###########


class FlyLogic:
    async def open(self):
        """Open files, etc"""
        pass

    async def scan(
        self, points: RemainingPoints, offset: int
    ) -> AsyncGenerator[int, None]:
        """Scan the given points, putting them at offset into file"""
        raise NotImplementedError(self)
        yield 0

    async def stop(self):
        """Stop where you are, without retracing"""
        raise NotImplementedError(self)

    async def close(self):
        """Close any files"""
        pass


# Logic
#######


class FlyDevice(Device):
    """Generic fly scan device that wraps some custom routines"""

    def __init__(self, logic: FlyLogic):
        self._logic = logic
        self._generator = CompoundGenerator(generators=[])
        self._when_configured = time.time()
        self._when_triggered = time.time()
        self._when_updated = time.time()
        self._completed_steps = 0
        self._total_steps = 0
        self._offset = 0
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

    def stage(self) -> List[Device]:
        self._offset = 0
        return [self]

    def unstage(self) -> List[Device]:
        # TODO: would be good to return a Status object here
        asyncio.create_task(self._logic.close())
        return [self]

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
            # Resuming where we last left off
            self._resuming = False
        else:
            # Start from the beginning
            self._completed_steps = 0
            self._generator.prepare()
            self._total_steps = self._generator.size
            self._when_triggered = time.time()
            if self._offset == 0:
                # beginning of the scan, open the file
                await self._logic.open()
        completed_at_start = self._completed_steps
        points = RemainingPoints(self._generator, completed_at_start)
        async for step in self._logic.scan(points, self._offset + completed_at_start):
            self._completed_steps = step + completed_at_start
            self._when_updated = time.time()
            for watcher in self._watchers:
                watcher(
                    name=self.name,
                    current=self._completed_steps,
                    initial=0,
                    target=self._total_steps,
                    unit="",
                    precision=0,
                    time_elapsed=self._when_updated - self._when_triggered,
                    fraction=self._completed_steps / self._total_steps,
                )
        self._offset += self._total_steps


@dataclass
class PMACMasterFlyLogic(FlyLogic):
    pmac: pmac.PMAC
    motors: List[motor.MotorDevice]

    async def scan(
        self, points: RemainingPoints, offset: int
    ) -> AsyncGenerator[int, None]:
        _, tracker = await asyncio.gather(
            pmac.move_to_start(self.pmac, self.motors, points.peek_point()),
            pmac.build_initial_trajectory(self.pmac, self.motors, points),
        )
        async for step in pmac.keep_filling_trajectory(self.pmac, tracker):
            yield step

    async def stop(self):
        await pmac.stop_trajectory(self.pmac)
