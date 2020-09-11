import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from scanpointgenerator import CompoundGenerator

from bluefly import detector, motor, pmac
from bluefly.core import (
    ConfigDict,
    DatasetDetails,
    Device,
    FileDetails,
    FilenameScheme,
    RemainingPoints,
    Status,
)


class FlyLogic:
    async def open(self, file_details: FileDetails) -> Dict[str, DatasetDetails]:
        """Open files, etc"""
        raise NotImplementedError(self)

    async def scan(
        self, points: RemainingPoints, offset: int, queue: "asyncio.Queue[int]"
    ):
        """Scan the given points, putting them at offset into file. Progress updates
        should be put on the queue"""
        raise NotImplementedError(self)

    async def stop(self):
        """Stop where you are, without retracing or closing files"""
        raise NotImplementedError(self)

    async def close(self):
        """Close any files"""
        raise NotImplementedError(self)


class FlyDevice(Device):
    """Generic fly scan device that wraps some custom routines"""

    def __init__(self, logic: FlyLogic, scheme: FilenameScheme):
        self._logic = logic
        self._generator = CompoundGenerator(generators=[])
        self._when_configured = time.time()
        self._when_triggered = time.time()
        self._when_updated = time.time()
        self._completed_steps = 0
        self._total_steps = 0
        self._offset = 0
        self._watchers: List[Callable] = []
        self._trigger_task: Optional[asyncio.Task] = None
        self._pause_task: Optional[asyncio.Task] = None
        self._resuming = False
        self._scheme = scheme

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
                await self._scheme.new_scan()
                assert self.name
                details = FileDetails(
                    self._scheme.file_path, self._scheme.file_template, self.name
                )
                await self._logic.open(details)
        points = RemainingPoints(self._generator, self._completed_steps)
        queue: asyncio.Queue[int] = asyncio.Queue()

        async def update_watchers(completed_at_start):
            while self._completed_steps < self._total_steps:
                self._completed_steps = await queue.get()
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

        await asyncio.gather(
            self._logic.scan(points, self._offset + self._completed_steps, queue),
            update_watchers(self._completed_steps),
        )
        self._offset += self._total_steps


@dataclass
class PMACMasterFlyLogic(FlyLogic):
    pmac: pmac.PMAC
    detectors: List[detector.DetectorDevice]
    motors: List[motor.MotorDevice]

    async def open(self, file_details: FileDetails) -> Dict[str, DatasetDetails]:
        return await detector.open_detectors(self.detectors, file_details)

    async def scan(
        self, points: RemainingPoints, offset: int, queue: "asyncio.Queue[int]"
    ):
        # Prepare the motors and arm detectors
        period, num = points.constant_duration, points.remaining
        tracker, _, _ = await asyncio.gather(
            pmac.build_initial_trajectory(self.pmac, self.motors, points),
            pmac.move_to_start(self.pmac, self.motors, points.peek_point()),
            detector.arm_detectors_triggered(self.detectors, num, offset, period),
        )
        # Kick off pmac, then show the progress of detectors
        await asyncio.gather(
            pmac.keep_filling_trajectory(self.pmac, tracker),
            detector.collect_detectors(
                self.detectors, num, offset, queue, timeout=period + 60
            ),
        )

    async def stop(self):
        await asyncio.gather(
            detector.stop_detectors(self.detectors), pmac.stop_trajectory(self.pmac)
        )

    async def close(self):
        await detector.close_detectors(self.detectors)
