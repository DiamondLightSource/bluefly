import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple

import numpy as np
from bluesky.run_engine import get_bluesky_event_loop
from scanpointgenerator import CompoundGenerator

from bluefly import detector, motor, pmac
from bluefly.core import ConfigDict, Device, RemainingPoints, Status
from bluefly.detector import DatumFactory, DetectorDevice, FilenameScheme


class FlyLogic(ABC):
    @abstractmethod
    async def scan(
        self,
        detectors: Sequence[DetectorDevice],
        points: RemainingPoints,
        offset: int,
        callback: Callable[[str, int], None],
    ):
        """Scan the given points, putting them at offset into file. Progress updates
        should call callback """

    @abstractmethod
    async def stop(self, detectors: Sequence[DetectorDevice]):
        """Stop where you are, without retracing or closing files"""


# Based on:
# https://github.com/NSLS-II/sirepo-bluesky/blob/7173258e7570904295bfcd93d5bca3dcc304c15c/sirepo_bluesky/sirepo_flyer.py
class FlyDevice(Device):
    """Generic fly scan device that wraps some custom routines"""

    def __init__(
        self, detectors: Sequence[DetectorDevice], logic: FlyLogic,
    ):
        assert detectors, "Need at least one detector"
        self._detectors = detectors
        self._logic = logic
        self._generator = CompoundGenerator(generators=[])
        self._when_configured = time.time()
        self._when_triggered = time.time()
        self._when_updated = time.time()
        self._start_offset = 0
        self._completed_steps = 0
        self._total_steps = 0
        self._watchers: List[Callable] = []
        self._complete_status: Optional[Status] = None
        self._pause_task: Optional[asyncio.Task] = None
        self._factories: Dict[str, DatumFactory] = {}
        self._scheme = FilenameScheme.get_instance()

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
        self._factories.clear()
        return [self]

    def unstage(self) -> List[Device]:
        asyncio.create_task(self._unstage())
        return [self]

    async def _unstage(self):
        det_coros = [det.logic.close() for det in self._detectors]
        await asyncio.gather(self._scheme.done_using_prefix(), *det_coros)

    @property
    def hints(self):
        return dict(fields=[f.summary_name for f in self._factories.values()])

    def collect(self) -> Generator[Dict[str, ConfigDict], None, None]:
        for factory in self._factories.values():
            for datum in factory.collect_datums():
                # this is horrible, write it better
                point = self._generator.get_point(datum.pop("point_number"))
                for p, v in point.positions.items():
                    datum["data"][p] = v
                    datum["filled"][p] = True
                    datum["timestamps"][p] = time.time()
                yield datum

    def collect_asset_docs(self):
        for factory in self._factories.values():
            yield from factory.collect_asset_docs()

    def describe_collect(self) -> Dict[str, ConfigDict]:
        d = {}
        for factory in self._factories.values():
            d.update(factory.describe())
        for axis in self._generator.axes:
            d[axis] = dict(source=axis, dtype="number", shape=[])
        return dict(primary=d)

    def kickoff(self) -> Status:
        status = Status(self._kickoff())
        return status

    async def _kickoff(self):
        self._completed_steps = 0
        self._generator.prepare()
        self._total_steps = self._generator.size
        self._when_triggered = time.time()
        if not self._factories:
            # beginning of the scan, open the file
            self._start_offset = 0
            file_prefix = await self._scheme.current_prefix()
            coros = []
            for det in self._detectors:
                assert det.name
                coros.append(det.logic.open(file_prefix + det.name))
            resources = await asyncio.gather(*coros)
            for det, resource in zip(self._detectors, resources):
                assert det.name
                self._factories[det.name] = DatumFactory(det.name, resource)

    def pause(self):
        assert self._complete_status, "Complete not called"
        self._complete_status.task.cancel()
        self._pause_task = asyncio.create_task(self._logic.stop(self._detectors))

    def resume(self):
        assert self._complete_status.task.cancelled(), "You didn't call pause"
        assert self._pause_task.done(), "You didn't wait for pause to finish"
        self._complete_status.task = get_bluesky_event_loop().create_task(
            self._complete()
        )

    def complete(self) -> Status:
        task = asyncio.create_task(self._complete())
        self._complete_status = Status(task, self._watchers.append)
        return self._complete_status

    async def _complete(self):
        completed_at_start = self._completed_steps
        points = RemainingPoints(self._generator, completed_at_start)
        queue: asyncio.Queue[int] = asyncio.Queue()

        async def update_watchers():
            steps: Dict[str, int] = {
                det.name: completed_at_start for det in self._detectors
            }
            last_updated: Dict[str, float] = {
                det.name: time.time() for det in self._detectors
            }

            while self._completed_steps < self._total_steps:
                oldest_det = time.time() - min(last_updated.values())
                # Allow the oldest detector to be up to 60s + exposure behind
                timeout = 60 + self._generator.duration - oldest_det
                name, step = await asyncio.wait_for(queue.get(), timeout)
                factory = self._factories[name]
                factory.register_collections(np.arange(steps[name], step))
                steps[name] = step
                new_completed_steps = min(steps.values())
                if new_completed_steps > self._completed_steps:
                    self._completed_steps = new_completed_steps
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
                        )

        await asyncio.gather(
            self._logic.scan(
                self._detectors,
                points,
                self._start_offset + self._completed_steps,
                lambda name, steps: queue.put_nowait(
                    (name, steps + completed_at_start)
                ),
            ),
            update_watchers(),
        )
        self._start_offset += self._total_steps


@dataclass
class PMACMasterFlyLogic(FlyLogic):
    pmac: pmac.PMAC
    motors: List[motor.MotorDevice]

    async def scan(
        self,
        detectors: Sequence[DetectorDevice],
        points: RemainingPoints,
        offset: int,
        callback: Callable[[str, int], None],
    ):
        # Prepare the motors and arm detectors
        period, num = points.constant_duration, points.remaining
        tracker, _, _ = await asyncio.gather(
            pmac.build_initial_trajectory(self.pmac, self.motors, points),
            pmac.move_to_start(self.pmac, self.motors, points.peek_point()),
            detector.arm_detectors_triggered(detectors, num, offset, period),
        )
        # Kick off pmac, then show the progress of detectors
        await asyncio.gather(
            pmac.keep_filling_trajectory(self.pmac, tracker),
            detector.collect_detectors(detectors, num, callback),
        )

    async def stop(self, detectors: Sequence[DetectorDevice]):
        await asyncio.gather(
            detector.stop_detectors(detectors), pmac.stop_trajectory(self.pmac)
        )
