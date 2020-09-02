import asyncio
from dataclasses import dataclass, field
from typing import AsyncGenerator, List

from scanpointgenerator import CompoundGenerator

from bluefly import pmac
from bluefly.epics_motor import EpicsMotor
from bluefly.scan import FlyScanLogic


@dataclass
class MyFlyScanLogic(FlyScanLogic):
    pmac: pmac.PMAC
    motors: List[EpicsMotor]
    _tracker: pmac.TrajectoryTracker = field(init=False)

    async def configure(self, generator: CompoundGenerator, completed: int = 0):
        _, self._tracker = await asyncio.gather(
            pmac.move_to_start(self.pmac, self.motors, generator, completed),
            pmac.build_initial_trajectory(self.pmac, self.motors, generator, completed),
        )

    async def run(self) -> AsyncGenerator[int, None]:
        async for step in pmac.keep_filling_trajectory(self.pmac, self._tracker):
            yield step

    async def stop(self):
        await pmac.stop_trajectory(self.pmac)
