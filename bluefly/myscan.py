import asyncio
from dataclasses import dataclass, field
from typing import AsyncGenerator, Dict, List

from scanpointgenerator import CompoundGenerator

from bluefly import pmac
from bluefly.scan import FlyScanLogic


@dataclass
class MyFlyScanLogic(FlyScanLogic):
    motors: Dict[str, pmac.PMACRawMotor]
    cs_list: List[pmac.PMACCoord]
    traj: pmac.PMACTrajectory
    _tracker: pmac.TrajectoryTracker = field(init=False)

    async def configure(self, generator: CompoundGenerator, completed: int = 0):
        _, self._tracker = await asyncio.gather(
            pmac.move_to_start(self.motors, self.cs_list, generator, completed),
            pmac.build_initial_trajectory(self.motors, self.traj, generator, completed),
        )

    async def run(self) -> AsyncGenerator[int, None]:
        async for step in pmac.keep_filling_trajectory(self.traj, self._tracker):
            yield step

    async def stop(self):
        await pmac.stop_trajectory(self.traj)
