import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator, List

from scanpointgenerator import CompoundGenerator

from bluefly import motor, pmac, scan


@dataclass
class MyFlyScanLogic(scan.FlyScanLogic):
    pmac: pmac.PMAC
    motors: List[motor.SettableMotor]

    async def go(
        self, generator: CompoundGenerator, completed: int = 0
    ) -> AsyncGenerator[int, None]:
        _, tracker = await asyncio.gather(
            pmac.move_to_start(self.pmac, self.motors, generator, completed),
            pmac.build_initial_trajectory(self.pmac, self.motors, generator, completed),
        )
        async for step in pmac.keep_filling_trajectory(self.pmac, tracker):
            yield step

    async def stop(self):
        await pmac.stop_trajectory(self.pmac)
