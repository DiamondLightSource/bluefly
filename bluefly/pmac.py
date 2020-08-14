from typing import Optional, Protocol

from scanpointgenerator import CompoundGenerator

"""
PMAC -> [cs1..16] -> [ma..z] -> name, demand, velocity, resolution
                  -> a..z, moveTime, defer
     -> [m1..32] -> name, demand, velocity, resolution
     -> traj -> a..z, time, build, append, abort
"""




class Pmac(Protocol):
    css: List[PmacCs]
    motors: List[PmacRawMotor]
    traj: Optional[PmacTrajectoryMover]


class PmacCs(Protocol):
    motors: List[PmacCompoundMotor]


class PmacMapper(ConiqlMapper):
    pass



class PartialTrajectory:
    def __init__(self, generator: CompoundGenerator, completed: int):
        self._generator = generator
        self._completed = completed


async def move_to_start(pmac: Pmac, generator: CompoundGenerator, completed: int = 0):
    pass


async def build_initial_trajectory(pmac: Pmac, generator: CompoundGenerator, completed: int = 0) -> PartialTrajectory:
    pass


async def keep_filling_trajectory(pmac: Pmac, trajectory: PartialTrajectory):
    pass


async def stop(pmac: Pmac):
    pass
