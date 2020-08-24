"""Cut down PMAC trajectory scanning logic. Interface is complete, but logic
has removed most of the maths and PMAC specific workarounds

Logical structure, each node is a separate ChannelTree:

PMAC: i10, iVariables, pVariables
    CS: demandA..Z, moveTime, deferMoves
        CompoundMotor: name, demand, velocity, resolution
    RawMotor: name, csPort, csAxis, demand, velocity, resolution
    TrajectoryMover: arrayA..Z, arrayTime, build, execute

This file structure is a bit arbitrary, should it all be in one file?
"""
from .logic import (
    TrajectoryBatch,
    TrajectoryTracker,
    build_initial_trajectory,
    keep_filling_trajectory,
    move_to_start,
    stop_trajectory,
)
from .tree import PMACCoord, PMACCoordMotor, PMACMotor, PMACRawMotor, PMACTrajectory

__all__ = [
    "TrajectoryBatch",
    "TrajectoryTracker",
    "build_initial_trajectory",
    "keep_filling_trajectory",
    "move_to_start",
    "stop_trajectory",
    "PMACCoord",
    "PMACCoordMotor",
    "PMACMotor",
    "PMACRawMotor",
    "PMACTrajectory",
]
