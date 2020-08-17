"""Cut down PMAC trajectory scanning logic. Interface is complete, but logic
has removed most of the maths and PMAC specific workarounds

Logical structure, each node is a separate ChannelTree:

PMAC: i10, iVariables, pVariables
    CS: demandA..Z, moveTime, deferMoves
        CompoundMotor: name, demand, velocity, resolution
    RawMotor: name, csPort, csAxis, demand, velocity, resolution
    TrajectoryMover: arrayA..Z, arrayTime, build, execute
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Sequence

import numpy as np
from scanpointgenerator import CompoundGenerator

from bluefly.channel import ChannelRO, ChannelTree, ChannelWO, ChannelX, channel_source

# 9 axes in a PMAC co-ordinate system
CS_AXES = "ABCUVWXYZ"


class PMACMotor(ChannelTree):
    demand: ChannelWO[float]
    readback: ChannelRO[float]
    done_moving: ChannelRO[bool]
    # These fields are actually RW, but we never write them from scanning code
    acceleration_time: ChannelRO[float]
    max_velocity: ChannelRO[float]
    resolution: ChannelRO[float]
    offset: ChannelRO[float]
    units: ChannelRO[str]
    inp: ChannelRO[str]


class PMACCoordMotor(PMACMotor):
    # TODO: we need velocity_settle_time, but there is no record for it
    pass


class PMACRawMotor(PMACMotor):
    cs_port: ChannelRO[str]
    cs_axis: ChannelRO[str]


@channel_source(demands={x: f"demand{x}" for x in CS_AXES})
class PMACCoord(ChannelTree):
    port: ChannelRO[str]
    demands: ChannelWO[Dict[str, float]]
    move_time: ChannelWO[float]
    defer_moves: ChannelWO[bool]


@channel_source(
    positions={x: f"position{x}" for x in CS_AXES}, use={x: f"use{x}" for x in CS_AXES},
)
class PMACTrajectory(ChannelTree):
    times: ChannelWO[Sequence[float]]
    velocity_modes: ChannelWO[Sequence[float]]
    user_programs: ChannelWO[Sequence[int]]
    positions: ChannelWO[Dict[str, Sequence[float]]]
    use: ChannelWO[Dict[str, bool]]
    cs: ChannelWO[str]
    build: ChannelX
    build_message: ChannelRO[str]
    build_status: ChannelRO[str]
    points_to_build: ChannelWO[int]
    append: ChannelX
    append_message: ChannelRO[str]
    append_status: ChannelRO[str]
    execute: ChannelX
    execute_message: ChannelRO[str]
    execute_status: ChannelRO[str]
    points_scanned: ChannelRO[int]
    abort: ChannelX
    program_version: ChannelRO[float]


@dataclass
class TrajectoryBatch:
    times: Sequence[float]
    velocity_modes: Sequence[float]
    user_programs: Sequence[int]
    positions: Dict[str, Sequence[float]]


BATCH_SIZE = 100


@dataclass
class TrajectoryTracker:
    generator: CompoundGenerator
    completed: int
    cs_axis: Dict[str, str]

    def get_next_batch(self) -> TrajectoryBatch:
        old_completed = self.completed
        self.completed += BATCH_SIZE
        points = self.generator.get_points(old_completed, self.completed)
        return TrajectoryBatch(
            times=points.duration,
            velocity_modes=np.zeros(points.duration),
            user_programs=np.zeros(points.duration),
            positions={self.cs_axis[k]: v for k, v in points.positions.items()},
        )


async def get_cs(motors: Dict[str, PMACMotor]) -> str:
    cs_ports = set()
    for motor in motors.values():
        if isinstance(motor, PMACRawMotor):
            cs_ports.add(await motor.cs_port.value())
        else:
            raise NotImplementedError("Not handled PMACCoordMotor yet")
    cs_ports_list = sorted(cs_ports)
    assert len(cs_ports_list) == 1, f"Expected one CS, got {cs_ports_list}"
    return cs_ports_list[0]


async def move_to_start(
    motors: Dict[str, PMACMotor],
    cs_list: List[PMACCoord],
    generator: CompoundGenerator,
    completed: int = 0,
):
    cs_port = await get_cs(motors)
    for cs in cs_list:
        if await cs.port.value() == cs_port:
            break
    else:
        raise ValueError(f"No CS given for {cs_port}")
    # TODO: insert real axis run-up calcs here
    first_point = generator.get_point(completed)
    await cs.defer_moves.put(True)
    demands = {
        await motors[axis].cs_axis.value(): value
        for axis, value in first_point.positions()
    }
    await cs.demands.put(demands)
    await cs.defer_moves.put(False)


async def build_initial_trajectory(
    motors: Dict[str, PMACMotor],
    traj: PMACTrajectory,
    generator: CompoundGenerator,
    completed: int = 0,
) -> TrajectoryTracker:
    cs_port = await get_cs(motors)
    await traj.cs.put(cs_port)
    cs_axis = {k: await motor.cs_axis.value() for k, motor in motors.items()}
    tracker = TrajectoryTracker(generator, completed, cs_axis)
    batch = tracker.get_next_batch()
    await asyncio.gather(
        traj.times.put(batch.times),
        traj.user_programs.put(batch.user_programs),
        traj.velocity_modes.put(batch.velocity_modes),
        traj.positions.put(batch.positions),
        traj.points_to_build.put(len(batch.times)),
        traj.use.put({x: x in batch.positions for x in CS_AXES}),
    )
    await traj.build()
    if not await traj.build_status.value() == "Success":
        raise ValueError(traj.build_message.value())
    return tracker


async def keep_filling_trajectory(traj: PMACTrajectory, tracker: TrajectoryTracker):
    task = asyncio.create_task(traj.execute())
    async for step in traj.points_scanned.values():
        if task.done():
            break
        if step + BATCH_SIZE < tracker.completed:
            # Push a new batch of points
            batch = tracker.get_next_batch()
            await asyncio.gather(
                traj.times.put(batch.times),
                traj.user_programs.put(batch.user_programs),
                traj.velocity_modes.put(batch.velocity_modes),
                traj.positions.put(batch.positions),
                traj.points_to_build.put(len(batch.times)),
            )
            await traj.append()
            if not await traj.append_status.value() == "Success":
                raise ValueError(traj.append_message.value())
    if not await traj.execute_status.value() == "Success":
        raise ValueError(traj.execute_message.value())


async def stop(traj: PMACTrajectory):
    await traj.abort()
