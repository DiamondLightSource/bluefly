import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Mapping, Sequence

import numpy as np
from scanpointgenerator import CompoundGenerator

from .tree import CS_AXES, PMACCoord, PMACMotor, PMACRawMotor, PMACTrajectory

BATCH_SIZE = 100


@dataclass
class TrajectoryBatch:
    times: Sequence[float]
    velocity_modes: Sequence[float]
    user_programs: Sequence[int]
    positions: Dict[str, Sequence[float]]


@dataclass
class TrajectoryTracker:
    generator: CompoundGenerator
    cs_axis: Dict[str, str]
    completed: int
    offset: int

    @property
    def incomplete(self):
        return self.completed < self.generator.size

    def get_next_batch(self) -> TrajectoryBatch:
        new_completed = min(self.completed + BATCH_SIZE, self.generator.size)
        points = self.generator.get_points(self.completed, new_completed)
        self.completed = new_completed
        return TrajectoryBatch(
            times=points.duration,
            velocity_modes=np.zeros(len(points)),
            user_programs=np.zeros(len(points)),
            positions={self.cs_axis[k]: v for k, v in points.positions.items()},
        )


async def get_cs(motors: Mapping[str, PMACMotor]) -> str:
    cs_ports = set()
    for motor in motors.values():
        if isinstance(motor, PMACRawMotor):
            cs_ports.add(await motor.cs_port.get())
        else:
            raise NotImplementedError("Not handled PMACCoordMotor yet")
    cs_ports_list = sorted(cs_ports)
    assert len(cs_ports_list) == 1, f"Expected one CS, got {cs_ports_list}"
    return cs_ports_list[0]


async def move_to_start(
    motors: Dict[str, PMACRawMotor],
    cs_list: List[PMACCoord],
    generator: CompoundGenerator,
    completed: int = 0,
):
    cs_port = await get_cs(motors)
    for cs in cs_list:
        if await cs.port.get() == cs_port:
            break
    else:
        raise ValueError(f"No CS given for {cs_port}")
    first_point = generator.get_point(completed)
    await cs.defer_moves.set(True)
    # TODO: insert real axis run-up calcs here
    demands = {
        await motors[axis].cs_axis.get(): value
        for axis, value in first_point.positions.items()
    }
    await cs.demands.set(demands)
    await cs.defer_moves.set(False)


async def build_initial_trajectory(
    motors: Dict[str, PMACRawMotor],
    traj: PMACTrajectory,
    generator: CompoundGenerator,
    completed: int = 0,
) -> TrajectoryTracker:
    cs_port = await get_cs(motors)
    await traj.cs.set(cs_port)
    cs_axis = {k: await motor.cs_axis.get() for k, motor in motors.items()}
    tracker = TrajectoryTracker(generator, cs_axis, completed, completed)
    batch = tracker.get_next_batch()
    await asyncio.gather(
        traj.times.set(batch.times),
        traj.user_programs.set(batch.user_programs),
        traj.velocity_modes.set(batch.velocity_modes),
        traj.positions.set(batch.positions),
        traj.points_to_build.set(len(batch.times)),
        traj.use.set({x: x in batch.positions for x in CS_AXES}),
    )
    await traj.build()
    if not await traj.build_status.get() == "Success":
        raise ValueError(await traj.build_message.get())
    return tracker


async def keep_filling_trajectory(
    traj: PMACTrajectory, tracker: TrajectoryTracker
) -> AsyncGenerator[int, None]:
    task = asyncio.create_task(traj.execute())
    async for num in traj.points_scanned.observe():
        step = num + tracker.offset
        yield step
        if task.done():
            break
        if tracker.incomplete and tracker.completed < step + BATCH_SIZE:
            # Push a new batch of points
            batch = tracker.get_next_batch()
            await asyncio.gather(
                traj.times.set(batch.times),
                traj.user_programs.set(batch.user_programs),
                traj.velocity_modes.set(batch.velocity_modes),
                traj.positions.set(batch.positions),
                traj.points_to_build.set(len(batch.times)),
            )
            await traj.append()
            if not await traj.append_status.get() == "Success":
                raise ValueError(await traj.append_message.get())
    if not await traj.execute_status.get() == "Success":
        raise ValueError(await traj.execute_message.get())


async def stop_trajectory(traj: PMACTrajectory):
    await traj.abort()
