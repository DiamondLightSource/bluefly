import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List, Sequence, Set, Tuple

import numpy as np
from bluesky.run_engine import get_bluesky_event_loop
from scanpointgenerator import CompoundGenerator

from bluefly.epics_motor import EpicsMotor
from bluefly.simprovider import SimProvider

from .core import (
    DeviceWithSignals,
    NotConnectedError,
    SignalR,
    SignalW,
    SignalX,
    signal_sources,
)

# 9 axes in a PMAC co-ordinate system
CS_AXES = "abcuvwxyz"


@signal_sources(demands={x: f"demand_{x}" for x in CS_AXES})
class PMACCoord(DeviceWithSignals):
    port: SignalR[str]
    demands: SignalW[Dict[str, float]]
    move_time: SignalW[float]
    defer_moves: SignalW[bool]


@signal_sources(
    positions={x: f"position_{x}" for x in CS_AXES},
    use={x: f"use_{x}" for x in CS_AXES},
)
class PMACTrajectory(DeviceWithSignals):
    times: SignalW[Sequence[float]]
    velocity_modes: SignalW[Sequence[float]]
    user_programs: SignalW[Sequence[int]]
    positions: SignalW[Dict[str, Sequence[float]]]
    use: SignalW[Dict[str, bool]]
    cs: SignalW[str]
    build: SignalX
    build_message: SignalR[str]
    build_status: SignalR[str]
    points_to_build: SignalW[int]
    append: SignalX
    append_message: SignalR[str]
    append_status: SignalR[str]
    execute: SignalX
    execute_message: SignalR[str]
    execute_status: SignalR[str]
    points_scanned: SignalR[int]
    abort: SignalX
    program_version: SignalR[float]


class PMAC(DeviceWithSignals):
    def __init__(self, device_id: str):
        super().__init__(device_id)
        self.cs_list = [PMACCoord(f"{device_id}CS{i+1}") for i in range(16)]
        self.traj = PMACTrajectory(f"{device_id}TRAJ")


class PMACCompoundMotor(EpicsMotor):
    input_link: SignalR[str]


class PMACRawMotor(EpicsMotor):
    cs_axis: SignalR[str]
    cs_port: SignalR[str]


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
    cs_axes: Dict[str, str]
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
            positions={self.cs_axes[k]: v for k, v in points.positions.items()},
        )


async def get_cs(motors: Sequence[EpicsMotor]) -> Tuple[str, Dict[str, str]]:
    cs_ports: Set[str] = set()
    cs_axes: Dict[str, str] = {}
    for motor in motors:
        assert motor.name
        if isinstance(motor, PMACRawMotor):
            cs_ports.add(await motor.cs_port.get())
            cs_axes[motor.name] = await motor.cs_axis.get()
        else:
            raise NotImplementedError("Not handled PMAC compound motor yet")
    cs_ports_list = sorted(cs_ports)
    assert len(cs_ports_list) == 1, f"Expected one CS, got {cs_ports_list}"
    return cs_ports_list[0], cs_axes


async def move_to_start(
    pmac: PMAC,
    motors: Sequence[EpicsMotor],
    generator: CompoundGenerator,
    completed: int = 0,
):
    cs_port, cs_axes = await get_cs(motors)
    for cs in pmac.cs_list:
        try:
            if await cs.port.get() == cs_port:
                break
        except NotConnectedError:
            # Some CS are not implemented for all PMACs
            continue
    else:
        raise ValueError(f"No CS given for {cs_port!r}")
    first_point = generator.get_point(completed)
    await cs.defer_moves.set(True)
    # TODO: insert real axis run-up calcs here
    demands = {cs_axes[axis]: value for axis, value in first_point.positions.items()}
    await cs.demands.set(demands)
    await cs.defer_moves.set(False)


async def build_initial_trajectory(
    pmac: PMAC,
    motors: Sequence[EpicsMotor],
    generator: CompoundGenerator,
    completed: int = 0,
) -> TrajectoryTracker:
    cs_port, cs_axes = await get_cs(motors)
    traj = pmac.traj
    await traj.cs.set(cs_port)
    tracker = TrajectoryTracker(generator, cs_axes, completed, completed)
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
    pmac: PMAC, tracker: TrajectoryTracker
) -> AsyncGenerator[int, None]:
    traj = pmac.traj
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


async def stop_trajectory(pmac: PMAC):
    await pmac.traj.abort()


def sim_trajectory_logic(p: SimProvider, traj: PMACTrajectory):
    """Just enough of a sim to make points_scanned tick at the right rate"""
    stopping = asyncio.Event(loop=get_bluesky_event_loop())
    times: List[float] = []

    @p.on_call(traj.abort)
    async def do_abort():
        stopping.set()
        times.clear()

    @p.on_call(traj.build)
    @p.on_call(traj.append)
    async def do_build_append():
        for t in await p.get(traj.times):
            times.append(t)
        await p.set(traj.build_status, "Success")
        await p.set(traj.append_status, "Success")

    @p.on_call(traj.execute)
    async def do_scan():
        # Do a fake scan that takes the right time
        stopping.clear()
        status = "Success"
        for i, t in enumerate(times):
            try:
                # See if we got told to stop
                await asyncio.wait_for(stopping.wait(), t)
            except asyncio.TimeoutError:
                # Carry on
                await p.set(traj.points_scanned, i + 1)
            else:
                # Stop
                status = "Aborted"
                break
        times.clear()
        await p.set(traj.execute_status, status)
