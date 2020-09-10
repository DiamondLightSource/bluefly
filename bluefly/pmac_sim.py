import asyncio
from typing import Dict, List

from bluesky.run_engine import get_bluesky_event_loop

from bluefly.pmac import CS_AXES, PMACRawMotor, PMACTrajectory
from bluefly.simprovider import SimProvider


def sim_trajectory_logic(p: SimProvider, traj: PMACTrajectory, **motors: PMACRawMotor):
    """Just enough of a sim to make points_scanned tick at the right rate"""
    stopping = asyncio.Event(loop=get_bluesky_event_loop())
    times: List[float] = []
    positions: Dict[str, List[float]] = {}

    for cs_axis, motor in motors.items():
        assert cs_axis in CS_AXES, f"{cs_axis} should be one of {CS_AXES}"
        p.set_value(motor.cs_axis, cs_axis.upper())

    @p.on_call(traj.abort)
    async def do_abort():
        stopping.set()
        times.clear()
        positions.clear()

    @p.on_call(traj.build)
    @p.on_call(traj.append)
    async def do_build_append():
        for t in p.get_value(traj.times):
            times.append(t)
        for cs_axis, ps in p.get_value(traj.positions).items():
            for pp in ps:
                positions.setdefault(cs_axis, []).append(pp)
        p.set_value(traj.build_status, "Success")
        p.set_value(traj.append_status, "Success")

    @p.on_call(traj.execute)
    async def do_scan():
        # Do a fake scan that takes the right time
        stopping.clear()
        status = "Success"
        for i, t in enumerate(times):
            for cs_axis, use in p.get_value(traj.use).items():
                if use and cs_axis in motors:
                    p.set_value(motors[cs_axis].readback, positions[cs_axis][i])
            try:
                # See if we got told to stop
                await asyncio.wait_for(stopping.wait(), t)
            except asyncio.TimeoutError:
                # Carry on
                p.set_value(traj.points_scanned, i + 1)
            else:
                # Stop
                status = "Aborted"
                break
        times.clear()
        positions.clear()
        p.set_value(traj.execute_status, status)
