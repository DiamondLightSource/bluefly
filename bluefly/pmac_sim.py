import asyncio
from typing import List

from bluesky.run_engine import get_bluesky_event_loop

from bluefly.pmac import PMACTrajectory
from bluefly.simprovider import SimProvider


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
        for t in p.get_value(traj.times):
            times.append(t)
        p.set_value(traj.build_status, "Success")
        p.set_value(traj.append_status, "Success")

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
                p.set_value(traj.points_scanned, i + 1)
            else:
                # Stop
                status = "Aborted"
                break
        times.clear()
        p.set_value(traj.execute_status, status)
