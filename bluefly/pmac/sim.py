import asyncio

from bluesky.run_engine import get_bluesky_event_loop

from bluefly.simprovider import SimProvider

from .tree import PMACCoord, PMACRawMotor, PMACTrajectory


class SimPMAC:
    """Just enough of a sim to make points_scanned tick at the right rate"""

    def __init__(self):
        p = SimProvider()
        self.motors = dict(
            tx=PMACRawMotor("T1.X", p),
            ty=PMACRawMotor("T1.Y", p),
            tz=PMACRawMotor("T1.Z", p),
        )
        self.cs_list = [PMACCoord(f"PMAC1.CS{cs}", p) for cs in range(1, 3)]
        self.traj = PMACTrajectory("PMAC1.TRAJ", p)

        # TODO: why can't we do the following
        # assert asyncio.get_event_loop() is get_bluesky_event_loop()
        stopping = asyncio.Event(loop=get_bluesky_event_loop())
        times = []

        @self.traj.abort.set_call
        async def do_abort():
            stopping.set()
            times.clear()

        @self.traj.build.set_call
        @self.traj.append.set_call
        async def do_build():
            for t in self.traj.times.sim_value:
                times.append(t)
            p.set_value(self.traj.build_status, "Success")
            p.set_value(self.traj.append_status, "Success")

        @self.traj.execute.set_call
        async def do_scan():
            # Do a fake scan that takes the right time
            stopping.clear()
            status = "Success"
            for i, t in enumerate(times):
                try:
                    # See if we got told to stop
                    await asyncio.wait_for(stopping.wait(), t)
                except asyncio.exceptions.TimeoutError:
                    # Carry on
                    p.set_value(self.traj.points_scanned, i + 1)
                else:
                    # Stop
                    status = "Aborted"
                    break
            times.clear()
            p.set_value(self.traj.execute_status, status)
