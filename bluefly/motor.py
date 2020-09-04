import asyncio
import time
from typing import Callable, List, Optional

from bluefly.core import (
    ConfigDict,
    HasSignals,
    SettableDevice,
    SignalR,
    SignalRW,
    SignalX,
    Status,
)
from bluefly.simprovider import SimProvider

# Interface
###########


class MotorRecord(HasSignals):
    demand: SignalRW[float]
    readback: SignalR[float]
    done_move: SignalR[bool]
    acceleration_time: SignalRW[float]
    velocity: SignalRW[float]
    max_velocity: SignalRW[float]
    # Actually read/write, but shouldn't write from scanning code
    resolution: SignalR[float]
    offset: SignalR[float]
    egu: SignalR[str]
    precision: SignalR[float]
    stop: SignalX


# Logic
#######


class MotorDevice(SettableDevice):
    def __init__(self, motor: MotorRecord):
        self.motor = motor
        self._trigger_task: Optional[asyncio.Task[float]] = None
        self._set_success = True

    def trigger(self) -> Status[float]:
        self._trigger_task = asyncio.create_task(self.motor.readback.get())
        return Status(self._trigger_task)

    def read(self) -> ConfigDict:
        assert self.name, "Motor not named"
        assert self._trigger_task, "trigger() not called"
        return {
            self.name: dict(value=self._trigger_task.result(), timestamp=time.time())
        }

    def describe(self) -> ConfigDict:
        assert self.name, "Motor not named"
        return {
            self.name: dict(source=self.motor.readback.source, dtype="number", shape=[])
        }

    def set(self, new_position: float, timeout: float = None) -> Status[float]:
        start = time.time()
        watchers: List[Callable] = []

        async def update_watchers(old_position):
            units, precision = await asyncio.gather(
                self.motor.egu.get(), self.motor.precision.get()
            )
            async for current_position in self.motor.readback.observe():
                for watcher in watchers:
                    watcher(
                        name=self.name,
                        current=current_position,
                        initial=old_position,
                        target=new_position,
                        unit=units,
                        precision=precision,
                        time_elapsed=time.time() - start,
                        # TODO: why do we have to specify fraction?
                        fraction=abs(
                            (current_position - old_position)
                            / (new_position - old_position)
                        ),
                    )

        async def do_set():
            old_position = await self.motor.demand.get()
            t = asyncio.create_task(update_watchers(old_position))
            await self.motor.demand.set(new_position)
            t.cancel()
            if not self._set_success:
                raise RuntimeError("Motor was stopped")

        self._set_success = True
        status = Status(asyncio.wait_for(do_set(), timeout=timeout), watchers.append)
        return status

    def stop(self, *, success=False):
        # TODO: we should return a status here
        self._set_success = success
        asyncio.create_task(self.motor.stop())


# Simulation
############


def sim_motor_logic(
    p: SimProvider, motor: MotorRecord, velocity=1, precision=3, units="mm"
):
    p.set_value(motor.velocity, velocity)
    p.set_value(motor.max_velocity, velocity)
    p.set_value(motor.precision, precision)
    p.set_value(motor.egu, units)

    task = None

    @p.on_set(motor.demand)
    async def do_move(new_position):
        async def actually_do_move():
            p.set_value(motor.done_move, 0)
            old_position = p.get_value(motor.readback)
            velocity = p.get_value(motor.velocity)
            # Don't try to be clever, just move at a constant velocity
            move_time = (new_position - old_position) / velocity
            for i in range(int(move_time / 0.1)):
                p.set_value(motor.readback, old_position + i * 0.1 * velocity)
                await asyncio.sleep(0.1)
            p.set_value(motor.readback, new_position)
            p.set_value(motor.done_move, 1)

        nonlocal task
        task = asyncio.create_task(actually_do_move())
        try:
            await task
        except asyncio.CancelledError:
            pass

    @p.on_call(motor.stop)
    async def do_stop():
        if task:
            task.cancel()
