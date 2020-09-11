import asyncio

from bluefly.motor import MotorDevice
from bluefly.simprovider import SimProvider


def sim_motor_logic(
    p: SimProvider, motor: MotorDevice, velocity=1, precision=3, units="mm"
):
    mr = motor.motor
    p.set_value(mr.velocity, velocity)
    p.set_value(mr.max_velocity, velocity)
    p.set_value(mr.precision, precision)
    p.set_value(mr.egu, units)

    task = None

    @p.on_set(mr.demand)
    async def do_move(new_position):
        async def actually_do_move():
            p.set_value(mr.done_move, 0)
            old_position = p.get_value(mr.readback)
            velocity = p.get_value(mr.velocity)
            # Don't try to be clever, just move at a constant velocity
            move_time = (new_position - old_position) / velocity
            for i in range(int(move_time / 0.1)):
                p.set_value(mr.readback, old_position + i * 0.1 * velocity)
                await asyncio.sleep(0.1)
            p.set_value(mr.readback, new_position)
            p.set_value(mr.done_move, 1)

        nonlocal task
        task = asyncio.create_task(actually_do_move())
        try:
            await task
        except asyncio.CancelledError:
            pass

    @p.on_call(mr.stop)
    async def do_stop():
        if task:
            task.cancel()
