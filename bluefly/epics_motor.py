from bluefly.core import DeviceWithSignals, SignalR, SignalRW, SignalW


class EpicsMotor(DeviceWithSignals):
    user_setpoint: SignalW[float]
    user_readback: SignalR[float]
    motor_done_move: SignalR[bool]
    acceleration: SignalRW[float]
    velocity: SignalRW[float]
    max_velocity: SignalRW[float]
    # Actually read/write, but shouldn't write from scanning code
    resolution: SignalR[float]
    user_offset: SignalRW[float]
    motor_egu: SignalRW[str]
    # TODO: add SettableDevice methods
