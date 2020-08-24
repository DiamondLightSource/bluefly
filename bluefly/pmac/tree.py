from typing import Dict, Sequence

from bluefly.channel import ChannelRO, ChannelTree, ChannelWO, ChannelX, channel_sources

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


@channel_sources(demands={x: f"demand{x}" for x in CS_AXES})
class PMACCoord(ChannelTree):
    port: ChannelRO[str]
    demands: ChannelWO[Dict[str, float]]
    move_time: ChannelWO[float]
    defer_moves: ChannelWO[bool]


@channel_sources(
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
