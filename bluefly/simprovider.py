from __future__ import annotations

import asyncio
import collections.abc
from typing import AsyncGenerator, Generic, Type, cast, get_args, get_origin

from bluesky.run_engine import get_bluesky_event_loop

from bluefly.channel import (
    Channel,
    ChannelProvider,
    ChannelRO,
    ChannelRW,
    ChannelSource,
    ChannelT,
    ChannelWO,
    ChannelX,
    ValueT,
)


class HasValue(Channel, Generic[ValueT]):
    def __init__(self, value: ValueT, q: asyncio.Queue[ValueT]):
        self.sim_value: ValueT = value
        self.sim_q: asyncio.Queue[ValueT] = q


class SimChannelRO(ChannelRO[ValueT], HasValue[ValueT]):
    async def get(self) -> ValueT:
        return self.sim_value

    async def observe(self) -> AsyncGenerator[ValueT, None]:
        # Implement for a single observer, that'll do for now
        while True:
            yield await self.sim_q.get()


class SimChannelWO(ChannelWO[ValueT], HasValue[ValueT]):
    """Channel that can be put to"""

    async def set(self, value: ValueT) -> ValueT:
        self.sim_value = value
        await self.sim_q.put(value)
        return value


class SimChannelRW(SimChannelRO, SimChannelWO, ChannelRW):
    pass


lookup = {
    ChannelRO: SimChannelRO,
    ChannelWO: SimChannelWO,
    ChannelRW: SimChannelRW,
}


class SimChannelX(ChannelX):
    async def _call(self):
        pass

    def set_call(self, call):
        self._call = call
        return call

    async def __call__(self):
        await self._call()


class SimProvider(ChannelProvider):
    def set_value(self, channel: HasValue, value):
        channel.sim_value = value
        channel.sim_q.put_nowait(value)

    def make_channel(
        self, device_id: str, source: ChannelSource, channel_type: Type[ChannelT]
    ) -> ChannelT:
        if channel_type is ChannelX:
            # No type, no value
            channel = cast(ChannelT, SimChannelX())
        else:
            base_channel_type = cast(Type[ChannelT], get_origin(channel_type))
            channel_cls = lookup[base_channel_type]
            value_type = get_args(channel_type)[0]
            origin = get_origin(value_type)
            if origin is None:
                # str, bool, int, float
                value = value_type()
            elif origin is collections.abc.Sequence:
                # Sequence[...]
                value = ()
            elif origin is dict:
                # Dict[...]
                value = origin()
            else:
                raise ValueError(f"Can't make {channel_type}")
            channel = cast(
                ChannelT,
                channel_cls(value, asyncio.Queue(loop=get_bluesky_event_loop())),
            )
        return channel
